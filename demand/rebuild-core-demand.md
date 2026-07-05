  # rebuild-core-demand 核心重构需求

  事项编号：`rebuild-core-demand`。

  我计划对项目核心交易链路进行重构，从架构层面和系统设计思路层面解决重构前已盘点的历史问题。权威覆盖基线是
  `/home/chillchill/btc-updown-marketmaker/mistake-log/2026-07-03-重构前错误汇总与优先级.md`。这些问题分布在数据采集、数据消费、模型训练、
  paper trade、live trade、SDK 下单、订单生命周期、真实平台潜规则、硬件利用率、artifact、审计和部署边界等多个板块，且问题彼此关联，不能只按单点
  bug 修复。

  本次重构的核心目标不是简单重写代码，而是建立一套可验证、可审计、可回放、能反映真实交易约束的系统边界。权威覆盖基线中的每类历史问题，都必须能
  追溯到新的需求、设计、测试或验收标准中；单条历史实例作为证据台账，不把需求范围扩展为无界重写。

  ## 总体系统架构图

  ### Paper / Live Trade 图表

  ```mermaid
  flowchart TD
    DATA["外部市场源<br>Polymarket market channel / Binance BTCUSDT depth / Binance BTCUSDT aggTrade / Polymarket RTDS Chainlink BTC/USD"] --> COLLECT["（共用）data-collect<br>外部市场源采集 / typed raw / coverage / manifest"]
    COLLECT --> CONTEXT["（共用）Market Context Builder<br>实时市场上下文 / 模型输入 / 可见信息边界"]
    CONTEXT --> TRADE["Paper / Live Trade 模块<br>市场数据 + Ledger 持续决策编排/模型输入供给"]
    BRIDGE["（共用）Intent Bridge / Execution Adapter<br>统一意图与执行合同"] -- paper --> SIM["（共用）模拟平台模块<br>文档 / 潜规则 / issues / artifact 校准"]
    BRIDGE -- live --> REAL["真实平台模块<br>官方 Polymarket Rust SDK"]
    SIM --> RAW["平台返回原始证据<br>成交 / 部分成交 / 不成交 / 拒单 / 撤单"]
    REAL --> RAW
    RAW --> NOE["（共用）NOE / 归一化合同<br>订单生命周期归一化"]
    NOE --> LEDGER["（共用）Ledger 合同<br>账户 / 持仓 / 挂单 / CASH / NAV"]
    LEDGER -- 交易状态反馈 --> TRADE
    TRADE --> MODEL_RUNTIME["（paper/live共用，不包含train）Model Runtime / 模型运行"]
    MODEL_RUNTIME -- 模型意图 --> BRIDGE
    TRADE --> TRADE_REPORT["交易可视化可审计报告"]

  ```

  ### Train 图表

  ```mermaid
  flowchart TD
    TRAIN["Train 模块<br>持续消费训练market数据+ledger数据<br>训练框架 / 奖惩办法 / 回放评估"] --> MODEL_TRAIN["模型产物 / Model Registry<br>active / shadow / candidate / retired"] & TRAIN_REPORT["训练可视化可审计报告"]
    TRAIN -- 训练回放意图 --> BRIDGE["（共用）Intent Bridge / Execution Adapter<br>统一意图与执行合同"]
    BRIDGE --> SIM["（共用）模拟平台模块<br>文档 / 潜规则 / issues / artifact 校准"]
    SIM --> RAW["模拟平台返回原始证据<br>成交 / 部分成交 / 不成交 / 拒单 / 撤单"]
    RAW --> NOE["（共用）NOE / 归一化合同<br>订单生命周期归一化"]
    NOE --> LEDGER["（共用）Ledger 合同<br>回放账户 / 持仓 / 挂单 / CASH / NAV"]
    LEDGER -- 反馈证据 / 奖惩依据 --> PIPELINE
    MODEL_TRAIN --> DEPLOY["供 Paper / Live Trade 审查加载"]
    DATA["外部市场源<br>Polymarket market channel / Binance BTCUSDT depth / Binance BTCUSDT aggTrade / Polymarket RTDS Chainlink BTC/USD"] --> COLLECT["（共用）data-collect<br>typed raw / coverage / manifest"]
    COLLECT --> PIPELINE["（共用）data-pipeline / data-processing<br>features / labels / tick_stream / replay context"]
    PIPELINE --> DATASET["训练可消费数据集 / 校准数据池<br>features / labels / replay context"]
    DATASET --> TRAIN
    TRAIN_REPORT --> RANK_LIST["训练批次中不同产物的排名表"]
  ```

  架构图只表达高层业务处理流程，不把权限/硬边界、schema 字段、具体订单状态枚举或 artifact 字段画成独立业务模块。Paper/live trade 图表表达在线运行闭环：`data-collect` 只把外部市场源转化为 typed raw、coverage 和 manifest；实时市场上下文和模型输入由 Market Context Builder / runtime 消费 `data-collect` 外部市场源证据、运行期 append-only log 和 ledger projection 后生成。模型产物加载到 paper/live，paper 走模拟平台，live 走真实平台，平台返回经 raw evidence、NOE 和 ledger 后只回到 paper/live 作为下一次交易状态反馈；进入训练只能通过离线反馈入库、训练数据集或校准数据池。

  Train 图表表达离线训练与回放闭环：`data-collect` 提供历史外部市场源 typed raw、coverage 和 manifest；`data-pipeline / data-processing / feedback-ingestion` 再把历史市场数据、NOE/Ledger 派生反馈、order evidence digest/reference 和 reports 处理为训练可消费数据集与校准数据池。训练模块再消费这些数据，使用同一套 `Intent Bridge / Execution Adapter` 合同接入模拟平台做回放、样本构造、规则校准和奖惩评估，生成模型产物、训练报告、训练批次排名表和下一轮可审查的模型版本。训练批次排名表用于比较同一批次内不同参数、不同随机种子或不同候选产物的表现，作为筛选进入 paper/live trade 审查加载的证据；它不能单独授权模型进入 active、paper 或 live。训练模块不在 paper/live 在线交易闭环中实时接收 ledger 反馈，也不得让 paper/live 的执行流程直接循环回训练。

  训练、paper 共享模拟平台；训练、paper、live 共享 Intent/NOE/Ledger 等合同;图中共用公共模块均在模块开头标注“（共用）”。共用表示 schema、manifest、时间线、causal cutoff、特征/输入合同和可回放语义一致，不要求训练离线处理与 paper/live 实时处理使用同一个运行实例。训练和 paper 只能通过模拟平台做回放、样本构造、规则校准或 paper 生命周期模拟；live 只能通过真实平台模块连接官方 Polymarket Rust SDK 或经批准的 SDK 缺口方案发送到平台。模拟平台用于训练、paper trade、replay 和规则校准，必须由真实平台 artifact、官方文档、潜规则和 issues 校准。模拟平台与真实平台返回都必须先保留 raw evidence，再经过同一套版本化 `NOE / NormalizedOrderEvent` 合同进入 ledger；模型不得直接把未归一化订单返回当作权威订单状态或训练标签。

  权限、SDK、wallet、schema、路径、平台约束、模型 bundle、关键 writer 等硬边界校验发生在程序启动、自检、热身、T-30 预加载以及运行前/运行中的权限模式校验中，不作为图中的独立业务决策模块。执行时硬边界只能允许发送、拒绝发送、维持 no-submit 权限模式、进入 cancel-only 权限模式或暂停运行，并写入 audit/operator；不得改写模型交易/风控意图，也不得把硬边界 verdict 当作训练标签或模型执行反馈。Ledger 是账户、持仓、挂单、CASH、NAV 等状态权威；训练报告和交易报告必须能追溯到市场上下文、模型意图、raw evidence、NOE、ledger、manifest 和 artifact。

  ### 模块边界术语

  1. `data-collect` / `./data-collect/` 专指外部市场源采集模块，只负责 Polymarket market channel、Binance BTCUSDT depth、Binance BTCUSDT aggTrade、Polymarket RTDS Chainlink BTC/USD 的 raw evidence、`recv_time_utc`、typed raw parquet、source coverage、source health/gap、reconnect trace、manifest 和 atomic publish；不负责 labels、features、tick_stream、训练切分、模型输入加工、paper/live append-only log、order evidence、NOE/Ledger 或 execution feedback ingestion。
  2. `Market Context Builder` / runtime context builder 专指 paper/live 热路径中的市场上下文和模型输入组装层；它可以消费 `data-collect` 外部市场源证据、运行期 append-only log、NOE/Ledger projection 和模型 bundle 合同，但不能把 finalized parquet 当作 paper/live 热路径唯一事实来源。
  3. `data-pipeline` / `data-processing` / training dataset builder 专指离线处理层，负责从 finalized typed raw、NOE/Ledger 派生反馈和审计引用生成 labels、features、tick_stream、replay context、训练切分和校准数据池。
  4. `feedback-ingestion` 专指把 paper/live/simulation 的 append-only log、live artifacts、order evidence digest/reference、NOE/Ledger 输出接入训练反馈链路；它不是 `data-collect`。
  5. `market_lifecycle_state`、`market_runtime_role` 和 `model_registry_state` 必须命名空间隔离：market lifecycle 可以使用 candidate / prefetched / active / retired；runtime role 表达 current / next 等相对角色；ModelRegistry 使用 active / shadow / candidate / retired。不得跨域比较裸字符串。

  ## 一、总体原则

  1. 重构必须先定义系统边界，再写实现。
  2. Polymarket 相关 submit、cancel、balance、approval allowance、user-channel、market-channel 等必须优先基于官方 Polymarket Rust SDK。
  3. 如果官方 SDK 能力不足，必须先列明具体缺口、影响和可选方案，经用户明确同意并限定缺口范围后，才能考虑 raw REST、manual signing、自建
  SDK 或其他旁路实现。
  4. 不允许出现第二套影子 feature pipeline、影子订单解释器、影子账本解释器或影子 SDK。
  5. 数据、模型、paper、live 必须共享同一套市场时间线、订单事件语义、artifact 合同和反馈标签语义。
  6. 交易决策、仓位/风控决策、暂停、不下单、撤单、减仓等策略性动作必须完全由模型输出或模型 bundle 明确声明的模型状态机输出；live-trade 不得内置第二套策略或风控来改写模型意图。
  7. live-trade 是模型与 Polymarket/外部平台之间的桥梁，只负责数据和真实平台事件归一化、模型输入供给、模型请求校验与发送、真实平台返回/成交/拒单/撤单/ledger 派生状态记录，以及权限、协议、schema、钱包、路径等硬边界 fail closed；它可以拒绝执行不满足硬边界的请求并把原因写入 audit/operator 记录，但不得生成替代交易决策。
  8. 本文中的“真实平台”专指 live trade 连接的 Polymarket/SDK/market channel/user channel/submit response/cancel response；“模拟平台”专指训练样本构造和 paper trade 使用的订单生命周期模拟器。模拟平台只能用真实平台 artifact 校准，不得在 live 高频热路径中作为账户、订单或风控状态源。
  9. 所有关键结论必须有 artifact、manifest、schema、日志或测试证据支撑，不能只靠 stdout 或人工判断。

  ## 二、新系统流程要求

  系统必须支持完整 5 分钟 BTC up/down 市场生命周期，并显式区分 active market 与 prefetched next market。

  1. 自检阶段：
     - 检查多个数据源是否正常加载。
     - 检查市场发现、当前活跃市场、下一个市场、token id、tick size、min size、fee/rebate、reward eligibility 等市场元数据是否可用且新鲜。
     - 检查 `data-collect` 四源合同中的 Binance BTCUSDT depth、Binance BTCUSDT aggTrade、Polymarket market channel、Polymarket RTDS Chainlink BTC/USD，以及 paper/live 所需 user channel、wallet/funder、模型 bundle、ONNX runtime、关键 append-only audit
     writer、磁盘、内存、GPU/CPU 资源是否满足运行条件。
     - 自检失败必须 fail closed，不能进入交易或训练热路径。

  2. 热身阶段：
     - 验证提交订单、取消订单、成交事件、卖单路径、market/user channel、订单回复、cancel 回复、本地 ledger 更新是否正常。
     - 热身必须明确区分 no-submit、paper、small-live、正式 live 四种权限级别。
     - 未获得真实资金授权时，不得用真实下单冒充热身通过。
     - small-live 与正式 live 均使用 `./wallets/.env.wallet1`；本地源路径为 `/home/chillchill/btc-updown-marketmaker/wallets/.env.wallet1`，`./wallets` 作为 gitignored 钱包 secret/env 部署材料上传或复制到交易服务器，远端实际落点必须由部署 receipt 记录且不得通过 Git 同步；需求层不限制资金规模；每次执行前必须单独向用户
     申请授权，获得批准后才能执行。
     - small-live 分为 1 个完整 5 分钟市场和 3 个完整 5 分钟市场；正式 live 为连续 1 小时程序运行。
     - 热身结果必须写入 artifact，不得只看控制台输出。

  3. T-30 秒预加载阶段：
     - 对任一目标 market `M`，必须在 `M.start - 30s` 前完成发现并开始外部市场源采集；`next` 只是相对角色，不是 lifecycle state。
     - `data-collect` 负责 T-30 source subscription、market metadata、orderbook/depth、tick/min size、token id、raw evidence、coverage 和 manifest；模型所需 feature context、runtime market context、资源预算和模型 bundle 由 Market Context Builder / Model Runtime / data-pipeline 承接。
     - market `M` 在 `M.start - 30s` 到 `M.start` 之间只能进入 `prefetched` context，`M.start` 后才允许 `prefetched -> active`；prefetched context 不能污染当前 active market 的订单、持仓、open order 或账本。

  4. Active market 持续决策阶段：
     - 当前活跃市场中，模型必须持续看到所有必要数据源。
     - 模型输入必须包括 price to beat、orderbook/depth、Binance/外部价格、本地 ledger 派生的 CASH、position、sellable、open
     orders、pending cancels、active resting count、账户级 NAV 字段、订单生命周期状态、最近 submit/cancel/fill/partial-fill 反馈，以及 cancel requested、cancel ack、cancel rejected、cancel retry、cancel timeout、matched-cancel、overlapped terminal 等 cancel order 生命周期反馈。
     - 模型必须能在完整 5 分钟市场窗口内持续做交易决策，而不是只看到 250ms 的孤立片段。
     - live-trade 不得基于自身规则改变模型输出的 side、price、size、submit、cancel、pause、no-submit 或风控动作；平台返回的执行结果进入本地事件和 audit，硬边界拒绝原因只进入 audit/operator 记录，不作为模型输入或模型反馈。
     - 系统必须估算并记录模型运行需要多少显存、内存、CPU、线程和 batch/sequence 配置。

  5. 当前市场结束前 T-30 秒：
     - 必须开始预加载下一个市场的全部模型输入。
     - 当前市场的最终取消、成交确认、持仓结算、ledger close、artifact close 与下个市场 preload 必须并行但隔离。
     - 任何 market switch 都不能让上一个市场的订单事件污染下一个市场。

  ## 三、模型输入与训练窗口要求

  1. 模型训练、paper trade、live trade 都必须持续输入目标 market 从 `start - 30s` 到 market end 的可见上下文；这 30 秒是目标 market 的 pre-start prefetched context，不是把上一个 market 的订单/账本状态混入目标 market。
  2. 模型不得只看到 250ms 数据片段，这是之前版本的核心错误，我们当前版本重点要修复这个问题。
  3. 每个训练样本、paper 决策、live 决策都必须带有 causal cutoff，确保模型只能看到当时真实可见的信息。
  4. 模型输入至少应包含：
     - market_id、token_id、side、time-to-expiry、active/prefetched 标记；
     - 所有数据源的时间对齐状态和 freshness；
     - orderbook、depth、spread、price to beat、tick size、min size；
     - 本地 ledger 派生的 CASH、current position、sellable balance、open orders、pending cancels、active resting count、账户级 NAV；
     - submit/cancel/fill/partial-fill；
     - 不得包含独立 account_state；如需账户状态，只能使用本地 ledger 派生字段；
  5. 训练、paper、live 的输入 schema 必须一致。允许字段在某些环境下为空，但不能为空缺语义。
  6. 模型输出必须包含可审计的 trading/risk intent，例如 submit、cancel、no-submit、cancel-only、pause、size、price、side 等；live-trade 只消费该 intent 并回传 execution feedback。
  7. 每个训练样本、paper 决策、live 决策必须在 audit metadata 或 run manifest 中记录 feature_schema_version、online_rule_version、model_version、feature_hash；如记录 orderbook 摘要，只能作为审计 hash，不作为模型输入特征。
  8. 模型不得直接以未归一化的真实平台或模拟平台订单返回作为权威订单状态、训练标签或策略输入；raw response 只能作为 raw evidence、digest/reference、审计材料或受控调试材料保留。模型可以学习由 `NormalizedOrderEvent`、ledger 和 causal projection 派生出的订单生命周期状态、submit/cancel/fill 执行反馈、cancel pending/retry/ack/terminal 状态、延迟分布、fill hazard、风险状态和策略响应。


  ## 四、数据采集与数据消费要求

  1. 数据采集和数据消费必须解耦，但共享同一套 schema、manifest 和时间线语义；本文中的“数据采集与处理”是 umbrella，不等于单一 `./data-collect/` 模块。
  2. `data-collect` 必须支持 T-30 秒外部源预订阅和预加载，不得只在市场开始后才启动；当前四源合同为 Polymarket market channel、Binance BTCUSDT depth、Binance BTCUSDT aggTrade、Polymarket RTDS Chainlink BTC/USD。Market Context Builder / data-pipeline 才负责模型输入、feature context、labels、features、tick_stream 和 replay context。
  3. 市场发现不能只依赖 slug，必须保留 market_id、condition_id、token_id、start/end、market lifecycle state、runtime current/next role 和真实平台原始响应。
  4. `new_market`、market switch、prefetch、active transition 等事件不能丢失。
  5. 原始数据必须保留 raw_json 或等价原始证据。
  6. Parquet 可以作为 typed raw mirror，但不能成为唯一事实来源；paper/live 必须保留 append-only event log 或等价可回放事件流，且该 event log 不由 `data-collect` 生成或解释。
  7. 数据处理模块必须覆盖历史采集数据处理，不能绕过已采集的三四个月历史数据直接重做新链路；这里的数据处理指 `data-pipeline / data-processing`，不是 `data-collect`。
  8. 必须先制定全盘数据处理计划，再加入旧数据处理；旧数据处理结果应作为设计新的“数据采集 -> 数据处理”工作流的基础。
  9. artifact 发布必须 atomic rename，禁止 partial artifact 被消费；discovery 必须只读取 finalized artifact，temp/running、`.rsync-partial`、外部同步未完成标记和 incomplete manifest 不得进入消费。
  10. 每次采集、训练、paper、live 都必须生成 run manifest，记录代码版本/git commit、数据范围、schema、输入源、硬件资源和验证结果；`data-collect` manifest 还必须记录 collector binary sha256、source config digest、validation verdict、required files 和 source coverage。
  11. 数据采集与处理必须作为训练、paper trade、live trade 的共用合同：`data-collect` 提供外部市场源 typed raw / coverage / manifest；paper/live 实时消费 Market Context Builder 生成的市场上下文和模型输入；训练离线消费 `data-pipeline` 生成的训练可用数据集、校准数据池和 replay context。两者可以是不同运行实例，但必须共享 schema、manifest、时间线、causal cutoff、feature/input contract 和可回放语义。
  12. 时间戳必须命名空间隔离：`source_ts` 是外部源时间，`collector_recv_time_utc(host,region)` 是 `data-collect` 字段 `recv_time_utc` 加 collector host/region 元数据后的语义，不要求新增独立 schema 字段；`paper_live_runtime_receive_ts` 是对应 paper/live 运行主机接收时间，`decision_ts` / `causal_cutoff_ts` 是模型可见边界；live/paper 延迟标签只能来自对应运行主机 artifact，不能用非爱尔兰采集主机接收时间冒充。

  ## 五、订单微结构与毫秒级竞态要求

  订单微结构必须作为 P0 需求，不能只写成实现细节。

  1. `post_orders` batch 语义：
     - SDK transport 成功不等于订单 accepted。
     - batch 整体成功不等于每个 item 成功。
     - 每个 item 只有同时满足非空 platform order id、状态非 UNKNOWN、无拒单/错误文本、scope/decision_id/client id 可对应、数量和响应顺序可解
     释，才允许进入 accepted hot path。
     - blank order id、UNKNOWN status、error-only response、count mismatch、response order mismatch、mixed decision timestamp、超出 SDK/
     platform batch limit，必须 no-submit 或 fail closed。
     - 未 accepted 的订单不得 reserve、不得 ledger apply、不得进入 open order 权威状态。

  2. venue dust / dust remainder：
     - prior cumulative fill 后，如果剩余量小于 venue dust 或真实平台最小可成交单位，只能生成 Terminal/Filled 或等价关闭证据。
     - dust close 不得再次 mint shares，不得重复 ledger apply，不得制造额外 fill。
     - matched-cancel 后的 dust remainder 必须 audit-only 或 terminal-only。
     - 必须有正反测试覆盖：正常 partial fill、matched-cancel close、dust close、late user-channel 到达、重复 evidence 到达。

  3. 毫秒级取消竞态：
     - 必须显式记录 decision_ts、submit_ts、exchange_arrival_ts、ttl_cancel_request_due_ts_ms、cancel_enqueue_ts、sdk_cancel_response_ts、
     user_channel_event_ts、clob_query_ts、ttl_cancel_expected_ack_ts_ms。
     - `ttl_cancel_request_due_ts_ms` 与 `ttl_cancel_expected_ack_ts_ms` 必须分开，不能合并成一个“取消时间”。
     - TTL cancel 到点必须发送 cancel request；ack 晚到是另一个问题。
     - TTL cancel、model cancel、final cancel-all、market-switch cancel、operator cancel 可能在不同毫秒同时触发，必须幂等处理；其中 operator cancel 只能是用户或操作员触发的运行安全/紧急停机审计动作，不得作为 live-trade 策略性撤单或风控决策。
     - 第一个权威 terminal event 获胜，后到的重复 terminal evidence 只能归一化为 audit-only/alias，不得重复入账或重复取消。

  4. cancel response 语义：
     - generic not-found、not_canceled、absent target 不能直接当作成交证明。
     - 只有 exact matched/filled 语义，或 user-channel/SDK 可验证成交证据，才能归一化为 Terminal/Filled。
     - plain not-found 应作为 order-local retryable warning 或 unresolved evidence，等待其他权威来源关闭。
     - cancel 网络入口必须收敛到 SDK cancel/cancel_orders 路径，不允许不同入口各自解释 terminal/fill/cancel。
     - 模型可见的 cancel order 生命周期状态必须覆盖 cancel requested/enqueued/pending、cancel accepted/ack、cancel rejected、retryable warning、retry scheduled、retry budget exhausted、transport/timeout、matched-cancel filled、overlapped terminal 和 final terminal；这些状态只能由 NormalizedOrderEvent 与 ledger/projection 派生，不得由 live-trade 生成替代撤单策略。

  5. NormalizedOrderEvent：
     - 所有 SDK submit、post_orders item、cancel、cancel batch、user-channel、fill/partial-fill、local timeout、synthetic close、
     audit-only evidence 都必须先进入 NormalizedOrderEvent 或等价权威事件层。
     - NormalizedOrderEvent 是订单反馈归一化事实层，也是 ledger、模型可见投影、训练标签和审计的共同输入；它不是 live-trade 生成新策略或风控决策的入口。
     - NormalizedOrderEvent 的业务归一化必须由程序化、版本化、可回放、可测试的合同完成；不得把 accepted classifier、terminal arbitration、matched-cancel/dust/late evidence 归一化、ledger_apply_policy 或 validator 事实一致性检查完全交给模型训练自行学习。
     - 真实平台与模拟平台的 raw evidence 都必须通过同一套归一化合同进入模型可见投影、训练标签、paper 账本和 live 审计；不同运行模式只能在 source_kind、authority_source、latency distribution 和权限级别上不同，不能拥有各自的影子订单解释器。
     - validator 只能验证 normalized rows、raw digest、reference integrity 和 artifact consistency，不能成为第二套业务解释器。

  ## 六、账户、订单与热路径 ledger 权威

  1. 交易时只允许本地热路径 ledger 作为账户、持仓、挂单、sellable、CASH、NAV 和 active resting 状态权威。ledger 只能由 Polymarket market/user channel、SDK submit response、SDK cancel response、fill/partial-fill 事件和本地 append-only event log/provisional
  overlay 推导。
  2. CASH 指当前可用于交易的余额，必须由本地热路径 ledger 在决策时刻派生；模型输入、account_state、artifact 和报表字段不得使用 wallet allowance 表达 CASH。Polymarket/SDK allowance 或 approval 只能作为权限、协议 preflight 或对账证据，不是 CASH、NAV 或 PnL 的替代来源。
  3. PnL 不是独立账本权威状态；未带限定词时，PnL 必须指由当前 NAV、程序启动 baseline NAV 和运行期间外部资金流调整项派生的程序启动后跨市场累计 PnL。若启动 baseline 只有 CASH 且不存在历史持仓、历史挂单或外部充值/提现，则可简化为 current NAV - startup CASH。若需要单笔交易 PnL 或单个市场 PnL，必须明确写成 per-order PnL 或 per-market PnL，并只能作为审计或报表切片，不能替代程序启动后跨市场累计 PnL。
  4. NAV 是 ledger 在决策时刻计算的账户级净值字段，表示当前账户资产估值；PnL 是相对程序启动 baseline 的累计变化。NAV 不是单笔订单 NAV，也不是单个 market 的独立状态源；它可以作为模型输入和审计字段，但不得与 PnL 或 CASH 混用。
  5. 真实平台 position/sellable/open-orders/CLOB 查询或快照不得在 live 高频热路径中生成、覆盖或否定 ledger；不得作为触发交易时 fail closed的第二路径。
  6. SELL reject 只能说明该笔 submit 失败，不能自动证明账户不可用或下一 tick 必须停止交易。
  7. credential、scope、schema、wallet/funder、关键 append-only audit writer、模型 bundle 合同属于真实下单硬边界；这些边界缺失、不匹配或不可验证时，live-trade 必须拒绝 submit，并记录原因。非关键审计摘要、运行外审计差异、短暂状态不一致不得触发策略性fail closed。
  8. 模糊 SELL maturity、SELL/sellable 暂态差异、late user-channel、generic not-found 应进入 warning/quarantine/audit-review，不得一律hard fail。
  9. ledger 必须可回放、可重建，并能从回放结果复算 CASH 和 NAV，再按启动 baseline 与外部资金流调整项派生程序启动后跨市场累计 PnL；run manifest 或等价 artifact 必须记录启动 baseline、计算口径版本和数据范围。
  10. ledger 及其派生状态是模型输入与审计证据；live-trade 不得用本地账户规则替代模型风控，硬边界只能拒绝发送、维持 no-submit 权限模式、进入 cancel-only 权限模式或暂停运行，并把原因写入 audit/operator 记录；硬边界 verdict 不得作为模型输入或模型反馈，也不得把模型 intent 改写成策略性 no-submit、cancel-only、撤单或下单动作。

  ## 七、训练、paper trade 与 live trade 的真实交易反馈

  1. 模型训练和 paper trade 必须以真实平台回流的订单生命周期、成交/撤单/拒单反馈、ledger 结果和由 NAV 派生、可回放的程序启动后跨市场累计 PnL 为训练标签、评估目标和验收依据，不能只优化离线 leaderboard。
  2. 训练样本构造规则和 paper 模拟平台规则必须从 sdk-live-trade、online-feedback、Polymarket 真实平台潜规则、issues、中心化与去中心化混合架构造成的真实交易问题中反推。
  3. 训练标签必须来自本地热路径 normalized order event 与 ledger，不得手写冷路径状态标签。每条订单事件至少记录 source_kind、transition、normalization_reason、ledger_apply_policy、cancel_outcome、error_category、order_id/local_intent_id、source_ts/receive_ts、cumulative_fill_size、ledger_delta_fill_size、remaining_after_cumulative_fill、terminal_reason；market switch、ledger open-order state、user-channel freshness 只能作为上下文或审计元数据，不得替代订单结果标签。
  4. 模型训练样本构造和 paper trade 必须使用由真实平台 artifact 校准的模拟平台，重放或生成模拟订单生命周期，而不是只按理想成交价记账。每笔模拟 accepted resting order 必须先生成模拟平台侧是否成交、成交时间、完整成交或部分成交，再生成 user-channel 成交事件返回到本地 ledger 的可见延迟；这些时间必须能导致 full fill、partial fill、cancel complete、matched-cancel、late fill、venue dust close 等生命周期结果。
  5. partial fill 不是所有订单的固定概率，而是已成交订单的条件概率。训练样本构造和 paper 模拟平台的初始规则采用 0.05~0.15 作为 partial-fill 条件概率范围；成交后按 1~5 笔生成 fill segments，其中 1 笔表示完整成交，2~5 笔表示 partial fill，每笔之间的本地可见间隔默认在 0ms~1000ms 随机，并由真实平台 artifact 定期校准。
  6. matched-cancel 必须作为训练样本构造和 paper 模拟平台中的订单竞态结果建模，不得作为独立随机标签直接抽样。在上述通用模拟生命周期中，如果模拟平台侧已经成交或部分成交，但模拟本地 ledger 在 TTL cancel 前尚未看到 user-channel fill，随后模拟 cancel response 证明剩余订单已 matched，则归一化为 matched-cancel 或对应的 remaining-fill/dust 终态。live trade 不使用模拟平台，真实平台 matched-cancel 只能来自 SDK cancel response 或 user-channel/SDK 可验证成交证据。历史 live-funds artifact 中，TTL cancel request 通常出现在 submit accepted 后 250ms~252ms；user-channel fill 可见延迟和 matched-cancel 可见延迟必须作为训练/paper 可校准分布记录。
  7. venue dust close 的初始定义为 remaining_after_cumulative_fill <= 0.01 shares 时关闭剩余订单，走 TerminalDustNoMint，不再生成额外成交或现金变化。SELL clean accept latency、matched-cancel rate、partial-fill rate、user-channel fill visible latency、cancel response latency 必须从 sdk-live-trade 真实资金 artifact 统计后进入训练样本构造和 paper 模拟平台校准，不得用真实平台 snapshot 或冷路径查询替代。
  8. paper trade 的 lifecycle 与 accounting 必须分离：订单状态先归一化，账本再消费权威事件；order closure、position closure、accounting closure 和 report closure 必须分别验收，dashboard 只能展示结果，不能作为账务权威。
  9. paper simulator 必须先生成模拟平台 raw evidence，再通过与 live 一致的 NormalizedOrderEvent 合同归一化；paper 决策、训练样本和 replay 不得直接消费模拟平台私有订单状态，也不得把模拟平台内部随机结果绕过归一化后直接写成训练标签。
  10. 训练 ingestion 必须拒绝缺少 normalized_event_id、authority_source、normalization_reason、ledger_apply_policy、causal_cutoff_ts、label_maturity_ts 或 raw evidence digest/reference 的订单反馈标签；execution feedback label 还必须对齐版本化 schema，保留 decision context、paper/live/simulation source、book hash、online_rule_version、settlement source、censoring reason 和 no-quote/no-submit opportunity 语义。缺失时必须标记为 coverage incomplete、quarantine 或 fail-closed-training。
  11. paper trade 不得作为无约束超参搜索器；模型选择必须有 frozen holdout、purged/embargo split、PBO/DSR 或等价过拟合检查。
  12. 每个训练批次必须生成训练批次排名表，比较同一批次内不同参数、不同随机种子或不同候选产物的训练质量、验证质量、冻结 holdout 表现、执行现实回放结果、风险指标、资源消耗、artifact sha256 和数据范围。排名表只能作为 candidate/shadow 选择和 paper/live 审查加载的证据，不能替代 ModelRegistry 状态、人工审查、paper 验证或 live 授权 gate。
  13. live-trade 必须把真实执行反馈完整写入 training/paper 的规则、标签、反馈链路和审计 artifact；这些反馈不得未经审查直接改变线上模型行为，live-trade 也不得在内部根据反馈调整线上交易或风控决策。

  ## 八、模型 bundle 与策略边界

  1. 模型 bundle 必须有明确合同：feature_schema_version、model_version、input shape、sequence length、normalization、label contract、action
  surface、risk limits、online_rule_version、sha256。
  2. ModelRegistry 必须区分 active、shadow、retired、candidate。
  3. active 模型才能真实下单；shadow 模型只能产出对照决策和评估 artifact。
  4. 训练批次排名表必须与 ModelRegistry 对齐：排名靠前的产物只能进入 candidate 或 shadow 审查，只有通过模型 bundle 合同、训练反馈 gate、paper/no-submit 验证和授权 gate 后，才允许进入 active。
  5. 不同 strategy family 必须分开训练、评估和上线，不能把不同动作空间的模型混在同一个 leaderboard 或训练批次排名表中比较。
  6. 上线前必须做 action-surface audit，确认模型能输出的动作与实际 SDK/order/venue 硬约束和模型风控输出合同一致。
  7. 模型 bundle 必须定义交易动作与风控动作输出合同；live-trade 只能验证 action surface 是否满足平台、SDK、权限、schema 和钱包硬约束，不能改写模型的交易或风控 intent。

  ## 九、硬件与训练效率要求

  1. 训练设计必须考虑硬件使用效率，而不是只让代码能跑。
  2. 计划最高使用 4 张 NVIDIA RTX 5090；对应资源按用户当前配置记录为：4 张 NVIDIA RTX 配置合计约 148G VRAM、100 vCPU、360GB RAM；单卡配置为
  NVIDIA RTX 5090 32GB VRAM、25 vCPU、Intel Xeon 8470Q、90GB RAM。
  3. 由于 32GB * 4 与 148G VRAM 存在口径不一致，需求中必须要求运行时用 `nvidia-smi`、`lscpu`、`free`、cgroup limit、driver/runtime manifest
  固化实测值，以实测值作为调度和验收依据。
  4. 必须建立 5090 run manifest/profiler contract，记录 Python interpreter、git commit、sync root、batch size、num workers、shard、epoch time、GPU util、VRAM、CPU、RAM、IO、throughput、质量指标。
  5. 不得用单一 GPU 利用率或单一 batch size 判断训练成功。
  6. batch 变大不必然更好；必须同时比较吞吐、显存、内存、rolling worst、验证集质量和端到端训练时间。
  7. DataLoader worker、prefetch、shard、cache、validator materialization 必须有 profiler 证据后再调整。
  8. 训练服务器内存、CPU、GPU 随 GPU 数增加而成套增长，但系统仍必须支持单卡、双卡、四卡分级运行。

  ## 十、P0 gates 与验收边界

  核心重构必须至少包含以下 P0 gate 或等价机制：

  1. OnlineFeedbackGate：online-feedback 规则版本、真实平台潜规则和 issues 是否已纳入。
  2. ModelBundlePreflight：模型 bundle、schema、ONNX/runtime、feature contract 是否匹配。
  3. MarketDiscoveryGate：active/next market、token、tick、min size、start/end、market lifecycle state 与 runtime role 是否可靠。
  4. MarketSwitchGate：active 与 prefetched context 是否隔离，切换是否可审计；旧 market 只有满足 `market_resolved` 事件路径或 fallback metadata retired predicate 后才能进入 retired，否则只能标记 stale/gap/quarantine。
  5. ResourceWatermarkGate：CPU/RAM/VRAM/IO/disk/artifact writer 是否满足阈值，并覆盖 writer queue、flush/finalize latency、backpressure 和 ResourceWatermark 阈值动作。
  6. HardwareTierGate：单卡/多卡训练配置、实际硬件、运行参数是否匹配。
  7. SupplyChainGate：SDK、依赖、模型 artifact、数据 artifact、部署产物 sha256 是否可追踪；外部 repo、脚本和依赖默认 pinned readonly，不得默认 install/run、读取 secrets 或联网出站，任何例外必须有隔离环境、审批和审计 artifact。
  8. TrainingFeedbackGate：真实交易反馈、paper 反馈、训练标签、online_rule_version、训练批次排名表是否闭环；必须覆盖 execution reality，包括 queue/fill hazard、post-only reject、tick/min size、fee/rebate、slippage/cost、no-quote/no-submit opportunity、trial ledger 和冻结 holdout/过拟合防护，不能只看 replay leaderboard。
  9. ModelIntentBridgeGate：模型 trading/risk intent、live-trade 发送合同、平台反馈回传合同和“禁止 live-trade 改写模型交易/风控决策”的边界是否满足。
  10. DataIngestRuntimeGate：`data-collect` 与数据消费运行时是否满足 Rust async/non-blocking、bounded queue、writer lag、backpressure、source health/gap/reconnect trace、finalized-only discovery、temp/running/rsync-partial 排除和 blocking pool 隔离。

  每个 gate 必须有明确 verdict，例如 PASS、WARNING、NO_SUBMIT、CANCEL_ONLY、FAIL_CLOSED。coverage incomplete、quarantine 可以作为原因标签，但必须映射到
  明确执行权限动作；不得用模糊的“看起来正常”作为通过标准。gate 输出的 NO_SUBMIT、CANCEL_ONLY 只能表示权限/硬边界 verdict，不能作为 live-trade 生成的策略性 no-submit、cancel-only 或风控动作。

  ## 十一、部署与运行边界

  1. AWS live-trade 服务器 `108.130.74.143` 是运行目标，不是构建机。
  2. 开发、代码编写与 live-trade release binary 本地构建必须使用 `/home/chillchill/btc-updown-marketmaker`，并记录 commit、sha256、产物路径后部署；各远端服务器源码/工作目录统一写作 `~/btc-updown-marketmaker`，不得把服务器上 `/home/ubuntu/btc-updown-marketmaker` 误作本地路径。
  3. live 服务器不得运行 Rust 编译命令，不得依赖服务器 git pull、源码 checkout 或 GitHub deploy key；该限制覆盖所有 Rust crate、`data-collect`、paper/live runtime 和工具程序。
  4. 交易服务器上的训练模型产物与交易程序二进制文件必须放在 `/home/ubuntu/live-trade`。
  5. 模型文件不得通过 GitHub 同步，不得放入 wallets；部署模型时必须从本地已验证 artifact 路径直接复制，并记录来源路径与 sha256。
  6. wallets 只保留 gitignored secret/env 与钱包公共派生配置。
  7. live-trade、paper-trade、training、data collection 的运行根目录、artifact 根目录和权限边界必须明确。
  8. 交易服务器上的训练模型产物与交易程序二进制文件路径解析到 `/home/ubuntu/live-trade` 之外必须 fail closed。
  9. live-trade release binary 不得包含独立交易策略或独立风控决策逻辑；它只包含模型加载/输入输出桥接、SDK 执行、平台反馈回传、artifact 记录和权限/协议/安全硬边界。
  10. live 交易小主机不得承担训练式重处理、历史全量 parquet 扫描、压缩归档批处理、archive/upload/listing/gzip 或 `data-collect` 编译/test/check/fmt；这些工作必须在具备硬件条件的构建机、数据服务器或训练服务器完成，且 archive/upload/listing/gzip 若纳入范围必须另开需求、计划和 changelog。

  ## 十二、验收标准

  本次核心需求文档完成后，必须满足以下验收标准：

  1. 权威覆盖基线 `/home/chillchill/btc-updown-marketmaker/mistake-log/2026-07-03-重构前错误汇总与优先级.md` 中每类历史问题都能映射到至少一个新需求、
  gate、artifact 或测试。
  2. 新系统流程覆盖自检、热身、T-30 预加载、active market 持续决策、market end T-30 下个模型预加载。
  3. 训练、paper、live 均使用完整 5 分钟数据 + 30 秒预加载上下文，而不是 250ms 孤立片段。
  4. 数据采集与数据消费有明确 schema、manifest、raw evidence、atomic artifact 和 append-only event log 边界；`data-collect`、Market Context Builder、`data-pipeline/data-processing`、feedback-ingestion、NOE/Ledger 的输入、输出和非范围必须可追溯，训练、paper、live 共享同一套时间线、schema 和可回放语义。
  5. 数据处理需求覆盖历史采集数据，且新数据采集到数据处理工作流以旧数据处理经验和结果为基础；labels/features/tick_stream/replay context 由 `data-pipeline/data-processing` 生成，不得写入 `data-collect` 范围。
  6. `post_orders` batch、venue dust、matched-cancel、generic not-found、TTL/model/final cancel、late user-channel、open order audit 都有明确
  订单生命周期语义。
  7. 真实平台和模拟平台的订单 raw evidence 都必须先通过同一套版本化 NormalizedOrderEvent 合同归一化，再进入模型输入、训练标签、paper 账本、live 账本和审计；模型不得直接把未归一化订单返回作为权威订单状态或标签。验收必须覆盖 unknown submit、generic not-found、matched-cancel、partial-fill、dust close、late user-channel、duplicate terminal evidence，以及 cancel requested、cancel pending、cancel accepted、cancel rejected、cancel retry、cancel timeout、overlapped terminal 和 cancel retry budget exhausted。
  8. 训练和 paper 能从真实 live 反馈、真实平台潜规则、online-feedback、issues 和混合架构约束中反推规则。
  9. 每个训练批次必须生成可审计训练批次排名表，记录候选产物、参数/随机种子、数据范围、核心质量指标、执行现实回放指标、资源消耗、artifact sha256、候选入选理由和淘汰理由；排名表只能作为进入 candidate/shadow、paper 或 live 审查加载的证据，不能单独授权 active 或 live。
  10. 5090 单卡/多卡训练有硬件实测、run manifest、profiler 和资源水位验收。
  11. SDK-first 边界、部署边界、模型 artifact 边界、wallet 边界、small-live/live 授权边界、`data-collect` 运行边界和 live 服务器运行边界全部明确。
  12. 任何不能验证的结论必须标记为 coverage incomplete、warning 或 quarantine，并映射到 NO_SUBMIT、CANCEL_ONLY 或 FAIL_CLOSED 等明确执行权限动作，不能冒充通过。
  13. 交易决策和风控完全由模型实现，live-trade 仅作为模型与真实平台之间的桥梁；所有模型请求、硬边界拒绝、真实平台返回和执行反馈都有可审计映射，且不存在 live-trade 改写模型交易或风控 intent 的路径。
