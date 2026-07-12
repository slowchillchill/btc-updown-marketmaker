  # rebuild-core-demand 核心重构需求

  事项编号：`rebuild-core-demand`。

  状态：`需求审核中；尚未获得用户最终批准；不授权创建对应 plan/changelog、执行代码重构、部署、paper、live 或资金操作`。

  本文件在用户审核通过前只作为当前需求草案。对应 plan 与 changelog 必须在用户明确批准本需求后再创建；本状态说明不改变已批准且正在运行的独立
  `data-collect` 事项，也不恢复用户已手动删除的旧 rebuild-core 子事项文档。

  本需求拟对项目核心交易链路进行重构，从架构层面和系统设计思路层面解决重构前已盘点的历史问题。权威覆盖基线是
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
    BRIDGE["（共用）Intent Bridge / Execution Adapter<br>BUY-only 意图与执行合同"] -- paper --> SIM["（共用）sim-system 模拟平台模块<br>训练 / paper 共用订单执行反馈模拟"]
    BRIDGE -- live --> REAL["真实平台模块<br>官方 Polymarket Rust SDK"]
    SIM --> RAW["平台 raw evidence<br>sim-system 模拟反馈 / 真实平台反馈"]
    REAL --> RAW
    RAW --> NOE["（共用）NOE / 归一化合同<br>订单生命周期归一化"]
    NOE --> LEDGER["（共用）Ledger 合同<br>账户 / 持仓 / 挂单 / pUSD CASH / NAV"]
    LEDGER -- 交易状态反馈 --> TRADE
    TRADE --> MODEL_RUNTIME["（paper/live共用，不包含train）Model Runtime / 模型运行"]
    MODEL_RUNTIME -- 模型意图 --> BRIDGE
    TRADE --> TRADE_REPORT["交易可视化可审计报告"]

  ```

  ### Train 图表

  ```mermaid
  flowchart TD
    TRAIN["Train 模块<br>持续消费训练market数据+ledger数据<br>训练框架 / 奖惩办法 / 回放评估"] --> MODEL_TRAIN["模型产物 / Model Registry<br>active / shadow / candidate / retired"] & TRAIN_REPORT["训练可视化可审计报告"]
    TRAIN -- 训练回放意图 --> BRIDGE["（共用）Intent Bridge / Execution Adapter<br>BUY-only 意图与执行合同"]
    BRIDGE --> SIM["（共用）sim-system 模拟平台模块<br>训练 / paper 共用订单执行反馈模拟"]
    SIM --> RAW["sim-system 返回模拟原始证据<br>成交 / 部分成交 / 不成交 / 拒单 / 撤单"]
    RAW --> NOE["（共用）NOE / 归一化合同<br>订单生命周期归一化"]
    NOE --> LEDGER["（共用）Ledger 合同<br>回放账户 / 持仓 / 挂单 / pUSD CASH / NAV"]
    LEDGER -- 反馈证据 / 奖惩依据 --> PIPELINE
    MODEL_TRAIN --> DEPLOY["供 Paper / Live Trade 审查加载"]
    DATA["外部市场源<br>Polymarket market channel / Binance BTCUSDT depth / Binance BTCUSDT aggTrade / Polymarket RTDS Chainlink BTC/USD"] --> COLLECT["（共用）data-collect<br>typed raw / coverage / manifest"]
    COLLECT --> PIPELINE["（共用）data-pipeline / data-processing<br>features / labels / tick_stream / replay context"]
    PIPELINE --> DATASET["训练可消费数据集 / 校准数据池<br>features / labels / replay context"]
    DATASET --> TRAIN
    TRAIN_REPORT --> RANK_LIST["训练批次中不同产物的排名表"]
  ```

  架构图只表达高层业务处理流程，不把权限/硬边界、schema 字段、具体订单状态枚举或 artifact 字段画成独立业务模块。Paper/live trade 图表表达在线运行闭环：`data-collect` 只把外部市场源转化为 typed raw、coverage 和 manifest；实时市场上下文和模型输入由 Market Context Builder / runtime 消费 `data-collect` 外部市场源证据、运行期 append-only log 和 ledger projection 后生成。模型产物加载到 paper/live，paper 把模拟订单提交给 `sim-system` 并消费其平台反馈，live 把真实订单提交给真实平台，二者返回的 raw evidence 经 NOE 和 ledger 后只回到 paper/live 作为下一次交易状态反馈；进入训练只能通过离线反馈入库、训练数据集或校准数据池。

  Train 图表表达离线训练与回放闭环：`data-collect` 提供历史外部市场源 typed raw、coverage 和 manifest；`data-pipeline / data-processing / feedback-ingestion` 再把历史市场数据、NOE/Ledger 派生反馈、order evidence digest/reference 和 reports 处理为训练可消费数据集与校准数据池。训练模块再消费这些数据，使用同一套 `Intent Bridge / Execution Adapter` 合同把模拟订单提交给 `sim-system`，由 `sim-system` 模拟订单提交到平台后的 accepted/rejected、成交、部分成交、不成交、撤单、延迟和结算反馈，再经 NOE/Ledger 用于回放、样本构造、规则校准和奖惩评估。训练模块生成模型产物、训练报告、训练批次排名表和下一轮可审查的模型版本。训练批次排名表用于比较同一批次内不同神经网络层级、训练参数、随机种子或候选产物的表现，作为筛选进入 paper/live trade 审查加载的证据；它不能单独授权模型进入 active、paper 或 live。训练模块不在 paper/live 在线交易闭环中实时接收 ledger 反馈，也不得让 paper/live 的执行流程直接循环回训练。

  `sim-system` 是训练模块与 paper trade 共同调用的唯一模拟平台模块；训练和 paper 可以使用隔离运行实例，但必须调用同一套版本化 `sim-system` 实现、规则合同和 artifact schema。训练、paper、live 共享 Intent/NOE/Ledger 等合同；图中共用公共模块均在模块开头标注“（共用）”。共用表示 schema、manifest、时间线、causal cutoff、特征/输入合同和可回放语义一致，不要求训练离线处理与 paper/live 实时处理使用同一个运行实例。训练和 paper 只能通过 `sim-system` 模拟订单提交平台后的反馈；live 只能通过真实平台模块连接官方 Polymarket Rust SDK 或经批准的 SDK 缺口方案发送到平台，禁止调用 `sim-system` 生成或覆盖真实执行事实。`sim-system` 必须由真实平台 artifact、当前规则证据、潜规则和 issues 校准。两类平台都必须先保留 raw evidence 并使用同一版本化 `NOE / NormalizedOrderEvent` 合同，但运行状态严格隔离：`sim-system -> train/paper NOE/ledger`，`真实平台 -> live NOE/ledger`；共享合同不代表共享 ledger 实例或允许模拟证据进入 live。

  权限、SDK、wallet、schema、路径、平台约束、模型 bundle、关键 writer 等硬边界校验发生在程序启动、自检、热身、T-30 预加载以及运行前/运行中的权限模式校验中，不作为图中的独立业务决策模块。执行时硬边界只能把 `execution_permission` 设为 `ALLOW_SUBMIT`、`NO_SUBMIT_PERMISSION`、`CANCEL_ONLY_PERMISSION` 或 `PAUSED_PERMISSION`，并写入 audit/operator；不得改写模型交易/风控意图，也不得把硬边界 verdict 当作训练标签或模型执行反馈。Ledger 是账户、持仓、挂单、pUSD CASH、NAV 等状态权威；训练报告和交易报告必须能追溯到市场上下文、模型意图、raw evidence、NOE、ledger、manifest 和 artifact。

  ### 模块边界术语

  1. `data-collect` / `./data-collect/` 专指外部市场源采集模块，只负责 Polymarket market channel、Binance BTCUSDT depth、Binance BTCUSDT aggTrade、Polymarket RTDS Chainlink BTC/USD 的 raw evidence、`recv_time_utc`、typed raw parquet、source coverage、source health/gap、reconnect trace、manifest 和 atomic publish；不负责 labels、features、tick_stream、训练切分、模型输入加工、paper/live append-only log、order evidence、NOE/Ledger 或 execution feedback ingestion。
  2. `data-collect` 的三种运行角色必须分别构建、记录 sha256 并独立部署为三个 release binary：`data-collect-training-collector` 只部署在数据采集服务器，`data-collect-paper-hot` 只部署在 paper trade 服务器，`data-collect-live-hot` 只部署在 live trade 服务器。三者共享 source/schema/handoff 合同，但不得共享进程、运行根、钱包、交易状态或消费者 artifact；`data-collect-live-hot` 是独立的外部市场源 handoff binary，不是 live-trade 主交易程序。
  3. `Market Context Builder` / runtime context builder 专指 paper/live 热路径中的市场上下文和模型输入组装层；它可以消费 `data-collect` 外部市场源证据、运行期 append-only log、NOE/Ledger projection 和模型 bundle 合同，但不能把 finalized parquet 当作 paper/live 热路径唯一事实来源。
  4. `data-pipeline` / `data-processing` / training dataset builder 专指离线处理层，负责从 finalized typed raw、NOE/Ledger 派生反馈和审计引用生成 labels、features、tick_stream、replay context、训练切分和校准数据池。
  5. `feedback-ingestion` 专指把 paper/live/simulation 的 append-only log、live artifacts、order evidence digest/reference、NOE/Ledger 输出接入训练反馈链路；它不是 `data-collect`。
  6. `sim-system` 是当前系统对模拟平台模块的唯一规范名称。它接收训练模块或 paper trade 产生的 BUY-only 模拟订单意图，模拟订单提交平台后的执行与生命周期反馈，输出模拟 raw evidence；它不是模型、训练框架、paper runtime、真实平台、NOE 或 ledger，也不得被 live trade 调用为真实状态源。
  7. `market_lifecycle_state`、`market_runtime_role`、`model_registry_state` 和 `deployment_hardware_status` 必须命名空间隔离：market lifecycle 可以使用 candidate / prefetched / active / retired；runtime role 表达 current / next 等相对角色；ModelRegistry 只使用 active / shadow / candidate / retired；`requires_hardware_upgrade` 只能是独立的部署硬件状态或 gate verdict，不能成为第五种 ModelRegistry 状态。不得跨域比较裸字符串。
  8. 模型策略态、gate 判定和执行权限必须使用独立字段：模型使用 `model_strategy_mode=ALLOW_SUBMIT|MODEL_NO_SUBMIT|MODEL_CANCEL_ONLY|MODEL_PAUSE`；gate 使用 `gate_verdict`；执行边界使用 `execution_permission=ALLOW_SUBMIT|NO_SUBMIT_PERMISSION|CANCEL_ONLY_PERMISSION|PAUSED_PERMISSION`。`ALLOW_SUBMIT` 允许执行通过 preflight 的模型 submit/cancel intent；`NO_SUBMIT_PERMISSION` 是 observe/reconcile-only，阻止所有模型来源的平台变更；`CANCEL_ONLY_PERMISSION` 阻止 submit，但允许执行明确的模型 cancel intent；`PAUSED_PERMISSION` 停止消费模型 intent。operator emergency cancel 是独立 actor，不因模型权限态消失。不得跨字段比较裸字符串；执行权限只能允许或阻断模型 intent，不能把一个 intent 改写成另一种模型策略。
  9. `account_global_projection` 专指跨 market 唯一的 pUSD 现金、reserve、matched-pending exposure、账户级 NAV 和 ledger sequence；`market_local_projection(market_id)` 专指目标 market 的两个 outcome position、订单列表、pending cancel 和 lifecycle 状态。旧 market 的经济影响必须进入 account-global 投影，但其订单、持仓和 lifecycle 明细不得混入新 market 的 market-local 投影。

  ## 一、总体原则

  1. 重构必须先定义系统边界，再写实现。
  2. Polymarket 相关 submit、cancel、balance、approval allowance、user-channel、market-channel 等必须优先基于官方 Polymarket Rust SDK。
  3. 如果官方 SDK 能力不足，必须先列明具体缺口、影响和可选方案，经用户明确同意并限定缺口范围后，才能考虑 raw REST、manual signing、自建
  SDK 或其他旁路实现。
  4. 不允许出现第二套影子 feature pipeline、影子订单解释器、影子账本解释器或影子 SDK。
  5. 数据、模型、paper、live 必须共享同一套市场时间线、订单事件语义、artifact 合同和反馈标签语义。
  6. 交易决策、仓位/风控决策、暂停、不下单和撤销未成交 BUY 订单等策略性动作必须完全由模型输出或模型 bundle 明确声明的模型状态机输出；live-trade 不得内置第二套策略或风控来改写模型意图。
  7. 当前版本全系统的主动下单动作面固定为 `BUY-only`：模型、训练样本、`sim-system`、paper trade、Intent Bridge、execution adapter 和 live trade 只能创建 BUY 意图与 BUY 订单，不得创建、接受、模拟、发送或保留自有 SELL 动作的 deferred/research/feature-flag/占位路径。外部 orderbook 的 bid/ask、历史成交 side 和平台返回 side 仍可作为完整市场事实输入，不属于本系统主动动作。
  8. 需要表达对 `outcome_up` 的反向方向时，系统通过 BUY `outcome_down` 实现；需要表达对 `outcome_down` 的反向方向时，通过 BUY `outcome_up` 实现。这只是方向暴露和持有至结算策略上的对侧替代，不是平台订单、现金流、库存或成本上的完全等价：互补 BUY 会继续占用资金并增加持仓与成本，但以此换取全系统动作、账本、模拟和执行链路的大幅简化。
  9. 模型决策必须由模型可见的业务事件触发，不得由固定频率、固定周期、周期性 polling 或固定 decision tick 触发。模型可见业务事件包括外部市场数据事件、NormalizedOrderEvent / ledger 状态变化、market preload/start/end 与 GTD 到期事件，以及 submit response、user-channel、cancel response 或 settlement evidence 实际在客户端变为可见的事件。平台内部“撮合延迟结束”本身不是模型可见事件，不得在相应 evidence 到达前触发模型。每个有效模型可见事件都必须先更新该 market 的持续因果状态，再触发模型交易与风控 intent 评估；事件触发不等于只向模型输入当前孤立事件。
  10. live-trade 是模型与 Polymarket/外部平台之间的桥梁，只负责数据和真实平台事件归一化、模型输入供给、BUY-only 模型请求校验与发送、真实平台返回/成交/拒单/撤单/ledger 派生状态记录，以及权限、协议、schema、钱包、路径等硬边界 fail closed；它可以拒绝执行不满足硬边界的请求并把原因写入 audit/operator 记录，但不得生成替代交易决策。
  11. 本文中的“真实平台”专指 live trade 连接的 Polymarket/SDK/market channel/user channel/submit response/cancel response；“模拟平台”统一称为 `sim-system`，专指训练模块和 paper trade 共同调用的订单提交后反馈模拟模块。`sim-system` 只能用真实平台 artifact 与版本化规则证据校准，不得在 live 高频热路径中作为账户、订单或风控状态源。
  12. 所有关键结论必须有 artifact、manifest、schema、日志或测试证据支撑，不能只靠 stdout 或人工判断；用户明确冻结的项目规则假设必须与官方公开事实分开标记 provenance、版本和适用边界。

  ## 二、新系统流程要求

  系统必须支持完整 5 分钟 BTC up/down 市场生命周期，并显式区分 active market 与 prefetched next market。

  1. 自检阶段：
     - 启动时必须先冻结 `runtime_role=data_collect_training|data_collect_paper_hot|data_collect_live_hot|training|paper_trade|live_trade`、对应 binary identity 和本次实际依赖，再执行该角色自己的自检合同；三种 `data_collect_*` 角色不得与三个消费者/交易角色共用同一进程身份。
     - 共用检查只覆盖该角色实际依赖的数据源、市场发现、当前/下一个市场、token id、tick size、min size、fee/rebate、reward eligibility、schema、关键 writer、磁盘和资源。
     - `data_collect_training` 检查四个外部源、typed raw writer、coverage/manifest、磁盘和采集服务器 CPU/RAM；`data_collect_paper_hot` / `data_collect_live_hot` 检查各自服务器上的四个外部源、realtime handoff frame/socket、heartbeat/status、source gap/reconnect/backpressure、binary/path identity 和本机资源。三者都不得要求模型 bundle、`sim-system`、NOE/ledger、wallet、funder、真实 user channel 或训练 GPU。
     - `training` 检查 finalized dataset/manifest、data-pipeline、`sim-system`、NOE/ledger、训练 CPU/GPU/RAM 和训练 artifact writer；不得要求真实 wallet、funder 或 live user channel。
     - `paper_trade` 检查来自独立 `data-collect-paper-hot` 进程的 realtime handoff、Market Context Builder、模型 bundle/runtime、`sim-system`、paper NOE/ledger 和 audit writer；不得要求真实 wallet/funder。
     - `live_trade` 检查来自独立 `data-collect-live-hot` 进程的 external-market realtime handoff、真实 user channel、官方 Rust SDK submit/cancel/reconciliation 能力、wallet/funder、模型 bundle/runtime、live NOE/ledger、关键 append-only audit writer、部署路径和本机 CPU/RAM；不得绕过 handoff 再建第二条外部 market-data feed，也不得要求训练 GPU。
     - 自检失败只能阻止对应组件或运行角色进入自身热路径；training-only、paper-only、live-only 或 data-collect-only 依赖不得互相阻断。共享合同本身不一致时，也必须分别记录每个受影响角色的 verdict 和 effect。

  2. 热身阶段：
     - 按 runtime role 验证热身合同：三种 `data_collect_*` binary 只验证各自外部 market source 与 writer/handoff；paper_trade 验证两个 outcome 的模拟 BUY submit/cancel/fill、`sim-system` response、NOE/ledger；live_trade 验证两个 outcome 的真实 BUY submit/cancel/fill、user channel、SDK response、NOE/ledger。不得让 data-collect binary 承担订单热身，也不得把 SELL 路径作为当前版本热身项。
     - 热身必须分别记录三条互不替代的轴：`runtime_role`；`funds_authorization_tier=NO_REAL_FUNDS|SMALL_LIVE_1_MARKET|SMALL_LIVE_3_MARKETS|FORMAL_LIVE_1H`；以及 `execution_permission=ALLOW_SUBMIT|NO_SUBMIT_PERMISSION|CANCEL_ONLY_PERMISSION|PAUSED_PERMISSION`。paper 是 `runtime_role=paper_trade` 且资金授权固定为 `NO_REAL_FUNDS`，不是 permission 或 funds tier；任何一轴都不得与 `model_strategy_mode` 或 `gate_verdict` 混用。
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
     - 每个有效模型可见事件到达并完成时间排序、去重、market 隔离、NOE/ledger projection 与 causal availability 判定后，必须更新该 market 的持续状态并触发一次模型交易与风控 intent 评估；没有新的模型可见事件时，不得由固定频率调度器凭空生成策略决策。
     - 模型输入必须包括 price to beat、orderbook/depth、Binance/外部价格、本地 ledger 派生的 pUSD CASH、position、目标 market 内完整的 `open_order_list`、`pending_submit_list`、pending cancels、active resting count、账户级 NAV 字段、订单生命周期状态、最近 submit/cancel/fill/partial-fill 反馈，以及 cancel requested、cancel ack、cancel rejected、cancel retry、cancel timeout、matched-cancel、overlapped terminal 等 cancel order 生命周期反馈。
     - `open_order_list` 中每笔 BUY order 至少包含 order reference、market/outcome/token、GTC/GTD order type、`limit_price_pusd_per_share`、accepted original shares、客户端当前已见 cumulative fill/remaining shares、`open_order_reserved_pusd`、created/accepted time、当前可见 execution/lifecycle state 和 pending-cancel state。FOK/FAK 不得产生 resting open remainder。该列表只能反映当时客户端已经可见的 NOE/ledger projection，不得泄露 `sim-system` 的平台真实状态或尚未到达的 fill。
     - `pending_submit_list` 只容纳 `UnknownSubmitOutcome` 等尚未获得 accepted/rejected 权威证据的本地 BUY intent，必须显式保留其 order type、对应的 limit 或 market BUY 输入字段及 `pending_submit_reserved_pusd`，标记 provisional，不能计入 open order 或 active resting count。只有官方 SDK 合同能够安全生成并验证 expected platform order hash，或 cancel-all/cancel-by-market 等 SDK scope 不需要伪造 order id 时，该 provisional entry 才能提供合法 cancel target/scope；否则模型只能等待可关联证据，不能构造 platform order id。
     - 模型必须能在完整 5 分钟市场窗口内持续做交易决策，而不是只看到 250ms 的孤立片段。
     - live-trade 不得基于自身规则改变模型输出的 outcome/token、price、quantity basis、submit、cancel、`MODEL_PAUSE`、`MODEL_NO_SUBMIT` 或风控动作；`order_action=BUY` 是当前版本系统常量，任何非 BUY 请求都必须在 action-surface preflight 中拒绝并进入 audit，不得被改写成另一笔交易。平台返回的执行结果进入本地事件和 audit，硬边界拒绝原因只进入 audit/operator 记录，不作为模型输入或模型反馈。
     - 系统必须估算并记录模型运行需要多少显存、内存、CPU、线程和 batch/sequence 配置。

  5. 当前市场结束前 T-30 秒：
     - 必须开始预加载下一个市场的全部模型输入。
     - 模型对当前市场 open order 发出的单笔或全部取消、平台对遗留订单产生的到期/市场关闭终态、成交确认、持仓结算、ledger close、artifact close 与下个市场 preload 必须并行但隔离；旧 market 的 reserve、matched-pending 和结算变化仍须更新唯一的 account-global 投影。
     - 任何 market switch 都不能让上一个市场的订单事件污染下一个市场。

  ## 三、模型输入与训练窗口要求

  1. 模型训练、paper trade、live trade 都必须持续输入目标 market 从 `start - 30s` 到 market end 的可见上下文；这 30 秒是目标 market 的 pre-start prefetched context，不得把上一个 market 的订单、持仓或 lifecycle 明细混入目标 market-local 投影。由旧 market 未结算订单或持仓产生的 account-global pUSD CASH、reserve、matched-pending exposure 和 NAV 影响不属于污染，必须通过带同一 ledger sequence 的只读 account-global snapshot 对目标 market 可见。
  2. 模型输入不得截断为孤立的 250ms 数据片段，必须保留第 1 条规定的完整可见上下文。
  3. 每个训练样本、paper 决策、live 决策都必须带有 causal cutoff，确保模型只能看到当时真实可见的信息。
  4. 训练样本、paper 决策和 live 决策必须与真实模型可见事件触发点对齐，并至少记录 trigger_event_id、trigger_event_type、event_available_time、decision_ts 和 causal_cutoff_ts。固定间隔重采样或 `tick_stream` 可以作为派生特征、统计、兼容分析或回放索引，但不得成为模型决策触发器，也不得生成没有真实业务事件对应的策略决策样本。
  5. 模型输入至少应包含：
     - market_id、两个 outcome 的 token_id、外部市场 book/trade side、time-to-expiry、active/prefetched 标记；
     - 所有数据源的时间对齐状态和 freshness；
     - orderbook、depth、spread、price to beat、tick size、min size；
     - `account_global_projection` 中的 `cash_balance_pusd`、`pending_submit_reserved_pusd`、`open_order_reserved_pusd`、`cancel_reconciliation_reserved_pusd`、`total_reserved_pusd`、`available_cash_pusd`、matched-pending settlement exposure、账户级 NAV 和 ledger sequence；以及 `market_local_projection(market_id)` 中两个 outcome 的 provisional/confirmed position、目标 market 内完整 open BUY order list、pending submit list、pending cancels 和 active resting count；各 market reserve 切片之和必须与 account-global 总额闭合，不能形成第二份可花现金；
     - submit/cancel/fill/partial-fill；
     - 不得包含独立 account_state；如需账户状态，只能使用本地 ledger 派生字段；
  6. 训练、paper、live 的输入 schema 必须一致。允许字段在某些环境下为空，但不能为空缺语义。
  7. 模型输出必须包含可审计的 BUY-only trading/risk intent，例如 `submit_buy`、`cancel_order(order_ref)`、`cancel_all_open_orders(scope)`、`model_strategy_mode=MODEL_NO_SUBMIT|MODEL_CANCEL_ONLY|MODEL_PAUSE`、目标 outcome/token、`order_type=FOK/FAK/GTC/GTD`、GTD expiration 和 post-only。GTC/GTD limit BUY 必须输出 `limit_price_pusd_per_share + size_shares`；FOK/FAK market BUY 必须输出 `spend_pusd + worst_price_pusd_per_share`。`order_action=BUY` 是当前合同常量；quantity basis 由 order type 决定，不得再把四种 order type 统一成同一份额数量输入。`MODEL_CANCEL_ONLY` 只限制模型不再 submit，实际撤单仍须由明确的 `cancel_order` 或 `cancel_all_open_orders` intent 产生。live-trade 只消费该 intent 并回传 execution feedback。
  8. 每个训练样本、paper 决策、live 决策必须在 audit metadata 或 run manifest 中记录 feature_schema_version、online_rule_version、model_version、feature_hash；审计 metadata 中额外记录的 orderbook 摘要只能作为 audit hash，不是额外模型特征，但这不禁止模型消费 feature schema 已声明、按 causal cutoff 生成的 orderbook/depth 特征。
  9. 模型不得直接以未归一化的真实平台或 `sim-system` 订单返回作为权威订单状态、训练标签或策略输入；raw response 只能作为 raw evidence、digest/reference、审计材料或受控调试材料保留。模型可以学习由 `NormalizedOrderEvent`、ledger 和 causal projection 派生出的订单生命周期状态、submit/cancel/fill 执行反馈、cancel pending/retry/ack/terminal 状态、延迟分布、fill hazard、风险状态和策略响应。
  10. 外部市场数据中的 bid/ask、maker/taker 或 BUY/SELL side 必须与本系统自有 `BUY-only` action surface 命名空间隔离；保留完整外部市场事实不代表系统具有自有 SELL 能力。

  ### 当前版本交易动作与盈利机制

  1. 当前版本只允许 BUY 两个 outcome，并只允许以下两种盈利机制；训练目标、`sim-system`、paper 报告、live 审计和模型评估不得引入第三种隐含交易机制。
  2. **互补 outcome 折价配对。** 只有同一已确认配对数量 `paired_qty = min(confirmed_outcome_up_qty, confirmed_outcome_down_qty)` 对应的 `outcome_up` 与 `outcome_down` 单位 all-in acquisition cost 之和小于 `1 pUSD` 时，才能把该部分记为配对机制，并以完整互补持仓的 `1 pUSD` 结算价值与实际总成本之间的差额获利。是否满足 `< 1` 必须按真实或 `sim-system` 的 confirmed fill、实际 fee 和已入账 rebate 计算；未配对数量、partial fill、未成交第二腿和相关资本占用必须另记为 incomplete/directional exposure 与风险，不能计入已闭合配对收益。静态盘口、中间价或下单意图不得冒充已实现配对收益。
  3. **结果趋明后的正确侧持有。** 当市场结果随因果可见信息逐渐明朗时，模型可以增加 BUY 其当时判断为正确的 outcome，并持有至最终结算获利。训练和评估只能使用决策时已可见信息，不得用最终结果倒灌决定“正确侧”；该机制必须完整计入判断错误、成交成本、资金占用、未成交和最终结算风险。
  4. 卖出 `outcome_up` 所表达的反向方向由 BUY `outcome_down` 表达，反之亦然；这是当前 BUY-only 策略对方向暴露的实现约束，不得把互补 BUY 记成 SELL、减仓、释放资金或提前实现收益。
  5. fee、已入账 rebate、现金占用和结算差异是上述两种盈利机制的经济或会计组成，不得被描述为独立第三种盈利策略。reward 只是从已闭合 ledger economics 派生的训练/评估信号，不是现金流、收入或第三种盈利机制。两种机制可以在同一账户轨迹中并存；归因必须分别报告并显式保留无法唯一拆分的 interaction/residual，全部归因合计必须与账户级 ledger、NAV 和 settled PnL 闭合。


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
  11. 数据采集与处理必须作为训练、paper trade、live trade 的共用合同：`data-collect` 提供外部市场源 typed raw / coverage / manifest；paper/live 实时消费 Market Context Builder 生成的市场上下文和模型输入；训练离线消费 `data-pipeline` 生成的训练可用数据集、校准数据池和 replay context。两者可以是不同运行实例，但必须共享 schema、manifest、时间线、causal cutoff、feature/input contract、事件触发语义和可回放语义；`tick_stream` 等固定间隔派生产物不得决定模型决策频率或替代真实 trigger event。
  12. 时间戳必须命名空间隔离：`source_ts` 是外部源时间；训练 typed raw 中的 `timestamp_received` 表达采集进程接收时间，并与 collector host/region 元数据共同确定 provenance；paper/live handoff 中对应字段为 `collector_recv_time_utc_ms`，进入 paper/live runtime 后必须另记该运行主机的 `runtime_receive_time_utc_ms`；`decision_ts` / `causal_cutoff_ts` 是模型可见边界。任何跨 schema 映射必须显式记录 source field、target field、host、region 和 clock evidence status，不得把上述时间戳互相覆盖。live/paper 实测延迟标签只能来自对应运行主机 artifact，不能用采集主机接收时间冒充。下文冻结的爱尔兰双向延迟 profile 是 `sim-system` 的项目代理假设，不是真实 live/paper 出站延迟标签。
  13. 训练与 paper 无论实际运行在哪台机器，通过 `sim-system` 模拟的订单请求和平台反馈都必须使用同一版本化爱尔兰延迟 profile；训练机或 paper 运行机当时的本地 RPC 延迟不得替代该 profile。订单/取消请求的 client-to-platform leg、平台反馈的 platform-to-client leg 和平台内部 accepted/matched/response-emission 时间必须分别记录，不得把一个延迟样本同时记作两段、RTT 或撮合延迟。历史训练行情已经以 `timestamp_received` 表达采集端可见时间时，不得再次叠加同一入站延迟。

  ## 五、订单微结构与毫秒级竞态要求

  订单微结构必须作为 P0 需求，不能只写成实现细节。

  1. 单笔与批量 submit response envelope：
     - 按当前官方 [Post a new order](https://docs.polymarket.com/api-reference/trade/post-a-new-order) 和 [Post multiple orders](https://docs.polymarket.com/api-reference/trade/post-multiple-orders)，`POST /order` 接收一笔并返回一个 response object；`POST /orders` 接收最多 `15` 笔的 array，批内订单并行处理，一次 HTTP response 返回一个 response array。单笔或批量形式由 Intent Bridge 实际调用的官方 SDK submit 路径决定，不是平台随机选择的结果。
     - [pinned 官方 Rust SDK](https://github.com/Polymarket/rs-clob-client-v2/blob/3ae1aae5e9ded38f984464c9fc0f307f8a9f41fb/src/clob/client.rs#L1839-L1885) 的 `post_order` 返回一个 `PostOrderResponse`，`post_orders` 返回 `Vec<PostOrderResponse>`；批量 HTTP body 完整接收并解析后才作为一个 envelope 对客户端可见，不得把同一 batch 的 item 模拟成多次独立 HTTP response 到达。多次独立 single submit 各自拥有独立 envelope 与 transport leg，可以乱序、迟到或缺失；批内每笔订单的平台处理、订单状态和 user-channel 事件仍各自独立，可以先于或后于整个 batch response envelope 可见。
     - SDK/HTTP transport 成功不等于订单 accepted，batch envelope 成功也不等于每个 item 成功。按官方 [CLOB error codes](https://docs.polymarket.com/resources/error-codes)，HTTP 200 的同一 response array 可以同时包含 live、matched 和逐项失败；request-level transport/non-2xx/解析失败与 HTTP 200 内的 per-item 结果必须分开建模，batch 不得按原子全成或全败处理。
     - 本项目真实资金 artifact 已记录[单笔 submit 产生一个 Post observation](/home/chillchill/sdk-live-trade/poly-arb-ai/rust/sdk-live-trade/artifacts/live-validation/pullback/platform-sellable-maturity-live-20260626T133250Z/platform_sellable_maturity_buy_submit_attempts.stream.jsonl:1)，也记录[一个 4-item batch 共享同一 batch id、raw response hash 和 actor completion/timestamp record，其中 1 笔 accepted、3 笔 rejected](/home/chillchill/sdk-live-trade/poly-arb-ai/rust/sdk-live-trade/artifacts/remote-r5-runs/r5-three-market-live-funds-20260621T131804Z/trading-runtime/orders.jsonl:70)。这些 artifact 未保留完整 HTTP wire body，只证明当时 SDK/Intent Bridge 实际观察到的返回形式和 mixed outcome 分支，不用于冻结频率或响应数组顺序保证。
     - 对由 create-order/submit response 推导的 accepted 路径，single object 或 batch item 只有同时满足非空 platform order id、状态非 UNKNOWN、无拒单/错误文本、scope/decision_id/local intent 可安全对应、数量可解释，才允许该 response 进入 accepted hot path；不得只看 `success` 字段。
     - 官方文档只声明批量请求中每笔订单有一个 response，未明确公开承诺 response array 与 request array 的索引顺序保持不变。正常路径只能在版本化的本项目 batch 关联合同下按 slot 对应 request item 与 response item：必须同时校验 request/response count、保留有序 signed request、batch/envelope id、request/response index、local intent id、signed-order digest/预计 order hash、raw request/response digest 和 envelope receive time，且不存在其他 mapping anomaly。该 slot 对应是项目关联假设，不是平台公开顺序保证，也不能单独作为 accepted 或 platform order identity 证明；关联合同无法满足时，对应 item 必须保持 `UnknownSubmitOutcome` 并等待 user-channel 或 reconciliation evidence，不得猜测映射。[SDK issue #30 的生产报告](https://github.com/Polymarket/rs-clob-client-v2/issues/30) 只作为“同批一项空 order id/status、另一项 matched”的社区异常场景证据，不是平台官方保证或发生率证据。
     - 能够安全关联到具体 request item 的明确 per-item rejection/error evidence 必须只把该 item 归为 rejected，不得污染 sibling item；明确且可验证为撮合前整体拒绝的 request-level validation error 才可把对应已发送项归为 rejected。blank order id + UNKNOWN/empty status 且无决定性 error、count mismatch、无法关联的 response item，以及整个 response envelope 超时、丢失、截断或解析失败，都不得当作 accepted 或 rejected；已发送项必须进入 `UnknownSubmitOutcome` 或等价 provisional 状态，不能盲目重提、不能伪造 platform order id。mixed decision timestamp 或超出当前 SDK/platform batch limit 必须在创建 reserve 和发送前由 batch preflight 明确拒绝。
     - 未通过 response accepted classifier 的订单不得仅凭该 response 进入 open order 权威状态或应用 fill ledger delta。已安全确认 rejected 的 item 按拒单合同释放 pending-submit reserve；仍为 `UnknownSubmitOutcome` 的已发送项继续占用 pending-submit reserve，直到能够与 local intent / client id / platform order id 安全关联的权威 user-channel order/fill/reconciliation evidence 将其解析为 accepted、rejected、partial-fill、filled 或 terminal，并仍须经过 NormalizedOrderEvent 与 ledger。

  2. 订单类型、流动性角色与费用：
     - BUY-only submit 必须明确选择 `FOK`、`FAK`、`GTC` 或 `GTD`。按 [Polymarket Orders Overview](https://docs.polymarket.com/trading/orders/overview)，`FOK/FAK` 是立即吃取对侧 resting liquidity 的 market-order types；FOK 只在模型给定的 worst-price/slippage bound 内能够全部成交时成交，否则整单零成交并取消，FAK 只成交该 bound 内当前可得数量并取消未成交余量。`GTC/GTD` 是带价格边界的 limit-order types，GTC 持续有效直至成交或取消，GTD 还可由平台在指定时间到期。
     - 订单金额域统一使用 Polymarket CLOB V2 的 `pUSD`，价格单位统一为 `pUSD/share`，份额数量使用 `shares`；依据当前官方 [pUSD 说明](https://docs.polymarket.com/concepts/pusd)，不得在业务 schema、模型 intent、ledger 或报告中继续使用含义不清的裸 `USD`、`USDC` 或以 quote/base 暗示资金单位的字段。pUSD 使用 6 位小数，但具体整数编码、舍入和最小单位必须委托 pinned 官方 Rust SDK，并由 parity 测试冻结，需求层不得另写手工换算旁路。
     - GTC/GTD limit BUY 的业务输入必须是 `limit_price_pusd_per_share + size_shares`，其最大本金占用 `max_principal_pusd = limit_price_pusd_per_share * size_shares`；价格改善后的实际成交金额只能来自权威 fill evidence，不能把最大本金冒充实际花费。marketable GTC/GTD 仍是限价单，不得因为立即成交就改写成市价单。
     - FOK/FAK market BUY 的业务输入必须是 `spend_pusd + worst_price_pusd_per_share`：模型决定愿意花费的 pUSD 金额和最差成交价保护，Intent Bridge 必须把这两项交给 pinned 官方 Rust SDK market-order builder，由 SDK 生成 signed maker/taker amounts 与隐含 shares，不得在领域层手工复刻编码、截断或舍入；实际成交份额、成交金额和未花费金额只由平台或 `sim-system` 权威 evidence 确定。虽然 pinned SDK 的兼容 API 符号仍可能写作 `Amount::usdc(spend_pusd)`，CLOB V2 中该值的业务语义必须记录为 pUSD，且该兼容符号只能存在于 SDK adapter 内；不得向模型或领域合同暴露 `Amount::shares` 作为 market BUY 的另一套数量口径。
     - Intent Bridge 对两类订单都必须记录订单类型、模型原始输入、SDK 编码输入、requested/accepted shares、signed maker/taker amounts、最大本金或 spend、fee、精度转换及 raw digest。队列与份额守恒统一使用 shares；pUSD 现金守恒独立计算，二者不得相加或共用同一 `size` 字段。
     - maker/taker 是每一段实际成交的流动性角色，不是订单类型的永久标签。订单入簿后被其他订单成交的数量为 maker；立即消耗 resting liquidity 的数量为 taker。marketable 的 GTC/GTD 不改变其 order type，立即成交部分按 taker 处理，未成交余量再按 GTC/GTD lifecycle 继续开放或到期；同一订单因此可以先产生 taker fill、再产生 maker fill。
     - post-only 只允许与 GTC/GTD 组合；会立即跨价成交时必须拒单，与 FOK/FAK 组合也必须拒单。只有通过 post-only accepted 并实际 resting 的订单才可在提交时保证不成为 taker。
     - fee、rebate 和 cost 必须使用撮合时该 market 生效的 fee metadata 与实际 maker/taker role 计算。按当前 [Polymarket Fees](https://docs.polymarket.com/trading/fees)，maker 平台费率为零，只有启用 fee 的市场才对 taker 成交收费；不得仅凭 order type 推导固定手续费，也不得把尚未入账的 maker rebate 当作该 fill 的即时负手续费。
     - 平台对 marketable order 返回的 delayed/unmatched 等状态及相应不可取消窗口必须按当前 market metadata、官方规则版本和真实 artifact 模拟；它们属于平台订单生命周期，不是程序自行生成的撤单决策。

  3. venue dust 与平台订单终态：
     - `sim-system`、NOE 和 ledger 必须分别记录 platform order status、order lifecycle state、actual cumulative executed base quantity、trade settlement state 和客户端可见状态。[真实资金 artifact](/home/chillchill/sdk-live-trade/poly-arb-ai/rust/sdk-live-trade/artifacts/live-validation/pullback/platform-sellable-maturity-live-20260626T133250Z/platform_sellable_maturity_samples.jsonl:18) 已证明：原始 `5` shares 的 BUY order 可以在 `size_matched=4.999165` 时由官方 SDK 查询为 `MATCHED`，cancel 返回 exact matched-cancel，且该 asset 的 open-order count 为 `0`；因此平台订单可以已经 terminal，而 `executed_base_qty` 仍小于 `accepted_base_qty`。该 sample 因 fill size 低于 probe 配置而标记 `valid=false`，只能证明此平台生命周期分支存在，不能用于估计发生频率或冻结统一 threshold。
     - `venue_dust_closed_base_qty` 只能在 platform status/terminal cause、精度或舍入证据、actual executed quantity、open-order 状态和版本化 artifact 共同支持 venue dust 时使用。FAK kill、模型 cancel、GTD expiry、平台市场关闭和 submit rejection 必须分别进入 canceled、expired、market-closed 或 rejected 语义，不能归入 dust。官方 [Get single order](https://docs.polymarket.com/api-reference/trade/get-single-order-by-id) 将 status、original size 与 size matched 分开返回，当前官方资料没有发布全局统一 dust threshold；成熟社区 [NautilusTrader Polymarket adapter](https://github.com/nautechsystems/nautilus_trader/blob/develop/docs/integrations/polymarket.md) 也把 rounding tolerance 明确作为 adapter normalization policy。dust tolerance 必须按协议版本、order type、数量/价格精度和本项目真实 artifact 校准并版本化，不得写成 Global 官方常量。
     - 权威 match/fill evidence 到达客户端时，ledger 必须按 actual executed quantity 一次性减少 `cash_balance_pusd`、等额消耗该订单当时所在的 pending-submit/open-order/cancel-reconciliation reserve、增加 provisional shares/cost basis，并把该 fill 标记为 matched-pending settlement exposure；matched-pending 只表达结算风险，不再从 `available_cash_pusd` 重复扣减。`CONFIRMED` 只把 provisional position 成熟为 confirmed，不重复应用现金或份额；`RETRYING` 保持 pending；`FAILED` 必须按权威失败证据精确冲回该 provisional cash/share/fee delta。venue dust terminal 本身不生成额外 fill、share 或交易现金流，只按 terminal cause 释放经权威数量确认的未花费 reserve。
     - venue dust 必须作为常规、可重复验收的订单生命周期分支，覆盖平台 `MATCHED` 但 `executed_base_qty < accepted_base_qty`、exact matched-cancel、late user-channel、重复 evidence、结算成功/失败和 reserve 释放；同一经济变化只能 ledger apply 一次。

  4. 模型撤单与毫秒级竞态：
     - 策略性撤单只能来自模型输出的 `cancel_order(order_ref)` 或 `cancel_all_open_orders(scope)`。程序不得按订单提交后的固定经过时长、market switch 或固定清理规则自行生成撤单决策；FOK/FAK 的 kill、GTD 到期、平台市场关闭和用户/操作员触发的紧急 cancel-all 是独立的订单或运行安全生命周期，其中 operator cancel 不得冒充模型策略或训练标签。
     - 时间合同必须为 `decision_ts`、`model_cancel_intent_ts`、`cancel_enqueue_ts`、`cancel_send_ts`、`cancel_exchange_arrival_ts`、`platform_match_ts`、`platform_cancel_effective_ts`、`cancel_response_emit_ts` / `cancel_response_receive_ts`、`user_channel_event_emit_ts` / `user_channel_event_receive_ts` 和 `reconciliation_ts` 提供独立 schema slot。每个非空时间戳必须同时记录 `timestamp_authority=local_measured|platform_reported|simulated`、`clock_domain`、`host_id`、`region`、来源字段/通道以及 clock-sync evidence status；模拟时间还必须记录 profile/scenario/seed。live 只有在官方 SDK/channel raw evidence 明确提供时才能写 platform-reported 的平台内部时间，不可观测 slot 必须为 null 并记录 unavailable reason，不能用客户端 send/receive time 倒推。不得把不同主机、平台真实顺序与客户端接收顺序合并成一个“取消时间”。
     - submit 与 cancel request leg 独立采样且按平台 arrival time 排序；cancel 可能先于原 submit 到达平台。此时平台返回的 not-found/unresolved 不能追溯取消随后才 accepted 的订单，原 submit 仍可进入 accepted/resting/matched lifecycle；模型在新证据可见后才能决定是否再次取消，程序不得复用旧 cancel intent 自动关闭该订单。
     - 模型只依据当时客户端可见的 `open_order_list` 与具有合法 cancel target/scope 的 `pending_submit_list` 决策。若一笔 accepted original size 为 `5 shares` 的订单已有 `3 shares` 在平台成交、但 fill evidence 尚未到达客户端，则模型仍会针对本地显示 remaining size 为 `5 shares` 的整笔订单发出 cancel intent；官方 cancel 路径按 order ID/scope 操作，并不由模型提交一个已知平台 remainder 数量。cancel 到达平台时，`sim-system` 必须只取消平台真实未成交的 `2 shares`。普通 cancel response 若只确认 order ID 已取消而没有返回 effective canceled quantity，则客户端必须进入 `cancel_effect_qty_pending_reconciliation`：先移出 open order，并把尚无法分解的 `open_order_reserved_pusd` 原额转入 `cancel_reconciliation_reserved_pusd`，既不能向模型泄露平台真实的 `2 shares`，也不能按本地可见的 `5 shares` 释放 reserve。迟到的 `3 shares` fill 或其他权威 cumulative evidence 到达后，才确定 `executed=3、canceled=2、open=0`，应用 fill、消耗对应 reserve 并准确释放未花费 reserve。
     - 若 cancel 到达前平台已经把全部数量 matched，必须返回 matched-cancel；若平台已经以 venue dust 关闭订单，则返回相应 terminal/matched-cancel evidence 并保留真实 executed quantity 与 dust quantity。cancel response 不得反向制造 fill。
     - 订单终态必须按平台因果顺序、单调 cumulative fill 和实际 cancel effect 共同仲裁，不得按客户端最先收到的 terminal message 简化裁决。先收到 cancel ack 后仍可接收并应用平台更早发生、但客户端更晚看到的 fill delta；重复或已覆盖 evidence 只能 alias/audit，不得重复入账、重复释放 reserve 或重新打开订单。

  5. cancel response 语义：
     - generic not-found、not_canceled、absent target 不能直接当作成交证明。
     - 本项目真实资金 artifact 中出现的精确 `matched orders can't be canceled` 是经 artifact 校准的“平台已无可取消 remainder”证据语义，不得扩写为 Global 官方公开保证。它可以关闭 cancel lifecycle，但经济成交数量只能来自先前或随后到达的权威 platform cumulative fill/trade evidence；若 cumulative quantity 仍不完整，对应 reserve 必须转入 `cancel_reconciliation_reserved_pusd` 并保持不可用，直至 fill/dust/reconciliation evidence 完成数量分解，不能由错误字符串单独制造成交或释放资金。
     - plain not-found 应作为 order-local retryable warning 或 unresolved evidence，等待其他权威来源关闭。
     - cancel 网络入口必须收敛到官方 SDK 的 single-order、multi-order 或 cancel-all 路径，不允许不同入口各自解释 terminal/fill/cancel。
     - 模型可见的 cancel order 生命周期状态必须覆盖 cancel requested/enqueued/pending、cancel accepted/ack、cancel rejected、retryable warning、retry scheduled、retry budget exhausted、transport/timeout、matched-cancel、overlapped terminal 和 final terminal；transport retry 只能幂等重试同一个模型 cancel intent，不得生成新的策略性撤单。这些状态只能由 NormalizedOrderEvent 与 ledger/projection 派生，不得由 live-trade 生成替代撤单策略。

  6. NormalizedOrderEvent：
     - 所有 SDK submit、`post_order` response object、`post_orders` response envelope/item、cancel、cancel batch、user-channel、fill/partial-fill、transport/observation timeout、平台到期/市场关闭/结算证据、operator emergency cancel 和 reconciliation evidence 都必须先进入 NormalizedOrderEvent 或等价权威事件层。不得用未定义来源、未携带 authority/provenance 的本地伪造关闭事件直接关闭订单或修改 ledger。
     - single/batch 归一化前必须保留平台原始 envelope 形状、raw digest 和 receive time。batch 内每个 item 分别生成可仲裁的 NormalizedOrderEvent，但必须共享同一 response_envelope_id/digest/receive time 并保留 request/response index 与关联证据；不得把归一化后的多个 item 伪装成多次平台 HTTP 返回。
     - NormalizedOrderEvent 是订单反馈归一化事实层，也是 ledger、模型可见投影、训练标签和审计的共同输入；它不是 live-trade 生成新策略或风控决策的入口。
     - NormalizedOrderEvent 的业务归一化必须由程序化、版本化、可回放、可测试的合同完成；不得把 accepted classifier、terminal arbitration、matched-cancel/dust/late evidence 归一化、ledger_apply_policy 或 validator 事实一致性检查完全交给模型训练自行学习。
     - `sim-system` raw evidence 必须通过同一套归一化合同后只进入训练标签、训练回放、paper ledger 与 paper audit；真实平台 raw evidence 必须通过该合同后进入 live ledger 与 live audit，并且只能经 `feedback-ingestion` 离线复制到训练反馈和 `sim-system` 校准链路。`sim-system` 证据不得进入 live ledger、live audit 或 live 状态，live runtime 也不得调用 `sim-system`。不同运行模式不能拥有各自的影子订单解释器。
     - validator 只能验证 normalized rows、raw digest、reference integrity 和 artifact consistency，不能成为第二套业务解释器。

  ## 六、账户、订单与热路径 ledger 权威

  1. training replay、paper、live 每个运行实例都必须拥有彼此隔离的本地 append-only ledger；共享的是版本化 schema、NOE 和 apply policy，不是状态实例。该实例的 ledger 是其账户、两个 outcome 持仓、BUY 挂单、pUSD 现金、NAV 和 active resting 状态唯一热路径权威。订单生命周期 evidence 必须严格路由为 `sim-system raw evidence -> training/paper NOE` 或 `真实平台 raw evidence -> live NOE`；除此之外，ledger 只能消费本实例的 local provisional intent 和第 2 条定义的版本化非订单经济/控制事件。任何实例都不得直接消费另一实例的 ledger 或 raw evidence。
  2. ledger 的全部经济变化必须来自带 authority、幂等 key、causal time 和 provenance 的版本化事件：启动 bootstrap、外部充值/提现、submit/cancel/fill/partial-fill NOE、fee/rebate、settlement/redemption 以及明确的 reconciliation event。market channel/orderbook 可以提供估值、market lifecycle 或 resolution evidence，但不能自行创造账户订单、成交、持仓或现金变化；raw snapshot 也不得直接覆盖 projection。
  3. 账户资金字段必须统一为 `cash_balance_pusd`、`pending_submit_reserved_pusd`、`open_order_reserved_pusd`、`cancel_reconciliation_reserved_pusd`、`total_reserved_pusd = pending_submit_reserved_pusd + open_order_reserved_pusd + cancel_reconciliation_reserved_pusd` 和 `available_cash_pusd = cash_balance_pusd - total_reserved_pusd`；本文模型输入中的 CASH 专指决策时刻由本地 ledger 派生的 `available_cash_pusd`。模型 BUY intent 只有通过 action-surface、订单参数、资金和协议 preflight，并在进入 submit queue 前，才依据 limit BUY 的 `max_principal_pusd` 或 market BUY 的 `spend_pusd`、当前 fee metadata 与 pinned SDK fee 语义计算并创建 `pending_submit_reserved_pusd`；reserve artifact 必须明确 fee 是否已包含，禁止重复加 fee。accepted 后转入 open-order reserve，immediate fill 则直接消耗对应 reserve；明确且可安全关联的 rejection/确定未接受释放一次，`UnknownSubmitOutcome` 持续保留。无 effective quantity 的 cancel ack 把 reserve 转入 cancel-reconciliation bucket，待权威累计成交证据完成分解。权威 fill 入账时同时减少 `cash_balance_pusd` 与对应 reserve；matched-pending settlement exposure 另记且不得从 available cash 重复扣减。取消、到期或 venue dust terminal 只释放经权威数量确认的未花费 reserve。
  4. 本地 preflight 拒绝发生在 submit queue 与 pending-submit reserve 创建之前，不产生平台订单、pending-submit item 或持久 reserve。已经发送到平台的 item 只有在 rejection evidence 能安全关联到该 item 时，才以 `accepted_base_qty=0` 关闭 submit lifecycle 并释放其 pending-submit reserve 一次；无法安全关联或 transport outcome 不确定时必须保持 `UnknownSubmitOutcome` 和 reserve，不得因“看起来像拒单”提前释放。
  5. 模型输入、artifact 和报表不得使用 wallet allowance 表达任何 pUSD cash 字段；Polymarket/SDK balance/allowance/approval 只能作为启动 bootstrap、权限/协议 preflight 或显式 reconciliation evidence，不是热路径第二账本，也不是 pUSD CASH、NAV 或 PnL 的替代来源。
  6. live 启动 baseline 必须由同一 causal cutoff 下可核验且完整的 SDK/account evidence 建立，至少覆盖 pUSD balance、所有相关 outcome positions、open/pending orders、未成熟成交/结算和外部资金流游标；证据不完整时不得宣称 baseline 完成或据此计算累计 PnL。启动后的充值与提现必须分别进入版本化 `external_fund_flow` ledger event，定义 `external_net_flow_pusd = deposits_pusd - withdrawals_pusd`。
  7. PnL 不是独立账本权威状态；未带限定词时，`cumulative_pnl_pusd = current_nav_pusd - startup_baseline_nav_pusd - external_net_flow_pusd`，表示程序启动后跨市场累计 PnL。只有 baseline 已证明仅含 pUSD CASH、无历史持仓/挂单/未成熟结算且期间无外部资金流时，才可简化为 `current_nav_pusd - startup_cash_pusd`。per-order/per-market PnL 只能作为可闭合的审计切片，不能替代累计 PnL。
  8. NAV 必须按版本化 `nav_valuation_contract=conservative_bid_depth_v1` 在决策时刻从同一 ledger projection 计算：`cash_balance_pusd` 只计一次，reserve 只是现金可用性分桶，不得再次从 NAV 扣除；provisional/confirmed positions 按 ledger 数量只计一次并显式标记 settlement maturity。市场权威 resolution evidence 可见后，按可赎回价值估值；此前按 causal cutoff 下新鲜、无 gap 的可成交 bid depth 做保守 liquidation valuation，深度不足的剩余份额估值为 `0 pUSD` 并记录 `unpriced_qty`。必要 orderbook 缺失、过期或 gap 未闭合时，NAV/PnL 必须标记 unavailable，不得用 midpoint、last price 或未来盘口补值。每个 NAV 必须可由 ledger sequence、valuation time、book digest、freshness 和 depth coverage 重放。
  9. 真实平台 balance/position/open-orders/trades 查询只允许用于启动 bootstrap、通道 gap/重连恢复、明确的 operator reconciliation 或定期审计，不得作为模型决策的 polling trigger，也不得直接生成、覆盖或否定热路径 ledger。查询结果必须先保留 raw evidence，再通过版本化 reconciliation/NOE 产生 ledger delta；订单未出现在一次 open-orders snapshot 中本身不是 fill 或 terminal 证明。
  10. 当前 action surface 不存在 SELL；任何自有非 BUY submit 都是模型 bundle、intent schema 或执行桥合同违规，必须在平台提交前拒绝并写入 audit，不得作为可重试交易、训练动作或 `sim-system` 候选。
  11. credential、scope、schema、wallet/funder、关键 append-only audit writer、模型 bundle 合同属于真实下单硬边界；这些边界缺失、不匹配或不可验证时，live-trade 必须将 `execution_permission` 设为 `NO_SUBMIT_PERMISSION` 或更严格权限，并记录原因。late user-channel、generic not-found 等证据应按具体 scope 进入 warning/quarantine/reconciliation，不得一律 hard fail，也不得由此生成非 BUY 动作或第二套订单解释路径。
  12. ledger 必须可回放、可重建，并能从回放结果复算 pUSD CASH、NAV 和累计 PnL；run manifest 必须记录 bootstrap evidence/digest、外部资金流游标、ledger/valuation contract version 和数据范围。ledger 派生状态可以作为模型输入与审计证据；硬边界只能设置 `execution_permission=ALLOW_SUBMIT|NO_SUBMIT_PERMISSION|CANCEL_ONLY_PERMISSION|PAUSED_PERMISSION`，不得作为模型输入/反馈，也不得把模型 intent 改写成策略性不下单、撤单或另一笔下单动作。

  ## 七、训练、paper trade 与 live trade 的真实交易反馈

  1. `sim-system` 是训练模块与 paper trade 共同调用的订单执行反馈模拟模块。训练业务和 paper trade 业务产生 BUY-only 模拟订单后，必须提交给 `sim-system`；`sim-system` 模拟订单到达平台后的 accepted/rejected、排队、成交、部分成交、不成交、取消竞态、延迟和结算反馈，再输出模拟 raw evidence。训练与 paper 不得各自实现另一套 simulator 或 fill 解释器。
  2. 模型训练和 paper trade 必须以真实平台回流的订单生命周期、成交/撤单/拒单反馈、ledger 结果和由 NAV 派生、可回放的程序启动后跨市场累计 PnL 校准并验收 `sim-system`，不能只优化离线 leaderboard。`sim-system` 规则必须从 sdk-live-trade、online-feedback、当前官方规则证据、用户冻结的项目规则假设、issues 和真实交易 artifact 中反推并版本化。
  3. 训练标签必须来自 `sim-system` raw evidence 经 train/paper NOE/ledger 归一化后的结果，或来自真实平台 NOE/live ledger 结果经 `feedback-ingestion` 离线复制并审查后的训练反馈；不得直接读取真实平台 raw response，也不得手写冷路径状态标签。每条订单事件至少记录 source_kind、transition、normalization_reason、ledger_apply_policy、cancel_outcome、error_category、order_id/local_intent_id、source_ts/receive_ts、order_type、limit_price_pusd_per_share、size_shares、spend_pusd、worst_price_pusd_per_share、max_principal_pusd、requested_base_qty、accepted_base_qty、client_visible_cumulative_executed_base_qty、client_visible_remaining_base_qty、platform_cumulative_executed_base_qty、cancel_effect_base_qty/state、venue_dust_closed_base_qty、canceled_base_qty、expired_base_qty、market_closed_base_qty、open_base_qty、ledger_delta_fill_base_qty、actual_execution_notional_pusd、fee_pusd、execution_class、lifecycle_state、terminal_reason 和 trade settlement state；不适用于该 order type 的另一套输入字段必须为空并带明确 not-applicable 语义。平台真实数量只能留在 `sim-system` truth/audit 与成熟标签中，不能在相应 evidence 到达前泄露给模型。market switch、ledger open-order state、user-channel freshness 只能作为上下文或审计元数据，不得替代订单结果标签。
  4. 每笔模拟 BUY order 必须先由 `sim-system` 按 exchange arrival、engine acceptance 和撮合顺序生成独立于客户端可见性的模拟平台真实状态，再分别生成 create-order/submit response 与 user-channel order/trade evidence 的 emission time、receive time、迟到或缺失/截尾结果。`sim-system` 必须保留 Intent Bridge 选择的 `single|batch` submit mode：single 生成一个 response object，batch 生成一个含逐项 mixed outcome 的 response array envelope，整个 envelope 只有一组 emit/receive/缺失结果；批内每笔订单的平台处理和 user-channel 事件仍独立调度。accepted resting order 的平台侧成交时间、完整成交或部分成交必须先于相应可见证据确定；这些时间必须能导致 full fill、partial fill、cancel complete、matched-cancel、late fill、venue dust close 等生命周期结果，而不是只按理想成交价记账或把本地尚未收到回报误写成平台尚未 accepted/matched。
  5. **FIFO 项目规则假设。** 本项目按用户在 `2026-07-12` 明确确认并冻结的项目规则假设，视目标 Polymarket Global 与 Polymarket US 在订单排序上都采用价格优先、同价按进入撮合系统的时间先后执行的 FIFO/price-time priority。`sim-system` 必须模拟该规则。该结论是用户冻结的项目规则假设，不得写成 Global 官方公开事实；[Polymarket US `2026-07-02` Rulebook §5.4](https://polymarketexchange.com/files/legal/Polymarket%20US%20Rulebook%20%282026.07.02%29.pdf) 与 [US FIX Order Entry](https://docs.polymarket.us/institutional/fix-api/fix-order-entry-overview) 只构成 US 官方证据。规则假设的 authority、版本、访问日和适用 market 必须进入 online_rule_version、run manifest 与 `sim-system` artifact，并在 paper/live 前重新核验。
  6. **Global 队列可见性边界。** 截至 `2026-07-12`，本轮核验的 [Global Orders](https://docs.polymarket.com/trading/orders/overview)、[Global Orderbook](https://docs.polymarket.com/trading/orderbook) 公开接口及官方 Rust SDK `main` commit `3ae1aae5e9ded38f984464c9fc0f307f8a9f41fb` 不提供 open order 的 queue_position、rank、quantity_ahead 或其他真实全局队列位置字段；聚合 L2 也不能唯一分解同价档内的新增、取消和成交。因此 `sim-system` 不得声称恢复我方订单的真实 FIFO 名次。该边界只针对 Global 当前公开接口；Polymarket US institutional FIX 的 MBO 能力是独立证据面，不能冒充 Global 可见性。
  7. **FIFO 同价队尾入队。** 对不会立即成交、被撮合引擎接受为 resting 的 BUY order，`sim-system` 必须在 engine acceptance time 把它追加到该价格档所有更早被引擎接受且尚未消耗的同价数量之后，初始 `quantity_ahead_at_acceptance` 等于这些前方数量之和，不得把我方订单随机插入任何更早位置。例如撮合引擎接受我方订单时该档已有 `100 size`，我方新增 `5 size`，则初始前方量为 `100`，我方数量对应累计数量区间 `(100, 105]`；这表示数量位置，不表示五笔独立订单排名。marketable BUY 必须走相应 taker/marketable lifecycle，包括适用的撮合延迟、FOK/FAK 行为或 GTC/GTD 未成交余量处理，不能套用 resting queue。
  8. **队列不确定性边界。** 本地看到的 L2 price-level size 只是 engine acceptance 前的聚合证据，不必然等于撮合引擎接受我方订单时的真实前方量。`sim-system` 必须按因果顺序处理 snapshot、client send、exchange arrival/acceptance 之间可见的同价新增、成交和取消：在我方之前被接受的同价新增量进入前方量，接受前已成交或已取消的数量不再排在前方。随机或情景假设只能用于无法从公开 L2 唯一识别的并发到达、网络/撮合延迟、gap/reconnect，以及我方入队后同价档取消发生在前方还是后方等归因；不得改变 FIFO 队尾入队规则，也不得把已知更早数量随机移到我方之后。
  9. 队列状态必须影响 time-to-first-fill、actual executed shares、quantity ahead、取消竞态和未成交结果，并输出 engine-acceptance 时间、证据边界、assumption/scenario id 及不确定性标签。marketable/taker leg 必须先在 price/worst-price bound 内按对侧价格优先、同价 FIFO 消耗唯一且不可重复使用的 eligible liquidity budget；只有随后被接受为 resting 的 GTC/GTD residual leg 才建立我方 `quantity_ahead`，并按第 7、8 条 FIFO 消耗前方量。每笔 engine-accepted order 的 shares 必须始终满足：`accepted_base_qty = executed_base_qty + venue_dust_closed_base_qty + canceled_base_qty + expired_base_qty + market_closed_base_qty + open_base_qty`，上述 `*_base_qty` 字段的单位均明确为 shares，各项不得为负且同一数量只能归入一个 bucket；submit rejection 的 `accepted_base_qty=0`，不进入平台订单数量守恒域、队列或 open-order reserve。若 item 已经发送，其 pending-submit reserve 仍必须按第六节第 4 条由安全关联的 rejection evidence 释放一次；本地 preflight rejection 则从未创建 reserve。`spend_pusd`、`max_principal_pusd`、actual execution notional、fee、open reserve 与 released reserve 必须在独立 pUSD 现金守恒中闭合，不能与 shares 相加。
  10. execution 与 lifecycle 必须分轴记录：`execution_class` 至少区分 no-fill、partial、full，`lifecycle_state/terminal_reason` 至少区分 open、fully-matched、canceled、expired、market-closed 和 venue-dust-terminal。FAK 可以是 partial+canceled，GTC/GTD 可以是 partial+open、partial+canceled 或 partial+expired，数值上 partial 的订单也可以因 venue dust 成为 terminal；FOK 则必须在 price bound 内 full，或零成交结束。模型 cancel、order type、平台到期、市场关闭和 venue dust 必须按各自 terminal cause 分桶。
  11. matched-cancel 必须由 `sim-system` 的平台状态机与事件时间线生成。模型只能基于当时已经可见的 create-order/submit response、user-channel、NOE、ledger、`open_order_list` 和具有合法 target/scope 的 `pending_submit_list` 决定取消；cancel intent 经过 client-to-platform 延迟到达模拟平台后，再按该时刻的平台真实 order status、open base quantity 和 terminal cause 判定。平台因 fully-matched 或 venue-dust-terminal 而无可取消 open remainder 时，`sim-system` 必须生成与真实资金 artifact 一致的 exact `matched orders can't be canceled` 模拟 raw evidence；FAK kill、GTD expiry、先前 cancel 或其他 terminal cause 必须生成各自对应的 canceled/expired/not-found evidence。generic `order can't be found - already canceled or matched` 不得替代 exact matched 证据。cancel response 只能描述平台实际 cancel effect，不能制造成交或覆盖随后到达的既有 fill。live trade 不使用 `sim-system`，真实平台 matched-cancel 只能来自 SDK cancel response 或 user-channel/SDK 可验证证据。
  12. `sim-system` 必须把 venue dust 作为平台可终止订单的正常分支：以真实平台 order status、actual cumulative executed base quantity、精度/舍入证据、terminal cause 和 open-order 状态共同生成 `venue_dust_closed_base_qty`，并按第 9 条数量守恒关闭订单。actual fill、dust terminal、reserve 释放和 trade settlement 必须生成彼此独立但可关联的 raw evidence 与 ledger policy。BUY accept latency、create-order/submit response latency、user-channel order/fill visible latency、cancel response latency、各通道先后关系、观察窗口内缺失/截尾和各生命周期分支的条件频率必须从 sdk-live-trade 真实资金 artifact 进入版本化联合校准；状态机、FIFO 与因果事件时间线是结果生成合同，观察频率用于校准和验收。
  13. paper trade 的 lifecycle 与 accounting 必须分离：订单状态先归一化，账本再消费权威事件；order closure、position closure、accounting closure 和 report closure 必须分别验收，dashboard 只能展示结果，不能作为账务权威。
  14. `sim-system` 必须先生成自身的模拟 raw evidence，再通过与 live 一致的 NormalizedOrderEvent 合同归一化；paper 决策、训练样本和 replay 不得直接消费 `sim-system` 私有订单状态，也不得把内部随机结果绕过归一化后直接写成训练标签。
  15. `sim-system`、训练回放和 paper runtime 必须采用离散事件语义推进模型决策：订单/取消请求到达、accepted/rejected、fill/partial-fill、create-order/submit response、user-channel order/trade、cancel response、matched-cancel、late fill 和 settlement 必须作为分别调度的未来事件；某通道缺失必须表达为带 observation horizon 与 censoring reason 的未观察结果，而不是立即生成 rejected/no-fill。事件只有在对应运行环境中变为模型可见后，才能更新持续状态并触发下一次模型决策，不得用固定频率 tick 代替平台事件时间。
  16. 训练 ingestion 必须拒绝缺少 normalized_event_id、authority_source、normalization_reason、ledger_apply_policy、causal_cutoff_ts、label_maturity_ts 或 raw evidence digest/reference 的订单反馈标签；execution feedback label 还必须对齐版本化 schema，保留 decision context、paper/live/simulation source、book hash、online_rule_version、settlement source、censoring reason 和 no-quote/no-submit opportunity 语义。缺失时必须在 `gate_scope=training` 标记 coverage incomplete、quarantine 或 `FAIL_CLOSED_TRAINING`；不得直接映射为 paper/live 的 execution permission。
  17. paper trade 不得作为无约束超参搜索器；模型选择必须有 frozen holdout、purged/embargo split、PBO/DSR 或等价过拟合检查。
  18. 每个训练批次必须生成训练批次排名表，比较同一批次内不同神经网络层级、训练参数、随机种子或候选产物的训练质量、验证质量、冻结 holdout 表现、`sim-system` 执行现实结果、两种盈利机制的分别归因及 interaction/residual、风险指标、资源消耗、artifact sha256 和数据范围。排名表只能作为 candidate/shadow 选择和 paper/live 审查加载的证据，不能替代 ModelRegistry 状态、人工审查、paper 验证或 live 授权 gate。
  19. live-trade 必须把真实执行反馈写入真实平台 raw evidence、NOE、live ledger 和 live audit；`feedback-ingestion` 再离线把经审查的真实反馈接入 training/paper 规则、标签与 `sim-system` 校准链路。live runtime 不得调用或更新 `sim-system`，这些离线反馈也不得未经审查直接改变线上模型行为。
  20. **爱尔兰行情入站观测延迟基线。** 本需求以 `/home/chillchill/new-datasets/2026-07-10/typed_raw/utc_day=2026-07-10/source_kind=polymarket_market` 下 `328` 个 manifest verdict 为 `Complete` 的 finalized Parquet 分段为证据，定义 `collector_observed_feed_delay_ms = timestamp_received - timestamp`。统计只选择 `event_type=book`，因为每条 `book` typed row 对应一条完整 orderbook event，而 `price_change` 会把一条 WebSocket message 展开为多行并造成 row-weighted 偏置。样本合同为 `schema=polymarket_orderbook_v1`、`collector_host_id=ip-172-26-10-0`、`collector_region=eu-west-1`、`source_endpoint=wss://ws-subscriptions-clob.polymarket.com`；有效样本 `N=1,474,557`，timestamp 缺失 `0`，负延迟 `0`。实测分布为：min `7ms`、mean `22.866ms`、P1 `11ms`、P5 `11ms`、P25 `13ms`、P50 `14ms`、P75 `17ms`、P90 `29ms`、P95 `52ms`、P99 `167ms`、P99.9 `506ms`、max `263,153ms`。
  21. 上述统计只能称为“Polymarket market-channel event 到爱尔兰 collector 的入站观测延迟”，其中还可能包含平台事件时间语义、两端时钟偏差、平台/本机调度和订阅重连；该数据目录没有提供可核验的 host clock-sync receipt。max `263,153ms` 等极端值必须保留为 stale snapshot/reconnect/长尾待分类证据，不得静默删除、截尾或冒充正常纯网络延迟。profile artifact 必须记录 dataset path/date、过滤口径、样本单位、分位数、异常/长尾政策、clock-sync evidence status、profile version/id、sha256 和生成代码版本。
  22. **双向延迟代理假设。** 用户在 `2026-07-12` 明确确认并冻结：`sim-system` 的 client-to-platform order/cancel request leg 与 platform-to-client create-order/submit response、user-channel、cancel response transport leg，都采用第 20 条同一版本化经验边际分布。该用法是项目模拟代理假设，不是已实测的爱尔兰订单出站、反馈通道、RTT、平台处理或撮合延迟。每一 leg 必须拥有独立 timestamp 和可回放 sample record，至少记录 profile_id/version/hash、seed、sample/scenario id、sampled_delay_ms、send/emit time 与 receive/arrival time；同一次 sample draw 不得被重复计算为多个 leg。训练历史行情已经按实际 `timestamp_received` 进入因果时间线时只能使用该实际可见时间，不得再抽取一次入站延迟。
  23. **多通道反馈竞态。** `sim-system` 必须覆盖并可强制复现 create-order/submit response 先到、user-channel 先到、两者同毫秒、任一通道迟到或在观察窗口内缺失、取消决策时两者均尚不可见等组合。对 batch submit，这里的 submit response 是一个整体 array envelope，其内所有已解析 item 共享同一客户端 envelope receive time；批内各订单的 user-channel 事件仍可以分别在 envelope 前后到达或缺失。平台 accepted/matched 状态与客户端 `UnknownSubmitOutcome`/provisional 状态必须同时存在且互不覆盖；平台撮合、FIFO 和 fill 不得因客户端没有收到 response 而暂停。本项目真实资金 artifact 已直接支持 response-first、user-channel-first 以及 user-channel/fill artifact 为空但 cancel 返回 exact matched-cancel 的分支；“取消决策窗口内 create-order response 与 user-channel 均不可见”按用户在 `2026-07-12` 确认的项目观察纳入需求，但当前本地 artifact 不足以冻结其发生率。每个随机/缺失/乱序结果必须由版本化联合校准 profile、固定 seed 与因果时间线生成并记录 platform truth、client-visible state、各通道 emit/receive time、observation horizon、censoring reason 和 scenario id；相同 profile 与 seed 必须逐事件重放一致。联合分布必须按真实资金 artifact 的规则版本与适用市场分层校准并记录证据边界。

  ## 八、模型 bundle 与策略边界

  1. 模型 bundle 必须有明确合同：feature_schema_version、model_version、input shape、sequence length、normalization、label contract、固定 `BUY-only` action surface、目标 outcome/token、risk limits、online_rule_version、decision_trigger_contract_version、模型可见 trigger event 合同、sha256；bundle 和 runtime preflight 必须拒绝自有 SELL 能力、固定频率 decision tick 或无法映射到权威 trigger event 的模型调用路径。
  2. ModelRegistry 必须区分 active、shadow、retired、candidate。
  3. active 模型才能真实下单；shadow 模型只能产出对照决策和评估 artifact。
  4. 训练批次排名表必须与 ModelRegistry 对齐：排名靠前的产物只能进入 candidate 或 shadow 审查，只有通过模型 bundle 合同、训练反馈 gate、paper 与 `NO_SUBMIT_PERMISSION` 场景验证和授权 gate 后，才允许进入 active。
  5. 当前模型框架的不同神经网络层级、模型容量和训练参数候选必须共享同一 BUY-only 动作合同、两种盈利机制、数据、`sim-system`、NOE、ledger、reward 和评估边界后再比较；不符合当前 BUY-only action contract 的候选不得进入当前 leaderboard 或训练批次排名表。
  6. 上线前必须做 action-surface audit，确认模型只能输出 BUY 两个 outcome、取消未成交 BUY、`MODEL_NO_SUBMIT`、`MODEL_PAUSE` 和允许的风控意图，并与实际 SDK/order/venue 硬约束一致；任何自有 SELL schema、head、candidate、consumer 或执行路径都必须判定失败。
  7. 模型 bundle 必须定义交易动作与风控动作输出合同；live-trade 只能验证 BUY-only action surface 是否满足平台、SDK、权限、schema 和钱包硬约束，不能改写模型的交易或风控 intent。
  8. 模型 bundle 与训练报告必须分别声明并报告“互补 outcome all-in cost < 1”和“结果趋明后增加正确侧并持有至结算”两种盈利机制的适用条件、风险、分别归因及 interaction/residual；不得用未结算方向收益或不完整配对冒充已实现利润，全部归因必须与总 ledger economics 对账。

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
  9. 训练模块必须系统探索当前模型框架在不同神经网络层数、深度、宽度、hidden/state 维度、参数量、序列/记忆容量及训练参数下的交易能力；训练参数至少覆盖经预注册的 optimizer、learning rate、batch、regularization、loss/reward 配置、seed 和训练轮次，不得把单一小模型配置预设为最终答案。
  10. 候选比较必须在相同数据、BUY-only 动作合同、两种盈利机制、`sim-system` 版本、评估区间和资源预算合同下进行，以交易能力、风险、稳定性、校准、结算后收益和资源成本共同判断，不能只按参数量、训练 loss 或单一 leaderboard 指标选择。
  11. 当前 live-trade 交易服务器的 CPU、内存或推理能力不得成为训练阶段选择神经网络层级、模型容量和体积上限的先验硬限制，也不得因此提前排除、压缩或降级交易表现更好的较大模型。
  12. 如果胜出候选超过当前交易服务器的推理延迟、内存或稳定运行能力，必须在独立字段或部署 gate verdict 中记录 `deployment_hardware_status=requires_hardware_upgrade`，不得改变其 active/shadow/candidate/retired ModelRegistry 状态。系统必须保留其原始模型与交易能力证据，并在用户升级交易服务器后重新执行 runtime parity、延迟、内存和长运行 gate；硬件升级完成前不得部署该候选，但也不得仅为适配旧服务器而自动改选较弱模型。
  13. 交易服务器可以升级不等于部署门取消。任何模型进入 paper/live 前仍必须在最终目标硬件上通过相同输入、输出、数值 parity、事件到 intent 延迟、峰值内存、连续运行和 fail-closed 验收。

  ## 十、P0 gates 与验收边界

  核心重构必须至少包含以下 P0 gate 或等价机制：

  1. OnlineFeedbackGate：online-feedback 规则版本、真实平台潜规则和 issues 是否已纳入。
  2. ModelBundlePreflight：模型 bundle、schema、ONNX/runtime、feature contract、固定 BUY-only action surface、两种盈利机制、decision trigger contract 与模型可见事件合同是否匹配，并拒绝任何自有 SELL 能力。
  3. MarketDiscoveryGate：active/next market、token、tick、min size、start/end、market lifecycle state 与 runtime role 是否可靠。
  4. MarketSwitchGate：active 与 prefetched context 是否隔离，切换是否可审计；旧 market 只有满足 `market_resolved` 事件路径或 fallback metadata retired predicate 后才能进入 retired，否则只能标记 stale/gap/quarantine。该 gate 只产生 verdict、隔离或切换阻断 effect，不是执行 actor；切换期间仍可由各自权威事件路径处理既有模型 cancel intent、平台到期/关闭或 operator emergency cancel，但 gate 与 market switch 都不得自行生成策略性撤单。
  5. ResourceWatermarkGate：CPU/RAM/VRAM/IO/disk/artifact writer 是否满足阈值，并覆盖 writer queue、flush/finalize latency、backpressure 和 ResourceWatermark 阈值动作。
  6. HardwareTierGate：单卡/多卡训练配置、实际硬件、运行参数是否匹配；训练候选容量探索与最终部署硬件准入必须分开判定，不得用当前交易服务器配置提前裁剪训练候选。
  7. SupplyChainGate：SDK、依赖、模型 artifact、数据 artifact、部署产物 sha256 是否可追踪；外部 repo、脚本和依赖默认 pinned readonly，不得默认 install/run、读取 secrets 或联网出站，任何例外必须有隔离环境、审批和审计 artifact。
  8. TrainingFeedbackGate：真实交易反馈、paper 反馈、训练标签、online_rule_version、训练批次排名表是否闭环；必须覆盖 execution reality，包括 FOK/FAK/GTC/GTD、每段 fill 的实际 maker/taker role、marketable GTC/GTD、post-only reject、queue/fill hazard、tick/min size、按 market metadata 计算的 fee/rebate、slippage/cost、no-quote/no-submit opportunity、trial ledger、两种盈利机制的分别归因、interaction/residual 和冻结 holdout/过拟合防护，不能只看 replay leaderboard。
  9. ModelIntentBridgeGate：BUY-only 模型 trading/risk intent、trigger_event_id 到模型调用与 intent 的可审计映射、live-trade 发送合同、平台反馈回传合同和“禁止 live-trade 改写模型交易/风控决策”的边界是否满足；非 BUY intent 必须 fail closed。
  10. DataIngestRuntimeGate：`data-collect` 与数据消费运行时是否满足 Rust async/non-blocking、bounded queue、writer lag、backpressure、source health/gap/reconnect trace、事件排序/去重/market 隔离、finalized-only discovery、temp/running/rsync-partial 排除和 blocking pool 隔离。
  11. SimSystemFidelityGate：训练与 paper 是否共同调用同一版本化 `sim-system`，BUY-only FOK/FAK/GTC/GTD、两套订单单位合同、实际 maker/taker role、single response object/batch response array envelope、batch 逐项 mixed outcome 与关联证据、FIFO 项目规则假设、resting order 在 engine acceptance time 的同价队尾入队、因果前方量更新、shares 数量守恒、pUSD 现金守恒、venue dust terminal、模型单笔/全部撤单、取消竞态、raw evidence -> NOE -> ledger、规则 provenance 和不确定性场景是否闭合；还必须核验爱尔兰延迟 profile 的 dataset/filter/sample/provenance/hash、request/feedback 各 leg 未重复计时、平台真实状态与客户端可见状态分离、single/batch submit envelope 及其整体超时/丢失/解析失败、create-order/submit response 与 user-channel 的先到/后到/同毫秒/迟到/缺失矩阵、cancel 到达前平台实际 executed/open quantity 的因果推导、dust close 的 `cash_balance_pusd` / reserve / `available_cash_pusd` 分账，以及相同 profile+seed 的逐事件确定性重放。不可见事件、通道可见性和 L2 变化归因使用版本化情景假设；FIFO 入队顺序保持冻结，matched-cancel 由平台状态机因果推导，入站观测 profile 继续保持项目代理假设边界。
  12. ModelCapacityExplorationGate：当前模型框架是否在预注册、公平预算内探索不同神经网络层级、容量和训练参数，是否用交易能力而非当前交易服务器硬件作先验筛选；较大胜出模型是否以独立 `deployment_hardware_status=requires_hardware_upgrade` 或等价 gate verdict 保留，并在升级后的目标硬件上重新验收，且没有污染 ModelRegistry 状态。

  每个 gate 必须同时记录 `gate_scope=data_collect|training|promotion|paper|live|deployment`、`gate_verdict=PASS|WARNING|QUARANTINE|FAIL_CLOSED|REQUIRES_HARDWARE_UPGRADE`、reason/evidence 和 scope-specific effect，不得用模糊的“看起来正常”作为通过标准。coverage incomplete 是 reason，不是通用执行权限；quarantine 必须明确隔离的 source、dataset、run、model 或 runtime。

  gate effect 必须按 scope 隔离：data-collect 只能隔离/拒绝受影响 source 或采集 run；training 只能 quarantine dataset/run 或 `FAIL_CLOSED_TRAINING`；promotion 只能阻止 candidate/shadow/active 晋级；deployment 的硬件不足只能阻止部署并保留 `deployment_hardware_status=requires_hardware_upgrade`；只有 paper/live runtime gate 才能设置 `execution_permission=ALLOW_SUBMIT|NO_SUBMIT_PERMISSION|CANCEL_ONLY_PERMISSION|PAUSED_PERMISSION`。`CANCEL_ONLY_PERMISSION` 还要求 cancel 路径自身已经通过硬边界验证；它只允许执行模型已发出的 cancel intent 或 operator emergency cancel，不会生成策略性撤单。gate verdict、execution permission 和 `model_strategy_mode` 不得互相替代。

  ## 十一、部署与运行边界

  1. AWS live-trade 服务器 `108.130.74.143` 是运行目标，不是构建机。
  2. 开发、代码编写以及 training collector、paper hot、live hot、live-trade 主程序等 release binary 的构建必须使用本地 `/home/chillchill/btc-updown-marketmaker`，并记录 commit、sha256、产物路径和 binary role 后分别部署；各远端服务器源码/工作目录统一写作 `~/btc-updown-marketmaker`，不得把服务器上 `/home/ubuntu/btc-updown-marketmaker` 误作本地路径。
  3. live 服务器不得运行 Rust 编译命令，不得依赖服务器 git pull、源码 checkout 或 GitHub deploy key；该限制覆盖所有 Rust crate、`data-collect`、paper/live runtime 和工具程序。
  4. 三个 `data-collect` 运行角色必须是三个独立 release binary 和进程：在 `data-collect` 三角色范围内，数据采集服务器只部署/运行 `data-collect-training-collector`，paper trade 服务器只部署/运行 `data-collect-paper-hot`，live trade 服务器只部署/运行 `data-collect-live-hot`；该“只”不排除 paper/live 服务器同时运行各自独立的交易主程序，但不得由一个远程采集进程或交易主程序兼任另一个 `data-collect` 角色。
  5. 模型文件不得通过 GitHub 同步，不得放入 wallets；部署模型时必须从本地已验证 artifact 路径直接复制，并记录来源路径与 sha256。
  6. wallets 只保留 gitignored secret/env 与钱包公共派生配置。
  7. live trade 服务器上的 `data-collect-live-hot` 是独立外部市场源 handoff binary，安装路径必须解析到 `/home/ubuntu/live-trade/data-collect/current/bin/data-collect-live-hot`；它与 live-trade 主交易 binary 独立部署、独立 sha256、独立进程和独立日志，但只能通过版本化本机 handoff 合同向主交易程序供给外部市场源事件，不得接管模型、钱包、订单或 ledger。
  8. live-trade、paper-trade、training、`sim-system`、data collection 的运行根目录、artifact 根目录和权限边界必须明确。live trade 服务器上的训练模型、live-trade 主交易 binary、`data-collect-live-hot` 及其 runtime config 路径解析到 `/home/ubuntu/live-trade` 之外必须 fail closed；paper/training 的服务器本地路径不得冒充该 live 根目录。
  9. live-trade release binary 不得包含独立交易策略、独立风控决策逻辑、`sim-system` 或自有 SELL 路径；它只包含 BUY-only 模型加载/输入输出桥接、SDK 执行、平台反馈回传、artifact 记录和权限/协议/安全硬边界。
  10. live 交易小主机不得承担训练式重处理、历史全量 parquet 扫描、压缩归档批处理、archive/upload/listing/gzip 或 `data-collect` 编译/test/check/fmt；这些工作必须在具备硬件条件的构建机、数据服务器或训练服务器完成，且 archive/upload/listing/gzip 若纳入范围必须另开需求、计划和 changelog。

  ## 十二、验收标准

  本次核心需求文档完成后，必须满足以下验收标准：

  1. 权威覆盖基线 `/home/chillchill/btc-updown-marketmaker/mistake-log/2026-07-03-重构前错误汇总与优先级.md` 中每类历史问题都能映射到至少一个新需求、
  gate、artifact 或测试。
  2. 新系统流程覆盖自检、热身、T-30 预加载、active market 持续决策、market end T-30 下个模型预加载。
  3. 训练、paper、live 均使用完整 5 分钟数据 + 30 秒预加载上下文，而不是 250ms 孤立片段。
  4. 数据采集与数据消费有明确 schema、manifest、raw evidence、atomic artifact 和 append-only event log 边界；`data-collect`、Market Context Builder、`data-pipeline/data-processing`、feedback-ingestion、NOE/Ledger 的输入、输出和非范围必须可追溯，训练、paper、live 共享同一套时间线、schema 和可回放语义。
  5. 数据处理需求覆盖历史采集数据，且新数据采集到数据处理工作流以旧数据处理经验和结果为基础；labels/features/tick_stream/replay context 由 `data-pipeline/data-processing` 生成，不得写入 `data-collect` 范围。
  6. single `post_order` response object、`post_orders` batch response array envelope 与 mixed per-item outcome、FOK/FAK market BUY 的 `spend_pusd + worst_price_pusd_per_share`、GTC/GTD limit BUY 的 `limit_price_pusd_per_share + size_shares`、两种订单各自的 SDK 编码/parity、实际 maker/taker role、marketable limit、post-only、venue dust、matched-cancel、generic not-found、模型单笔/全部撤单、平台到期/关闭、operator emergency cancel、late user-channel、open order audit 都有明确订单生命周期语义；程序不存在把市价单改写为限价单、把限价单改写为市价单，或按固定经过时长/market switch 自行生成策略性撤单的路径。
  7. 真实平台和 `sim-system` 的订单 raw evidence 都必须先通过同一套版本化 NormalizedOrderEvent 合同归一化，但消费路径严格分离：`sim-system -> NOE -> training/paper ledger/audit`，`真实平台 -> NOE -> live ledger/audit`，真实反馈只能再经 `feedback-ingestion` 离线进入训练与 `sim-system` 校准。验收必须证明 `sim-system` 证据不能进入 live ledger、live audit 或 live 状态，并覆盖 unknown submit、generic not-found、matched-cancel、partial-open、partial-terminal、平台在 `executed_base_qty<accepted_base_qty` 时发生的 venue dust terminal、late user-channel、duplicate terminal evidence，以及 cancel requested、cancel pending、cancel accepted、cancel rejected、cancel retry、cancel timeout、overlapped terminal 和 cancel retry budget exhausted；每笔 engine-accepted order 满足 `accepted_base=executed_base+dust_base+canceled_base+expired_base+market_closed_base+open_base`，上述数量单位均为 shares，rejected intent 不进入该守恒域，pUSD 现金独立守恒，actual fill、dust terminal 和 reserve 释放均只入账一次。
  8. 训练和 paper 能从真实 live 反馈、真实平台潜规则、online-feedback、issues 和混合架构约束中反推规则。
  9. 每个训练批次必须生成可审计训练批次排名表，记录候选神经网络层级、模型容量、训练参数、随机种子、数据范围、核心质量指标、`sim-system` 执行现实指标、两种盈利机制的分别归因及 interaction/residual、资源消耗、artifact sha256、候选入选理由和淘汰理由；排名表只能作为进入 candidate/shadow、paper 或 live 审查加载的证据，不能单独授权 active 或 live。
  10. 5090 单卡/多卡训练有硬件实测、run manifest、profiler 和资源水位验收；当前 live-trade 服务器性能不得成为训练模型层级或体积上限，超过当前服务器能力的胜出候选必须保留并进入硬件升级门，而不是被静默淘汰或降级。
  11. SDK-first 边界、部署边界、模型 artifact 边界、wallet 边界、small-live/live 授权边界、`data-collect` 运行边界和 live 服务器运行边界全部明确；验收能分别证明 training collector、paper hot、live hot 是部署在三台对应服务器上的三个独立 binary/进程，且 live hot binary 位于 `/home/ubuntu/live-trade/data-collect/current/bin/data-collect-live-hot`、不由 live-trade 主程序兼任。
  12. 任何不能验证的结论都必须记录具体 `gate_scope`、coverage incomplete reason、`gate_verdict` 和 scope-specific effect，不能冒充通过；只有 paper/live scope 可以映射为 `NO_SUBMIT_PERMISSION`、`CANCEL_ONLY_PERMISSION` 或 `PAUSED_PERMISSION`，training/promotion/data-collect/deployment 必须使用各自的隔离、拒绝、晋级阻断或部署阻断效果。
  13. 交易决策和风控完全由模型实现，当前 action surface 全链路只有 BUY 两个 outcome，不存在自有 SELL；live-trade 仅作为模型与真实平台之间的桥梁。所有模型请求、硬边界拒绝、真实平台返回和执行反馈都有可审计映射，且不存在 live-trade 改写模型交易或风控 intent 的路径。
  14. 训练、paper 和 live 的模型决策都由真实模型可见事件触发，不存在固定频率、固定周期、周期性 polling 或固定 decision tick 的策略决策路径；每个模型调用和 intent 都能追溯到 trigger_event_id、event_available_time、decision_ts 与 causal_cutoff_ts。`sim-system` 延迟通过未来事件调度表达，固定间隔 `tick_stream` 只能作为派生产物，不能成为模型决策触发器。
  15. `sim-system` 是模拟平台模块的唯一规范名称，并且只有训练模块和 paper trade 可以调用；两者提交模拟 BUY 订单后，`sim-system` 能生成可归一化、可回放、可审计的平台执行与生命周期反馈，live trade 不调用 `sim-system`。
  16. 两种盈利机制均有分别验收：同一 confirmed paired quantity 的实际 all-in `outcome_up + outcome_down < 1` 配对闭合，以及基于因果可见信息在结果趋明时增加当时判断正确侧并持有至结算；归因必须保留 interaction/residual，全部合计与账户级 ledger、NAV 和 settled PnL 对账，不存在第三种隐含交易机制。
  17. `sim-system` 按“用户确认并冻结的项目规则假设，而非 Global 官方公开事实”模拟价格优先、同价 FIFO：resting BUY 在 engine acceptance time 必须排在所有更早接受且尚未消耗的同价 shares 之后；若此前为 `100 shares`、我方为 `5 shares`，则初始 `quantity_ahead_at_acceptance=100`，我方数量区间为 `(100, 105]`。不确定性只能作用于 acceptance 前后的不可见事件与后续 L2 变化归因，不能随机改变我方同价队尾入队顺序；具体归因和场景算法只能在本需求获批后的计划文档中展开。
  18. 训练模块完成当前模型框架不同神经网络层级、容量和训练参数的公平探索；候选不会因当前交易服务器性能被提前排除，较大胜出模型在服务器升级后通过目标硬件 parity、延迟、内存和长运行验收才能部署。
  19. 爱尔兰延迟 profile 的复算结果必须与第七节冻结口径一致：`328` 个 Complete finalized Parquet 分段、`event_type=book`、`N=1,474,557`、缺失 `0`、负值 `0` 以及已列分位数和极值均能从指定数据路径重现；profile 必须保留 clock-sync evidence 缺失和长尾分类边界。训练历史行情按 `timestamp_received` 只计算一次可见延迟，order/cancel request 与各 feedback transport leg 分别采样、分别记时，且任何 artifact 或报告都不得把该代理 profile 写成真实测得的订单出站、RTT、平台处理或撮合延迟。
  20. 使用固定 latency/joint-race profile 与 seed，`sim-system` 必须逐事件可复现并通过 raw evidence -> NOE -> ledger 对账：create-order/submit response 先于 user-channel；user-channel 先于 response；两者未到时 provisional intent 只进入 `pending_submit_list`，仅使用合法 cancel target/scope，并持续占用 `pending_submit_reserved_pusd`；cancel request 先于原 submit 到达时返回 not-found/unresolved，原 submit 随后仍可 accepted，旧 cancel 不追溯生效；模型可见 accepted/remaining size 为 `5 shares`、平台已成交但尚未通知 `3 shares` 时，模型对可见订单发出整单 cancel，平台只取消真实 open `2 shares`，无 quantity 的 cancel ack 先进入 `cancel_effect_qty_pending_reconciliation`，把 reserve 原额转入 `cancel_reconciliation_reserved_pusd`，不泄露 `2 shares`、不按本地可见数量释放 reserve，迟到 fill 随后确定 `executed=3、canceled=2` 并各应用一次；fully-matched 与 venue-dust-terminal 返回 matched-cancel，FAK kill/GTD expiry 等返回各自终态；venue dust 下 actual fill、dust terminal、reserve 释放、`cash_balance_pusd` / `total_reserved_pusd` / `available_cash_pusd` 和 settlement failure/retry 分账正确；任一通道迟到、缺失或重复后均不重复 reserve、fill、cash/position apply。验收必须证明模型只基于当时可见状态决定取消，而 cancel 结果由到达模拟平台时的真实订单状态决定。
  21. 使用固定 single/batch submit 测试用例、latency/joint-race profile 与 seed，`sim-system` 必须可重放：single submit 生成一个 response object；batch submit 生成一个最多 `15` 项的 response array envelope，同一 envelope 内可同时出现 live/matched/rejected/ambiguous item，每项独立归一化且不污染 sibling item。必须分别验收 HTTP 200 mixed result、明确 request/per-item rejection、结果未知的 transport/非决定性 non-2xx/丢失/截断/解析失败、有/无决定性 error 的空 order id/status、count mismatch 和无法关联项；截断/无法解析的 batch body 不得向客户端暴露部分 `Vec`。批量 envelope 只可有一次客户端可见到达，批内每笔订单的平台真实状态和 user-channel 事件仍独立；归一化后的每个 item 必须共享该 response_envelope_id/digest/receive time 并保留 raw envelope 形状与关联证据。验收必须证明不仅凭 `success` 或 response array 位置认定 accepted，明确且可安全关联的 rejection 释放对应 pending-submit reserve，无法安全关联或 envelope 不可见时保留 `UnknownSubmitOutcome` 与 pending-submit reserve，再由后续权威 user-channel/reconciliation evidence 逐项解析。
