  # rebuild-core-demand 核心重构需求

  事项编号：`rebuild-core-demand`。

  生命周期：`DRAFT_AWAITING_USER_REVIEW`。

  当前执行 authority：`none`。

  对应计划：`plan/rebuild-core-plan.md`。

  对应变更日志：`changelog/rebuild-core-changelog.md`。

  执行授权：`none`。

  状态：`需求审核中；离散 next_wake 已获批作为目标候选；data-collect 当前新写入的 aggTrade 为 v7，depth 快照/更新为 v3/v4，当前模块已经用户验收关闭；本文整体尚未获得用户最终批准，不授权执行本文其余代码重构、paper、live、部署或资金操作`。

  本文件在用户审核通过前只作为当前需求草案。为审核和三件套对齐可以创建明确标为
  `authority=none` 的对应 plan 与 changelog 草案；只有需求及对应计划获批后，计划范围内
  的本地实施才取得游标。本状态说明不改变已经完成的独立 `data-collect` 事项，也不恢复
  用户已手动删除的旧 rebuild-core 子事项文档。

  > `data-collect` 文档路由：`data-collect/docs/CURRENT.md` 是唯一当前入口；本文
  > 是跨模块审核中草案，不是 `data-collect` 的当前权威文档。本文中涉及后续
  > training/data-pipeline/paper/live 的“必须”只属于 rebuild-core 候选需求，不能
  > 覆盖 `data-collect` 的当前代码、测试或完成状态。
  >
  > 2026-07-30 `data-collect` 当前能力基线：当前只有
  > `data-collect-training-collector` 连接真实外部源；paper/live 二进制程序只有
  > 心跳/状态功能；真实适配器不属于 `model-train`，保持延期。必须先完成
  > `model-train`（模型训练模块，不是 `data-collect-training-collector`）；之后仍
  > 只有在用户决定返回 `data-collect` 时，才另立三件套。
  > 用户在 2026-08-03 纠正并取代此前“先完成 `sim-system`、再实现
  > `data-pipeline(training)`”的顺序。当前跨模块开发主线为：已完成
  > `data-collect(training)` 后，先冻结 `data-pipeline(training)` 需求和共享合同，并完成
  > “最终化类型化原始数据 -> 规范全序 -> ExecutionMarketEvent”最小真实纵向链路；
  > 随后由 `sim-system` 同时使用合同有效的合成/黄金事件和真实管道事件开发、验收；再
  > 补齐同一 `data-pipeline(training)` 的归一化反馈/Ledger 投影合并、完整
  > `Observation`、标签、切分、归一化与训练产物闭环，最后进入 `model-train`。黄金事件
  > 仍负责组件符合性、边界和确定性回归，但不能替代真实纵向链路。完整归一化层与 Ledger
  > 仍不是 `sim-system` 开发前置；RewardContract 的实际计算属于 `model-train` 首门，
  > 训练闭环门只需验证其输入合同兼容性。当前计划也不得在 `sim-system` 内
  > 实现原始文件排序器或第二套 `data-pipeline`。`sim-calibration` 仍是独立
  > 后续角色，不属于当前 `sim-system` 首版计划；当前阶段不得实现其写入器、校准 CLI、
  > `feedback-ingestion`、校准切分或发布链。
  > 用户在 2026-08-04 进一步冻结训练数据资格：只使用谱系源自
  > `/home/chillchill/new-datasets` 且 UTC 日不早于 `2026-07-30` 的数据。更早采集数据
  > 不得读取、迁移、回填、统计、拟合、标准化、训练、校准或评估；日期达到下限也不自动
  > 取得训练/开发/校准/留出集归属，仍须显式清单准入和版本化切分。真实订单回执、SDK
  > 行为和平台规则等非训练数据证据不因该日期下限被删除，但不得伪装成训练输入。
  > 用户同日进一步冻结：整个项目只支持 Polymarket Global CLOB 的
  > `btc-updown-5m`，不会扩展到其他市场类型。低频平台规则不经 `data-collect`、
  > `data-pipeline` 或 `ExecutionMarketEvent` 传播；每个新的 `sim-system` 运行必须在首个
  > 业务请求前，通过窄型、只读、无凭据的官方 HTTPS 规则加载器，按有界目标
  > `condition_id` 集合执行一次冷启动加载并密封不可变规则快照。逐市场 `itode=true` 时
  > 才应用版本化文档规则中的 250ms；运行中
  > 动态 `tick_size_change` 仍由规范事件推进，体育/赛事 `secondsDelay` 不在项目范围。
  > 用户在 2026-08-02 至 2026-08-03 纠正此前首版遗漏返佣的决定：maker rebate 与
  > taker rebate 都是重要经济结果，`sim-system` 首版必须模拟两类应计、按日结算和付款。
  > maker 使用撮合时逐市场官方 `feeSchedule`；taker 固定为 Silver `$20,000 wV`/`8%`，
  > 但不累计或校验 wV，不实现动态等级或一次性等级奖金。官方 taker 名义付款时刻为每日
  > `00:00 UTC`；首版基于账户观察，把 D 日 taker 在 D+1 `00:10 UTC`、maker 在 D+1
  > `00:45 UTC` 冻结为正常现金可用时刻。正常时刻到达时，已经归属 D 的潜在合格贡献
  > 必须全部在模拟平台内部最终闭合才日结；否则原日桶保持未日结，待最后一项闭合后在首个
  > 因果可用内部时刻只日结、付款一次。普通训练窗口结束或调用方主动截尾本身不生成未知；
  > 无论是否跨过正常现金可用时刻，尚未闭合的日桶都只保留未日结状态和已有应收款，不提前
  > 日结；客户端反馈迟到不推迟内部付款。两类日桶互不合并，
  > 观察到的长尾到账不证明真实平台因成交最终性而延迟；上述等待闭合只是首版为避免漏付或
  > 猜付而冻结的内部确定性规则。
  > 低于 `1 pUSD` 不付款且不跨日凑门槛，达到门槛时向下截取至四位小数，低于门槛金额和
  > 截取余数继续作为不可支出且不参与后续门槛的应收款。应计只增加一次经济价值，付款只把
  > 对应应收款转为现金，不重复产生 NAV、PnL 或训练奖励；详见
  > `demand/sim-system-demand.md` 第 10.5 节。
  > 当前官方 SDK 基线为 `0.7.0` 加仓库唯一的 `0.7.0-p1` 补丁，六个新写入 schema 为
  > 订单簿 v3、元数据 v5、深度快照 v3、深度更新 v4、aggTrade v7、
  > RTDS v4。training RTDS 的唯一业务静默例外是稳定期 300 秒、首次新鲜数据 60 秒的
  > 完整运行时替换；默认容量为 50 GB，启动时超限直接退出 75，运行期达到上限时
  > 协调最终化并退出 75，不自动删除原始数据。
  > 截至 2026-08-04，元数据 v5 没有规则专用列，且不再要求增加。当前四日合格数据中的
  > 同名或相似值只存在于 Gamma 原始证据 JSON；删除十个相似键的反事实压缩收益约 9,317
  > 字节，占全部 9,202,902,694 字节约 0.000101%，并会破坏原始证据摘要，因此不删除。
  > 当前固定版及官方主线 Rust SDK 对 `itode`、完整 `feeSchedule.rebateRate` 和若干字段
  > 存在性的类型覆盖不足仍是已核实技术事实，但不再构成 `sim-system` 冷启动实施阻断，也
  > 不是 `data-collect` 或 DPT Gate A 缺口。冷启动加载器固定只访问官方 CLOB
  > `GET /clob-markets/{condition_id}` 与 Gamma `GET /markets/keyset`；Gamma 对有界目标集合
  > 使用重复 `condition_ids` 查询参数，并分别查询 `closed=false` 与 `closed=true`。响应原始
  > 字节先计算 SHA-256，再由专用窄 DTO 解析；除官方明确允许 `itode=false` 时省略字段、费用
  > 禁用时省略费用对象外，必须区分字段缺失与真实 `0/false`。加载器逐项校验目标
  > `condition_id`、代币和结果，以 CLOB `mos/mts`、Gamma `negRisk` 和完整 `feeSchedule` 为对应
  > 规则来源，并在费用启用时核对 CLOB `fd` 的重叠字段；全部目标唯一闭合后才密封快照。必需
  > 语义缺失、非法、重复或冲突仍使本次新运行在首个业务请求前失败即关闭；未消费的 `oas` 只登记响应覆盖情况，不单独
  > 阻断。该窄型读取边界不授权通用原始 REST、`serde_json::Value` 任意取值、手工签名、影子
  > 交易客户端或网页常量旁路。固定 Silver `8%` 是项目/账户规则，不要求查询 wV。
  > 2026-07-28 事故事项已由用户最终验收，旧 Attempt6 不是当前游标。本文后续任何
  > 与该摘要冲突的旧 data-collect 版本、部署或恢复表述均为历史，不得覆盖当前代码。

  本需求拟对项目核心交易链路进行重构，从架构层面和系统设计思路层面解决重构前已盘点的历史问题。权威覆盖基线是
  `/home/chillchill/btc-updown-marketmaker/mistake-log/2026-07-03-重构前错误汇总与优先级.md`。这些问题分布在数据采集、数据消费、模型训练、
  paper 交易、live 交易、SDK 下单、订单生命周期、真实平台潜规则、硬件利用率、产物、审计和部署边界等多个板块，且问题彼此关联，不能只按单点
  缺陷修复。

  本次重构的核心目标不是简单重写代码，而是建立一套可验证、可审计、可回放、能反映真实交易约束的系统边界。权威覆盖基线中的每类历史问题，都必须能
  追溯到新的需求、设计、测试或验收标准中；单条历史实例作为证据台账，不把需求范围扩展为无界重写。

  ## 总体系统架构图

  ### 模拟交易 / 实盘交易图表

  ```mermaid
  flowchart TD
    DATA["外部市场源<br>Polymarket 市场频道 / Binance BTCUSDT 深度 / Binance BTCUSDT aggTrade / Polymarket RTDS Chainlink BTC/USD"] --> COLLECT["（共用）data-collect<br>外部市场源采集 / 实时交接 / 覆盖范围 / 清单"]
    COLLECT --> CONTEXT["paper/live 各自独立的 data-pipeline 在线运行时 / Market Context Builder<br>调用共用确定性处理核心 / 实时模型输入 / 可见信息边界"]
    CONTEXT --> TRADE["模拟交易 / 实盘交易模块<br>持续决策编排"]
    BRIDGE["（共用）Intent Bridge / Execution Adapter<br>SDK 对齐方法面：post_order / post_orders / cancel_order / cancel_orders<br>cancel_all_orders / order / orders / trades"] -- "paper：模拟领域请求/响应<br>业务请求阶段无签名、无网络" --> SIM["（共用）sim-system 模拟平台模块<br>training / offline-evaluation / paper 共用合同"]
    RULEHTTP["新运行规则冷启动<br>官方 HTTPS 只读加载器"] --> SIM
    CONTEXT -- "paper: ExecutionMarketEvent" --> SIM
    BRIDGE -- "live：官方 Client 同名方法<br>提交使用 SignedOrder" --> REAL["真实平台模块<br>官方 Polymarket Rust SDK"]
    SIM --> RAW["平台原始证据<br>sim-system 模拟反馈 / 真实平台反馈"]
    REAL --> RAW
    RAW --> NOE["（共用）归一化层<br>订单 NOE / 非订单经济归一化合同"]
    NOE --> LEDGER["（共用）Ledger 合同<br>账户 / 持仓 / 挂单 / pUSD CASH / 返佣应收款 / NAV"]
    LEDGER -- 本运行实例只读投影 --> CONTEXT
    TRADE --> MODEL_RUNTIME["paper/live 各自独立实例（不包含训练）<br>Model Runtime / 模型运行"]
    MODEL_RUNTIME -- 模型意图 --> BRIDGE
    TRADE --> TRADE_REPORT["交易可视化可审计报告"]

  ```

  ### 训练图表

  ```mermaid
  flowchart TD
    TRAIN["训练模块<br>持续消费训练市场数据 + 账本数据<br>训练 / RewardContract / 训练内诊断与选优"] --> MODEL_TRAIN["不可变候选 ModelBundle<br>TrainingCandidateReceipt"] & TRAIN_REPORT["训练可视化可审计报告"]
    TRAIN -- 训练回放意图 --> BRIDGE["（共用）Intent Bridge / Execution Adapter<br>SDK 对齐方法面"]
    BRIDGE -- "模拟：模拟领域请求/响应<br>业务请求阶段无签名、无网络" --> SIM["（共用）sim-system 模拟平台模块<br>training / offline-evaluation / paper 共用合同"]
    RULEHTTP["新运行规则冷启动<br>官方 HTTPS 只读加载器"] --> SIM
    SIM --> RAW["sim-system 返回模拟原始证据<br>订单生命周期 / 两类返佣应计、日结与付款 / 结算"]
    RAW --> NOE["（共用）归一化层<br>订单 NOE / 非订单经济归一化合同"]
    NOE --> LEDGER["（共用）Ledger 合同<br>回放账户 / 持仓 / 挂单 / pUSD CASH / 返佣应收款 / NAV"]
    LEDGER -- 本训练实例只读投影 --> PIPELINE
    TRAIN -- 模型动作 / 决策 ID --> REWARD["训练专属 RewardContract<br>账本前后状态 + 估值/终态证据"]
    LEDGER -- 可回放状态 / 增量 --> REWARD
    REWARD -- 奖励信号 --> TRAIN
    MODEL_TRAIN --> PROMOTION["Promotion Controller<br>验证并注册 ModelRegistry candidate"]
    PROMOTION --> OFFLINE["交给独立 offline-evaluation<br>不得直接进入 paper/live"]
    DATA["外部市场源<br>Polymarket 市场频道 / Binance BTCUSDT 深度 / Binance BTCUSDT aggTrade / Polymarket RTDS Chainlink BTC/USD"] --> COLLECT["（共用）data-collect<br>类型化原始数据 / 覆盖范围 / 可用性诊断 / 最后合格时间 / 缺失性因果基础 / 清单"]
    COLLECT --> PIPELINE["training 独立 data-pipeline 离线运行时<br>调用共用确定性处理核心 / 特征 / tick_stream / 回放上下文"]
    PIPELINE -- "ExecutionMarketEvent" --> SIM
    PIPELINE --> DATASET["训练可消费数据集 / 校准数据池<br>特征 / 市场监督标签 / 执行反馈标签"]
    DATASET --> TRAIN
    TRAIN_REPORT --> RANK_LIST["训练批次中不同产物的排名表"]
  ```

  ### 离线评估图表

  ```mermaid
  flowchart TD
    HOLDOUT["最新合格且完整、未进入训练或任何自适应决策的时间外留出集<br>OfflineEvaluationDatasetManifest"] --> PIPELINE["offline-evaluation 独立 data-pipeline 离线运行时<br>调用共用确定性处理核心"]
    PIPELINE --> MODEL["冻结候选 Model Runtime<br>只读推理"]
    PIPELINE -- "ExecutionMarketEvent" --> SIM
    MODEL --> BRIDGE["（共用）Intent Bridge / Execution Adapter<br>SDK 对齐方法面"]
    BRIDGE -- "模拟：模拟领域请求/响应<br>业务请求阶段无签名、无网络" --> SIM["隔离 sim-system 实例<br>加载冻结规则/校准版本"]
    RULEHTTP["新运行规则冷启动<br>官方 HTTPS 只读加载器"] --> SIM
    SIM --> RAW["模拟平台原始证据"]
    RAW --> NOE["（共用）归一化层<br>订单 NOE / 非订单经济归一化合同"]
    NOE --> LEDGER["隔离的评估 Ledger 实例<br>含不可支出的返佣应收款"]
    LEDGER -- 本评估实例只读投影 --> PIPELINE
    LEDGER --> REPORT["OfflineEvaluationReport<br>OfflineEvaluationReceipt 或证据不足/失败判定"]
  ```

  ### 模型评估与晋级业务流程

  ```mermaid
  flowchart LR
    TRAINING["model-train<br>训练 + 训练内诊断/选优"] --> CANDIDATE["冻结 ModelBundle<br>TrainingCandidateReceipt"]
    CANDIDATE -- PromotionDecision: 注册并准入 --> OFFLINE["独立 offline-evaluation<br>最新合格且完整、未进入训练或任何自适应决策的时间外留出集"]
    OFFLINE -- OfflineEvaluationReceipt PASS + PromotionDecision --> PAPER["paper 交易<br>NO_REAL_FUNDS"]
    PAPER -- PaperQualificationReceipt PASS + PromotionDecision --> SHADOW["live shadow<br>NO_REAL_FUNDS / NO_SUBMIT_PERMISSION"]
    SHADOW -- LiveShadowReceipt PASS + PromotionDecision --> READY["可申请首次真实资金授权<br>仍不可提交"]
    READY -- UserFundsAuthorizationReceipt + 运行准入 PromotionDecision --> SMALL1["ModelRegistry active<br>SMALL_LIVE_1_MARKET"]
    SMALL1 --> SMALL1_RECEIPT["SmallLive1Receipt PASS"]
    SMALL1_RECEIPT -- 完成 PromotionDecision --> SMALL1_PASSED["SMALL_LIVE_1_PASSED<br>可申请下一级授权，仍不可提交"]
    SMALL1_PASSED -- 新 UserFundsAuthorizationReceipt + 运行准入 PromotionDecision --> SMALL3["SMALL_LIVE_3_MARKETS"]
    SMALL3 --> SMALL3_RECEIPT["SmallLive3Receipt PASS"]
    SMALL3_RECEIPT -- 完成 PromotionDecision --> SMALL3_PASSED["SMALL_LIVE_3_PASSED<br>可申请正式授权，仍不可提交"]
    SMALL3_PASSED -- 新 UserFundsAuthorizationReceipt + 运行准入 PromotionDecision --> FORMAL["FORMAL_LIVE_1H"]
    FORMAL --> FINAL_RECEIPT["FormalLive1HReceipt PASS"]
    FINAL_RECEIPT -- 完成 PromotionDecision --> COMPLETE["FORMAL_LIVE_1H_PASSED<br>完成后可审查，不自动续权"]
  ```

  上述模型生命周期是不可跳级的业务顺序。图中每条带凭据的转换，都表示 Promotion Controller 校验前序证据后签发 `PromotionDecision`；凭据本身和阶段运行时均无权写入 ModelRegistry、`promotion_stage` 或资金准入状态。真实资金阶段必须使用两个独立决策：运行启动前的准入决策只绑定当前已通过阶段与本次运行授权，不提前推进阶段；运行完成后的完成决策只在 PASS 凭据已存在且准入已关闭后推进到 `*_PASSED`。下一档用户授权只能在该完成决策落盘后签发，禁止把前一档完成与下一档准入合并成一次状态转换。训练内评估只负责诊断训练是否有效、在预算内选优并生成可解释报告，不是 paper/live 晋级门禁；`model-train` 只能签发 `TrainingCandidateReceipt` 并落盘不可变模型包，由 Promotion Controller 验证后才能注册 ModelRegistry `candidate`。只有独立的 `offline-evaluation` 可以读取晋级用时间外留出集并签发 `OfflineEvaluationReceipt`。paper、live shadow 和各级真实资金运行必须消费前一阶段绑定同一 `ModelBundle` 与合同身份的 PASS 凭据；任何缺失、失效或不匹配都必须阻止进入下一阶段。ModelRegistry 状态、`promotion_stage`、资金授权和执行权限是四条独立轴：模型成为 `active` 也不自动获得任何真实资金授权，每次 small-live / formal-live 仍必须按本文要求单独取得对本次运行有效的用户授权。

  架构图只表达高层业务处理流程，不把权限/硬边界、schema 字段、具体订单状态枚举或产物字段画成独立业务模块。Paper/live 交易图表表达在线运行闭环：`data-collect` 只把外部市场源转化为实时交接、覆盖范围、健康状态/缺口和清单；paper 与 live 各自独立运行 `data-pipeline` 的在线流运行时 / Market Context Builder，消费本机 `data-collect` 实时交接、运行期仅追加日志和本实例账本投影，并调用与训练完全相同的版本化确定性处理核心生成实时市场上下文和模型输入。只有通过相应前序晋级凭据的模型产物才能加载到 paper/live；两者都先把 BUY-only 执行意图交给共用 `Intent Bridge / Execution Adapter`：paper 实例固定选择模拟后端，以 SDK 对齐方法名把模拟领域请求路由到隔离 `sim-system`；live 实例固定选择实盘后端，只通过官方 `Client` 同名方法和 `SignedOrder` 调用真实平台。两类后端返回的原始证据经本实例归一化层（订单 NOE / 非订单经济归一化合同）和 Ledger 后，只回到本运行实例作为下一次交易状态反馈；进入训练只能通过离线反馈入库、训练数据集或校准数据池。

  训练图表表达离线训练与回放闭环：`data-collect` 提供历史外部市场源的类型化原始数据、覆盖范围、最后合格时间、可用性运行诊断、缺失性因果基础和清单；训练只读取清单准入且已经最终化的数据分片，训练轨迹可以从其中切取不对齐完整 UTC 日的时间范围，原始文件、目录、分片和 UTC 日边界的物理顺序不具有跨源时间语义。训练在独立的 `data-pipeline` 离线回放运行时中，由唯一共享语义规范化器建立稳定全序，再按因果可见顺序把历史市场数据与本训练实例的归一化层/Ledger 投影送入同一版本化确定性处理核心，由 `ObservationContract` 因果派生信息年龄/掩码，并生成与 offline-evaluation/paper/live 相同的模型输入。只有离线数据构建侧的标签构建器、`feedback-ingestion`、训练切分和校准数据池可以额外消费权威市场判定结果、达到成熟度的归一化层/Ledger 派生反馈、订单证据摘要/引用和报告。训练模块再消费明确分配给训练/开发/校准/验证的数据，通过同一套 `Intent Bridge / Execution Adapter` 的 SDK 对齐方法面和模拟后端，把模拟领域订单及会话/幂等/因果上下文路由到隔离 `sim-system`；`sim-system` 模拟订单提交到平台后的 `accepted/rejected`、成交、部分成交、不成交、撤单、延迟、maker/taker rebate 应计、日结/付款和结算反馈，再经本实例归一化层/Ledger 用于回放和样本构造。训练运行只能加载冻结的模拟器规则/校准版本；其结果若要影响模拟器，必须经受控离线校准流程生成后续新版本。奖励只能由训练模块唯一的版本化 RewardContract 根据模型动作、本训练实例可回放的账本前后状态、估值和终态证据计算；返佣应计已通过 Ledger/NAV 体现一次，付款只迁移资产分桶，两者都不得再叠加返佣奖励。训练模块生成不可变候选 `ModelBundle`、训练报告和训练批次排名表。训练内表现指标用于诊断和选优，不能因单一或组合表现阈值阻断整个训练批次完成，也不能代替独立的 `offline-evaluation`；训练产物不得跳过 `offline-evaluation` 直接进入 paper/live。训练模块不在 paper/live 在线交易闭环中实时接收账本反馈，也不得让 paper/live 的执行流程直接循环回训练。

  `sim-system` 是训练模块、独立 offline-evaluation 与 paper 交易唯一共用的模拟平台实现；三者必须使用隔离运行实例，并且只能经共用桥接器的模拟后端调用同一套版本化实现、规则合同和产物 schema。training、offline-evaluation、paper、live 共享同一版本化 `data-pipeline` 确定性处理核心以及 Intent/桥接器/归一化层/Ledger 等合同；图中共用公共模块均在模块开头标注“（共用）”。共用表示同一代码实现、schema、配置、归一化产物、时间线、因果截止点、特征/输入合同、SDK 对齐方法词汇和可回放语义，不表示共享进程、后端、队列、检查点、缓存、运行根或账本状态；四个角色必须分别运行各自实例并在创建时固定后端。训练、offline-evaluation 和 paper 只能经模拟后端取得模拟订单提交平台后的反馈；live 当前只能经实盘后端委托官方 Polymarket Rust SDK 发送到平台；若发现 SDK 覆盖缺口，必须停止受影响路径并按本文决策门另行取得用户决定，当前合同不预先授权任何旁路，禁止调用或回退到 `sim-system` 生成或覆盖真实执行事实。`sim-system` 必须由真实平台产物、当前规则证据、潜规则和议题校准。两类平台都必须先保留原始证据，并使用同一版本化归一化层，其中订单生命周期进入 `NOE / NormalizedOrderEvent`，maker/taker rebate 应计、日结/付款和结算/赎回进入非订单经济归一化合同；运行状态严格隔离：`Intent Bridge（模拟后端） -> sim-system -> training/offline-evaluation/paper 归一化层/Ledger`，`Intent Bridge（实盘后端） -> 官方 SDK/真实平台 -> live 归一化层/Ledger`。共享合同不代表共享账本实例，也不允许模拟证据进入 live。

  权限、SDK、钱包、schema、路径、平台约束、模型包、关键写入器等硬边界校验发生在程序启动、自检、热身、T-30 预加载以及运行前/运行中的权限模式校验中，不作为图中的独立业务决策模块。执行时硬边界只能把 `execution_permission` 设为 `ALLOW_SUBMIT`、`NO_SUBMIT_PERMISSION`、`CANCEL_ONLY_PERMISSION` 或 `PAUSED_PERMISSION`，并写入审计/运维记录；不得改写模型交易/风控意图，也不得把硬边界判定当作训练标签或模型执行反馈。Ledger 是账户、持仓、挂单、`pUSD CASH`、返佣应收款、NAV 等状态的权威来源；训练报告、`OfflineEvaluationReport` 和交易报告必须能追溯到市场上下文、模型意图、原始证据、归一化层、Ledger、清单和产物。

  ### 目标仓库结构与构建单元

  以下目录是本文整体获批并进入实施后的目标结构，不是当前工作树现状，也不授权现在创建空目录或空 crate。当前根 `Cargo.toml` 的工作区成员只有 `data-collect`。下列 `./` 统一表示正式本地开发与 Rust 发布构建仓库 `/home/chillchill/btc-updown-marketmaker` 的逻辑项目根；当前 `/home/chillchill/sim-system` 与 `/home/chillchill/model-train` 都只是分支工作树，不得写入部署合同，也不得把 `data-collect` 搬入前者的 `data-pipeline` 子目录。

  ```text
  ./
  ├── Cargo.toml                         # 唯一 Rust 工作区清单
  ├── Cargo.lock                         # 全部第一方工作区 crate 共用的唯一锁文件
  ├── demand/                            # 需求文档
  ├── plan/                              # 获批事项的执行计划
  ├── changelog/                         # 获批事项的变更记录
  ├── mistake-log/                       # 历史问题与证据台账
  ├── third_party/
  │   └── polymarket_client_sdk_v2-0.7.0-p1/ # 当前官方 SDK 基线的仓库补丁；不属于第一方工作区
  ├── data-collect/                      # 既有外部市场源采集 crate 与三个采集二进制程序
  ├── contracts/                         # 窄叶子 crate：无行为的跨模块版本化数据合同
  ├── data-pipeline/
  │   ├── core/                          # 唯一确定性语义核心；无 I/O、网络和 Python
  │   ├── training/                      # 可嵌入 Rust 库 + 可独立验收的薄 CLI
  │   └── online/                        # 未来目标；对应事项获批前不得创建实现占位
  ├── sim-system/
  │   ├── config/platform-rules-v1.toml  # 文档规则、项目/账户规则与冷启动查询合同
  │   └── src/                            # 冷启动官方 HTTPS 只读加载边界 + 无网络确定性模拟核心
  ├── execution/
  │   ├── core/                          # Intent Bridge 方法面与执行领域行为
  │   ├── normalization/                 # 唯一订单/非订单经济证据归一化实现
  │   ├── sim/                           # 模拟后端；依赖 sim-system
  │   └── live/                          # 实盘后端；只委托官方 Polymarket Rust SDK
  ├── ledger/                            # Rust 账本库；状态按运行实例隔离
  ├── model-runtime/                     # paper/live 共用的 Rust 模型推理库
  ├── paper-trade/                       # 纯 Rust 发布二进制程序组合根
  ├── live-trade/                        # 纯 Rust 发布二进制程序组合根
  ├── model-train/
  │   ├── pyproject.toml                 # Python 项目与 maturin 构建配置
  │   ├── uv.lock                        # Python 依赖唯一锁文件
  │   ├── python/model_train/
  │   │   ├── __init__.py
  │   │   ├── envs/                      # 薄 Gymnasium 环境适配
  │   │   ├── reward/                    # 唯一 RewardContract 训练实现
  │   │   ├── trainers/                  # 训练框架与算法
  │   │   ├── export/                    # ModelBundle 导出
  │   │   └── cli.py                     # 训练服务器入口
  │   ├── native/
  │   │   ├── Cargo.toml                 # 唯一 Python/Rust 绑定 crate
  │   │   └── src/lib.rs                 # PyO3 模块；组合 Rust 训练闭环
  │   ├── configs/
  │   ├── tests/
  │   └── deploy/
  ├── offline-evaluation/                # 未来独立组合根；获批前不得创建实现占位
  └── sim-calibration/                   # 未来独立组合根；获批前不得创建实现占位
  ```

  物理目录必须服从以下依赖与所有权规则：

  1. `contracts` 只能保存框架中立、可序列化、可哈希且无行为的跨模块数据合同，例如 `ExecutionMarketEvent`、`LedgerProjection`、执行领域请求/响应和平台原始证据外壳；它不拥有任何生产、排序、撮合、归一化或账本语义，不得依赖其他业务 crate、Python 或官方 SDK。`ExecutionMarketEvent` 的唯一生产语义仍属于 `data-pipeline/core`。
  2. `data-pipeline/core` 是唯一共享确定性语义核心，只依赖 `contracts` 等必要叶子库，只读取 `LedgerProjection` 合同；不得依赖文件系统、网络、Arrow/Parquet 驱动、Python、`sim-system`、`ledger` 实现或任何具体运行角色。`data-pipeline/training` 才拥有清单准入、Arrow/Parquet 读取、离线回放、训练产物和薄 CLI，同时必须提供可嵌入库接口，不能退化为只能启动子进程的 CLI。
  3. `sim-system` 只消费公开合同并输出模拟平台原始证据；它不依赖 `data-pipeline`、Python、Gymnasium、归一化层或 Ledger。`data-pipeline` 同样不得依赖 `sim-system`。二者的动态闭环只能由上层组合根编排。
  4. `execution/core` 定义 SDK 对齐的方法词汇和执行行为；`execution/sim` 与 `execution/live` 分别把模拟领域请求和官方 SDK 请求映射为公共原始证据。`execution/normalization` 是订单 NOE 与非订单经济证据的唯一归一化实现，只消费公共证据合同，不得依赖 `execution/sim`、`execution/live`、`sim-system` 或官方 SDK。模拟和实盘后端不得各自实现第二套归一化器。
  5. `paper-trade` 组合并静态包含本实例在线 `data-pipeline`、`model-runtime`、`execution/sim`、`sim-system`、`execution/normalization` 和 `ledger`；不得包含 `execution/live`。`live-trade` 组合并静态包含本实例在线 `data-pipeline`、`model-runtime`、`execution/live`、`execution/normalization` 和 `ledger`；其依赖闭包不得出现 `sim-system`、`execution/sim`、PyO3、Python 或离线训练专用 Arrow/Parquet 路径。两个主程序都是独立纯 Rust 发布二进制程序；`data-collect-paper-hot` 与 `data-collect-live-hot` 仍是各自同机但独立部署、独立进程的外部市场源交接程序，不得合并进交易主程序。
  6. `model-train/native` 是训练进程唯一 Rust 组合根：它在一个隔离训练会话内组合 `data-pipeline/training` 的可嵌入接口、`sim-system`、`execution/sim`、`execution/normalization` 和 `ledger`。模型动作必须在同一会话内动态经过“模拟执行 -> 归一化 -> Ledger -> `data-pipeline/core` -> 下一 Observation”；不得以预生成未来模型动作、文件轮询、第二套 Python 模拟器或第二套 Python 数据管道替代该闭环。
  7. `data-pipeline/online`、`offline-evaluation` 和 `sim-calibration` 只表示已知的未来独立交付边界；在各自需求、计划与变更记录获批前，不进入当前 Cargo 工作区，也不创建空实现占位。未来 `offline-evaluation` 必须是独立组合根；未来 `sim-calibration` 继续是模拟器版本和校准产物唯一写入器。
  8. 目标结构固定采用进程内嵌入：paper/live 由各自 Rust 二进制程序静态组合共享库，training 由 `model-train/native` 把共享 Rust 库嵌入 Python 训练进程。当前目标不保留本机旁车进程（sidecar）或远程共享服务作为第二种部署方式；若未来进程崩溃隔离或独立升级成为硬需求，必须另立需求并重新评估 IPC、序列化、背压、会话恢复和性能成本。

  `model-train` 的 Python/Rust 边界选择 PyO3 与 maturin：PyO3 是唯一进程内绑定层，只能作为 `model-train/native` 的 crate 依赖；maturin 只作为 `model-train/pyproject.toml` 配置的构建、打包和安装工具，不是运行时桥接器。两者都不得进入 `contracts`、`data-pipeline`、`sim-system`、`execution`、`ledger`、`model-runtime`、paper 或 live 的依赖闭包。maturin 配置必须保持 Python 源码与叶子绑定 crate 分离：

  ```toml
  [tool.maturin]
  manifest-path = "native/Cargo.toml"
  python-source = "python"
  module-name = "model_train._native"
  ```

  `native` 必须作为根 Cargo 工作区成员，与其他第一方 crate 共用根 `Cargo.lock`，不得建立第二个第一方 Rust 工作区或锁文件；被根工作区显式排除的 `third_party` 上游补丁不受此条约束。首版使用常规 GIL 版 CPython，并针对获准训练服务器的实际解释器和平台构建、验证版本匹配的 wheel；在没有完成共享可变状态、全部传递依赖和打包矩阵审查前，不默认启用自由线程版 Python、`abi3` 或 `abi3t`。Python `GymnasiumEnv` 只负责 `reset`、`step`、spaces、`terminated`/`truncated` 和训练框架适配；单次 `step(action)` 必须以粗粒度调用让 Rust 运行到下一决策点后返回紧凑 Observation，禁止逐条市场事件跨语言调用。长时间纯 Rust 计算必须允许 Python 解释器执行其他工作；每个训练工作进程必须创建独立 Rust 会话，不得共享全局模拟器或 Ledger 状态，也不得假定 PyO3 对象可以跨进程序列化。Rust 领域错误必须转换为结构化 `PyResult`，panic 不得作为正常错误路径。

  截至 2026-08-03 的首轮兼容性候选是 PyO3 `0.29.1` 与 maturin `1.14.1`；这是技术审查快照，不是永久追随最新版的要求。PyO3 `0.24.0` 至低于 `0.29.0` 的版本受已公开并在 `0.29.0` 修复的 `RUSTSEC-2026-0176` 影响，不得作为首版候选。进入实现前必须重新查询官方发布说明和安全公告，在本地开发环境与每台获准训练服务器完成解释器版本、SOABI、体系结构、glibc、Rust 最低版本和 wheel 安装矩阵后，才分别把确切 PyO3 解析版本锁入根 `Cargo.lock`，并把 maturin 的构建要求及实际解析版本记录到 `model-train/pyproject.toml`、`uv.lock` 和构建清单；任一门不通过时必须先报告具体缺口，再由用户决定升级环境、选择受支持版本，或改用进程隔离方案。

  ### 模块边界术语

  1. `data-collect` / `./data-collect/` 专指外部市场源采集模块，只负责市场发现/元数据（`market_id`、`condition_id`、`token_id`、开始/结束时间、市场生命周期和运行时角色）以及 Polymarket 市场频道、Binance BTCUSDT 深度、Binance BTCUSDT aggTrade、Polymarket RTDS Chainlink BTC/USD 的原始证据、`recv_time_utc`、类型化原始 Parquet、实时交接、覆盖/健康/缺口/重连、清单和原子发布；不负责模拟器低频平台规则配置、标签、特征、`tick_stream`、训练切分、模型输入加工、paper/live 仅追加日志、订单证据、归一化层/Ledger 或执行反馈入库。当前 metadata v5 没有规则专用列；其 `source_evidence_payload_json` 中自然存在的 Gamma 同名或相似键继续作为采集时点原始证据无损保留，既不改名为模拟规则，也不为节省约 9.3 KB 而删除或重写摘要。maker/taker rebate 与 Liquidity Rewards 必须保持不同语义；固定 Silver `8%` 不要求采集器查询账户等级或 wV。
  2. 当前仓库构建三个二进制程序，但只有 `data-collect-training-collector` 已部署并连接真实外部源。paper/live 真实适配器不属于 `model-train`，保持延期；必须先完成 `model-train`（模型训练模块，不是 `data-collect-training-collector`），之后仍只有在用户决定返回 `data-collect` 时，后续 paper/live 适配器阶段才另立三件套。该阶段的目标合同是分别构建、记录 SHA-256 并独立部署 `data-collect-paper-hot` 与 `data-collect-live-hot`；在此之前二者只有心跳/状态功能。三者不得共享进程、运行根、钱包、交易状态或消费方产物；后续 `data-collect-live-hot` 是独立的外部市场源交接程序，不是 live-trade 主交易程序。
  3. `Market Context Builder` / 运行时上下文构建器，专指 `data-pipeline` 在 paper/live 热路径中的在线流驱动与运行编排层；它消费本机 `data-collect` 实时交接、运行期仅追加日志、归一化层/Ledger 投影和模型包合同，驱动第 4 条的同一确定性处理核心，但不得另行实现特征、归一化或模型输入组装语义，也不能把已最终化 Parquet 当作 paper/live 热路径的事实来源。
  4. `data-pipeline` / `data-processing` 专指 training、offline-evaluation、paper、live 共用的数据处理模块。它必须只有一套版本化、确定性的模型可见处理核心；该核心包含唯一的共享语义规范化器，统一负责把无损解码的数据源证据映射为规范事件，包括字段、单位、枚举、时间戳/因果可用性映射，以及后续全序、同毫秒决胜规则、去重、迟到/缺口、市场隔离、因果持续状态、特征、归一化、Ledger 投影合并和 `ObservationContract` 组装。原始文件枚举、源目录、Parquet 分片和 UTC 日分区的物理顺序不具有跨源时间语义；唯一共享语义规范化器是建立规范全序的唯一归属方，`model-train`、`sim-system` 和验证器不得再排序原始数据或实现第二套业务解释。training 与 offline-evaluation 各自以独立的离线回放运行时消费彼此权限隔离、已被清单准入且已最终化的类型化原始数据分片，paper 与 live 各自以独立的在线流运行时 / Market Context Builder 消费实时交接；驱动层只能负责 I/O、分帧和无损传输解码，不得拥有语义规范化逻辑。四个运行实例不得共享进程、队列、检查点、状态缓存、运行根或 Ledger；只有离线数据构建侧可以额外生成市场监督标签、执行反馈标签、`tick_stream`、回放上下文、训练切分、校准数据池和 offline-evaluation 时间外留出集，且 `model-train` 不得读取该留出集。`ExecutionMarketEvent` 必须保留精确市场身份和运行中动态 `tick_size_change`，但不得携带 `min_order_size`、`feeSchedule`、`itode`、`oas` 或其他模拟器低频字段；模拟器通过市场身份查询自己冷启动快照中的必需规则，未消费的 `oas` 只登记为官方规则查询响应中的未消费字段。奖励不属于 `data-pipeline` 输出，只能由训练模块唯一的版本化 RewardContract 计算。目标结构固定使用进程内嵌入：training 通过 `model-train/native` 嵌入 Python 训练进程，offline-evaluation、paper、live 分别嵌入各自独立组合根；当前不保留本机旁车进程或远程服务作为替代实现。每次构建必须固化由版本与源代码/构建摘要组成的 `pipeline_core_identity`，并同时记录宿主二进制程序或 native wheel 的 SHA-256。
  5. `feedback-ingestion` 专指按来源路由 paper/live/simulation 的仅追加日志、live 产物、订单证据摘要/引用与归一化层/Ledger 输出：paper/simulation 反馈只能进入后续训练数据构建和审计；只有经审查的真实平台/live 反馈被纳入预注册的独立校准切分时，才能作为 `sim-calibration` 的经验校准目标。paper/simulation 证据最多用于回归验证模拟器，禁止用模拟器自身输出或 paper 模拟结果反向校准模拟器。它不是 `data-collect`，也不授权 offline-evaluation 用历史行为策略的执行反馈替代当前候选的评估结果。
  6. 执行边界包含两个不可混同的概念。`Intent Bridge / Execution Adapter` 是 training、offline-evaluation、paper、live 共用的唯一业务执行方法面，公开操作词汇精确为 `post_order`、`post_orders`、`cancel_order`、`cancel_orders`、`cancel_all_orders`、`order`、`orders`、`trades`；模型层的 `submit_buy` 等意图名称可以保留，但不是模拟器公开方法。模拟后端使用模拟领域参数/响应以及会话、账户、幂等、逻辑时间和因果上下文，不接受 `SignedOrder`；实盘后端只使用官方构建器、签名、认证和 `Client` 的同名方法，提交参数为 `SignedOrder`。方法名复用不表示两类后端必须复用完整 Rust 签名或具体类型，也不要求 `sim-system` 实现或伪装官方 `Client`；模拟器内部派发类型不得成为跨模块公开 schema。`sim-system` 则是当前系统对模拟平台模块的唯一规范名称，只接收桥接器的模拟后端路由的 training、offline-evaluation 或 paper BUY-only 模拟领域请求，模拟订单提交平台后的执行、生命周期、最终成功 maker/taker 成交的返佣应计、按日结算和付款，并输出模拟原始证据；应计是不可支出的经济事实，付款只是相同价值从应收款转为现金，二者都不是训练模块额外生成的返佣奖励。每个新模拟运行在业务请求前通过窄型、只读、无凭据的官方 HTTPS 规则加载器，按有界目标市场集合加载一次低频平台规则并密封快照；同一运行恢复只读原快照，加载器随快照密封而释放，确定性核心和运行中事件处理不访问平台。它不是桥接器、模型、训练框架、评估器、paper 运行时、真实平台、归一化层或 Ledger，也不得被 live 交易调用为真实状态源。
  7. `market_lifecycle_state`、`market_runtime_role`、`model_registry_state` 和 `deployment_hardware_status` 必须命名空间隔离：市场生命周期可以使用 `candidate|prefetched|active|retired`；运行时角色使用 `current|next|resolution_pending|none` 等相对值，E 只把 `current` 切到 `resolution_pending`，终态证据才把 `active` 切到 `retired`；ModelRegistry 只使用 `active|shadow|candidate|retired`；`requires_hardware_upgrade` 只能是独立的部署硬件状态或门禁判定，不能成为第五种 ModelRegistry 状态。不得跨域比较裸字符串。
  8. 模型策略态、门禁判定和执行权限必须使用独立字段：模型使用 `model_strategy_mode=ALLOW_SUBMIT|MODEL_NO_SUBMIT|MODEL_CANCEL_ONLY|MODEL_PAUSE`；门禁使用 `gate_verdict`；执行边界使用 `execution_permission=ALLOW_SUBMIT|NO_SUBMIT_PERMISSION|CANCEL_ONLY_PERMISSION|PAUSED_PERMISSION`。`ALLOW_SUBMIT` 允许执行通过前置检查的模型提交/撤单意图；`NO_SUBMIT_PERMISSION` 只允许观察/对账，阻止所有模型来源的平台变更；`CANCEL_ONLY_PERMISSION` 阻止提交，但允许执行明确的模型撤单意图；`PAUSED_PERMISSION` 停止消费模型意图。运维紧急撤单是独立参与方，不因模型权限态消失。不得跨字段比较裸字符串；执行权限只能允许或阻断模型意图，不能把一个意图改写成另一种模型策略。
  9. `account_global_projection` 专指跨市场唯一的 pUSD 现金、预留、待确认成交敞口、不可支出的 `maker_rebate_receivable_pusd`、`taker_rebate_receivable_pusd`、两类独立 UTC 日桶/锁定余数、账户级 NAV 和 Ledger 序号；`market_local_projection(market_id)` 专指目标市场的两个结果持仓、订单列表、待处理撤单和生命周期状态。旧市场的经济影响必须进入账户全局投影，但其订单、持仓和生命周期明细不得混入新市场的市场本地投影。`DayFinalized -> Paid` 的稳定同刻子顺序不得扩展成长期待付款状态或第二套付款状态机。
  10. `offline-evaluation` 专指训练完成后的独立、只读、由产物驱动的晋级评估模块。它加载冻结的候选 `ModelBundle` 与最新合格且完整、从未进入训练或任何自适应决策的时间外留出集；“任何自适应决策”至少覆盖开发/校准/验证、提前停止、HPO、候选排名/选优、归一化拟合，以及人工特征、RewardContract、评估阈值、基线或 `sim-system` 参数/规则设计。该模块使用独立的 `data-pipeline` 离线回放运行时、`sim-system`、归一化层和 Ledger 实例生成 `OfflineEvaluationReport` 与 `OfflineEvaluationReceipt`；不得训练或微调模型、重拟合归一化、改变候选模型包、改写留出集、校准模拟器、写入 ModelRegistry 或直接授权 paper/live。它可以先实现为独立批处理/CLI 任务，不要求成为常驻远程服务，但必须有独立的运行时角色、命令入口、权限和产物根。
  11. `Promotion Controller` 专指消费各阶段 PASS 凭据、用户明确授权证据并签发 `PromotionDecision` / 运行准入的唯一写入器；只有它可以注册或改变 ModelRegistry、推进 `promotion_stage`、为本次运行设置 `funds_authorization_tier`，以及写入凭据/资金授权的仅追加状态事件。每个决策必须绑定唯一决策 ID、`expected_current_stage`、前序凭据/决策摘要和目标状态，形成仅追加审计并保证同一请求幂等；真实资金准入还必须绑定目标环境、公开钱包/出资方/地址身份、稳定且不会随事件变化的 `account_global_ledger_id`、`admission_start_ledger_sequence/digest`，原子检查唯一性并设置 `active_admission_run_id`。唯一性键必须同时满足：同一 `(environment, wallet, funder, account_global_ledger_id)` 跨所有晋级链最多一个未关闭准入；同一晋级链无论账户为何也最多一个未关闭准入。旧运行只有在完成、失败、中断或撤销事件关闭准入后，才能在同一阶段重试。陈旧状态（`stale`）、重复消费、并发竞态、当前阶段/账户/环境不匹配，或任一唯一性键已有未关闭准入时必须拒绝，具体事务实现留给获批后的计划。它可以是后续批准实现中的最小本地控制组件，不是模型、评估器、用户授权来源或交易策略。`model-train` 只能提交不可变模型包及 `TrainingCandidateReceipt` 的注册请求；training、offline-evaluation、paper、shadow/live 运行时和排名表都不得自行跨级、写上述三条轴或给自身签发下一阶段权限。
  12. `UserFundsAuthorizationReceipt` 是 Promotion Controller 根据用户对单次真实资金运行的明确批准生成的不可变、一次性运行准入产物，至少绑定凭据/schema ID、授权 ID、用户批准证据摘要、候选模型包、签发时当前晋级阶段、目标 `funds_authorization_tier`、运行 ID、目标环境、公开钱包/出资方/地址身份、稳定的 `account_global_ledger_id`、目标市场或最大市场数/时长、签发/到期时间和前序凭据；不得写入任何敏感凭据（`secret`），也不得用随事件变化的 Ledger 摘要代替稳定 ID。凭据本体不得原地修改；授权状态只允许 `UNCONSUMED -> BOUND_TO_THIS_RUN|EXPIRED|REVOKED|INVALIDATED`，以及 `BOUND_TO_THIS_RUN -> COMPLETED|FAILED|INTERRUPTED|EXPIRED|REVOKED|INVALIDATED`，所有转换必须由 Promotion Controller 在独立的仅追加 `FundsAuthorizationStateLog` 中写状态事件，并绑定授权 ID、事件 ID/时间、前序状态事件摘要、环境/账户身份、原因和活动准入运行 ID；任一终态不可再转出。该日志是控制面的授权状态权威，不是记录现金/持仓/订单的交易 Ledger。live 运行时必须联合校验不可变凭据、运行准入 `PromotionDecision` 与该日志最新权威事件中的环境/账户身份完全相等；`BOUND_TO_THIS_RUN` 表示不得启动第二个运行，不表示已获准的当前运行当场失权。它不是长期权限；当前运行完成、失败、中断、超时、到期或用户撤销后必须关闭准入、转为不可复用，并立即阻止新的真实提交。
  13. `sim-calibration` 专指 `sim-system` 可校准规则与参数的受控离线校准角色，是 `sim_system_version` 与 `sim_calibration_artifact` 的唯一写入器。它只能消费经审查的真实平台/`feedback-ingestion` 证据、官方规则、用户冻结的项目规则假设和与当前晋级留出集独立的预注册校准切分，输出包含数据源证据摘要、规则/参数、适用市场/版本、代码/配置、验证结果和 SHA-256 的不可变产物，并原子发布新版本；不得读取当前未暴露留出集的结果、训练模型、签发晋级判定或改写正在被 training/offline-evaluation/paper 使用的版本。冷启动查询的 `itode` 不是校准参数；文档规则 `taker_order_delay_ms=250` 只能由版本化平台规则配置提供，校准产物可以记录并验证其规则身份和配置摘要，但不得配置、覆盖或现场修改。改变该文档规则必须先有新需求和用户决策，再发布配置/代码版本并重新验收。新版本只能供后续新轮次选择；发布本身不得改写或自动失效仍绑定旧身份标识的谱系。只有后续轮次采用新身份标识，或权威新证据证明旧规则已不再适用时，才由 Promotion Controller 从 training 起追加受影响凭据的失效事件。

  ## 一、总体原则

  1. 重构必须先定义系统边界，再写实现。
  2. Polymarket 相关提交、撤单、余额、授权额度、用户频道、市场频道等交易与账户能力必须优先基于官方 Polymarket Rust SDK。唯一已批准例外是 `sim-system` 新运行在首个业务请求前，通过窄型、只读、无凭据的官方 HTTPS 规则加载器读取本次运行低频规则；该加载器不得进入执行桥接器、data-collect、DPT 或 live 路径。
  3. 除上述已批准的只读规则加载器外，如果官方 SDK 能力不足，必须先列明具体缺口、影响和可选方案，经用户明确同意并限定缺口范围后，才能考虑原始 REST、手工签名、自建 SDK 或其他旁路实现。本条是未来缺口的决策门，不授权本版实盘执行方法采用任何旁路；当前八个执行方法一旦出现 SDK 覆盖缺口，必须停止受影响路径并另立需求。
  4. 不允许出现第二套影子特征流水线、影子订单解释器、影子账本解释器或影子 SDK。
  5. 数据、model-train、offline-evaluation、paper、live 必须共享同一套市场时间线、订单事件语义和产物合同；training、offline-evaluation、paper、live 的独立 `data-pipeline` 运行时必须调用同一处理核心，禁止离线驱动、在线驱动或 Market Context Builder 形成第二套特征、归一化或 `ObservationContract` 实现。离线标签/奖励的可见权限可以按角色收窄，但其 schema、来源追溯与成熟度语义必须一致。
  6. 交易决策、仓位/风控决策、暂停、不下单和撤销未成交 BUY 订单等策略性动作必须完全由模型输出，或由模型包明确声明的模型状态机输出；live-trade 不得内置第二套策略或风控来改写模型意图。
  7. 当前版本全系统的主动下单动作面固定为 `BUY-only`：模型、训练样本、offline-evaluation、`sim-system`、paper 交易、Intent Bridge、执行适配器和 live 交易只能创建 BUY 意图与 BUY 订单，不得创建、接受、模拟、发送或保留自有 SELL 动作的延后/研究/功能开关/占位路径。外部订单簿的买价/卖价、历史成交方向和平台返回方向仍可作为完整市场事实输入，不属于本系统主动动作。
  8. 需要表达对 `outcome_up` 的反向方向时，系统通过 BUY `outcome_down` 实现；需要表达对 `outcome_down` 的反向方向时，通过 BUY `outcome_up` 实现。这只是方向暴露和持有至结算策略上的对侧替代，不是平台订单、现金流、库存或成本上的完全等价：互补 BUY 会继续占用资金并增加持仓与成本，但以此换取全系统动作、账本、模拟和执行链路的大幅简化。
  9. 模型决策必须由版本化 `decision_trigger_contract` 中明确、可审计且能在 training、offline-evaluation、paper、live 间因果重放的模型可见事件触发，不得由固定频率、固定周期、周期性轮询或固定决策节拍触发。模型可见事件包括：外部市场数据事件；由 `ObservationContract` 明确准入并进入共享规范事件带的数据源覆盖范围/已确认缺口证据；NormalizedOrderEvent / 账本状态变化；市场预加载/开始/结束与 GTD 到期事件；提交响应、用户频道、撤单响应或结算证据实际在客户端变为可见的事件；由上一轮模型或模型包调度输出因果安排的稀疏一次性 `DecisionWakeEvent`；以及仅在冷启动/恢复后存在应求值范围且没有可恢复有效待处理唤醒时，按第 4 项产生一次的 `DecisionBootstrapEvent`。采集器的数据源本地传输/订阅恢复状态与运维诊断不是模型触发条件；每日清单中的 `collection_day_coverage` 也只在已有模型可见事件、`DecisionBootstrapEvent` 或 `DecisionWakeEvent` 求值时参与已知/未知掩码计算，不自行创建触发条件。没有逐事件类型化证据时，不得由在线运行时临时暴露具体传输原因，也不得由离线运行从操作计数猜测或伪造。仅由时间推移产生的 `source_information_age` / 缺失性掩码变化也不会自行创造另一个触发条件，只在上述求值时按当前逻辑时间重算。平台内部“撮合延迟结束”本身不是模型可见事件，不得在相应证据到达前触发模型；本地时钟持续推进本身也不会自动调用模型。每个有效模型可见事件都必须先更新该市场的持续因果状态，再按冻结的合并与排序合同触发模型交易、风控和下一次调度评估；事件触发不等于只向模型输入当前孤立事件。
  第 9 项中的外部市场数据事件，也包括独立 data-collect 事项已批准、由共享语义规范化器从 `binance_aggtrade_v7 record_kind=invalid_input` 生成并经 `ObservationContract` 准入的版本化无效数据源事件；类型化原始数据中的原始字节、哈希、解析器诊断或审计详情不得直接进入模型。该事件必须使用采集器接纳坐标，而非 Parquet 物理行序或单独接收时间戳进入规范全序。
  10. live-trade 承载本运行实例的共用 `Intent Bridge / Execution Adapter` 实盘后端，负责编排本实例 `data-pipeline` 在线流运行时 / Market Context Builder 与 Model Runtime、校验 BUY-only 模型请求，并由实盘后端唯一委托官方 SDK 构建/签名 `SignedOrder`、调用官方 `Client` 同名方法；真实平台返回/成交/拒单/撤单及非订单经济证据再送入归一化层/Ledger并记录派生状态。live-trade 在权限、协议、schema、钱包、路径等硬边界上失败关闭，但不得生成替代交易决策、调用模拟后端或另行实现 SDK 客户端、特征、归一化或 `ObservationContract`。
  11. 本文中的“真实平台”专指共用桥接器的实盘后端连接的 Polymarket/SDK/市场频道/用户频道/提交响应/撤单响应；“模拟平台”统一称为 `sim-system`，专指 training、offline-evaluation 和 paper 经共用桥接器的模拟后端调用的订单提交后反馈模拟模块。`sim-system` 只能用真实平台产物与版本化规则证据校准，不得在 live 高频热路径中作为账户、订单或风控状态源。
  12. 所有关键结论必须有产物、清单、schema、日志或测试证据支撑，不能只靠标准输出或人工判断；用户明确冻结的项目规则假设必须与官方公开事实分开标记来源追溯、版本和适用边界。

  ### 训练采集的上游退化、持续运行与缺失数据合同

  本节关于进程连续性和模型学习缺失数据的要求适用于全部 training 外部源。普通业务静默或 `stale` 状态保持中性，不生成缺口、告警或采集器退出；经正向分类的传输层故障由相应数据源归属方在同一 PID 内持续恢复。当前代码已经实现一个精确例外：training RTDS 在曾收到合格新鲜数据后，稳定期连续静默 300 秒，或完整运行时替换后 60 秒内仍未收到首条新鲜数据，都会触发先断后连（`break-before-make`）的完整运行时替换；收到首条新鲜数据前不启用该监视器。paper/live 真实适配器不属于 `model-train`，保持延期；必须先完成 `model-train`（模型训练模块，不是 `data-collect-training-collector`）；之后仍只有在用户决定返回 `data-collect` 时，其连接恢复、执行权限和运行约束才另立三件套，不得从本节直接推导部署授权。

  1. 必须把上游可用性/缺失性事实与本地完整性故障分开。DNS/TCP/TLS/WebSocket 连接失败或断开、心跳超时和限流，是有直接证据的数据源可用性退化；服务端无数据、业务消息静默、持续 `stale`、单个源不可用或全部外部源同时不可用，是训练必须保留的正常上游可用性/数据不完整形态。有直接证据的传输层/恢复状态形成采集器可见的运行诊断，陈旧载荷等类型化数据源证据按其自身合同保留；没有新业务消息本身不产生采集器事件，只能在已知覆盖范围内，根据最近合格时间与当前逻辑求值时间因果派生信息年龄/掩码。传输层/订阅诊断只有在未来通过新的类型化证据与 `ObservationContract` 决策门，并且能跨运行时重放后，才可进入模型 Observation 或成为触发条件。上述上游形态都不得仅因持续时间变长而升级为缺口判定、告警、训练运行失败或采集器进程退出条件；没有数据源节奏、序号缺口或上游证据时，只能表达已观察静默/新鲜度中断，不能虚构已确认的消息缺失。无法验证的解析器/schema 载荷、写入器/清单/原子发布失败、磁盘或资源硬边界、内部状态不变量破坏等不属于该豁免，仍按其受影响范围隔离或失败关闭。
  对第 1 项最后一句作已批准的精确取代：上限内完整、能够形成并接纳版本化证据的普通外部 aggTrade 无效应用输入，不是本地解析器/schema 终态；它必须逐条保存在既有 aggTrade 产物的 v7 判别联合中，不伪装成成交并继续运行。解析器实现故障、证据编码/队列接纳失败、类型化 schema 内部不变量、写入器/清单/原子发布、序号溢出和所有权丢失仍按受影响范围隔离或失败关闭，其他数据源的解析器/schema 合同不因该子边界放宽。
  2. `data-collect-training-collector` 在本地写入器、清单、schema、路径、磁盘和资源前置条件成立时必须能够启动并持续运行，不得把外部源当前可达或当前有业务数据作为进程存活前置条件。任一源退化时，其他健康源必须继续采集、分区、写盘和发布事实；全部传输层不可达时，进程以有直接证据的退化状态保持存活并继续逐源恢复；传输层按协议存活但全部业务流静默时，进程保持活动/空闲状态，并保存覆盖范围与可空的最近合格时间。当前传输层事实只存在于数据源本地运行态及有界运维日志，不进入每日清单，也不得虚构退化或静默状态转换。连接类恢复只能在同一采集器进程的数据源本地传输层归属方内进行；禁止依赖整进程退出、systemd 新 PID 或重新初始化全部健康源来实现单源恢复。
  3. 普通业务静默、持续 `stale` 或服务端暂时没有业务数据，只保留中性业务可用性与下游缺失性因果基础，不得调用订阅/退订、改变期望订阅修订版本、主动切换连接代次或主动断开仍然存活的连接。唯一的当前窄例外是上段 training RTDS 的 300 秒/60 秒完整运行时替换；它不泛化到 CLOB/Gamma/Binance。经正向分类的 DNS/TCP/TLS/HTTP/WebSocket 关闭、EOF、读取/写入/协议错误、心跳超时、限流或服务端关闭，才进入相应数据源的传输层恢复；每次连接、请求、读取、退避、并发和资源必须有界，但总耗时、总尝试次数或熔断器耗尽状态不得结束数据源或采集器。运维方明确停止和第 1 项本地完整性硬失败除外。
  4. 各源共享第 1--3 项的结果与错误分类合同，但保留源特定协议：RTDS/CLOB 使用各自 SDK 的连接/订阅归属方，Gamma 使用 HTTP 市场发现，Binance 深度保留快照与更新 ID 拼接，Binance aggTrade 只按 WebSocket 真实到达顺序因果直录。深度继续由现有 `reqwest + tokio-tungstenite + Tokio + Serde` 和每个数据源唯一的直接 WebSocket 归属方实现；每个成功代次只有一个 WebSocket 读取器、至多一个活动快照 `Future` 和一个按行数/字节数有界的缓冲区，不引入 Binance SDK、市场数据引擎、第二归属方或第三深度产物。aggTrade 原始 `a` 必须保留，但数值前跳不证明缺失，不得触发 REST 回补、待处理、合并、缺口、主动重连或退出；连接窗口中未实际收到的数据保持缺失。只有适用 Polymarket 订阅代次的数据源，才在真实新代次单次回放最新完整的期望订阅集合；不得把 RTDS 的代次、订阅或恢复实现机械复制给其他数据源。Polymarket 继续遵守 SDK-first；同一连接重复订阅的引用计数/过滤限制只作为“不采用业务静默刷新”的证据，不产生新的强制刷新 SDK 补丁。长期循环不得令任务、文件描述符、`socket`、队列、`watch` 通道、`actor` 任务、接收器、深度快照/缓冲区或订阅引用计数单调增长。
  5. 采集器必须保留类型化原始数据的接收时间来源追溯、最近合格业务/新鲜数据接收时间、真实相邻事件空档，以及每日唯一清单中顶层、单归属方、版本化的 `collection_day_coverage`；六个既有数据源计数器继续只记录产物首末接收事实及既有操作计数，不复制覆盖范围。该覆盖范围只表达全部必需生产者、写入器及协调器归属的队列/接收器/入站路径共同受监督的观察资格，不表达传输层/订阅健康或业务节奏；从未启动、全部已就绪但零行、已知结束与非正常未知结束必须可区分。当前日恢复前必须取得并持续持有数据集根目录级、由操作系统保障的独占实例租约，相同身份标识不能代替旧进程退出证明；协调器建立队列/接收器/入站前置条件，六写入器与四个生产者包装器再就绪并阻塞共同门禁，归属方只执行一次不重试的激活状态变更；原子提交决定持久状态、最近启动时间与 `reactivated_within_day`，确认只决定能否轮询网络和释放原始数据许可。当前日从 `not_started|active|unclean_end_unknown|ended_known` 转为 `active` 时，必须分别准确更新首次/最近/结束时间与布尔值：首次激活写 `false`，任一同一 UTC 日内的后续激活写 `true` 且当日保持单调，非正常结束、已知结束或对账不改变该值；顶层已关闭、身份标识不匹配或非法嵌套均拒绝。原始事件按 UTC 日/激活代次许可进入最终队列；本节“已接纳”只指已取得该许可并把所有权转移到共同有界处理中、队列或延迟准入的事件，仍位于深度代次本地待处理缓冲区的更新尚未接纳。可靠运维方停止或运行截止时间关闭共同接收入口后，新的未接纳事件不得再取得准入，未接纳深度待处理项可以清空且不生成行、缺口、告警或终态；截止前已接纳事件必须排空/最终化。`CapacityLimit`、写入器/清单、队列所有权、解析器/schema 内部不变量及其他终态不能套用该豁免。跨日切换、停止与各必需终态对可靠的已知关闭候选采用先关闭者优先，但候选只有在必需栅栏/会合点确认无非协调失败后才能最终写 `ended_known`；此前无法由已完成边界栅栏证明只发生在旧日覆盖范围结束之后的未知先后 `panic`、中止或 `join` 失败，必须以非正常状态优先。协调中止不能只靠代次/原因字符串，必须由本次关闭的精确一次性通道确认，或预登记并由同一句柄 `JoinError::is_cancelled()` 精确消费的任务/代次/关闭 ID 许可证明；`panic`、错误/缺失许可或无法证明因果的退出仍为非正常。没有待处理退出的跨日切换必须先关闭旧原始数据代次、排空并收齐栅栏/最终化确认，再在最终写入中保存旧嵌套结束并关闭旧顶层，随后创建新日活动状态提交，确认后才释放。若可靠停止/终态在逾期边界之后、旧日尚未最终关闭，独立的 `PendingExit` 必须使旧日按该边界已知关闭；普通路径的新日只能处于已打开/未启动状态，且不得激活或恢复；`M8` 边界停止时连新日未启动清单也不创建。旧日最终关闭提交前，无法由栅栏排除的非正常情况使旧日状态为未知；旧日已关闭而新日激活提交尚未发生时的失败绝不重写旧日，新日保持缺失/未启动且布尔值不变；激活提交后的状态、最近启动时间与布尔值已经归属新代次，即使确认丢失也不得回滚，确认仅控制释放。旧日关闭后收到声称早于旧栅栏的失败报告，属于栅栏完整性错误。`PendingExit` 必须在代次切换前保存并消费，不能作为旧代次迟到命令丢弃。每日清单不持久化传输层范围/原因/当前打开状态摘要或时段，也不保存本地运行分段。类型化原始数据和已经产生的 `Observation` 不回写、平滑或伪造；`reactivated_within_day=true` 时，最近启动时间之前无法证明连续覆盖范围的区间不得生成要求四运行时一致性的历史模型 `Observation`，不能用日终重启事实倒灌未知掩码；任何非正常共同尾界只能是最近启动时间，不能用某个健康数据源的更晚行证明其他必需成员存活。不得用 `PING/PONG` 填充业务覆盖范围、用陈旧值冒充当前，或因后续恢复抹去既有空档。当前业务目标不要求精确传输层时段产物，也没有默认建设计划；只有未来明确要求模型按具体传输层/订阅原因采取不同策略并证明现有事实不足时，才可新建类型化可用性产物/schema 与新的 `ObservationContract` 决策门。
  对第 5 项中“共同有界处理中/队列/延迟准入”的旧合称作已批准的精确取代：共同处理中预留只提供内存上限；只有事件取得对应原始数据日许可并把所有权转移到有界队列时才成为已接纳事实。尚无新日许可的跨日 `DeferredRawBatch` 只是有界的接纳前暂存，不得提前分配采集器接纳；正常跨日切换在新日激活后按暂存顺序排空、取得许可与队列所有权并发布关联缺口副作用。可靠运维方停止/运行截止时间先获胜时可以清空该暂存及深度代次本地待处理项，且不生成行、缺口、告警、`dropped_rows` 或终态；已经取得许可及队列所有权的事件仍必须排空/最终化。本取代不改变 `PendingExit` 禁止伪造新日激活的合同。

  6. `data-pipeline` 必须在每个模型求值时刻因果计算并编码：`source_information_age = decision_logical_time - last_qualifying_event_available_time`、保留值的 `value_age`、`fresh|stale|missing|invalid|unknown_not_observed` 掩码、已确认缺口的独立证据，以及恢复后的首条合格 `fresh` 事件。掩码分类及任何信息年龄边界属于版本化 `ObservationContract` / 处理流水线配置，不属于 `DecisionScheduleContract`；`missing` 只表示 `known-observed` 覆盖范围内按冻结规则缺少合格信息，不等于已确认缺口、具体缺失条数或传输层原因，也不得控制订阅/重连。精确信息年龄只允许在 `known-observed` 覆盖范围内产生；数据源启动后尚无合格值时必须使用 `no_qualifying_value_yet + known_active_elapsed`，覆盖范围不明时不得伪造最近新鲜时间、缺失性开始或缺失条数。`confirmed_gap_age` 只适用于 SDK `Lagged(n)`、有界队列丢弃、Binance 深度更新 ID 缺口等具有直接合同证据的确定性缺失，不能与普通业务静默或 aggTrade 的 `a` 数值前跳混用。training 必须保留并因果重放已知覆盖范围内的真实缺失性，不得仅因上游无数据、网络不稳定或一个/多个源缺失而删除样本、停止训练或把数据集判为结构不完整；本地采集器覆盖范围无法证明的区间必须保留类型化原始数据，但隔离出要求四运行时一致性的 `Observation` 回放，这不是把外部缺失性样本删除。这里的“完整”指产物、schema、清单、来源追溯、因果时间和缺失编码完整，不要求每个源在每个时刻都有行情。training 还可以通过预注册、版本化且有来源追溯的场景产物屏蔽相应源事件或注入中性可用性事实，不得改写采集器类型化原始数据、伪造市场价格，或把“这是注入场景”的事后标记泄漏给模型。
  7. 面对模型可见的数据源退化，暂停交易、`MODEL_NO_SUBMIT`、撤销未成交 BUY 订单以及 BUY 对侧结果侧中和已有方向风险，都必须由模型或模型包状态机决定；采集器、`data-pipeline`、live-trade 或执行门禁不得替模型生成这些策略动作。这里的“风险中和 BUY”不是 SELL 或真实减仓：它继续占用 pUSD、增加总份额与总成本，只能在模型基于当时可见账本/市场状态判定能降低既定方向风险时作为 `BUY-only` 意图输出，`sim-system`、NOE 与 Ledger 必须按真实双侧持仓和现金变化记账。

  对 aggTrade v7 而言，无损类型化证据必须同时保留数据源任务 `epoch`、WebSocket 连接代次、原始接收坐标与采集器接纳坐标：原始坐标用于审计真实输入先后，接纳坐标定义模型实际因果可见顺序和同毫秒决胜规则。WebSocket 完整文本帧在任何应用层解析前冻结入站序号；合法重复项仍按结构去重，不产生类型化/模型证据，但不回收真实到达时已消耗的编号，该编号空洞只能由有界结构去重计数解释，不能当作数据源缺失或模型事件。每条合法 WebSocket 成交都不得因原始 `a` 数值关系等待：当前日事件按接收顺序进入有界队列并取得准入；跨日激活未完成时遵循第 5 项精确取代的接纳前暂存边界，正常排空后才取得准入，受控清空时保持缺失。`trade 100 -> trade 103 -> invalid -> trade 104` 不得等待、回填或回排。数据源序号 `epoch` 在同一数据源任务跨重连/UTC 跨日切换时保持稳定，重启后更换；每次成功 WebSocket 连接分配经检查的非零 `connection_generation`，同一连接的全部成交/无效输入共享该值，重连后严格增加。新写入流程不得调用 aggTrade REST 回补或产生 `REST-only`/`merged` 行；既有可空 REST 列、旧版 v5/v6 历史 REST/merged 行和旧清单连续性字段只读兼容，不参与当前运行控制。旧版 v5/v6 不得伪造当前 v7 已删除或新增的字段，也不得被宣称为当前新写入 schema。队列接纳后、持久发布前异常退出的尾部仍由 `unclean_end_unknown` 界定，不能用已分配序号冒充已发布产物。

  对 Binance 深度而言，独立 `data-collect` 事项已批准并实施：继续使用两个既有产物，并升级为 `binance_depth_snapshot_v3` 与 `binance_depth_update_v4`。二者共同保存数据源任务 `epoch`、连接代次、共享域深度入站序号及采集器接纳序号/时间：入站序号记录采集器完整观察 REST 快照响应或 WebSocket 更新的真实顺序，准入才定义该类型化事实对下游的因果可见时点。快照待处理期间，唯一 WebSocket 读取器继续消费并把更新放入单一有界缓冲区；可用快照必须先接纳，缓冲更新再按入站顺序分类和接纳，禁止把快照返回后才知道的拼接结果回写成更早的模型可见事实。正常运行中的快照最终不可用、`Close`/`EOF`、普通重连、UTC 跨日重置或代次放弃时，结构可验证的缓冲更新仍以中性 `not-applied` 判定有界释放，不冒充连续性就绪或缺口；可靠运维方停止/运行截止时间关闭共同接收入口后，尚未取得共同有界接纳所有权的待处理项可以清空，并在四个运行时中一致保持缺失，已接纳证据仍须排空。`stale`、重复及 `confirmed-hole` 更新都保留原始数据行，但只有满足首桥 `U <= lastUpdateId + 1 <= u` 或后续连续谓词的更新可以推进快照锚点/`local_u` 连续性；缺口行不得在新快照后改写或应用。采集器原样保留 `bids/asks` 与 `b/a`，但本阶段不维护价位级订单簿；价位簿重建仍属于后续唯一共享处理核心。旧版快照 v2/更新 v3 只读为排序未知，不得伪造新坐标。该批准只冻结采集器证据与交接可承载语义，不在本文中实现共享规范化器、Observation、模型订单簿或调度逻辑。

  ### 稀疏决策调度与离散 `next_wake` 目标候选

  1. 当前唯一训练框架内，把“模型或模型包状态机输出有界离散 `next_wake_bucket`”作为目标候选；这表示共享运行时、模拟器、Observation、输出、奖励、产物和一致性合同必须能够按同一预注册条件支持并验证该候选，不表示它已经成为胜出者、`active` 模型或获准实施/部署。确定性的单次信息年龄跨阈值、风险承载状态下有界重新设定唤醒（`re-arm`）的调度器保留为同合同下的基线：每次真实事件或唤醒后都按预注册算法/配置重新计算下一截止时间，单次跨阈值已经发生不能令仍有风险的范围永久失去唤醒；有限多级截止时间可作为预注册对照；连续延迟、危险率（hazard）、Neural ODE/CDE 事件函数仍是当前框架内的后续研究候选，未获当前实现授权。
  2. 共享模型输出必须把 `trading_risk_intent` 与 `decision_schedule_intent` 分成两个命名空间。目标候选的后者至少包含 `next_wake_bucket`、由运行时提供且模型只能回显的 `decision_scope_id`，以及父 `decision_id`；模型不得创建、改绑或任意选择范围。档位集合、最小正间隔、最大允许间隔、量化/舍入、显式 `NO_PENDING_WAKE`（若允许）和适用状态必须由版本化 `decision_schedule_contract_version` 冻结，不得复用旧网络恢复的 `3s/6s` 数值。`next_wake` 不是 BUY/SELL 交易动作，不改变 BUY-only 动作面；无法识别的档位编码、越界、在合同要求有限档位的状态中缺失，或在不允许 `NO_PENDING_WAKE` 的状态选择该值，均属于当前运行时范围的合同失败：training 使对应试验/候选无效，offline-evaluation 拒绝签发凭据，paper/live 进入对应执行权限的失败关闭状态；所有范围都必须审计，调度器不得自动生成撤单、暂停或对侧 BUY。
  3. `decision_scope_id` 必须来自版本化、有限且由运行时唯一管理的范围注册表；`decision_schedule_contract_version` 必须冻结范围种类、创建/退役条件、模型可见事件到受影响范围的映射，以及单个运行时的活动范围和待处理定时器总上界。每个 `(model_runtime_instance, decision_scope_id)` 同时最多存在一个待处理唤醒；范围退役时必须取消定时器。模型可见的业务、账本、市场生命周期或经合同准入且可重放的数据源覆盖范围/已确认缺口事件在唤醒前到达时，只抢占合同映射到的受影响范围：先更新持续状态、使旧定时器代次失效并立即重新评估，新输出原子替换旧定时器。仅采集器可见的传输层/订阅诊断不抢占模型定时器。`DecisionWakeEvent` 到达时只提供一次新的模型决策机会，不直接执行策略动作。相同逻辑时间的真实/账本/生命周期/数据源证据事件与定时器必须采用冻结的稳定排序，并避免对同一范围/时间重复求值；重启/检查点恢复后不得重复、遗漏或复活已失效定时器。
  4. 在活跃市场、活动订单、待处理提交/撤单、未结算持仓或其他冻结的风险承载状态存在时，调度输出必须选择有限档位，不能选择 `NO_PENDING_WAKE` 或永久 `WAIT_FOR_EVENT`；无风险且没有活动决策范围时是否允许显式 `NO_PENDING_WAKE`，由同一调度合同冻结。冷启动完成因果状态引导且存在活动决策范围，或恢复后处于风险承载状态但没有可恢复的有效待处理唤醒时，运行时必须按版本化触发合同产生恰好一次 `DecisionBootstrapEvent`，使模型/基线生成首个调度意图；若检查点已有有效待处理唤醒，则只恢复它，不再生成引导事件；已过期的待处理唤醒按第 7 项只触发一次迟到唤醒。最小正间隔必须阻止近零延迟忙循环/Zeno，最大间隔只保证模型再次获得决策机会，不代表缺口、告警、订阅刷新、重连或自动风险动作。禁止的是运行时脱离上轮调度意图自行启动外生固定周期；模型或预注册基线在连续决策中因果输出相同有限档位本身不构成固定决策节拍，但每次都必须有独立父决策和定时器审计。
  5. 时间戳可以具有微秒或更高表示精度，但表示精度不产生同频率模型调用。training/offline-evaluation 使用确定性虚拟逻辑时钟，paper/live 使用单调时钟调度并另记 UTC 审计时间；两者运行同一调度状态机。不得每微秒、每毫秒或按其他固定网格物化“时间 + 空市场 + 空账本”上下文、空 `token`（空序列元素）、密集空操作样本或策略决策。每次稀疏触发都读取截至该因果截止点的完整持续市场/账本/数据源状态。
  6. `next_wake` 会改变下一次观察和决策机会，训练与 Model Runtime 必须按可变持续时间/SMDP 语义处理：状态转移记录真实经过时间 `delta_t`，奖励覆盖该区间内的账本、估值、风险与终态事实；若使用折扣，必须按冻结的经过时间合同计算，不能对不同持续时间机械使用同一每事件折扣。`sim-system` 只按全序推进该区间内的平台事件，不消费 `next_wake`，也不计算奖励。真实事件提前到达导致的定时器抢占必须记录为被抢占/删失的调度结果，不能伪称原档位已完整执行。训练合同还必须使用可解释的实际推理成本、决策次数约束或等价资源目标，避免“更多免费观察”令候选恒选最短档位；具体成本/约束必须预注册并进入当前训练比较合同，不得事后调节。
  7. `decision_schedule_contract_version` 必须冻结档位的 `base_logical_time` 来源、`effective_due_logical_time` 计算、逻辑时间到在线单调时钟截止时刻的映射、推理完成时截止时刻已过期的处理，以及检查点字段与恢复规则。检查点至少保存待处理范围、基准/到期时间、档位、父决策和定时器代次；恢复时未过期定时器继续等待，已过期定时器只产生一次带迟到量的 `DecisionWakeEvent`，不得补发多个追赶唤醒或启动周期循环。每次调度必须记录父决策/触发条件、决策范围、建议/生效档位、基准/生效的计划逻辑时间、实际触发时间、迟到量、定时器代次、创建/取消/替换/恢复原因、抢占事件、模型/模型包/配置身份标识和最终触发事件 ID。offline-replay、offline-evaluation、paper、live 必须使用同一定时器创建、抢占、排序、恢复与审计 schema，并验证在相同输入、相同模型包和相同初始状态下，决策次数/顺序、触发条件和 Observation 一致；在线调度延迟必须显式记录，不能被伪装成零延迟或回排到较早时间。

  ## 二、新系统流程要求

  整个项目只支持 Polymarket Global CLOB 的 `btc-updown-5m` 完整市场生命周期，并显式区分 `active` 市场与 `prefetched next` 市场，不保留其他市场类型的兼容或扩展路径。市场系列、窗口、条件和代币身份由 `data-collect` 与后续规范事件合同限定；`sim-system` 不重复解析 slug 或建立第二套市场系列准入判定，只允许冷启动规则加载器按已绑定 `condition_id` 查询官方 CLOB/Gamma 规则并与上游身份交叉校验，不能据此发现、扩大或重新准入市场。未来若改变这一市场类型范围，必须另立新需求并由用户决定。

  模型生命周期必须固定为：`model-train 完成并冻结 candidate -> offline-evaluation PASS -> paper trade PASS -> live shadow / NO_SUBMIT_PERMISSION PASS -> 用户逐级授权 small-live 1 个市场 -> small-live 3 个市场 -> formal-live 1 小时`。任何模型、数据切分、评估器、paper/live 运行时、ModelRegistry API 或人工操作都不得绕过前置阶段；每次真实资金运行仍受独立用户授权、资金授权档和执行权限共同约束。

  1. 自检阶段：
     - 启动时必须先冻结 `runtime_role=data_collect_training|data_collect_paper_hot|data_collect_live_hot|training|offline_evaluation|paper_trade|live_trade|promotion_controller|sim_calibration`、对应二进制程序身份标识和本次实际依赖，再执行该角色自己的自检合同；三种 `data_collect_*` 角色不得与四个模型生命周期消费者/交易角色共用同一进程身份，training 与 offline_evaluation 也不得共用运行实例或产物写入器身份；Promotion Controller 与 sim-calibration 都可以是短生命周期 CLI 任务，但必须使用各自独立写入器身份标识。
     - 共用检查只覆盖该角色实际依赖的数据源、市场发现、当前/下一个市场、代币 ID、最小价格变动单位、最小订单数量、手续费/返佣、奖励资格、schema、关键写入器、磁盘和资源。
     - `data_collect_training` 检查四个外部源的配置与逐源恢复能力、类型化原始数据写入器、覆盖范围/可用性诊断、最近合格时间/缺失性因果基础、清单、磁盘和采集服务器 CPU/RAM；外部源在自检时传输层不可达只能令对应源进入有证据的退化状态并持续恢复，传输层存活但暂时无业务数据只形成空闲/零行业务事实，两者都不得阻止采集器启动。`data_collect_paper_hot`/`data_collect_live_hot` 检查各自服务器上的四个外部源、实时交接帧/`socket`、心跳/状态、数据源缺口/重连/背压、二进制程序/路径身份标识和本机资源。三者都不得要求模型包、`sim-system`、NOE/账本、钱包、出资方、真实用户频道或训练 GPU。
     - `training` 检查分配给训练/开发/校准/验证的已最终化数据集/清单、`data-pipeline` 离线回放运行时、共享处理核心及冻结归一化产物、共用桥接器的模拟后端、`sim-system`、归一化层（订单 NOE / 非订单经济归一化合同）/Ledger、训练 CPU/GPU/RAM 和训练产物写入器；桥接器必须暴露 SDK 对齐方法面且不得加载钱包、签名或网络路径，不得拥有 offline-evaluation 时间外留出集的读取权限，也不得要求真实钱包、出资方或 live 用户频道。
     - `offline_evaluation` 检查不可变候选 ModelBundle、时间外留出集及其不重叠/成熟度/覆盖范围清单、独立 `data-pipeline` 离线回放运行时、共享处理核心及模型包内冻结归一化产物、共用桥接器的模拟后端、`sim-system`、归一化层（订单 NOE / 非订单经济归一化合同）/Ledger、评估合同和报告/凭据写入器；桥接器不得加载钱包、签名或网络路径，也不得加载优化器、修改权重、重拟合归一化、要求真实钱包/出资方或写入 training/ModelRegistry 产物根。
     - `paper_trade` 检查来自独立 `data-collect-paper-hot` 进程的实时交接、`data-pipeline` 在线流运行时 / Market Context Builder、共享处理核心及冻结归一化产物、模型包/运行时、有效 `OfflineEvaluationReceipt`、共用桥接器的模拟后端、`sim-system`、paper 归一化层（订单 NOE / 非订单经济归一化合同）/Ledger 和审计写入器；桥接器不得加载钱包、签名或真实网络路径，也不得要求真实钱包/出资方。
     - `live_trade` 检查来自独立 `data-collect-live-hot` 进程的外部市场实时交接、`data-pipeline` 在线流运行时 / Market Context Builder、共享处理核心及冻结归一化产物、前序资格凭据、真实用户频道、共用桥接器的实盘后端、官方 Rust SDK 能力、模型包/运行时、live 归一化层（订单 NOE / 非订单经济归一化合同）/Ledger、关键仅追加审计写入器、部署路径和本机 CPU/RAM；实盘后端必须只委托官方 `Client` 同名方法且不得包含或回退到 `sim-system`，不得绕过交接再建第二条外部市场数据源，也不得要求训练 GPU。live shadow 必须固定为 `NO_REAL_FUNDS + NO_SUBMIT_PERMISSION`，不得要求或加载可提交钱包/出资方；真实资金运行才额外检查本次运行有效的 `UserFundsAuthorizationReceipt`、钱包/出资方、对应资金档和提交/撤单/对账能力。
     - `promotion_controller` 检查唯一写入器权限、ModelRegistry/当前阶段、仅追加决策/凭据状态/`FundsAuthorizationStateLog` 写入器、凭据 schema/摘要、最新消费/撤销/失效事件、`active_admission_run_id` 和用户授权证据；不得加载训练优化器、模型推理、钱包密钥或交易提交/撤单能力，也不得替阶段运行时生成结果凭据。
     - `sim_calibration` 检查唯一校准写入器权限、预注册校准切分与真实平台证据来源追溯、当前晋级留出集不可达、版本/规则/参数/代码/配置摘要、验证与原子发布写入器；不得加载模型优化器/钱包、读取当前晋级结果或就地改写任何运行中模拟器产物。
     - 自检失败只能阻止对应组件或运行角色进入自身热路径；training 专用、offline-evaluation 专用、paper 专用、live 专用或 `data-collect` 专用依赖不得互相阻断。共享合同本身不一致时，也必须分别记录每个受影响角色的判定和影响。

  2. 热身阶段：
     - 按运行时角色验证热身合同：三种 `data_collect_*` 二进制程序只验证各自外部市场数据源与写入器/交接；training 与 offline_evaluation 分别在隔离回放运行时通过共用桥接器的模拟后端验证 T-30/引导、两个结果侧的模拟 BUY 提交/撤单/成交、全部八个 SDK 对齐方法中适用的方法、`sim-system` 响应、归一化层（订单 NOE / 非订单经济归一化合同）/Ledger，其中评估器仍只读候选；paper_trade 在在线输入下验证同一模拟闭环。上述三类模拟热身不得加载钱包、签名或真实交易网络路径；唯一网络例外是新运行在业务请求前通过窄型、只读、无凭据的官方 HTTPS 规则加载器读取公开平台规则并密封快照，密封后立即释放网络能力。live shadow 通过共用桥接器的实盘后端，只验证只读/禁止提交模式下的用户频道、官方 `Client` 查询/对账方法、归一化层（订单 NOE / 非订单经济归一化合同）/Ledger；只有持本次运行有效用户授权的 small-live/formal-live 才能在授权范围内由实盘后端验证真实 BUY 提交/撤单/成交。不得让 `data-collect` 二进制程序承担订单热身，也不得把 SELL 路径作为当前版本热身项。
     - 所有角色都必须记录 `runtime_role`；只有 paper/live 角色记录 `funds_authorization_tier=NO_REAL_FUNDS|SMALL_LIVE_1_MARKET|SMALL_LIVE_3_MARKETS|FORMAL_LIVE_1H` 和 `execution_permission=ALLOW_SUBMIT|NO_SUBMIT_PERMISSION|CANCEL_ONLY_PERMISSION|PAUSED_PERMISSION`。`data-collect`、training、offline-evaluation、promotion_controller、sim_calibration 对后两字段必须写显式 `null`/`N/A` 及“不适用”原因，不能伪造 paper/live 权限。paper 与 live shadow 的资金授权都固定为 `NO_REAL_FUNDS`，live shadow 的执行权限固定为 `NO_SUBMIT_PERMISSION`；它们不是模型策略态。任何一轴都不得与 `model_strategy_mode` 或 `gate_verdict` 混用。
     - `UserFundsAuthorizationReceipt` 本体必须与 `FundsAuthorizationStateLog` 最新权威事件联合验证：运行准入前最新状态必须为有效 `UNCONSUMED`、未撤销且未过期，准入后必须为只绑定本次运行的 `BOUND_TO_THIS_RUN`；同一 `(environment, wallet, funder, account_global_ledger_id)` 或同一晋级链不得存在另一个未关闭的 live 准入。不满足时不得用真实下单冒充热身通过。任何真实资金运行只能使用凭据声明的资金档、市场/数量/时长，结束后必须追加终态事件并关闭准入。
     - small-live 与 formal-live 均使用 `./wallets/.env.wallet1`；本地源路径为 `/home/chillchill/btc-updown-marketmaker/wallets/.env.wallet1`。`./wallets` 作为被 Git 忽略的敏感凭据（`secret`）与环境变量（`env`）部署材料上传或复制到交易服务器，远端实际落点必须由部署凭据记录且不得通过 Git 同步；需求层不限制资金规模；每次执行前必须单独向用户
     申请授权，获得批准后才能执行。
     - small-live 分为 1 个完整 5 分钟市场和 3 个完整 5 分钟市场；formal-live 为连续 1 小时程序运行。
     - 热身结果必须写入产物，不得只看控制台输出。

  3. T-30 秒预加载阶段：
     - 对任一目标市场 `M`，必须在 `M.start - 30s` 前完成发现并开始外部市场源采集；`next` 只是相对角色，不是生命周期状态。
     - `data-collect` 负责 T-30 数据源订阅、市场身份/生命周期元数据、订单簿/深度、运行中最小价格变动单位事件、代币 ID、原始证据、覆盖范围、健康状态/缺口和清单；模拟器低频最小订单数量与手续费/暂缓规则由 `sim-system` 新运行冷启动快照拥有。模型所需特征上下文与运行时市场上下文由本角色独立的 `data-pipeline` 运行时调用共享处理核心生成，paper/live 的 Market Context Builder 是其在线流驱动器；资源预算和模型包由 Model Runtime 承接。
     - 市场 `M` 在 `M.start - 30s` 到 `M.start` 之间只能进入 `prefetched` 上下文，`M.start` 后才允许 `prefetched -> active`；预加载上下文不能污染当前活跃市场的订单、持仓、挂单或账本。

  4. 活跃市场持续决策阶段：
     - 当前活跃市场中，模型必须在每个决策点看到所有必要数据源当时可证明的 `fresh|stale|missing|invalid|unknown_not_observed` 状态、`source_information_age` 与 `value_age`；源不可用、数据源从未启动或覆盖范围未知时不得用伪造、保留值或未来数据补成“全部可用”，也不得把业务静默冒充已确认缺口。
     - 每个有效模型可见业务/账本/生命周期事件、经合同准入且可重放的数据源覆盖范围/`confirmed_gap` 事件、一次性 `DecisionBootstrapEvent` 或 `DecisionWakeEvent` 到达，并完成时间排序、去重、市场隔离、定时器代次、NOE/账本投影与因果可用性判定后，必须更新该市场的持续状态，并按调度合同触发一次模型交易、风控与下一次调度评估；仅采集器可见的传输层/订阅诊断不得成为仅在线触发条件，没有新的模型可见事件时也不得由固定频率调度器凭空生成策略决策。
     - 模型输入必须包括待超越价格、订单簿/深度、Binance/外部价格、逐源可用性掩码、`source_information_age`、`value_age`、已知/未知覆盖范围、独立的 `confirmed_gap` 证据、本地账本派生的 `pUSD CASH`、持仓、目标市场内完整的 `open_order_list`、`pending_submit_list`、待处理撤单、活动挂单数量、账户级 NAV 字段、订单生命周期状态、最近提交/撤单/成交/部分成交反馈，以及撤单已请求、撤单确认、撤单已拒绝、撤单重试、撤单超时、`matched-cancel`、重叠终态等撤单订单生命周期反馈。具体传输层/订阅原因默认只是运行诊断，不进入共享 Observation；未来若要成为模型输入，必须先通过新的可回放类型化证据与 `ObservationContract` 决策门。
     - `open_order_list` 中每笔 BUY 订单至少包含订单引用、市场/结果侧/代币、GTC/GTD 订单类型、`limit_price_pusd_per_share`、接受时原始份额、客户端当前已见累计成交/剩余份额、`open_order_reserved_pusd`、创建/接受时间、当前可见执行/生命周期状态和待处理撤单状态。FOK/FAK 不得产生挂单余量。该列表只能反映当时客户端已经可见的 NOE/账本投影，不得泄露 `sim-system` 的平台真实状态或尚未到达的成交。
     - `pending_submit_list` 只容纳 `UnknownSubmitOutcome` 等尚未获得 `accepted`/`rejected` 权威证据的本地 BUY 意图，必须显式保留其订单类型、对应的限价或市场 BUY 输入字段及 `pending_submit_reserved_pusd`，标记暂定，不能计入挂单或活动挂单数量。只有官方 SDK 合同能够安全生成并验证预期平台订单哈希，或全部撤单/按市场撤单等 SDK 范围不需要伪造订单 ID 时，该暂定项才能提供合法撤单目标/范围；否则模型只能等待可关联证据，不能构造平台订单 ID。
     - 模型必须能在完整 5 分钟市场窗口内基于持续因果状态做交易决策，而不是只看到 250 ms 的孤立片段；“完整窗口”不表示按时间戳精度物化每个微秒/毫秒，也不要求把无事件时刻作为空记录或空 `token`（序列元素）输入模型。
     - live-trade 不得基于自身规则改变模型输出的结果侧/代币、价格、数量口径、提交、撤单、`MODEL_PAUSE`、`MODEL_NO_SUBMIT` 或风控动作；`order_action=BUY` 是当前版本系统常量，任何非 BUY 请求都必须在动作面前置检查中拒绝并进入审计，不得被改写成另一笔交易。平台返回的执行结果进入本地事件和审计，硬边界拒绝原因只进入审计/运维记录，不作为模型输入或模型反馈。
     - 系统必须估算并记录模型运行需要多少显存、内存、CPU、线程和批大小/序列长度配置。

  5. 当前市场结束前 T-30 秒：
     - 必须开始预加载下一个市场的全部模型输入。
     - 模型对当前市场挂单发出的单笔或全部取消、平台对遗留订单产生的到期/市场关闭终态、成交确认、持仓结算、账本关闭、产物关闭与下个市场预加载必须并行但隔离；旧市场的预留、待确认成交和结算变化仍须更新唯一的账户全局投影。
     - 任何市场切换都不能让上一个市场的订单事件污染下一个市场。

  ## 三、模型输入与训练窗口要求

  1. 模型训练、offline-evaluation、paper 交易、live 交易都必须维护目标市场从 `start - 30s` 到市场结束的持续可见上下文，并在稀疏模型可见触发点读取截至当前因果截止点的状态；这 30 秒是目标市场开始前的预加载上下文，不得把上一个市场的订单、持仓或生命周期明细混入目标市场本地投影。由旧市场未结算订单或持仓产生的账户全局 `pUSD CASH`、预留、待确认成交敞口和 NAV 影响不属于污染，必须通过带同一账本序号的只读账户全局快照对目标市场可见。
  2. 模型输入不得截断为孤立的 250 ms 数据片段，必须保留第 1 条规定的完整持续状态与可见历史边界；不得把完整上下文误写成固定时间网格、密集空样本或需要模型逐 `token`（逐序列元素）记忆所有无事件时刻。
  3. 每个训练样本、offline-evaluation 决策、paper 决策、live 决策都必须带有因果截止点，确保模型只能看到当时真实可见的信息。
  4. 训练样本、offline-evaluation、paper 和 live 决策必须与权威模型可见事件触发点对齐；该触发点可以是实际变为可见的业务/账本/生命周期事件、经 `ObservationContract` 准入且进入共享规范事件带的数据源覆盖范围/`confirmed_gap` 事件、一次性 `DecisionBootstrapEvent`，也可以是由上一轮调度输出安排的 `DecisionWakeEvent`。每次决策至少记录 `trigger_event_id`、`trigger_event_type`、`event_available_time`、`decision_ts`、`causal_cutoff_ts`、决策范围与适用的定时器代次/父决策。仅采集器可见的诊断、固定间隔重采样或 `tick_stream` 可以作为运行审计、派生特征、统计、兼容分析或回放索引，但不得成为模型决策触发器，也不得生成没有权威触发条件对应的策略决策样本。
  5. 模型输入至少应包含：
     - `market_id`、两个结果侧的 `token_id`、外部市场订单簿/成交方向、距到期时间、`active`/`prefetched` 标记；
     - 所有数据源的时间对齐状态、已知/未知覆盖范围、`source_information_age`、`value_age`、`fresh|stale|missing|invalid|unknown_not_observed` 掩码及独立的 `confirmed_gap` 证据；
     - 订单簿、深度、价差、待超越价格、最小价格变动单位、最小订单数量；
     - `account_global_projection` 中的 `cash_balance_pusd`、`pending_submit_reserved_pusd`、`open_order_reserved_pusd`、`cancel_reconciliation_reserved_pusd`、`total_reserved_pusd`、`available_cash_pusd`、`matched-pending` 结算敞口、`maker_rebate_receivable_pusd`、`taker_rebate_receivable_pusd`、两类日桶的汇总/日结投影、账户级 NAV 和账本序号；这些字段只能由本实例 Ledger 派生，模型和 `data-pipeline` 不得重算返佣，也不新增长期待付款投影。还必须包含 `market_local_projection(market_id)` 中两个结果侧的暂定/已确认持仓、目标市场内完整的尚未终止 BUY 订单列表（`open_order_list`）、待处理提交列表、待处理撤单和活动挂单数量；各市场预留切片之和必须与账户全局总额闭合，不能形成第二份可花现金；
     - 提交、撤单、成交和部分成交；
     - 不得包含独立 `account_state`；如需账户状态，只能使用本地账本派生字段；
  6. training、offline-evaluation、paper、live 的模型输入必须由同一版本化 `data-pipeline` 确定性处理核心生成，并使用同一 `ObservationContract`、特征 schema、处理配置、冻结归一化产物、处理流水线状态 schema，以及统一的冷启动、T-30 热身/引导、重置、重连和缺口/回补合同；检查点恢复只适用于对应角色，training 首版按第 12 条从干净轨迹起点重放。允许字段在某些环境下为空，但不能为空缺语义；不同环境的实际市场值或账本值可以不同，但给定相同数据源证据、因果截止点、内容及序号/摘要相同的本实例只读账本投影、初始状态及适用的检查点和上述版本化产物，输出的非浮点/规范字段必须精确相同，浮点模型张量必须满足冻结数值容差。`canonical_observation_hash` 必须基于版本化规范化与浮点量化规则计算，不得要求容差内但原始字节不同的浮点值直接产生相同原始字节哈希。
  7. 模型输出必须分为可审计的 `trading_risk_intent` 与 `decision_schedule_intent`。前者保持 `BUY-only`，例如 `submit_buy`、`cancel_order(order_ref)`、`cancel_all_open_orders(scope)`、`model_strategy_mode=MODEL_NO_SUBMIT|MODEL_CANCEL_ONLY|MODEL_PAUSE`、目标结果侧/代币、`order_type=FOK/FAK/GTC/GTD`、GTD 到期时间和 `post-only`；这些是模型内部意图词汇，不是 `sim-system` 的第二套公开方法名，必须由 Intent Bridge 映射到 `post_order`、`post_orders`、`cancel_order`、`cancel_orders` 或 `cancel_all_orders`。GTC/GTD 限价 BUY 必须输出 `limit_price_pusd_per_share + size_shares`，FOK/FAK 市价 BUY 必须输出 `spend_pusd + worst_price_pusd_per_share`。`order_action=BUY` 是当前交易合同常量；数量口径由订单类型决定，不得再把四种订单类型统一成同一份额数量输入。`MODEL_CANCEL_ONLY` 只限制模型不再提交，实际撤单仍须由明确的 `cancel_order` 或 `cancel_all_open_orders` 意图产生。后者按本需求的调度合同输出离散 `next_wake_bucket` 或基线模型包的等价确定性调度结果；它只有唯一调度器消费方，不进入 Intent Bridge 或交易平台。四类业务运行时只把前者交给 Intent Bridge 并回传执行反馈，Model Runtime 调度器只消费后者且不得生成替代交易动作。
  8. 每个训练样本、offline-evaluation 决策、paper 决策和 live 决策必须在审计元数据或运行清单中记录 `pipeline_core_identity`、`runtime_binary_sha256`、`pipeline_config_digest`、`pipeline_state_schema_version`、`initial_state_digest`、`checkpoint_digest`、`state_provenance`、`observation_schema_version`、`feature_schema_version`、`normalization_artifact_sha256`、`online_rule_version`、`model_version`、`causal_input_digest`、`ledger_projection_sequence`、`ledger_projection_digest`、`canonical_observation_hash`、`feature_hash`、`decision_trigger_contract_version`、`decision_schedule_contract_version` 及适用的定时器轨迹身份标识；不适用的初始/检查点/定时器字段必须为空并带明确语义。审计元数据中额外记录的订单簿摘要只能作为审计哈希，不是额外模型特征，但这不禁止模型消费特征 schema 已声明、按因果截止点生成的订单簿/深度特征。
  9. 模型只能读取 `data-pipeline` 输出的模型可见 `ObservationContract` 及其中来自本实例的只读账本投影，不得直接调用、写入或绕过 NOE/Ledger。模型不得直接以未归一化的真实平台或 `sim-system` 订单返回作为权威订单状态、执行反馈标签或策略输入；原始响应只能作为原始证据、摘要/引用、审计材料或受控调试材料保留。模型可以学习由 `NormalizedOrderEvent`、账本和因果投影派生出的订单生命周期状态、提交/撤单/成交执行反馈、撤单待处理/重试/确认/终态状态、延迟分布、成交危险率、风险状态和策略响应；市场监督标签、执行反馈标签、奖励、未来市场结果和标签成熟度字段不得进入 paper/live 依赖路径或任何模型可见 `Observation`。
  10. 外部市场数据中的买价/卖价、maker/taker 或 BUY/SELL 方向必须与本系统自有 `BUY-only` 动作面命名空间隔离；保留完整外部市场事实不代表系统具有自有 SELL 能力。
  11. 训练轨迹只能从清单准入且已最终化的类型化原始数据分片切取，但时间范围不必对齐或覆盖完整 UTC 日。物理文件、源目录、Parquet 分片和 UTC 日边界的枚举顺序均不作为事件顺序；训练只消费 `data-pipeline` 建立的唯一规范全序。普通外部市场空档继续使用既有 `fresh|stale|missing|invalid|unknown_not_observed` 和 `confirmed_gap` 合同，不因轨迹不是完整 UTC 日而新增拒绝或校验。
  12. 首版每条训练账户轨迹由场景直接声明干净初始账户：固定初始现金，历史持仓、订单、预留、返佣应收款和返佣日桶均为空；不扫描更早历史证明该声明。完成既有 T-30 热身/引导后可以从任意逻辑时刻开始，同一账户的可变 `sim-system`、归一化层和 Ledger 状态在内存中按规范事件顺序跨文件、跨市场和跨 UTC 日连续推进，不同隔离账户轨迹才可并行。首版不支持从训练中途的模拟器/账户/Ledger 检查点续跑；中断后从干净轨迹起点确定性重放。该限制不影响 paper 的恢复合同，也不限制模型权重和优化器检查点。
  13. 训练模块只有在某训练单元对应训练目标所需的 `sim-system -> 归一化层 -> Ledger -> RewardContract` 字段达到冻结成熟度后，才把该单元标记为就绪；可以重新安排彼此独立且已就绪的单元，包括算法明确定义为独立、但来自同一账户回放的单元，并保持序列/窗口单元内部顺序和算法明确声明的跨单元依赖。首版不强制随机打乱，也不把单个状态转换硬编码为唯一训练单元。轨迹在未来返佣付款前结束时不得提前付款，终点 Ledger 保留返佣应收款；返佣应计已通过 Ledger/NAV 进入一次经济结果。只有明确依赖未来可支出现金的训练目标才使用普通截尾掩码，不建设返佣专用右删失系统，也不要求完整 UTC 日、固定市场数量或人工快进到付款。

  ### 当前版本交易动作与盈利机制

  1. 当前版本只允许 BUY 两个结果侧，并只允许以下两种盈利机制；训练目标、`sim-system`、paper 报告、live 审计和模型评估不得引入第三种隐含交易机制。
  2. **互补结果侧折价配对。** 只有同一已确认配对数量 `paired_qty = min(confirmed_outcome_up_qty, confirmed_outcome_down_qty)` 对应的 `outcome_up` 与 `outcome_down` 单位全成本之和小于 `1 pUSD` 时，才能把该部分记为配对机制，并以完整互补持仓的 `1 pUSD` 结算价值与实际总成本之间的差额获利。是否满足 `< 1` 必须按真实或 `sim-system` 的已确认成交、实际手续费，以及调用方 Ledger 已确认且可回放的 maker/taker rebate 应收款计算；后续付款与既有应收款相等的部分只是同一经济价值的状态迁移，不能重复降低成本，未付款应收款也不能增加可用现金。未配对数量、部分成交、未成交第二腿和相关资本占用必须另记为未完成/方向性敞口与风险，不能计入已闭合配对收益。静态盘口、中间价或下单意图不得冒充已实现配对收益。
  3. **结果趋明后的正确侧持有。** 当市场结果随因果可见信息逐渐明朗时，模型可以增加 BUY 其当时判断为正确的结果侧，并持有至最终结算获利。训练和评估只能使用决策时已可见信息，不得用最终结果倒灌决定“正确侧”；该机制必须完整计入判断错误、成交成本、资金占用、未成交和最终结算风险。
  4. 卖出 `outcome_up` 所表达的反向方向由 BUY `outcome_down` 表达，反之亦然；这是当前 BUY-only 策略对方向暴露的实现约束，不得把互补 BUY 记成 SELL、减仓、释放资金或提前实现收益。
  5. 手续费、已确认 maker/taker rebate 应计及其后续付款、现金占用和结算差异是上述两种盈利机制的经济或会计组成，不得被描述为独立第三种盈利策略。应计只产生一次经济价值，后续应收款中与付款相等的部分转现金不得再次产生 PnL；低于门槛金额和四位截取余数仍是不可支出应收资产，但不进入后续日期门槛。奖励只是由版本化 RewardContract 从已入账、可回放的账本状态以及同一因果边界内的估值/终态证据派生的训练目标/诊断信号，不是现金流、收入、offline-evaluation 晋级判定或第三种盈利机制；同一返佣不得经 Ledger/NAV 变化和额外 rebate bonus 重复计奖。两种机制可以在同一账户轨迹中并存；归因必须分别报告并显式保留无法唯一拆分的交互项/残差，全部归因合计必须与账户级账本、NAV 和已结算 PnL 闭合。


  ## 四、数据采集与数据消费要求

  > 本节是跨模块后续候选需求，不是当前 `data-collect` 实现或部署的权威依据。
  > 判断现状必须先读 `data-collect/docs/CURRENT.md`；本节不得恢复旧事项、旧命令或
  > 已结束游标。

  1. 数据采集和数据消费必须解耦，但共享同一套 schema、清单和时间线语义；本文中的“数据采集与处理”是总称，不等于单一 `./data-collect/` 模块。
  2. `data-collect` 必须支持 T-30 秒外部源预订阅和预加载，不得只在市场开始后才启动；当前四源合同为 Polymarket 市场频道、Binance BTCUSDT 深度、Binance BTCUSDT aggTrade、Polymarket RTDS Chainlink BTC/USD。模型可见输入/特征只能由 `data-pipeline` 共享处理核心生成；Market Context Builder 只是 paper/live 的在线流驱动器与运行编排层。市场监督标签、执行反馈标签、`tick_stream`、训练/开发/校准/验证切分、回放上下文和 offline-evaluation 时间外留出集属于 `data-pipeline` 的离线专用数据构建分支；`model-train` 只能消费训练清单分片，offline-evaluation 只能消费互斥的留出市场数据与外生市场结果/判定/结算评分证据，当前候选的执行反馈必须由本次评估闭环生成，奖励信号仍只属于训练模块的 RewardContract。上述离线专属信号和产物不得进入 paper/live 模型输入路径，留出集的标签/成熟度也不得进入 offline-evaluation 的模型可见 `Observation`。
  3. 市场发现不能只依赖 slug，必须保留 `market_id`、`condition_id`、`token_id`、开始/结束时间、市场生命周期状态、运行时的 `current|next|resolution_pending` 角色和真实平台原始响应。
  4. `new_market`、市场切换、预取和激活状态转换等事件不能丢失。
  5. 原始数据必须保留 `raw_json` 或等价原始证据。
  6. Parquet 可以作为类型化原始数据镜像，但不能成为唯一事实来源；paper/live 必须保留仅追加事件日志或等价可回放事件流，且该事件日志不由 `data-collect` 生成或解释，只能由对应在线流运行时按共享处理核心的输入合同消费。
  7. 当前 `data-pipeline(training)` 只准入谱系源自 `/home/chillchill/new-datasets`、UTC 日不早于 `2026-07-30`、由显式关闭清单列出且最终化的数据；更早数据不能满足当前门，也不要求兼容、迁移、处理或据此扩大样本。
  8. 必须先制定全盘数据处理计划，再以日期合格的数据建立“数据采集 -> 数据处理”工作流。2026-07-30 至 2026-08-02 只是首批已核实验收基线；后续合格日期仍须清单准入和版本化切分，不能自动加入训练、开发、校准或留出集。
  9. 产物发布必须原子重命名，禁止部分产物被消费；发现逻辑必须只读取已最终化产物，临时/运行中、`.rsync-partial`、外部同步未完成标记和不完整清单不得进入消费。
  10. `data-collect` 是本条按运行生成清单合同的明确例外：它每个 UTC 日只生成一个由单一归属方原子维护的事实清单 `manifest.json`，记录采集器构建版本/二进制程序身份、配置、主机、区域、已最终化文件，以及逐数据源行数、首末接收、缺口、重连、回补等既有计数器；顶层只直接嵌套一个独立版本化的 `collection_day_coverage`，不生成逐运行清单、验证判定或 `required_files` 门禁。顶层继续使用 `data_collect_daily_manifest_v3`；旧版 v3 缺少覆盖范围时定义为未知且不得回填，新写入器必须写入合法的 `data_collect_collection_day_coverage_v1`，非法对象按清单完整性失败关闭。同日同身份标识恢复必须保持顶层为 `open`、持有数据集独占实例租约，并按嵌套四态原子激活；首次激活写 `reactivated_within_day=false`，同一 UTC 日任一次后续激活写 `true` 且当日保持单调，顶层为 `closed` 时不得重开。前次未关闭状态对账必须显式覆盖嵌套的 `active|unclean_end_unknown|not_started|ended_known` 及旧版缺失状态：`active` 转为非正常但保留布尔值；既有非正常对象保持不变，并以幂等方式确保非正常缺口/关闭记录；`not_started`、`ended_known` 和旧版缺失状态分别中性关闭；不得从摘要伪造或推断非正常事故次数。该每日清单不保存传输层范围、原因、时段或逐数据源覆盖范围，也不得冒充训练时段的精确记录；外部覆盖范围不足不等于 schema、清单、来源追溯和缺失编码不完整。旧 M8 和 2026-07-18 首次直接部署只属于已结束的历史部署事实，不构成当前 `data-collect` 或 `rebuild-core` 的部署、启停、替换或删除授权；未来任何部署必须依据届时真实代码、产物、服务状态、清单、数据目录和运行环境，另建并批准对应需求、计划与 changelog。内容校验、语义去重和训练准入由 `data-pipeline` 在消费已关闭日时完成，并在其自身运行清单中引用输入日清单身份标识。每次 training、offline-evaluation、paper、live 与 sim-calibration 仍必须生成运行清单，记录代码版本/Git 提交、数据范围、schema、输入源、硬件资源和验证结果；training/offline-evaluation/paper/live 清单还必须记录 `pipeline_core_identity`、`runtime_binary_sha256`、`pipeline_config_digest`、`pipeline_state_schema_version`、`initial_state_digest`、`checkpoint_digest`、`state_provenance`、`observation_schema_version`、`normalization_artifact_sha256`、`decision_trigger_contract_version`、`decision_schedule_contract_version` 和运行时角色，不适用的初始/检查点/调度器字段必须为空并带明确语义；training/offline-evaluation/paper 清单必须额外记录并校验相同的 `sim_system_version`、`sim_calibration_artifact_sha256` 和规则/参数摘要；真实资金 live 清单必须记录授权 ID、运行准入决策摘要、环境、公开钱包/出资方/地址、稳定的 `account_global_ledger_id`、准入开始时与运行结束时的账本序号/摘要，以及终态 `FundsAuthorizationStateLog` 事件摘要；live shadow 对这些授权字段写 `null`/`N/A` 与原因；sim-calibration 清单必须记录校准切分、真实平台数据源证据、代码/配置、前后版本、验证、原子发布与留出集不可达证明。
  11. 数据采集与处理必须作为 training、offline-evaluation、paper 交易、live 交易的共用合同：`data-collect` 提供外部市场源类型化原始数据、覆盖范围、清单或实时交接；training、offline-evaluation、paper、live 分别运行隔离的离线回放或在线流运行时，但都必须调用唯一的版本化 `data-pipeline` 确定性处理核心生成模型可见特征与 `ObservationContract`，并运行同一版本化决策调度器。驱动器只负责 I/O、分帧、无损传输层解码和本实例运行编排；从已解码数据源证据到规范事件的字段、单位、枚举、时间戳/因果可用性映射必须由共享语义规范化器完成。离线专用标签构建器、`feedback-ingestion`、训练切分、校准数据池和评估留出集不得进入 paper/live 依赖路径，`model-train` 不得读取评估留出分片。四个运行时只共享代码、schema、配置、归一化产物、状态合同、时间线、因果截止点、特征/输入合同、事件触发、调度和可回放语义，不共享进程、队列、检查点、状态缓存、运行根或账本；仅采集器可见的传输层/订阅诊断和 `tick_stream` 等固定间隔派生产物不得决定模型决策频率，也不得替代权威业务/账本/生命周期/经准入数据源证据/`DecisionBootstrapEvent`/`DecisionWakeEvent` 触发条件。
  12. 时间戳必须命名空间隔离：`source_ts` 是外部源时间；训练类型化原始数据中的 `timestamp_received` 表达采集进程原始接收时间，并与采集器主机/区域元数据共同确定来源追溯；对具有已批准接纳合同的当前 schema，`collector_admission_time_utc_ms/sequence` 表达完成可信分类并进入有界因果所有权的模型可见边界，较早原始入站不得冒充准入。paper/live 交接必须携带等价采集器接纳并另记 `collector_recv_time_utc_ms`，进入 paper/live 运行时后还要记录该运行主机的 `runtime_receive_time_utc_ms`；`decision_ts`/`causal_cutoff_ts` 是模型求值边界，`scheduled_wake_logical_time` 与 `actual_wake_time/lateness` 属于独立调度命名空间。共享处理核心必须按版本化且跨运行时一致的全序键、同毫秒决胜规则、重复、迟到、缺口/回补、定时器代次和因果可用性合同处理模型可见证据；不得按较早的 `source_ts`、原始入站或计划唤醒时间回排迟到/尚未准入事件，改写已经产生的 `Observation`，或让使用已最终化数据的回放静默利用未来回补修补先前模型输入。已确认缺口、业务缺失性、回补和调度定时器只有在对应因果证据进入共享规范事件带时才作为模型事实可回放；真实重连及其原因若只存在于数据源本地运维诊断或既有每日重连计数器，只能用于运行解释，不得成为模型触发条件或从计数伪造精确原因/时段。任何跨 schema 映射必须显式记录数据源字段、目标字段、主机、区域和时钟证据状态，不得把上述时间戳互相覆盖。live/paper 实测延迟标签只能来自对应运行主机产物，不能用采集主机接收时间冒充。下文冻结的爱尔兰双向延迟 `profile`（经验分布配置）是 `sim-system` 的项目代理假设，不是真实 live/paper 出站延迟标签。
  13. 当前 `rebuild-core` 范围内的正式 training、offline-evaluation 与 paper 运行，无论实际运行在哪台机器，通过 `sim-system` 模拟的订单请求和平台反馈都必须使用同一版本化爱尔兰延迟 `profile`；各运行机当时的本地 RPC 延迟不得替代该 `profile`。订单/取消请求的客户端到平台链路段、平台反馈的平台到客户端链路段和平台内部接受/撮合/响应发出时间必须分别记录，不得把一个延迟样本同时记作两段、RTT 或撮合延迟。历史训练或评估行情已经以 `timestamp_received` 表达采集端可见时间时，不得再次叠加同一入站延迟。规则快照启用时的 250ms taker 暂缓是平台内部业务规则，不属于爱尔兰网络/反馈时延 `profile`，任何 `profile` 都不得修改、关闭或重复叠加它。该要求是当前正式运行的准入配置，不把爱尔兰 `profile` 硬编码为 `sim-system` 模块唯一合法 `profile`；其他 `profile` 只能用于明确标识的模块级一致性测试、受控 `sim-calibration` 或后续新版本，未经新需求和版本决策不得替代当前正式运行的 `profile`。

  ## 五、订单微结构与毫秒级竞态要求

  订单微结构必须作为 P0 需求，不能只写成实现细节。

  1. 单笔与批量提交响应封装：
     - 按当前官方[提交单笔订单](https://docs.polymarket.com/api-reference/trade/post-a-new-order)和[提交多笔订单](https://docs.polymarket.com/api-reference/trade/post-multiple-orders)，`POST /order` 接收一笔并返回一个响应对象；`POST /orders` 接收最多 `15` 笔的数组，批内订单并行处理，一次 HTTP 响应返回一个响应数组。单笔或批量形式由 Intent Bridge 选择 `post_order` 或 `post_orders` 明确决定：模拟后端调用 `sim-system` 的同名模拟领域方法，实盘后端只调用官方 `Client` 同名方法；不是平台随机选择的结果。
     - [锁定的官方 Rust SDK](https://github.com/Polymarket/rs-clob-client-v2/blob/222143d321eba97d5711a848265eb9aab3bc7ff4/src/clob/client.rs#L1859-L1906) 中，`post_order` 返回一个 `PostOrderResponse`，`post_orders` 返回 `Vec<PostOrderResponse>`。固定版本 SDK 的实际返回类型和实现与当前官方 API 均按项返回结果，并明确允许混合成功/失败；`post_orders` 必须逐项、非原子处理，一项失败不得回滚已接受的其他项。批量 HTTP 响应主体完整接收并解析后，才作为一个封装对客户端可见，不得把同一批量的各项模拟成多次独立 HTTP 响应到达。多次独立单笔提交各自拥有独立封装与传输层链路段，可以乱序、迟到或缺失；批内每笔订单的平台处理、订单状态和用户频道事件仍各自独立，可以先于或后于整个批量响应封装可见。
     - SDK/HTTP 传输层成功不等于订单 `accepted`，批量封装成功也不等于每个项成功。按官方 [CLOB 错误代码](https://docs.polymarket.com/resources/error-codes)，HTTP 200 的同一响应数组可以同时包含 `live`、`matched` 和逐项失败；请求级传输层错误/non-2xx/解析失败与 HTTP 200 内的逐项结果必须分开建模，批量不得按原子全成或全败处理。
     - 本项目真实资金产物已记录[单笔提交产生一次提交观测](/home/chillchill/sdk-live-trade/poly-arb-ai/rust/sdk-live-trade/artifacts/live-validation/pullback/platform-sellable-maturity-live-20260626T133250Z/platform_sellable_maturity_buy_submit_attempts.stream.jsonl:1)，也记录[一个 4 项批量共享同一批量 ID、原始响应哈希和 `actor` 完成状态/时间戳记录，其中 1 笔 `accepted`、3 笔 `rejected`](/home/chillchill/sdk-live-trade/poly-arb-ai/rust/sdk-live-trade/artifacts/remote-r5-runs/r5-three-market-live-funds-20260621T131804Z/trading-runtime/orders.jsonl:70)。这些产物未保留完整 HTTP 传输报文主体，只证明当时 SDK/Intent Bridge 实际观察到的返回形式和混合结果分支，不用于冻结频率或响应数组顺序保证。
     - 对由创建订单/提交响应推导的 `accepted` 路径，单笔对象或批量项只有同时满足非空平台订单 ID、状态非 `UNKNOWN`、无拒单/错误文本、范围/`decision_id`/本地意图可安全对应、数量可解释，才允许该响应进入 `accepted` 热路径；不得只看 `success` 字段。
     - 官方文档只声明批量请求中每笔订单有一个响应，未明确公开承诺响应数组与请求数组的索引顺序保持不变。正常路径只能在版本化的本项目批量关联合同下，按槽位对应请求项与响应项：两类后端都必须校验请求/响应数量，并保留批量/封装 ID、请求/响应索引、本地意图 ID、有序领域请求摘要、原始响应摘要和封装接收时间；模拟后端额外保留规范模拟领域请求摘要，不得生成或要求 `SignedOrder`，实盘后端才保留有序 `SignedOrder` 摘要/预计订单哈希和真实原始请求摘要。该槽位对应是项目关联假设，不是平台公开顺序保证，也不能单独作为 `accepted` 或平台订单身份标识证明；关联合同无法满足时，对应项必须保持 `UnknownSubmitOutcome` 并等待 user-channel 或对账证据，不得猜测映射。[SDK issue #30 的生产报告](https://github.com/Polymarket/rs-clob-client-v2/issues/30) 只作为“同批一项空订单 ID/状态、另一项 `matched`”的社区异常场景证据，不是平台官方保证或发生率证据。
     - 能够安全关联到具体请求项的明确逐项拒绝/错误证据，必须只把该项归为 `rejected`，不得污染同批其他项；明确且可验证为撮合前整体拒绝的请求级验证错误，才可把对应已发送项归为 `rejected`。空订单 ID + `UNKNOWN`/空状态且无决定性错误、请求项数与响应项数不匹配、无法关联的响应项，以及整个响应封装超时、丢失、截断或解析失败，都不得当作 `accepted` 或 `rejected`；已发送项必须进入 `UnknownSubmitOutcome` 或等价暂定状态，不能盲目重提，也不能伪造平台订单 ID。混合决策时间戳或超出当前 SDK/平台批量上限，必须在创建预留和发送前由批量前置检查明确拒绝。
     - 未通过响应 `accepted` 分类器的订单不得仅凭该响应进入挂单权威状态或应用成交账本增量。已安全确认 `rejected` 的项按拒单合同释放待处理提交预留；仍为 `UnknownSubmitOutcome` 的已发送项继续占用待处理提交预留，直到能够与本地意图、客户端 ID 或平台订单 ID 安全关联的权威 user-channel 订单/成交/对账证据，将其解析为 `accepted`、`rejected`、`partial-fill`、`filled` 或终态，并仍须经过 NormalizedOrderEvent 与账本。

  2. 订单类型、流动性角色与费用：
     - BUY-only 提交必须明确选择 `FOK`、`FAK`、`GTC` 或 `GTD`。按 [Polymarket Orders Overview](https://docs.polymarket.com/trading/orders/overview)，`FOK/FAK` 是立即吃取对侧挂单流动性的市价单类型；FOK 只在模型给定的最差价格/滑点上限内能够全部成交时成交，否则整单零成交并取消；FAK 只成交该上限内当前可得数量，并取消未成交余量。`GTC/GTD` 是带价格边界的限价单类型；GTC 持续有效直至成交或取消，GTD 还可由平台在指定时间到期。
     - 订单金额域统一使用 Polymarket CLOB V2 的 `pUSD`，价格单位统一为 `pUSD/share`，份额数量使用 `shares`；依据当前官方 [pUSD 说明](https://docs.polymarket.com/concepts/pusd)与 [V2 迁移说明](https://docs.polymarket.com/v2-migration)，不得在业务 schema、模型意图、账本或报告中继续使用含义不清的裸 `USD`、`USDC`，或以报价/基础资产暗示资金单位的字段。pUSD 使用 6 位小数，但具体整数编码、舍入和最小单位必须委托锁定的官方 Rust SDK，并由一致性测试冻结，需求层不得另写手工换算旁路；`sim-system` 首版不实现历史 USDC.e 迁移或兼容分支。
     - GTC/GTD 限价 BUY 的业务输入必须是 `limit_price_pusd_per_share + size_shares`；其最大本金占用 `max_principal_pusd` 必须按锁定官方 Rust SDK 对价格和份额编码后的提交 BUY `makerAmount` 确定，在没有精度变化时才等于 `limit_price_pusd_per_share * size_shares`。价格改善后的实际成交金额只能来自权威成交证据，不能把最大本金冒充实际花费。可立即成交的 GTC/GTD 仍是限价单，不得因为立即成交就改写成市价单。
     - FOK/FAK 市场 BUY 的业务输入必须是 `spend_pusd + worst_price_pusd_per_share`：模型决定愿意花费的 pUSD 金额和最差成交价保护，共用 Intent Bridge 必须按锁定官方 Rust SDK 的数值、精度和编码语义形成一致的 maker/taker 数量与隐含份额，不得在领域层手工复刻截断或舍入。模拟后端据此构造不含签名、凭据和网络材料的模拟领域订单，并保存 SDK 编码等价投影；只有实盘后端把相同业务输入交给官方 SDK 构建器并签名生成 `SignedOrder`。实际成交份额、成交金额和未花费金额只由平台或 `sim-system` 权威证据确定。虽然锁定 SDK 的兼容 API 符号仍可能写作 `Amount::usdc(spend_pusd)`，CLOB V2 中该值的业务语义必须记录为 pUSD，且该兼容符号只能存在于 SDK 适配器内；不得向模型或领域合同暴露 `Amount::shares` 作为市场 BUY 的另一套数量口径。
     - Intent Bridge 对两类订单都必须记录订单类型、模型原始输入、SDK 编码输入、请求/接受份额、maker/taker 数量编码等价投影、最大本金或花费额、手续费和精度转换；模拟后端记录规范模拟领域请求摘要，实盘后端记录 `SignedOrder` 摘要和真实原始请求摘要。不得要求模拟后端保存“已签名请求”，也不得让实盘后端使用模拟领域摘要代替官方签名请求证据。队列与份额守恒统一使用份额；pUSD 现金守恒独立计算，二者不得相加或共用同一 `size` 字段。
     - 两类 BUY 都必须按 `demand/sim-system-demand.md` 的版本化规则校验 CLOB
       `min_order_size`（shares）：GTC/GTD 比较 `size_shares`，FOK/FAK 比较固定 SDK
       编码后的规范 shares，不得把 Gamma 近似命名字段另建为 pUSD 门。另按真实资金证据，
       只对在平台校验截点可立即成交的 GTC/GTD 限价 BUY 检查固定 SDK 编码的提交
       `makerAmount`/`max_principal_pusd`；当前规则版本 `< 1 pUSD` 拒绝，非可立即成交
       GTC/GTD 不适用，价格改善后的实际花费也不用于本门。FOK/FAK 当前无直接证据，
       不扩展该 `$1` 门；门槛值、证据类别和适用范围必须随规则版本冻结，不得散落硬编码。
     - maker/taker 是每一段实际成交的流动性角色，不是订单类型的永久标签。订单入簿后被其他订单成交的数量为 maker；立即消耗挂单流动性的数量为 taker。可立即成交的 GTC/GTD 不改变其订单类型，立即成交部分按 taker 处理，未成交余量再按 GTC/GTD 生命周期继续挂单或到期；同一订单因此可以先产生 taker 成交、再产生 maker 成交。
     - `post-only` 只允许与 GTC/GTD 组合；会立即跨价成交时必须拒单，与 FOK/FAK 组合也必须拒单。只有通过 `post-only` 接受检查并实际挂单的订单，才可在提交时保证不成为 taker。
     - 手续费、返佣和成本必须使用撮合时该市场生效的官方逐市场 `feeSchedule` 与实际 maker/taker 角色计算。按当前 [Polymarket Fees](https://docs.polymarket.com/trading/fees)、[Maker Rebates Program](https://docs.polymarket.com/programs/maker-rebates)、[Taker Rebates Program](https://docs.polymarket.com/programs/taker-rebates) 和 [Market Details](https://docs.polymarket.com/market-data/market-details#trading-fees)，maker 平台费率为零，只有启用手续费的市场才对 taker 成交收费；只有实际加入订单簿并最终成功以 maker 角色成交的数量产生 maker 返佣应计，最终成功 taker 成交则按实际收取的 taker 手续费与首版固定 Silver `8%` 产生 taker 返佣应计。`sim-system` 必须按 `demand/sim-system-demand.md` 第 10.5 节输出两类应计、独立日结和付款，不得仅凭订单类型推导固定手续费，不得把尚未入账的返佣当作该成交的即时负手续费，也不得把当前 `0.07/1/0.20` 硬编码为全部历史/未来市场。首版不累计或校验 wV，不实现动态等级或一次性等级奖金，也不建设返佣程序注册表、插件机制、规则专用语言（DSL）或其他通用返佣框架。项目固定只支持 `btc-updown-5m`，因此无需另建市场类别、slug 或 Liquidity Rewards 资格校验器；逐市场 `feeSchedule` 是经济计算输入，不是第二套市场准入校验。
     - 整个项目只支持 `btc-updown-5m`。每个新 `sim-system` 运行必须在业务请求前，通过窄型、只读、无凭据的官方 HTTPS 规则加载器，按目标 `condition_id` 从 CLOB `GET /clob-markets/{condition_id}` 查询并冻结逐市场 `itode`，并以 Gamma `GET /markets/keyset` 的重复 `condition_ids`、`closed=false/true` 查询交叉校验市场身份；只有 `itode=true` 时，才从版本化平台规则配置取得 `taker_order_delay_ms=250`，模拟 API 返回前的平台内部 taker 待处理阶段。该值不从 `data-collect`、`data-pipeline` 或 `ExecutionMarketEvent` 传播。订单到达时先完成适用的接受前检查，适用暂缓时结束后必须按期间已到达的市场、账户和规则状态重新校验，仍可立即成交时才消费流动性并产生 `matched`，否则产生 `unmatched` 或 `rejected` 最终结果。暂缓目标自身不能被暂缓期间到达的全撤取消，但同一全撤中的其他可撤活动订单仍正常处理；该阶段不产生客户端可见的 `delayed`。普通非可立即成交 maker 挂单和因 post-only 会立即成交而被拒绝的订单不得进入该暂缓。体育/赛事等秒级延迟市场完全不在项目范围，`sim-system` 不建立第二套市场系列发现；查询身份必须与上游冻结市场身份相等。官方响应缺失、非法、重复、查询失败或跨端点冲突时停止受影响的新运行，不得使用无条件常量、通用 REST 客户端、任意 JSON 取值或网页值回退。官方 Rust SDK 的字段覆盖缺口不再阻断该只读加载器。该规则仅约束 `sim-system`，真实 `live-trade` 仍由官方 Rust SDK 和真实平台决定实际行为，程序不得自行插入本地 250ms 等待。

  3. 平台 dust 与平台订单终态：
     - `sim-system`、NOE 和账本必须分别记录平台订单状态、订单生命周期状态、实际累计已执行基础数量、成交结算状态和客户端可见状态。[真实资金产物](/home/chillchill/sdk-live-trade/poly-arb-ai/rust/sdk-live-trade/artifacts/live-validation/pullback/platform-sellable-maturity-live-20260626T133250Z/platform_sellable_maturity_samples.jsonl:18) 已证明：原始 `5` 份额的 BUY 订单可以在 `size_matched=4.999165` 时由官方 SDK 查询为 `MATCHED`，撤单返回精确的 `matched-cancel`，且该资产的挂单数量为 `0`；因此平台订单可以已经终态，而 `executed_base_qty` 仍小于 `accepted_base_qty`。该样本因成交数量低于探测配置而标记 `valid=false`，只能证明此平台生命周期分支存在，不能用于估计发生频率或冻结统一阈值。
     - `venue_dust_closed_base_qty` 只能在平台状态/终态原因、精度或舍入证据、实际执行数量、未完成订单状态和版本化产物共同支持平台 dust（零碎量）时使用。FAK 终止、模型撤单、GTD 到期、平台市场关闭和提交拒绝必须分别进入 `canceled`、`expired`、`market-closed` 或 `rejected` 语义，不能归入 dust。官方[获取单笔订单](https://docs.polymarket.com/api-reference/trade/get-single-order-by-id)将状态、原始数量与已成交数量分开返回；当前官方资料没有发布全局统一的 dust 阈值。成熟社区的 [NautilusTrader Polymarket 适配器](https://github.com/nautechsystems/nautilus_trader/blob/develop/docs/integrations/polymarket.md)也把舍入容差明确作为适配器归一化策略。dust 容差必须按协议版本、订单类型、数量/价格精度和本项目真实产物校准并版本化，不得写成 Global 官方常量。
     - 权威撮合/成交证据到达客户端时，账本必须按实际已执行数量一次性减少 `cash_balance_pusd`，等额消耗该订单当时所在的待处理提交/挂单/撤单对账预留，增加暂定份额/成本基础，并把该成交标记为 `matched-pending` 结算敞口；`matched-pending` 只表达结算风险，不再从 `available_cash_pusd` 重复扣减。`CONFIRMED` 只把暂定持仓成熟为已确认，不重复应用现金或份额；`RETRYING` 保持待处理；`FAILED` 必须按权威失败证据精确冲回该暂定现金/份额/手续费增量。平台 dust 终态本身不生成额外成交、份额或交易现金流，只按终态原因释放经权威数量确认的未花费预留。
     - 平台 dust 必须作为常规、可重复验收的订单生命周期分支，覆盖平台 `MATCHED` 但 `executed_base_qty < accepted_base_qty`、精确的 `matched-cancel`、迟到 user-channel、重复证据、结算成功/失败和预留释放；同一经济变化只能应用到账本一次。

  4. 模型撤单与毫秒级竞态：
     - 策略性撤单只能来自模型输出的 `cancel_order(order_ref)` 或 `cancel_all_open_orders(scope)`。程序不得按订单提交后的固定经过时长、市场切换或固定清理规则自行生成撤单决策；FOK/FAK 终止、GTD 到期、平台市场关闭和用户/操作员触发的紧急全部撤单，是独立的订单或运行安全生命周期，其中运维方撤单不得冒充模型策略或训练标签。
     - 时间合同必须为 `decision_ts`、`model_cancel_intent_ts`、`cancel_enqueue_ts`、`cancel_send_ts`、`cancel_exchange_arrival_ts`、`platform_match_ts`、`platform_cancel_effective_ts`、`cancel_response_emit_ts` / `cancel_response_receive_ts`、`user_channel_event_emit_ts` / `user_channel_event_receive_ts` 和 `reconciliation_ts` 提供独立 schema 槽位。每个非空时间戳必须同时记录 `timestamp_authority=local_measured|platform_reported|simulated`、`clock_domain`、`host_id`、`region`、来源字段/通道以及时钟同步证据状态；模拟时间还必须记录 `profile`、场景和随机种子。live 只有在官方 SDK/频道原始证据明确提供时才能写 `platform_reported` 的平台内部时间，不可观测槽位必须为 `null` 并记录不可用原因，不能用客户端发送/接收时间倒推。不得把不同主机、平台真实顺序与客户端接收顺序合并成一个“取消时间”。
     - 提交与撤单请求链路段独立采样且按平台到达时间排序；撤单可能先于原提交到达平台。此时平台返回的 `not-found`/`unresolved` 不能追溯取消随后才被接受的订单，原提交仍可进入已接受/挂单/已撮合生命周期；模型在新证据可见后才能决定是否再次取消，程序不得复用旧撤单意图自动关闭该订单。
     - 模型只依据当时客户端可见的 `open_order_list` 与具有合法撤单目标/范围的 `pending_submit_list` 决策。若一笔接受时原始数量为 `5 shares` 的订单已有 `3 shares` 在平台成交，但成交证据尚未到达客户端，则模型仍会针对本地显示剩余数量为 `5 shares` 的整笔订单发出撤单意图；官方撤单路径按订单 ID/范围操作，并不由模型提交一个已知的平台剩余数量。撤单到达平台时，`sim-system` 必须只取消平台真实未成交的 `2 shares`。普通撤单响应若只确认订单 ID 已取消而没有返回实际取消数量，则客户端必须进入 `cancel_effect_qty_pending_reconciliation`：先移出挂单列表，并把尚无法分解的 `open_order_reserved_pusd` 原额转入 `cancel_reconciliation_reserved_pusd`，既不能向模型泄露平台真实的 `2 shares`，也不能按本地可见的 `5 shares` 释放预留。迟到的 `3 shares` 成交或其他权威累计证据到达后，才确定 `executed=3`、`canceled=2`、`open=0`，应用成交、消耗对应预留并准确释放未花费预留。
     - 若撤单到达前平台已经撮合全部数量，必须返回 `matched-cancel`；若平台已经以 dust 终态关闭订单，则返回相应终态/`matched-cancel` 证据并保留真实已执行数量与 dust 数量。撤单响应不得反向制造成交。
     - 订单终态必须按平台因果顺序、单调累计成交和实际撤单影响共同仲裁，不得按客户端最先收到的终态消息简化裁决。先收到撤单确认后，仍可接收并应用平台更早发生、但客户端更晚看到的成交增量；重复或已覆盖证据只能作为别名/审计材料，不得重复入账、重复释放预留或重新打开订单。

  5. 撤单响应语义：
     - 泛化的 `not-found`、`not_canceled` 或目标不存在，不能直接当作成交证明。
     - 本项目真实资金产物中出现的精确错误 `matched orders can't be canceled`，是经产物校准的“平台已无可取消余量”证据语义，不得扩写为 Global 官方公开保证。它可以关闭撤单生命周期，但经济成交数量只能来自先前或随后到达的权威平台累计成交/成交证据；若累计数量仍不完整，对应预留必须转入 `cancel_reconciliation_reserved_pusd` 并保持不可用，直至成交/dust/对账证据完成数量分解，不能由错误字符串单独制造成交或释放资金。
     - 普通 `not-found` 应作为订单本地可重试警告或 `unresolved` 证据，等待其他权威来源关闭。
     - 撤单网络入口必须收敛到官方 SDK 的单笔、多笔或全部撤单路径，不允许不同入口各自解释终态、成交或撤单。
     - 模型可见的撤单订单生命周期状态必须覆盖撤单已请求/已入队/待处理、撤单已接受/确认、撤单已拒绝、可重试警告、已安排重试、重试预算耗尽、传输层错误/超时、`matched-cancel`、重叠终态和最终终态；传输层重试只能幂等重试同一个模型撤单意图，不得生成新的策略性撤单。这些状态只能由 NormalizedOrderEvent 与账本/投影派生，不得由 live-trade 生成替代撤单策略。

  6. NormalizedOrderEvent：
     - 所有 SDK 提交、`post_order` 响应对象、`post_orders` 响应封装/项、撤单、批量撤单、user-channel、成交/部分成交、传输层/观察超时、平台到期/市场关闭/结算证据、运维方紧急撤单和对账证据，都必须先进入 NormalizedOrderEvent 或等价权威事件层。不得用未定义来源、未携带权威性/来源追溯的本地伪造关闭事件直接关闭订单或修改账本。
     - 单笔/批量归一化前必须保留平台原始封装形状、原始摘要和接收时间。批量内每个项分别生成可仲裁的 NormalizedOrderEvent，但必须共享同一 `response_envelope_id`、摘要和接收时间，并保留请求/响应索引与关联证据；不得把归一化后的多个项伪装成多次平台 HTTP 返回。
     - NormalizedOrderEvent 是订单反馈归一化事实层，也是账本、模型可见投影和审计的共同输入；执行反馈标签只能由本来源实例达到成熟度的 NormalizedOrderEvent 与账本应用结果共同构建，不能在账本应用/成熟度前由 NOE 直接生成。NormalizedOrderEvent 不是 live-trade 生成新策略或风控决策的入口。
     - NormalizedOrderEvent 的业务归一化必须由程序化、版本化、可回放、可测试的合同完成；不得把接受分类、终态仲裁、`matched-cancel`/dust/迟到证据归一化、`ledger_apply_policy` 或验证器事实一致性检查完全交给模型训练自行学习。
     - `sim-system` 原始证据必须通过同一套归一化层后，分别进入 training、offline-evaluation 或 paper 各自隔离的账本/审计/回放；订单生命周期进入 NOE，maker/taker rebate 应计、日结/付款和结算进入非订单经济归一化合同。达到标签成熟度的订单 NOE、非订单经济归一化结果与账本应用结果，才可以按各自权限进入离线专用反馈标签构建器。真实平台原始证据必须通过同一归一化层后进入 live 账本/审计，并且只能经 `feedback-ingestion` 离线复制到离线专用执行反馈标签构建器和 `sim-system` 校准链路。`sim-system` 证据不得进入 live 账本、live 审计或 live 状态，live 运行时也不得调用 `sim-system`。不同运行模式不能拥有各自的影子订单解释器或返佣计算器。
     - 验证器只能验证规范化数据行、原始摘要、引用完整性和产物一致性，不能成为第二套业务解释器。

  ## 六、账户、订单与热路径账本权威

  1. training 回放、offline-evaluation 回放、paper、live 每个运行实例都必须拥有彼此隔离的本地仅追加账本；共享的是版本化 schema、订单 NOE、非订单经济/控制归一化合同和应用策略，不是状态实例。该实例的账本是其账户、两个结果侧持仓、BUY 挂单、pUSD 现金、两类返佣应收款、NAV 和活动挂单状态的唯一热路径权威。订单生命周期证据必须严格路由为 `sim-system 原始证据 -> training/offline-evaluation/paper NOE` 或 `真实平台原始证据 -> live NOE`；maker/taker rebate 应计、日结/付款和结算/赎回必须走同一归一化层中的非订单经济合同。除此之外，账本只能消费本实例的本地暂定意图和第 2 条定义的版本化非订单经济/控制事件。`data-pipeline` 只能读取本实例带序号/摘要的账本投影，模型只能读取由处理核心纳入 `ObservationContract` 的只读投影；二者都不得写账本。任何实例都不得直接消费另一实例的账本或原始证据。
  2. 账本的全部经济变化必须来自带权威来源、幂等键、因果时间和来源追溯的版本化事件：启动引导、外部充值/提现、提交/撤单/成交/部分成交 NOE、手续费、maker/taker rebate 应计/付款、结算/赎回以及明确的对账事件。逐笔成交反馈中的返佣贡献只供 `sim-system` 审计和汇总，其 Ledger 应用策略恒为不应用；只有 `MakerRebateAccrualFinalized` 与 `TakerRebateAccrualApplied` 可以增加对应应收款，只有 `MakerRebatePaid` 与 `TakerRebatePaid` 可以把对应可付款应收款转为现金。两类日结事件只冻结日桶终态和付款判定，不得重新创造应计价值。市场频道/订单簿可以提供估值、市场生命周期或市场判定结果证据，但不能自行创造账户订单、成交、持仓、返佣应收款或现金变化；原始快照也不得直接覆盖投影。
  3. 账户资金字段必须统一为 `cash_balance_pusd`、`pending_submit_reserved_pusd`、`open_order_reserved_pusd`、`cancel_reconciliation_reserved_pusd`、`total_reserved_pusd = pending_submit_reserved_pusd + open_order_reserved_pusd + cancel_reconciliation_reserved_pusd`、`available_cash_pusd = cash_balance_pusd - total_reserved_pusd`，以及独立不可支出的 `maker_rebate_receivable_pusd` 和 `taker_rebate_receivable_pusd`；两类应收款中还必须分别标识低于门槛或四位截取后永久不参与后续日期门槛的锁定金额，该标识是应收款子分桶，不能重复计为资产。返佣应收款不得进入 `cash_balance_pusd`、任一预留字段或 `available_cash_pusd`。本文模型输入中的 `CASH` 专指决策时刻由本地账本派生的 `available_cash_pusd`。模型 BUY 意图只有通过动作面、订单参数、资金和协议前置检查，并在进入提交队列前，才依据限价 BUY 的 `max_principal_pusd` 或市场 BUY 的 `spend_pusd`、当前手续费元数据与锁定 SDK 手续费语义计算并创建 `pending_submit_reserved_pusd`；预留产物必须明确手续费是否已包含，禁止重复加手续费。订单被接受后转入挂单预留，立即成交则直接消耗对应预留；明确且可安全关联的拒绝/确定未接受只释放一次，`UnknownSubmitOutcome` 持续保留。无实际数量的撤单确认把预留转入撤单对账分桶，待权威累计成交证据完成分解。权威成交入账时同时减少 `cash_balance_pusd` 与对应预留；`matched-pending` 结算敞口另记且不得从可用现金重复扣减。取消、到期或平台 dust 终态只释放经权威数量确认的未花费预留。`MakerRebateAccrualFinalized` 与 `TakerRebateAccrualApplied` 分别只增加一次对应返佣应收款；D 日 taker 在 D+1 `00:10 UTC`、maker 在 D+1 `00:45 UTC` 是正常现金可用时刻。此时只有已经归属 D 的潜在合格贡献全部在模拟平台内部最终闭合才日结、付款；否则原日桶保持未日结，待最后一项闭合后在首个因果可用内部时刻只日结、付款一次。客户端反馈迟到不改变内部付款时刻。权威付款事件只把 `paid_pusd` 从对应应收款转为现金。maker/taker 日桶不得合并；低于 `1 pUSD` 的同类日桶不付款，达到门槛时向下截取至四位小数，低于门槛金额与截取余数保持不可支出且永久不参与后续日门槛。应计只产生一次经济价值，付款不得再次产生 NAV、PnL 或奖励。
  4. 本地前置检查拒绝发生在提交队列与待处理提交预留创建之前，不产生平台订单、待处理提交项或持久预留。已经发送到平台的项只有在拒绝证据能安全关联到该项时，才以 `accepted_base_qty=0` 关闭提交生命周期并释放其待处理提交预留一次；无法安全关联或传输层结果不确定时必须保持 `UnknownSubmitOutcome` 和预留，不得因“看起来像拒单”提前释放。
  5. 模型输入、产物和报表不得使用钱包 `allowance` 表达任何 pUSD 现金字段；Polymarket/SDK 的余额、授权额度和批准状态只能作为启动引导、权限/协议前置检查或显式对账证据，不是热路径第二账本，也不是 `pUSD CASH`、NAV 或 PnL 的替代来源。
  6. live 启动基线必须由同一因果截止点下可核验且完整的 SDK/账户证据建立，至少覆盖 pUSD 余额、所有相关结果侧持仓、挂单/待处理订单、未成熟成交/结算和外部资金流游标；证据不完整时不得宣称基线完成或据此计算累计 PnL。启动后的充值与提现必须分别进入版本化 `external_fund_flow` 账本事件，定义 `external_net_flow_pusd = deposits_pusd - withdrawals_pusd`。
  7. PnL 不是独立账本权威状态；未带限定词时，`cumulative_pnl_pusd = current_nav_pusd - startup_baseline_nav_pusd - external_net_flow_pusd`，表示程序启动后跨市场累计 PnL。只有基线已证明仅含 `pUSD CASH`、无历史持仓/挂单/未成熟结算且期间无外部资金流时，才可简化为 `current_nav_pusd - startup_cash_pusd`。逐订单/逐市场 PnL 只能作为可闭合的审计切片，不能替代累计 PnL。
  8. NAV 必须按版本化 `nav_valuation_contract=conservative_bid_depth_v1` 在决策时刻从同一账本投影计算：`cash_balance_pusd` 只计一次，预留只是现金可用性分桶，不得再次从 NAV 扣除；`maker_rebate_receivable_pusd` 与 `taker_rebate_receivable_pusd` 分别作为独立应收资产只计一次且不得冒充可用现金，低于门槛和四位截取后的锁定子分桶不得再次计入资产，后续 `paid_pusd` 从对应应收款转现金只改变资产分桶、不改变 NAV/PnL；暂定/已确认持仓按账本数量只计一次并显式标记结算成熟度。权威市场判定结果证据可见后，按可赎回价值估值；此前按因果截止点下新鲜、无缺口的可成交买价深度做保守清算估值，深度不足的剩余份额估值为 `0 pUSD` 并记录 `unpriced_qty`。必要订单簿缺失、过期或存在未闭合缺口时，受影响范围的 NAV/PnL 必须标记为不可用；maker rebate 只有在规则已验证、可能存在正返佣且运行时应计最终性未知或不完整时才使对应经济范围不可用，没有正 maker 成交或权威规则明确费用关闭/零返佣时必须确定为零，不得因缺少未使用的费率字段过度阻断。taker rebate 只使用已实际应用的 taker 手续费和固定 `8%`，不得因未查询 wV 或动态等级而阻断；实际 taker 手续费未知时才把对应范围标为不完整。不得用中间价、最近价格、硬编码当前逐市场 `feeSchedule` 参数或未来证据补值。每个 NAV 必须可由账本序号、估值时间、订单簿摘要、新鲜度、深度覆盖范围和两类返佣应计/付款身份重放。
  第 8 项中“所需规则未知或不完整”不再是合法运行分支：模拟器冷启动或恢复必须在核心入口前
  拒绝缺少必需规则、非法或冲突的快照。只有规则已验证、但异常输入终态使潜在合格成交的
  运行时应计最终性永远无法闭合时，maker rebate 及其对应 NAV 范围才标记不完整。
  9. 真实平台的余额、持仓、挂单和成交查询只允许用于启动引导、通道缺口/重连恢复、明确的运维方对账或定期审计，不得作为模型决策的轮询触发条件，也不得直接生成、覆盖或否定热路径账本。查询结果必须先保留原始证据，再通过版本化对账/NOE 产生账本增量；订单未出现在一次挂单快照中，本身不是成交或终态证明。
  10. 当前动作面不存在 SELL；任何自有非 BUY 提交都是模型包、意图 schema 或执行桥合同违规，必须在平台提交前拒绝并写入审计，不得作为可重试交易、训练动作或 `sim-system` 候选。
  11. 凭据、范围、schema、钱包/出资方、关键仅追加审计写入器和模型包合同属于真实下单硬边界；这些边界缺失、不匹配或不可验证时，live-trade 必须将 `execution_permission` 设为 `NO_SUBMIT_PERMISSION` 或更严格权限，并记录原因。迟到 user-channel、普通 `not-found` 等证据应按具体范围进入警告/隔离/对账，不得一律硬性失败，也不得由此生成非 BUY 动作或第二套订单解释路径。
  12. 账本必须可回放、可重建，并能从回放结果复算 `pUSD CASH`、NAV 和累计 PnL；运行清单必须记录引导启动证据/摘要、外部资金流游标、账本/估值合同版本和数据范围。账本派生状态可以作为模型输入与审计证据；硬边界只能设置 `execution_permission=ALLOW_SUBMIT|NO_SUBMIT_PERMISSION|CANCEL_ONLY_PERMISSION|PAUSED_PERMISSION`，不得作为模型输入/反馈，也不得把模型意图改写成策略性不下单、撤单或另一笔下单动作。

  ## 七、训练、offline-evaluation、paper 交易与 live 交易的真实交易反馈

  1. `sim-system` 是训练模块、offline-evaluation 与 paper 交易经共用 `Intent Bridge / Execution Adapter` 模拟后端调用的订单执行反馈模拟模块。三类业务产生 BUY-only 模拟订单后，只能通过 `demand/sim-system-demand.md` 第 7 节冻结的八个 SDK 对齐方法和模拟领域类型路由到各自隔离的 `sim-system` 实例；调用方不得直连模拟器私有派发类型。每个新运行先加载并冻结逐市场规则快照；随后 `sim-system` 模拟订单到达平台后的 `accepted`/`rejected`、排队、成交、部分成交、不成交、取消竞态、按 `itode` 适用的 250ms taker 暂缓、网络/反馈时延、最终成功 maker/taker 成交的返佣应计、两类独立日结/付款和结算反馈，再输出模拟原始证据。maker 应计必须按 `demand/sim-system-demand.md` 第 10.5 节使用撮合时逐市场官方 `feeSchedule`，taker 应计使用已实际收取的 taker 手续费乘固定 Silver `8%`；两类应计分别只进入不可支出的对应应收款，付款只把 `paid_pusd` 转为现金。三者不得各自实现另一套模拟器、成交解释器、返佣计算器、wV/等级引擎或付款调度器。
  2. `sim-system` 的受控离线校准流程必须由唯一 `sim-calibration` 写入器，使用真实平台回流的订单生命周期、成交/撤单/拒单反馈、账本结果和由 NAV 派生、可回放的程序启动后跨市场累计 PnL 反推并冻结版本；证据至少来自 `sdk-live-trade`、`online-feedback`、当前官方规则、用户冻结的项目规则假设、议题和真实交易产物。training、offline-evaluation 和 paper 只能加载并验证已经冻结的同一 `sim-system` 版本及校准证据，不能在各自运行中修改参数或用本轮训练/评估/paper 结果现场校准。冷启动查询的 `itode` 和文档规则 `taker_order_delay_ms=250` 都不是校准参数；校准产物只能记录规则快照/配置身份和验证结果，不得覆盖。改变文档规则必须经新需求、用户决策、配置/代码版本和重新验收。新校准只能原子发布为供后续新轮次选择的新版本，发布不改写旧谱系，只有采用新身份标识或旧规则被权威证据判定不再适用时，才触发相应凭据失效。
  3. 离线目标与信号必须分为三个不可混用的命名空间：市场监督标签由离线专用标签构建器从已最终化类型化原始数据与权威市场判定/最终结果证据生成，必须记录因果截止点、标签成熟度和来源追溯；执行反馈标签必须来自 `sim-system` 原始证据，经本来源 training/offline-evaluation/paper NOE 归一化、由本实例账本应用并达到成熟度后的结果，或来自真实平台证据，经 live NOE/live 账本应用达到成熟度，再由 `feedback-ingestion` 离线复制并审查后的训练反馈；maker/taker rebate 应计与付款属于同一模拟平台经济事实链，必须分别按 `MakerRebateAccrualFinalized | TakerRebateAccrualApplied -> 本实例非订单经济归一化合同 -> 本实例 Ledger` 和 `MakerRebatePaid | TakerRebatePaid -> 本实例非订单经济归一化合同 -> 本实例 Ledger` 幂等记录，日结事件只冻结日桶终态和付款判定。订单 NOE、非订单经济归一化合同、Ledger 与 RewardContract 均不得重新计算返佣公式、日门槛或付款金额；逐笔成交反馈中的返佣贡献只供审计和 `sim-system` 内部汇总，Ledger 应用策略恒为不应用。奖励信号必须由训练模块唯一的版本化 RewardContract 根据模型交易/调度动作、`decision_id`、本训练实例的 `ledger_before_sequence`、`ledger_after_sequence`、可回放账本增量、真实经过时间 `delta_t`、估值/终态证据摘要和 `reward_maturity_ts` 派生，并记录 `reward_contract_version` 与 `reward_config_digest`，必须可重算且不得修改账本；若 Ledger/NAV 已包含返佣应收款变化，RewardContract 不得再叠加第二笔 rebate bonus，后续付款因 NAV 不变也不得产生第二笔奖励。对离散 `next_wake` 候选，RewardContract 还必须区分定时器完整到期与被真实事件抢占/删失，使用冻结的经过时间折扣语义，并绑定预注册的实际推理成本、决策次数约束或等价资源目标；不得把固定的每事件折扣或事后调整的唤醒惩罚混入公平比较。训练可以按冻结切分清单消费前两类标签；offline-evaluation 在运行前只能把未进入模型 `Observation` 的权威市场结果、判定和结算证据作为独立评分面；当前候选的成交、取消、持仓、maker/taker rebate 应收款、付款、PnL 和执行结果必须由本次评估自身的 `sim-system 原始证据 -> 同一归一化层 -> Ledger` 产生，禁止用历史行为策略的执行反馈标签代替或推断当前候选的结果。奖励不得进入 offline-evaluation/paper/live 模型输入或晋级判定。执行反馈标签不得直接读取真实平台原始响应，也不得手写冷路径状态标签。每条订单执行反馈标签至少记录 `source_kind`、`transition`、`normalization_reason`、`ledger_apply_policy`、`cancel_outcome`、`error_category`、`order_id`/`local_intent_id`、`source_ts`/`receive_ts`、`order_type`、`limit_price_pusd_per_share`、`size_shares`、`spend_pusd`、`worst_price_pusd_per_share`、`max_principal_pusd`、`requested_base_qty`、`accepted_base_qty`、`client_visible_cumulative_executed_base_qty`、`client_visible_remaining_base_qty`、`platform_cumulative_executed_base_qty`、`cancel_effect_base_qty`/状态、`venue_dust_closed_base_qty`、`canceled_base_qty`、`expired_base_qty`、`market_closed_base_qty`、`open_base_qty`、`ledger_delta_fill_base_qty`、`actual_execution_notional_pusd`、`fee_pusd`、`execution_class`、`lifecycle_state`、`terminal_reason` 和成交结算状态；返佣应计、日结和付款是独立结算经济反馈，不得硬塞成订单成交字段。不适用于该订单类型的另一套输入字段必须为空并带明确“不适用”语义。平台真实数量只能留在 `sim-system` 真值/审计与成熟标签中，不能在相应证据到达前泄露给模型。市场切换、账本挂单状态、用户频道新鲜度只能作为上下文或审计元数据，不得替代订单结果标签。
  4. 每笔模拟 BUY 订单必须先由 `sim-system` 按交易所到达、引擎接受和撮合顺序，生成独立于客户端可见性的模拟平台真实状态，再分别生成创建订单/提交响应与 user-channel 订单/成交证据的发送时间、接收时间、迟到或缺失/截尾结果。`sim-system` 必须保留由 Intent Bridge 的 `post_order|post_orders` 派生的内部审计模式 `single|batch`；该字段不是第二套公开方法词汇。单笔生成一个响应对象，批量生成一个含逐项混合结果的响应数组封装；整个封装只有一组发送/接收/缺失结果，批内每笔订单的平台处理和 user-channel 事件仍独立调度。被接受的挂单订单，其平台侧成交时间、完整成交或部分成交必须先于相应可见证据确定；这些时间必须能导致完全成交、部分成交、撤单完成、`matched-cancel`、迟到成交、平台 dust 关闭等生命周期结果，而不是只按理想成交价记账，或把本地尚未收到回报误写成平台尚未接受/撮合。
  5. **FIFO 项目规则假设。** 本项目按用户在 `2026-07-12` 明确确认并冻结的项目规则假设，视唯一目标 Polymarket Global CLOB `btc-updown-5m` 在订单排序上采用价格优先、同价按进入撮合系统的时间先后执行的 FIFO/价格时间优先规则。`sim-system` 必须模拟该规则。该结论是用户冻结的项目规则假设，不得写成 Global 官方公开事实；[Polymarket US `2026-07-02` Rulebook §5.4](https://polymarketexchange.com/files/legal/Polymarket%20US%20Rulebook%20%282026.07.02%29.pdf) 与 [US FIX Order Entry](https://docs.polymarket.us/institutional/fix-api/fix-order-entry-overview) 只构成独立类比证据，不表示本项目支持 Polymarket US。规则假设的权威来源、版本、访问日和适用市场必须进入 `online_rule_version`、运行清单与 `sim-system` 产物，并在 paper/live 前重新核验。
  6. **Global 队列可见性边界。** 截至 `2026-07-12`，本轮核验的 [Global Orders](https://docs.polymarket.com/trading/orders/overview)、[Global Orderbook](https://docs.polymarket.com/trading/orderbook) 公开接口及官方 Rust SDK `main` 提交 `3ae1aae5e9ded38f984464c9fc0f307f8a9f41fb`，不提供挂单的 `queue_position`、`rank`、`quantity_ahead` 或其他真实全局队列位置字段；聚合 L2 也不能唯一分解同价档内的新增、取消和成交。因此 `sim-system` 不得声称恢复我方订单的真实 FIFO 名次。该边界只针对 Global 当前公开接口；Polymarket US 机构级 FIX 的 MBO 能力是独立证据面，不能冒充 Global 可见性。
  7. **FIFO 同价队尾入队。** 对不会立即成交、被撮合引擎接受为挂单的 BUY 订单，`sim-system` 必须在引擎接受时间把它追加到该价格档所有更早被引擎接受且尚未消耗的同价数量之后，初始 `quantity_ahead_at_acceptance` 等于这些前方数量之和，不得把我方订单随机插入任何更早位置。例如，撮合引擎接受我方订单时该档已有 `100` 份额，我方新增 `5` 份额，则初始前方量为 `100`，我方数量对应累计数量区间 `(100, 105]`；这表示数量位置，不表示五笔独立订单排名。通过适用接受前检查的可立即成交 BUY 必须走相应 taker/可立即成交订单生命周期，包括规则快照启用时的 250ms 平台内部暂缓、FOK/FAK 行为或 GTC/GTD 未成交余量处理，不能套用挂单队列。
  8. **队列不确定性边界。** 本地看到的 L2 价位级数量只是引擎接受前的聚合证据，不必然等于撮合引擎接受我方订单时的真实前方量。`sim-system` 必须按因果顺序处理快照、客户端发送、交易所到达/接受之间可见的同价新增、成交和取消：在我方之前被接受的同价新增量进入前方量，接受前已成交或已取消的数量不再排在前方。随机或情景假设只能用于无法从公开 L2 唯一识别的并发到达、网络/撮合延迟、缺口/重连，以及我方入队后同价档取消发生在前方还是后方等归因；不得改变 FIFO 队尾入队规则，也不得把已知更早数量随机移到我方之后。首版中，本条所称 L2 变化归因的版本化情景假设唯一映射为 `L2_BOOK_DELTA_FIFO_ESTIMATE_V1`：去重后的合同有效价位/`book` 净缩量按估计队头确定性消耗，结果标记为 `queue_result_quality=DETERMINISTIC_INFERRED` 和 `not_official_fifo=true`；首版不提供保守、乐观、比例或随机队列 `profile`。后续替代方案必须发布新的队列 `profile` 版本，不得在同一会话内切换或追溯改写旧运行。
  9. 队列状态必须影响首次成交耗时、实际已执行份额、前方数量、取消竞态和未成交结果，并输出引擎接受时间、证据边界、假设/场景 ID 及不确定性标签。可立即成交/taker 链路段必须先在价格/最差价格上限内，按对侧价格优先、同价 FIFO 消耗唯一且不可重复使用的合格流动性预算；只有随后被接受为挂单的 GTC/GTD 残余链路段才建立我方 `quantity_ahead`，并按第 7、8 条 FIFO 消耗前方量。每笔按 base 计量的引擎接受订单必须满足：`accepted_base_qty = executed_base_qty + venue_dust_closed_base_qty + canceled_base_qty + expired_base_qty + market_closed_base_qty + open_base_qty`。上述 `*_base_qty` 字段的单位均明确为份额，各项不得为负且同一数量只能归入一个分桶。按 quote 预算计量的 FOK/FAK 不适用该 base 等式；其固定 SDK 编码等价 `takerAmount`（shares）只作为 `accepted_base_qty` 审计投影，响应 `takingAmount` 和逐笔撮合成交表达实际执行证据；订单类型终止时 `canceled_base_qty=max(accepted_base_qty-executed_base_qty,0)`。价格改善或精度语义使实际执行份额超过该投影时 canceled 为零，正差额只属于实际成交；`accepted_quote_budget` 的独立 quote 等式才是其资金守恒权威。提交拒绝的适用接受量为零，不进入平台订单数量守恒域、队列或挂单预留。若该项已经发送，其待处理提交预留仍必须按第六节第 4 条由安全关联的拒绝证据释放一次；本地前置检查拒绝则从未创建预留。`spend_pusd`、`max_principal_pusd`、实际执行名义金额、手续费、挂单预留与已释放预留必须在独立 pUSD 现金守恒中闭合，不能与份额相加。
  10. 执行与生命周期必须分轴记录：`execution_class` 至少区分零成交、部分成交和完全成交；`lifecycle_state/terminal_reason` 至少区分挂单、完全撮合、已取消、已过期、市场已关闭和平台 dust 终态。FAK 可以是部分成交 + 已取消，GTC/GTD 可以是部分成交 + 挂单、部分成交 + 已取消或部分成交 + 已过期；数值上部分成交的订单也可以因平台 dust 成为终态。FOK 则必须在价格上限内完全成交，或以零成交结束。模型撤单、订单类型、平台到期、市场关闭和平台 dust 必须按各自终态原因分桶。
  11. `matched-cancel` 必须由 `sim-system` 的平台状态机与事件时间线生成。模型只能基于当时已经可见的创建订单/提交响应、user-channel、NOE、账本、`open_order_list` 和具有合法目标/范围的 `pending_submit_list` 决定取消；撤单意图经 Intent Bridge 的模拟后端和客户端到平台延迟到达模拟平台后，再按该时刻的平台真实订单状态、未成交基础数量和终态原因判定。平台因完全撮合或平台 dust 终态而无可取消余量时，`sim-system` 必须生成与真实资金产物一致的精确 `matched orders can't be canceled` 模拟原始证据；FAK 终止、GTD 到期、先前撤单或其他终态原因必须生成各自对应的已取消/已过期/`not-found` 证据。泛化错误 `order can't be found - already canceled or matched` 不得替代精确的已撮合证据。撤单响应只能描述平台实际撤单影响，不能制造成交或覆盖随后到达的既有成交。live 交易不使用 `sim-system`；真实平台撤单/查询证据只能来自 Intent Bridge 的实盘后端对官方 `Client::cancel_order`、`cancel_orders`、`cancel_all_orders`、`order`、`orders`、`trades` 的委托，以及 user-channel/SDK 可验证证据。
  12. `sim-system` 必须把平台 dust 作为平台可终止订单的正常分支：以真实平台订单状态、实际累计已执行基础数量、精度/舍入证据、终态原因和挂单状态共同生成 `venue_dust_closed_base_qty`，并按第 9 条数量守恒关闭订单。实际成交、dust 终态、预留释放和成交结算必须生成彼此独立但可关联的原始证据与账本策略。冷启动 `itode` 与文档规则 `taker_order_delay_ms=250` 不是联合校准参数；除适用的平台内部暂缓外，BUY 请求到达/引擎接受处理时延、创建订单/提交响应时延、user-channel 订单/成交可见时延、撤单响应时延、各通道先后关系、观察窗口内缺失/截尾和各生命周期分支的条件频率，必须从 `sdk-live-trade` 真实资金产物进入版本化联合校准；状态机、FIFO 与因果事件时间线是结果生成合同，观察频率用于校准和验收。
  13. paper 交易的生命周期与会计必须分离：订单状态先归一化，账本再消费权威事件；订单关闭、持仓关闭、会计关闭和报告关闭必须分别验收，仪表板只能展示结果，不能作为账务权威。
  14. `sim-system` 必须先生成自身的模拟原始证据，再通过与 live 一致的 NormalizedOrderEvent 合同归一化；training/offline-evaluation 回放与 paper 决策不得直接消费 `sim-system` 私有订单状态，也不得把内部随机结果绕过归一化后直接写成执行反馈标签。
  15. training/offline-evaluation 回放和 paper 宿主运行时中的 Model Runtime 调度器必须采用离散事件语义推进模型决策：订单/取消请求到达、`accepted`/`rejected`、成交/部分成交、创建订单/提交响应、user-channel 订单/成交、撤单响应、`matched-cancel`、迟到成交、结算和 `DecisionWakeEvent` 必须作为分别调度的未来事件。`sim-system` 只调度平台内部事件并输出模拟平台证据，不拥有 `DecisionWakeEvent`、模型决策范围或模型定时器；宿主运行时负责将其输出与模型调度事件按稳定全序合并。`DecisionBootstrapEvent` 只按触发合同在冷启动/恢复时缺少有效待处理唤醒时生成一次，不由模拟器周期调度。某通道缺失必须表达为带观察窗口与删失原因的未观察结果，而不是立即生成 `rejected`/零成交。事件只有在对应运行环境中变为模型可见后，才能更新持续状态并触发下一次模型决策；业务/账本/生命周期/经准入数据源证据必须按冻结范围映射抢占旧定时器，不得用固定频率决策节拍代替平台事件时间或模型因果调度。
  16. training 与 offline-evaluation 必须按各自权限验证评分/反馈证据。训练入库必须拒绝缺少 `normalized_event_id`、`authority_source`、`normalization_reason`、`ledger_apply_policy`、`causal_cutoff_ts`、`label_maturity_ts` 或原始证据摘要/引用的订单反馈标签；执行反馈标签还必须对齐版本化 schema，保留决策上下文、paper/live/simulation 数据源、订单簿哈希、`online_rule_version`、结算数据源、删失原因和无报价/无提交机会语义。offline-evaluation 在运行前只验证外生的市场、市场结果、判定和结算评分证据已经成熟且与留出清单匹配；运行后再验证当前候选自身原始证据、NOE、Ledger 和终态/结算证据的完整性；不得把历史执行反馈标签当作本候选的执行结果。缺失时必须分别在 `gate_scope=training|offline_evaluation` 标记覆盖范围不完整、隔离或相应范围的失败关闭影响；不得直接映射为 paper/live 的执行权限。
  17. paper 交易不得作为无约束超参搜索器。`model-train` 只能使用明确分配给训练/开发/校准/验证的数据做权重学习、归一化拟合、提前停止、HPO 和候选选优；训练内可以计算清洗/隔离诊断、PBO/DSR 或等价过拟合诊断，但这些表现结果默认只能形成 `WARNING`、排名和研究证据。晋级用冻结时间留出集只能由独立 offline-evaluation 在候选冻结后读取，paper 结果也不得自动回流为当前候选的调参输入。
  18. 每个训练批次必须生成训练批次排名表，比较同一批次内不同神经网络层级、训练参数、随机种子或候选产物的训练质量、内部验证质量、`sim-system` 回放诊断、两种盈利机制的分别归因及交互项/残差、风险指标、资源消耗、产物 SHA-256 和数据范围。排名表不包含晋级用时间外留出集结果，只能作为冻结候选 ModelBundle 的训练内选优证据；它不能替代 `OfflineEvaluationReceipt`、ModelRegistry 状态、人工审查、paper 验证或 live 授权门禁，也不能直接把任何产物送入 paper/live。
  19. live-trade 必须把真实执行反馈写入真实平台原始证据、NOE、live 账本和 live 审计；`feedback-ingestion` 再离线把经审查的真实执行反馈/标签送入后续 training 数据构建，并把独立校准证据送入受控 `sim-calibration`。只有 `sim-calibration` 原子发布的冻结新规则/校准产物，才能供后续 training、offline-evaluation、paper 共同只读加载；offline-evaluation 仍只能读取外生评分证据并由本次运行生成当前候选执行结果，paper 热路径不得加载离线反馈数据。live 运行时、评估器、paper 运行时和训练运行时都不得调用校准写入器或更新当前 `sim-system`，这些离线反馈也不得未经审查直接改变线上模型行为。
  20. **爱尔兰行情入站观测延迟基线。** 先前从 `2026-07-10` 数据计算的经验分布已被 2026-08-04 数据资格决定取代，不再具有当前配置档权威，也不得继续作为训练、离线评估或模拟盘的正式时延输入。新基线只能从 `/home/chillchill/new-datasets` 中 UTC 日不早于 `2026-07-30`、清单准入且已最终化的合格数据重新计算；必须定义 `collector_observed_feed_delay_ms` 的字段、单位、事件过滤和逐消息加权口径，不能因 `price_change` 展开为多行而引入未说明的行权重偏差。重新计算并通过获批计划定义的配置档门禁和独立复核、形成版本化身份前，正式爱尔兰时延配置档状态为 `NOT_READY`；这里不要求用户逐个配置档人工批准。明确标识的模块级黄金/符合性测试可以使用固定合成配置档，但不得冒充正式配置档。
  21. 新延迟 `profile` 必须绑定 `dataset_lineage_root`、`dataset_date_floor`、精确日期/范围、输入清单及文件 SHA-256、schema、采集器主机/区域、来源端点、过滤口径、样本单位、样本数、缺失/负值计数、分位数、异常/长尾政策、主机时钟同步证据状态、生成代码版本、`profile_id`、版本和 SHA-256。统计只能称为“Polymarket 市场频道事件到爱尔兰采集器的入站观测延迟”，仍可能包含平台事件时间语义、两端时钟偏差、平台/本机调度和订阅重连；极端值必须保留为待分类证据，不得静默删除、截尾或冒充正常纯网络延迟。
  22. **双向延迟代理假设。** 用户在 `2026-07-12` 冻结的建模选择仍是：正式配置档就绪后，`sim-system` 的客户端到平台订单/撤单请求链路段，与平台到客户端的创建订单/提交响应、user-channel、撤单响应传输层链路段，使用第 20 至 21 条同一版本化经验边际分布。该选择不使已经失效的旧分布恢复权威；新配置档未就绪时正式运行保持阻断。该用法是项目模拟代理假设，不是已实测的爱尔兰订单出站、反馈通道、RTT、平台处理或撮合延迟。每一链路段必须拥有独立时间戳和可回放样本记录，至少记录 `profile_id`、`profile` 版本和哈希、随机种子、样本/场景 ID、`sampled_delay_ms`、发送/产生时间与接收/到达时间；同一次样本抽取不得被重复计算为多个链路段。训练或离线评估历史行情已经按实际因果可用时间进入时间线时，不得再次叠加入站延迟。
  23. **多通道反馈竞态。** `sim-system` 必须覆盖并可强制复现创建订单/提交响应先到、user-channel 先到、两者同毫秒、任一通道迟到或在观察窗口内缺失、取消决策时两者均尚不可见等组合。对批量提交，这里的提交响应是一个整体数组封装，其内所有已解析项共享同一客户端封装接收时间；批内各订单的 user-channel 事件仍可以分别在封装前后到达或缺失。平台的 `accepted`/`matched` 状态与客户端 `UnknownSubmitOutcome`/暂定状态必须同时存在且互不覆盖；平台撮合、FIFO 和成交不得因客户端没有收到响应而暂停。本项目真实资金产物已直接支持响应先到、user-channel 先到，以及 user-channel/成交产物为空但撤单返回精确 `matched-cancel` 的分支；“取消决策窗口内创建订单响应与 user-channel 均不可见”按用户在 `2026-07-12` 确认的项目观察纳入需求，但当前本地产物不足以冻结其发生率。每个随机、缺失或乱序结果必须由版本化联合校准 `profile`、固定随机种子与因果时间线生成，并记录平台真值、客户端可见状态、各通道产生/接收时间、观察窗口、删失原因和场景 ID；相同 `profile` 与随机种子必须逐事件重放一致。联合分布必须按真实资金产物的规则版本与适用市场分层校准并记录证据边界。
  24. **两层评估边界。** 第一层 `training_internal_evaluation` 只负责训练过程诊断、受限资源预算管理、候选比较和产物生成；第二层独立 `offline-evaluation` 才负责使用未见时间外数据签发晋级判定。训练内只有可确定的有效性错误可以隔离受影响候选/运行或 `FAIL_CLOSED_TRAINING`：包括数据集/schema/清单/切分损坏，因果或标签泄漏，Observation/归一化/模型包/动作合同不匹配，不可恢复的 NaN/Inf，`sim-system -> 归一化层 -> Ledger` 无法回放或数量/现金不守恒，RewardContract 无法重算，以及关键产物/身份标识缺失。单个候选的错误不得无依据终止其他候选或整个训练批次。
  25. 训练内损失、奖励、回放 PnL、Sharpe/DSR/PBO、最大回撤、胜率、成交率、校准、交易频率、两种盈利机制归因、相对排名和资源效率等表现指标，在尚未用与本轮数据独立的 offline-evaluation -> paper/live 配对证据证明不会系统性淘汰下游有效模型前，只能产生 `WARNING`、排名或研究证据，不得自动提前停止试验；首轮表现型提前停止默认禁用。试验只能因第 24 条的确定性无效问题，或预先冻结且与表现无关的时间/算力/显存预算到达而停止。未来若证据支持启用表现型提前停止，规则仍必须预先冻结，包含最小训练预算与耐心值，只作用于相应试验，保留可恢复的最佳有效模型权重/优化器检查点、完整停止原因和继续完整预算训练的能力；该检查点不包含训练中途的模拟器、账户或 Ledger 续跑状态。任何单项或组合表现阈值都不得阻止合同有效候选被保存、恢复或人工选为可审查产物，也不得冒充离线晋级判定。合法的 `MODEL_NO_SUBMIT`、低频或零成交本身不等于训练无效；排名不得自动删除或永久淘汰这类产物。
  26. **最新未见时间外留出集。** 用户所称“最新日期且没有进入训练的训练数据集”，在本需求中规范命名为 `offline-evaluation 时间外留出集`：它与训练数据来自同一数据源/schema/处理合同，但不得进入任何训练或自适应决策。训练启动前冻结的 `split-policy` / `adaptive-data-lineage` 清单必须锁定 `model-train` 可访问数据范围和仅评估器可见的保留规则，但不得提前解析具体留出日期、市场 ID 或结果；精确的 `OfflineEvaluationDatasetManifest` 只能在候选与下述评估参数全部冻结后由评估器生成，`model-train` 始终没有读取权限。`evaluation_snapshot_cutoff` 必须在候选、评估合同、阈值、基线策略、初始 Ledger 策略和场景/随机种子集合全部冻结后，由预先冻结的调度时间和数据最终化/成熟度水位线规则自动计算并记录来源追溯；这些参数必须在解析留出集日期、市场 ID 或结果前决定，禁止操作者按数据内容或结果挑选日期/参数。系统先在截止点以内构造合格区间集合，再选择 `end_time` 最新的连续区间；截止点之后或绝对最新但仍在进行、存在缺口、未最终化或未成熟的日期不属于合格集合，其存在本身不导致失败。只有合格集合为空，或最新合格区间仍达不到预注册最小覆盖时，才输出 `QUARANTINE / EVIDENCE_INSUFFICIENT`。
  27. 合格区间必须同时满足数据已最终化且原子发布已完成、具有足以区分 `known-observed` 缺失性与 `unknown_not_observed` 的数据源覆盖范围证据、具有完整市场生命周期与 T-30 上下文、外生权威市场结果/判定/结算评分证据已经成熟，并且位于候选全部自适应数据谱系的最晚边界及清洗期/隔离期之后。“覆盖范围达标”不要求每个数据源全时段都有业务消息；自然业务静默、`stale` 或预注册的数据源屏蔽可以作为评分场景，但采集器未运行、数据源未启动或非正常未知尾部不得冒充可评分缺失性。offline-evaluation 必须预注册并覆盖一个、多个和全部必要数据源缺失及恢复边界，且候选与基线使用相同场景/随机种子；自然缺失与注入缺失必须分别报告，注入标记不得进入模型 Observation。清洗期/隔离期下界必须由最大 Observation 回看窗口、T-30、目标、市场结果与结算的时域、重叠市场以及未闭合账户全局状态的最长影响共同推导，不得只写一个任意天数。该区间不得用于权重/优化器、训练/开发/校准/验证、提前停止、HPO、候选排名/选择、归一化拟合，也不得被开发者查看并用于特征、RewardContract、评估阈值、基线或 `sim-system` 参数/规则设计。T-30 上下文即使落在前一 UTC 日也属于留出集；评估时可以使用事后成熟证据评分，但不得把未来市场结果或成熟度证据回填为模型当时可见 Observation。
  28. `OfflineEvaluationDatasetManifest` 至少记录截止点及其计算来源追溯、完整时间范围与市场 ID、纳入/排除原因、类型化原始数据/覆盖范围/数据源清单/内容摘要、最终化状态与外生评分证据成熟度、候选自适应数据谱系摘要、访问/暴露日志、不重叠证明与清洗期/隔离期证明、`pipeline_core_identity`、归一化、`sim-system`、订单 NOE/非订单经济归一化合同、Ledger、评估合同、候选/基线身份标识、统一初始账本/现金/持仓/预留/挂单状态、场景/随机种子集合、快照/清单 SHA-256 和敞口状态。一次官方晋级留出集只能评估一个已冻结候选；同一批多个候选必须先在训练内完成选择，不能在该留出集上竞赛。候选与预注册基线必须消费同一规范事件带、初始账本状态和场景/随机种子集合；首轮没有历史晋级模型时允许使用预注册的无交易/参考基线，不得为当前结果事后更换基线。
  29. offline-evaluation 只能只读加载冻结候选，并通过本实例 `data-pipeline -> Model Runtime -> Intent Bridge（模拟后端） -> sim-system -> 归一化层 -> Ledger` 闭环运行；当前候选的执行/终态成熟度是本次闭环的输出完整性，不得与运行前外生评分证据成熟度混为输入条件。晋级结论必须报告可回放账本经济指标、已结算 PnL、风险、执行现实、相对基线和统计不确定性，不得生成或读取训练奖励来决定判定。硬性晋级标准、最小覆盖和阈值必须在解析留出集及查看结果前冻结并有来源追溯：首轮或尚无可靠历史校准时，硬性标准只能覆盖合同/因果/账本正确性、BUY-only 与权限边界、现金/数量守恒、不得超支/资不抵债，以及 ModelBundle/业务安全合同已经明示且并非根据留出结果选择的风险上限；已结算 PnL、Sharpe/DSR/PBO、最大回撤、胜率、成交率、交易频率或相对基线等表现阈值，在尚未证明能预测后续 paper/live 前一律只能作为建议，不得以“绝对经济可行性”名义变成首轮硬门。以后只能用与本轮晋级留出集独立、已积累的 offline-evaluation -> paper/live 配对证据校准未来评估合同，不能追溯改变本轮标准。评估合同必须逐项给出估计区间、样本规模和不确定性；市场数/评估回合（`episode`）数/成交数不足以支持结论时必须输出 `QUARANTINE / EVIDENCE_INSUFFICIENT`，不得用不透明综合分数一票否决或把证据不足写成模型 `FAIL`。完整合格数据上的有效 `PASS` 才能签发绑定全部身份标识的 `OfflineEvaluationReceipt` 并进入 paper。
  30. 候选必须先冻结，offline-evaluation 才能解析并锁定留出集。模型推理或任何结果一旦对训练/选择/设计人员暴露，该快照即为 `EXPOSED`；此后只要模型包/权重、处理流水线/配置/归一化、特征/奖励、`sim-system`/在线规则、基线、初始 Ledger 策略、场景/随机种子、评估阈值或评估器实现中任何会影响模型输出或判定的身份标识改变，就必须使用更新的合格未见区间，不能以“模型名称未变”复用旧留出集。允许使用与原运行完全相同的全部身份标识执行只读确定性审计重放，但只能核对原始事件、指标和摘要，不得产生新指标、改变原判定、重新选模或签发第二份独立晋级凭据；除此之外，只有在模型推理和结果暴露前发生基础设施故障、未签发判定且生成失效凭据时，才允许把重跑作为原评估的替代执行。

  ## 八、模型包与策略边界

  1. 模型包必须有明确合同：`pipeline_core_identity`、`pipeline_config_digest`、`pipeline_state_schema_version`、`observation_schema_version`、`feature_schema_version`、`normalization_artifact_sha256`、`sim_system_version`、`sim_calibration_artifact_sha256`、`model_version`、输入形状、序列长度、标签合同、固定的 `BUY-only` 交易/风险动作面、目标结果侧/代币、风险上限、`online_rule_version`、`decision_trigger_contract_version`、模型可见触发事件合同、`decision_schedule_contract_version`、调度器类型 `deterministic_baseline|learned_discrete_next_wake`、运行时归属范围注册表/总上限、离散档位身份标识及其最小值/最大值/量化规则、基准/到期时间、定时器排序/抢占/恢复/过期/引导合同、调度成本或决策预算身份标识，以及模型包 SHA-256。模型包和运行时前置检查必须拒绝处理核心、配置、状态、Observation、归一化、训练模拟器或调度谱系不匹配，以及自有 SELL 能力、外生固定频率决策节拍、非法/缺失调度输出、模型创建/改绑范围或无法映射到权威触发事件的模型调用路径。归一化只能从冻结的 training 切分拟合并作为模型包产物发布；offline-evaluation、paper、live 只能加载并校验，禁止重新拟合或漂移。
  2. ModelRegistry 必须区分 `active`、`shadow`、`retired` 和 `candidate` 状态。
  3. 只有 `active` 模型才能真实下单；`shadow` 模型只能产出对照决策和评估产物。
  4. 训练批次排名表必须与 ModelRegistry 对齐：训练内选出的产物只能先冻结为候选，不得直接进入 `shadow`、paper 或 live。候选只有依次通过模型包/训练完整性门禁、独立 `OfflineEvaluationGate` 和 paper 资格验证，才可进入 live shadow / `NO_SUBMIT_PERMISSION` 验证；shadow 通过后，还必须由 Promotion Controller 校验完整凭据链，并在用户明确授权首次 `SMALL_LIVE_1_MARKET` 时才允许转为 `active`。`active` 只是可提交模型状态，不自动产生真实资金授权；后续 `SMALL_LIVE_3_MARKETS` 与 `FORMAL_LIVE_1H` 仍须逐级验证和逐次用户授权。
  5. 当前模型框架的不同神经网络层级、模型容量和训练参数候选，必须共享同一 BUY-only 动作合同、两种盈利机制、数据、`sim-system`、归一化层（订单 NOE / 非订单经济归一化合同）、Ledger、奖励和评估边界后再比较；不符合当前 BUY-only 动作合同的候选不得进入当前候选排名表或训练批次排名表。
  6. 上线前必须做动作面审计，确认模型只能输出 BUY 两个结果侧、取消未成交 BUY、`MODEL_NO_SUBMIT`、`MODEL_PAUSE` 和允许的风控意图，并与实际 SDK/订单/平台硬约束一致；任何自有 SELL schema、输出头、候选、消费方或执行路径都必须判定失败。
  7. 模型包必须定义交易动作与风控动作输出合同；live-trade 只能验证 BUY-only 动作面是否满足平台、SDK、权限、schema 和钱包硬约束，不能改写模型的交易或风控意图。
  8. 模型包与训练报告必须分别声明并报告“互补结果侧总成本 < 1”和“结果趋明后增加正确侧并持有至结算”两种盈利机制的适用条件、风险、分别归因及交互项/残差；不得用未结算方向收益或不完整配对冒充已实现利润，全部归因必须与总账本经济指标对账。
  9. `promotion_stage` 必须与 ModelRegistry、`funds_authorization_tier`、`execution_permission` 分轴记录，至少覆盖 `TRAINED_CANDIDATE -> OFFLINE_EVALUATION_PASSED -> PAPER_PASSED -> LIVE_SHADOW_PASSED -> SMALL_LIVE_1_PASSED -> SMALL_LIVE_3_PASSED -> FORMAL_LIVE_1H_PASSED`。每次转换必须由 Promotion Controller 消费前序 `PASS` 凭据并输出不可变 `PromotionDecision`；禁止跨级、倒序、缺证据推进，禁止把这些阶段名称加入 `active`/`shadow`/`candidate`/`retired` 枚举。
  10. 全部阶段凭据与 `PromotionDecision` 必须区分跨阶段不变量和阶段本地证据。跨阶段必须保持同一候选模型包 SHA-256、处理核心/配置/状态、Observation/归一化、BUY-only Intent、归一化层/Ledger 合同身份以及可适用的在线规则来源追溯；TrainingCandidateReceipt、OfflineEvaluationReceipt 与 PaperQualificationReceipt 还必须绑定同一冻结的 `sim_system_version + sim_calibration_artifact_sha256`，live 凭据只能绑定真实平台/官方 SDK/运行时证据并引用该训练谱系，不得写成 live 使用 `sim-system`。首次 small-live 1 准入建立的环境、公开钱包/出资方/地址和稳定的 `account_global_ledger_id`，必须在 small-live 1、small-live 3、formal-live 1h 全链保持相同；账户身份标识变化必须使全部真实资金凭据失效、关闭准入并回到 `LIVE_SHADOW_PASSED`，重新从 small-live 1 授权；环境、live 运行时或硬件身份标识变化还必须使 `LiveShadowReceipt` 失效、回到 `PAPER_PASSED` 重做 live shadow。每个凭据本体都必须不可变，至少记录凭据/schema ID、阶段、签发方、运行时二进制程序 SHA-256、签发/到期时间、判定、证据/运行清单摘要和前序凭据摘要；`consume`、`revoke`、`expire`、`invalidate` 只能通过 Promotion Controller 的仅追加状态事件引用凭据 ID 与前序状态摘要，不能原地修改凭据。运行时二进制程序、主机、硬件身份标识、初始/检查点和运行 ID 是各阶段本地证据，不要求跨 training/offline-evaluation/paper/live 相同，但必须证明对应目标阶段的一致性和资源门禁。模型权重/模型包、归一化、处理流水线/Observation/动作、`sim_system_version`/校准产物、关键归一化层/Ledger/评估合同或适用平台规则变化时，必须按受影响范围使既有凭据失效，并从 training 起最早受影响阶段重新验证；评估器、运行时或人工操作不得自行豁免。
  11. 各阶段凭据的签发者和最低完成条件必须固定：`model-train` 在训练有效性检查完成、唯一候选冻结且未读取留出集后，签发绑定 training 切分/数据清单、训练报告/排名、`sim_system_version` 与校准产物摘要的 `TrainingCandidateReceipt`；offline-evaluation 在最新未见留出集闭环得到 `PASS` 后，签发绑定 `OfflineEvaluationDatasetManifest`、评估报告、初始 Ledger/检查点/现金/持仓/预留/挂单状态和场景/随机种子集合的 `OfflineEvaluationReceipt`；paper 运行时在 `NO_REAL_FUNDS` 下完成预注册运行与对账后签发 `PaperQualificationReceipt`；live shadow 运行时在最终 live 硬件上以 `NO_REAL_FUNDS + NO_SUBMIT_PERMISSION` 完成只读数据、推理、SDK 查询/对账、重启和长运行验证后签发 `LiveShadowReceipt`。small-live 1、small-live 3、formal-live 1h 只有在各自一次性用户授权范围内完整结束、账本/订单/结算/资源证据闭合，且 Promotion Controller 已追加 `COMPLETED` 授权终态事件并关闭准入后，运行时才能依次签发 `PASS` 的 `SmallLive1Receipt`、`SmallLive3Receipt`、`FormalLive1HReceipt`；每份真实资金结果凭据必须绑定授权 ID、运行准入决策摘要、环境、公开钱包/出资方/地址、稳定的 `account_global_ledger_id`、`admission_start_ledger_sequence/digest`、`completion_end_ledger_sequence/digest` 和终态 `FundsAuthorizationStateLog` 事件摘要。阶段运行时只能签发本阶段结果凭据，只有 Promotion Controller 可以验证凭据与终态事件，并在完成决策中同时绑定二者、推进阶段。
  12. offline-evaluation、paper、shadow 或任一 small-live/formal-live 发生 `FAIL`、`QUARANTINE`、证据不完整、超时、中断或授权撤销时，不得签发 `PASS` 凭据、不得推进阶段，并必须保留原因和最后一个已通过阶段；Promotion Controller 必须为真实资金授权追加对应 `FAILED`/`INTERRUPTED`/`EXPIRED`/`REVOKED`/`INVALIDATED` 终态事件并关闭 `active_admission_run_id`，运行时必须停止新提交，并按本次运行已验证的权限路径完成必要对账或模型/运维方撤单。不得原地修改不可变凭据。上述结果不得把训练运行改写为失败，也不得自动把模型设为 `retired`；ModelRegistry 回退或退役必须是独立、可审计的 PromotionDecision。paper/live 反馈不得自动更新当前模型包权重，任何基于反馈的重训都必须产生新的 `model_version`，并重新执行适用的未见数据与晋级链。
  13. live 运行时只有在以下条件同时成立时才能令 `execution_permission=ALLOW_SUBMIT`：`model_registry_state=active`；当前 `promotion_stage` 与申请的资金档顺序匹配；存在 Promotion Controller 针对同一模型包、运行、资金档、市场、数量、时长、环境和账户身份标识签发的运行准入 `PromotionDecision`；不可变 `UserFundsAuthorizationReceipt`、该决策、`FundsAuthorizationStateLog` 最新权威事件和 live 前置检查实际读取的公开钱包/出资方/地址、`account_global_ledger_id`、环境完全相等；准入前状态为 `UNCONSUMED`，随后为本次运行唯一有效的 `BOUND_TO_THIS_RUN`，未撤销、未过期且 `active_admission_run_id` 等于当前运行；同一 `(environment, wallet, funder, account_global_ledger_id)` 或同一晋级链没有其他未关闭的 live 准入；目标硬件、SDK、schema、写入器、对账与相应 `LiveStageGate.admission_verdict` 全部为 `PASS`。实际提交还必须来自合法 BUY-only 模型意图并通过逐单前置检查；运行结束后的 `LiveStageGate.completion_verdict=PASS` 只负责签发本阶段结果凭据，不能倒置为启动前置条件。任一准入条件缺失或不匹配都必须失败关闭；`active` 状态或用户授权任何一项单独存在都不构成下单权限。

  ## 九、硬件与训练效率要求

  1. 训练设计必须考虑硬件使用效率，而不是只让代码能跑。
  2. 计划最高使用 4 张 NVIDIA RTX 5090；对应资源按用户当前配置记录为：4 张 NVIDIA RTX 配置合计约 148 GB VRAM、100 vCPU、360 GB RAM；单卡配置为
  NVIDIA RTX 5090、32 GB VRAM、25 vCPU、Intel Xeon 8470Q、90 GB RAM。
  3. 由于 32 GB * 4 与 148 GB VRAM 存在口径不一致，需求中必须要求运行时使用 `nvidia-smi`、`lscpu`、`free`、cgroup 限制和驱动器/运行时清单
  固化实测值，以实测值作为调度和验收依据。
  4. 必须建立 5090 运行清单/性能分析合同，记录 Python 解释器、Git 提交、同步根目录、批量大小、工作进程数、分片、每轮时间、GPU 利用率、VRAM、CPU、RAM、I/O、吞吐量和质量指标。
  5. 不得用单一 GPU 利用率或单一批大小判断训练成功。
  6. 批量变大不必然更好；必须同时比较吞吐量、显存、内存、滚动最差值、验证集质量和端到端训练时间。
  7. DataLoader 工作进程、预取、分片、缓存和验证器物化，必须有性能分析证据后再调整。
  8. 训练服务器内存、CPU、GPU 随 GPU 数增加而成套增长，但系统仍必须支持单卡、双卡、四卡分级运行。
  9. 训练模块必须系统探索当前模型框架在不同神经网络层数、深度、宽度、隐藏/状态维度、参数量、序列/记忆容量及训练参数下的交易能力；训练参数至少覆盖经预注册的优化器、学习率、批量大小、正则化、损失/奖励配置、随机种子和训练轮次，不得把单一小模型配置预设为最终答案。
  10. 候选比较必须在相同数据、BUY-only 动作合同、两种盈利机制、`sim-system` 版本、评估区间和资源预算合同下进行，以交易能力、风险、稳定性、校准、结算后收益和资源成本共同判断，不能只按参数量、训练损失或单一排名表指标选择。
  11. 当前 live-trade 交易服务器的 CPU、内存或推理能力不得成为训练阶段选择神经网络层级、模型容量和体积上限的先验硬限制，也不得因此提前排除、压缩或降级交易表现更好的较大模型。
  12. 如果胜出候选超过当前交易服务器的推理延迟、内存或稳定运行能力，必须在独立字段或部署门禁判定中记录 `deployment_hardware_status=requires_hardware_upgrade`，不得改变其 `active`/`shadow`/`candidate`/`retired` ModelRegistry 状态。系统必须保留其原始模型与交易能力证据，并在用户升级交易服务器后重新执行运行时一致性、延迟、内存和长运行门禁；硬件升级完成前不得部署该候选，但也不得仅为适配旧服务器而自动改选较弱模型。
  13. 交易服务器可以升级不等于部署门取消。任何模型进入 paper 前必须在 paper 目标硬件上通过相同输入、输出、数值一致性、事件到意图延迟、峰值内存、连续运行和失败关闭验收；进入 live shadow 或任一真实资金档前，必须在最终 live 目标硬件上重新通过对应验收。live 目标硬件不足不得反向阻断训练完成、offline-evaluation 或不依赖该硬件的 paper 资格，只能阻止 live 部署并保留 `deployment_hardware_status=requires_hardware_upgrade`。

  ## 十、P0 门禁与验收边界

  核心重构必须至少包含以下 P0 门禁或等价机制：

  1. OnlineFeedbackGate：是否已纳入 `online-feedback` 规则版本、真实平台潜规则和议题。
  2. ModelBundlePreflight：模型包、处理核心身份标识、处理流水线配置/状态 schema、Observation/特征 schema、归一化产物、ONNX/运行时、固定 BUY-only 交易/风险动作面、两种盈利机制、决策触发/调度合同、调度器类型、运行时归属范围注册表/总上限、离散档位及其最小值/最大值、基准/到期时间、定时器排序/抢占/恢复/过期/引导、调度成本或决策预算与模型可见事件合同是否匹配；是否拒绝任何自有 SELL 能力、外生固定决策节拍、非法/缺失调度输出、模型创建/改绑范围，或 offline-evaluation/paper/live 重拟合归一化的路径。
  3. MarketDiscoveryGate：`active`/`next` 市场、代币、最小价格变动单位、最小订单数量、开始/结束时间、市场生命周期状态与运行时角色是否可靠。
  4. MarketSwitchGate：`active` 与 `prefetched` 上下文是否隔离，切换是否可审计；旧市场到 E 时只从当前角色进入 `resolution_pending`，并在无终态证据时保持 `active`；只有满足 `market_resolved` 事件路径或备用元数据退役谓词后才能进入 `retired`，预算耗尽只退出待处理角色。该门禁只产生判定、隔离或切换阻断影响，不是执行参与方；切换期间仍可由各自权威事件路径处理既有模型撤单意图、平台到期/关闭或运维方紧急撤单，但门禁与市场切换都不得自行生成策略性撤单。
  5. ResourceWatermarkGate：CPU/RAM/VRAM/I/O/磁盘/产物写入器是否满足阈值，并覆盖写入器队列、排空/最终化延迟、背压和 ResourceWatermark 阈值动作。
  6. HardwareTierGate：单卡/多卡训练配置、实际硬件、运行参数是否匹配；训练候选容量探索与最终部署硬件准入必须分开判定，不得用当前交易服务器配置提前裁剪训练候选。
  7. SupplyChainGate：SDK、依赖、模型产物、数据产物、部署产物 SHA-256 是否可追踪；外部仓库、脚本和依赖默认锁定为只读，不得默认安装/运行、读取任何敏感凭据（`secret`）或联网出站，任何例外必须有隔离环境、审批和审计产物。
  8. TrainingFeedbackGate：真实交易反馈、paper 反馈、市场监督标签、执行反馈标签、奖励信号、`online_rule_version`、内部切分、训练批次排名表是否各自闭环且未混入模型可见 Observation；必须验证本次训练实际消费的数据/schema/清单/切分/因果性、归一化、RewardContract、产物身份标识和 `sim-system -> 归一化层 -> 试验账本` 的可回放完整性。尚未成为本轮冻结必需输入的 paper/live 反馈缺失或覆盖不足只能形成覆盖范围 `WARNING`，不能阻止训练完成；执行现实、PBO/DSR 和训练内表现必须进入报告，但未经独立下游证据校准的弱表现只能形成 `WARNING`、排名或研究证据，不能自动提前停止、据此 `FAIL_CLOSED_TRAINING`、阻止合同有效产物落盘，或把训练内评估冒充晋级判定。
  9. ModelIntentBridgeGate：BUY-only 模型交易/风险意图、独立决策调度意图、两者各自唯一消费方、`trigger_event_id` 到模型调用与两类意图的可审计映射、平台反馈回传合同，以及“禁止 live-trade 改写模型交易/风控决策、禁止调度器把唤醒改写为策略动作”的边界是否满足；是否只有共用 `Intent Bridge / Execution Adapter` 暴露 `post_order`、`post_orders`、`cancel_order`、`cancel_orders`、`cancel_all_orders`、`order`、`orders`、`trades`，不存在旧公开词汇或直连旁路；模拟后端是否只使用模拟领域类型，实盘后端是否只委托官方 `Client` 并仅在提交时使用 `SignedOrder`，两类后端是否在实例创建时固定且不能互相回退。
  10. DataIngestRuntimeGate：`data-collect` 与数据消费运行时是否满足 Rust 异步/非阻塞、有界队列、写入器滞后、背压、数据源健康状态/缺口/重连轨迹、上游可用性退化下的数据源本地无限恢复和 training 采集器进程连续性、事件排序/去重/市场隔离、仅发现已最终化产物、排除临时/运行中产物及 `.rsync-partial` 文件，以及阻塞线程池隔离。
  11. SimSystemFidelityGate：training、offline-evaluation 与 paper 是否只经共用桥接器的模拟后端调用同一版本化 `sim-system`，是否使用八个 SDK 对齐方法名和模拟领域类型，内部派发类型是否不对调用方公开，且不存在直连或 live 回退；每个新运行是否按有界市场集合通过窄型、只读、无凭据的官方 HTTPS 规则加载器完成一次规则冷启动，是否只访问 CLOB `GET /clob-markets/{condition_id}` 和 Gamma `GET /markets/keyset`，是否对 Gamma 使用重复 `condition_ids` 与 `closed=false/true`，并验证窄 DTO、原始响应 SHA-256、严格身份/规则闭合、可验证快照、恢复复用原快照、密封后断网和运行中不热刷新；是否只使用上游固定的 `btc-updown-5m` 合同且没有第二套市场系列分类器，并只在逐市场 `itode=true` 时对通过适用接受前检查的可立即成交订单恰好执行一次 250ms taker 暂缓、到期重校验后才消费流动性、暂缓目标自身不可撤且不生成 `delayed`；BUY-only FOK/FAK/GTC/GTD、两套订单单位合同、实际 maker/taker 角色、单笔响应对象/批量响应数组封装、批量逐项混合结果与关联证据、FIFO 项目规则假设、挂单订单在引擎接受时间的同价队尾入队、因果前方量更新、份额数量守恒、pUSD 现金守恒、平台 dust 终态、模型单笔/全部撤单、取消竞态、原始证据 -> 归一化层 -> 账本、规则来源追溯和不确定性场景是否闭合。还必须核验 maker 逐市场 `feeSchedule` 应计、taker 实际手续费乘固定 Silver `8%`、两类独立 UTC 日桶、D+1 `00:10/00:45 UTC` 正常现金可用时刻、同类日累计 `1 pUSD` 门槛、四位向下截取、锁定余数、应计只进入不可支出应收款以及付款只迁移对应应收款到现金且 NAV/PnL/奖励不变；正常时刻存在已经归属 D 但内部最终性未闭合的潜在合格贡献时，原日桶必须保持未日结，最后一项闭合后只日结、付款一次，客户端反馈迟到不得改变内部付款时刻。普通训练窗口结束或调用方主动截尾本身不生成 `UNKNOWN_INCOMPLETE`；无论是否跨过正常现金可用时刻，尚未闭合的原日桶都保持未日结。只有已经到达应结算边界且既有异常封存/隔离终态证明闭合永久不可能时才复用既有 `UNKNOWN_INCOMPLETE`。付款事件只携带被消费应计的数量和摘要；不得新增第二套待付款状态机或复制无界身份集合。不得出现 wV 校验、动态等级、一次性等级奖金、市场类别/slug 第二校验器或 Liquidity Rewards 混用。还必须核验爱尔兰网络/反馈时延 `profile` 的数据集、过滤条件、样本、来源追溯和哈希，请求/反馈各链路段未重复计时，也未把适用的 250ms 暂缓作为地域时延参数重复叠加；平台真实状态与客户端可见状态分离，单笔/批量提交封装及其整体超时/丢失/解析失败，创建订单/提交响应与 user-channel 的先到/后到/同毫秒/迟到/缺失矩阵，撤单到达前平台实际已执行/未成交数量的因果推导，dust 关闭的 `cash_balance_pusd` / 预留 / `available_cash_pusd` 分账，以及相同 `profile` + 随机种子的逐事件确定性重放。不可见事件和通道可见性使用版本化情景假设；首版 L2 变化归因必须按第七节第 8 条唯一映射为 `L2_BOOK_DELTA_FIFO_ESTIMATE_V1`。FIFO 入队顺序保持冻结，`matched-cancel` 由平台状态机因果推导，入站观测 `profile` 继续保持项目代理假设边界。
  12. ModelCapacityExplorationGate：当前模型框架是否在预注册、公平预算内探索不同神经网络层级、容量和训练参数，是否用交易能力而非当前交易服务器硬件作先验筛选；较大胜出模型是否以独立 `deployment_hardware_status=requires_hardware_upgrade` 或等价门禁判定保留，并在升级后的目标硬件上重新验收，且没有污染 ModelRegistry 状态。
  13. ObservationParityGate：training 离线回放、offline-evaluation 离线回放、paper 在线流、live 在线流是否分别运行隔离实例并调用同一处理核心与决策调度器。第一层必须把同一批可关联、被 `ObservationContract` 准入的数据源证据分别编码为已最终化类型化原始数据与实时交接，验证经共享语义规范化器后的规范事件 ID、载荷、因果可用性、模型可见覆盖范围/`confirmed_gap` 字段和 `total-order-key` 摘要精确一致；仅采集器可见的传输层/订阅健康状态/重连诊断必须在独立 `data-collect` 验收中验证，不得作为仅在线 Observation 字段或触发条件混入该一致性。第二层必须使用同一规范事件带、内容及序号/摘要相同但来自隔离实例的账本投影、相同初始状态及适用角色的相同检查点、因果截止点、处理流水线配置/状态 schema、归一化产物、模型包与调度合同，验证运行时归属的范围注册表、定时器创建/取消/替换/代次、模型可见事件抢占、同刻排序、基准/到期时间、适用角色的重启/检查点恢复、过期后单次迟到触发、决策次数/顺序、触发事件、Observation 非浮点/规范字段和按冻结规范化/量化规则计算的哈希精确一致，模型张量浮点字段处于冻结数值容差内。training 首版只使用干净初始账户并从轨迹起点确定性重放，不要求训练中途账户/模拟器检查点；paper 等适用角色的恢复合同保持不变。还必须覆盖冷启动/T-30 引导，以及适用角色恢复时有有效待处理唤醒、恢复时无定时器所产生的单次 `DecisionBootstrapEvent`、重置、缺口/回补、全部外部源静默、唤醒前事件、事件前唤醒、同刻冲突、迟到触发和失效定时器；若测试重连，只能比较其后实际进入规范事件带的数据源证据与状态影响，不能伪造或要求模型看到未持久化的传输层原因/时段。验收必须证明不存在影子规范化、特征、归一化、Observation、调度器路径和标签/奖励泄漏。任何首个分歧都必须生成可审计产物，并按范围阻止 training 候选有效性、offline-evaluation 凭据、paper 或 live 部署。
  14. TrainingCandidateGate：训练运行是否只因数据/因果/合同/数值/账本/RewardContract/产物的确定性无效问题失败关闭；未经独立下游证据校准的训练内弱表现是否只形成 `WARNING`、排名或研究证据且不自动提前停止；停止是否仅因确定性无效或预注册的非表现资源预算，并保留可恢复的模型权重/优化器检查点而不保存训练中途的模拟器/账户/Ledger 续跑状态；唯一候选是否在留出集解析/暴露前冻结并生成有效 `TrainingCandidateReceipt`。只有 Promotion Controller 可以消费该凭据并注册 ModelRegistry `candidate`。
  15. OfflineEvaluationGate：是否在候选、评估合同、阈值、基线策略、初始 Ledger 策略和场景/随机种子集合冻结后，通过确定性截止点从全部合格集合自动选择 `end_time` 最新、完全未进入自适应谱系的时间外留出集，并由独立运行时对最终 ModelBundle 完成闭环评估；是否具备不重叠、清洗期/隔离期、覆盖范围、外生评分成熟度、本候选执行结果成熟度、基线、统一初始账本/场景/随机种子、账本经济指标、已结算 PnL、风险、执行现实、统计不确定性和全部身份标识证据。首轮或未经 offline-evaluation -> paper/live 独立配对证据校准时，利润、Sharpe、成交率、胜率等表现阈值必须只作建议；数据或统计证据不足必须判为 `QUARANTINE`，不能判模型 `FAIL`。完整合格数据上只有按本文受限硬性标准得到的 `gate_verdict=PASS` 才能签发 `OfflineEvaluationReceipt` 并允许进入 paper；`WARNING`、`QUARANTINE`、`FAIL_CLOSED` 都不得推进。
  16. PaperPromotionGate：paper 是否只加载 `OfflineEvaluationReceipt` 绑定的同一模型包，在 `NO_REAL_FUNDS`、独立在线运行时 / 共用桥接器的模拟后端 / `sim-system` / 归一化层 / Ledger 下完成预注册覆盖、持续运行、重启恢复、账本对账、模型行为、风险、延迟和资源验收，并输出 `PaperQualificationReceipt`；是否不存在绕过桥接器的模拟器直连或签名/真实网络路径；没有 `PASS` 凭据不得进入 live shadow。
  17. LiveStageGate：必须分开记录 `admission_verdict` 与 `completion_verdict`。live shadow 是否始终为 `NO_REAL_FUNDS + NO_SUBMIT_PERMISSION` 并在最终 live 硬件上完成后签发 `LiveShadowReceipt`；small-live 1、small-live 3、formal-live 1h 是否分别只在不可变 `UserFundsAuthorizationReceipt`、最新 `FundsAuthorizationStateLog=BOUND_TO_THIS_RUN`、唯一 `active_admission_run_id`、运行准入 `PromotionDecision` 和准入 `PASS` 均匹配本模型包、运行、资金档、范围、环境、公开钱包/出资方/地址与稳定账户全局账本身份标识时启动；完整结束时是否先追加 `COMPLETED` 终态、关闭准入，再签发绑定授权/准入/环境/账户身份标识、账本起止序号/摘要与终态事件摘要的结果凭据。任一失败、中断、超时、证据不完整或授权到期/撤销，是否立即阻止新提交、追加相应授权终态事件、关闭准入且不自动晋级。完成判定不得作为尚未运行前取得 `ALLOW_SUBMIT` 的循环前置条件。
  18. PromotionSequenceGate：Promotion Controller 是否为 ModelRegistry、`promotion_stage`、逐运行资金准入和仅追加授权/凭据状态的唯一写入器；是否逐项校验凭据链、跨阶段身份标识、阶段本地运行时/硬件证据和用户资金授权；是否要求授权凭据、状态事件、运行准入决策、真实资金结果凭据与实际环境、公开钱包/出资方/地址、稳定账户全局账本身份标识完全相等；是否把每个真实资金阶段的准入决策与完成决策分开，完成决策是否同时绑定结果凭据与终态 `FundsAuthorizationStateLog` 事件，且只有完成推进阶段后才允许签发下一档授权；是否阻止候选 -> paper/live、offline-evaluation -> live、paper -> 真实资金等跨级路径，阻止同一 `(environment, wallet, funder, account_global_ledger_id)` 跨晋级链并发，也阻止同一晋级链内任何并发准入；真实资金账户身份标识或环境变化时是否按第八节回到最早受影响阶段；`ALLOW_SUBMIT` 是否只有在 `active` + 正确阶段 + 有效且唯一绑定本次运行/账户/环境的用户授权状态 + 运行准入决策 + LiveStageGate/运行时前置检查全部同时成立时出现。
  19. SimCalibrationArtifactGate：`sim-calibration` 是否为模拟器版本/产物唯一写入器，输入是否只来自经审查的真实平台证据与预注册独立校准切分，当前晋级留出集是否不可达，数据源、规则、参数、代码、配置、版本摘要、验证和原子发布是否闭合；是否只审计而不能配置或覆盖冷启动 `itode` 与文档规则 `taker_order_delay_ms=250`；training/offline-evaluation/paper 是否只读加载同一冻结身份标识；失败是否只隔离本次校准运行/产物，既有运行中版本是否不可就地修改；新版本发布是否保持旧谱系不变，并仅在后续采用新身份标识或旧规则被权威证据判定不再适用时，由 Promotion Controller 使从 training 起受影响的凭据失效。

  每个门禁必须同时记录 `gate_scope=data_collect|training|offline_evaluation|sim_calibration|promotion|paper|live|deployment`、`gate_verdict=PASS|WARNING|QUARANTINE|FAIL_CLOSED|REQUIRES_HARDWARE_UPGRADE`、原因/证据和范围特定影响，不得用模糊的“看起来正常”作为通过标准。覆盖范围不完整是原因，不是通用执行权限；隔离必须明确隔离的数据源、数据集、运行、模型、校准产物或运行时。

  门禁影响必须按范围隔离：`data-collect` 只能隔离/拒绝受影响数据源或采集运行；training 只能隔离受影响数据集/候选/运行或 `FAIL_CLOSED_TRAINING`，且表现弱不能成为该硬性影响的理由；offline_evaluation 只能拒绝签发或使 `OfflineEvaluationReceipt` 失效，不能改写训练结果；sim_calibration 只能隔离受影响的校准输入/运行/产物、拒绝发布，不能改写正在使用的模拟器或任何模型/凭据；晋级只能阻止 `candidate`/`shadow`/`active` 晋级；deployment 的硬件不足只能阻止相应目标部署并保留 `deployment_hardware_status=requires_hardware_upgrade`；只有 paper/live 运行时门禁才能设置 `execution_permission=ALLOW_SUBMIT|NO_SUBMIT_PERMISSION|CANCEL_ONLY_PERMISSION|PAUSED_PERMISSION`。`CANCEL_ONLY_PERMISSION` 还要求撤单路径自身已经通过硬边界验证；它只允许执行模型已发出的撤单意图或运维方紧急撤单，不会生成策略性撤单。门禁判定、执行权限和 `model_strategy_mode` 不得互相替代。

  ## 十一、部署与运行边界

  1. AWS live-trade 服务器 `108.130.74.143` 是运行目标，不是构建机。
  2. 开发、代码编写以及 training 采集器、paper 热路径、live 热路径、live-trade 主程序等发布版二进制程序的构建必须使用本地 `/home/chillchill/btc-updown-marketmaker`，并记录提交、SHA-256、产物路径和二进制程序角色后分别部署；各远端服务器源码/工作目录统一写作 `~/btc-updown-marketmaker`，不得把服务器上的 `/home/ubuntu/btc-updown-marketmaker` 误作本地路径。
  3. live 服务器不得运行 Rust 编译命令，不得依赖服务器 `git pull`、源码检出或 GitHub 部署密钥；该限制覆盖所有 Rust crate、`data-collect`、paper/live 运行时和工具程序。
  4. 后续 paper/live 适配器阶段完成后，三个 `data-collect` 运行角色必须是三个独立发布版二进制程序和进程：数据采集服务器只部署/运行 `data-collect-training-collector`，paper 交易服务器只部署/运行 `data-collect-paper-hot`，live 交易服务器只部署/运行 `data-collect-live-hot`。当前只有 training 连接真实源，paper/live 只有心跳/状态；不得把本条后续目标写成现状或部署授权。
  5. 模型文件不得通过 GitHub 同步，不得放入 `wallets`；部署模型时必须从本地已验证产物路径直接复制，并记录来源路径与 SHA-256。
  6. `wallets` 只保留被 Git 忽略的敏感凭据（`secret`）、环境变量（`env`）与钱包公共派生配置。
  7. 后续 live 适配器获批并实施时，live 交易服务器上的 `data-collect-live-hot` 必须作为独立外部市场源交接二进制程序，安装路径解析到 `/home/ubuntu/live-trade/data-collect/current/bin/data-collect-live-hot`；它与 live-trade 主交易二进制程序独立部署、独立 SHA-256、独立进程和独立日志，但只能通过版本化本机交接合同供给外部市场源事件，不得接管模型、钱包、订单或账本。该路径合同不表示当前真实适配器已实现或获准部署。
  8. live-trade、paper-trade、training、offline-evaluation、Promotion Controller、sim-calibration、`sim-system`、数据采集的运行根目录、产物根目录和权限边界必须明确。live 交易服务器上的训练模型、live-trade 主交易二进制程序、`data-collect-live-hot` 及其运行时配置路径解析到 `/home/ubuntu/live-trade` 之外必须失败关闭；paper/training/offline-evaluation 的服务器本地路径不得冒充该 live 根目录，training 与 offline-evaluation 也不得互写数据集、检查点、账本或报告/凭据根；Promotion Controller 的 ModelRegistry/决策/凭据状态/`FundsAuthorizationStateLog` 写入器根，以及 sim-calibration 的校准写入器/发布根，不得被任何阶段运行时写入，二者也不得互写。
  9. live-trade 发布版二进制程序不得包含独立交易策略、独立风控决策逻辑、`sim-system`、模拟后端、平行 SDK 客户端或自有 SELL 路径；它只包含 BUY-only 模型加载、共用 `Intent Bridge / Execution Adapter` 的实盘后端、对官方 Rust SDK 的唯一委托、平台反馈回传、产物记录和权限/协议/安全硬边界。
  10. 在数据处理范围内，live 交易小主机只允许本实例增量式 `data-pipeline` 在线流运行时 / Market Context Builder，不得承担训练/离线评估式重处理、离线专用标签或训练专用奖励构建、历史全量 Parquet 扫描、压缩归档批处理、归档/上传/列举/gzip，或 `data-collect` 的编译/测试/检查/格式化；本条不排斥同机按本节边界运行已授权的 `data-collect-live-hot`、live-trade 主程序和其他热路径组件。禁止的重处理与构建工作必须在具备硬件条件的构建机、数据服务器或训练服务器完成；归档/上传/列举/gzip 若纳入范围，必须另开需求、计划和 changelog。
  11. `model-train` 的 Python 源码、`native` Rust 源码、配置和锁文件在正式本地仓库编写与审查，再同步到获准训练服务器的 `~/btc-updown-marketmaker`；训练运行只能在训练服务器进行。首版 native wheel 必须在目标训练服务器上使用清单记录的明确 Python 解释器和锁定 Rust 工具链构建、安装与验收，以避免把本地较新 glibc 生成的本机 wheel 未经验证直接部署；未来若改用经验证的 manylinux 构建产物，必须另行记录兼容标签、构建环境、目标解释器矩阵和 SHA-256。训练服务器缺少兼容解释器、Rust 工具链或构建依赖时必须由部署门阻止运行并报告，不能临时降级为纯 Python 影子实现。本条不授权在 live 服务器安装工具链或编译。

  ## 十二、验收标准

  本次核心需求文档完成后，必须满足以下验收标准：

  1. 权威覆盖基线 `/home/chillchill/btc-updown-marketmaker/mistake-log/2026-07-03-重构前错误汇总与优先级.md` 中每类历史问题都能映射到至少一个新需求、
  门禁、产物或测试。
  2. 新系统流程覆盖自检、热身、T-30 预加载、活跃市场持续决策、市场结束前 T-30 下一个模型预加载。
  3. training、offline-evaluation、paper、live 均使用完整 5 分钟数据 + 30 秒预加载上下文，而不是 250 ms 孤立片段。
  4. 数据采集与数据消费有明确的 schema、清单、原始证据、原子产物和仅追加事件日志边界；`data-collect`、`data-pipeline` 共享处理核心、离线/在线运行时、Market Context Builder、`feedback-ingestion`、归一化层/Ledger 的输入、输出和非范围必须可追溯。training、offline-evaluation、paper、live 必须由四个隔离运行时调用同一版本化处理核心，共享时间线、schema、配置、归一化产物、模型输入和可回放语义，不存在第二套特征/归一化/Observation 实现。
  5. 数据处理只使用谱系源自 `/home/chillchill/new-datasets`、UTC 日不早于 `2026-07-30` 且通过显式清单准入和版本化切分的数据；日期下限前的数据在读取入口失败，不能迁移、兼容、回填、统计、拟合、训练、校准或评估。模型可见特征/Observation 由 `data-pipeline` 共享处理核心生成，市场监督标签、执行反馈标签、`tick_stream`、训练切分、回放上下文和 offline-evaluation 时间外留出集，只能由 `data-pipeline` 离线专用分支生成。`model-train` 只能读取训练清单分片；评估器只能读取与其互斥、同样满足日期下限的留出市场数据和外生市场结果/判定/结算评分证据，不能读取历史执行反馈标签作为当前候选的结果；当前执行证据必须由本次评估运行自身生成；奖励信号只能由训练模块唯一的 RewardContract 生成。上述离线专属信号和产物不得写入 `data-collect` 范围或进入 paper/live 模型输入路径，标签/奖励/成熟度也不得进入评估器的模型可见 Observation。
  6. 共用 `Intent Bridge / Execution Adapter` 的公开执行方法精确为 `post_order`、`post_orders`、`cancel_order`、`cancel_orders`、`cancel_all_orders`、`order`、`orders`、`trades`，且不存在旧公开词汇、调用方可见的模拟器内部派发类型或绕过桥接器的执行路径。模拟后端使用模拟领域类型；实盘后端只调用官方 `Client` 同名方法，并只在提交时使用 `SignedOrder`。每个新的模拟运行必须新建不跨运行复用的窄型、只读、无凭据官方 HTTPS 规则加载器，对有界目标市场集合执行一次冷启动：逐个调用 CLOB `GET /clob-markets/{condition_id}`，并对 Gamma `GET /markets/keyset` 使用重复 `condition_ids` 及分别的 `closed=false`、`closed=true` 查询。加载器必须先对每份原始响应字节计算 SHA-256，再以专用窄 DTO 严格区分缺失与真实 `0/false`；每个目标在 CLOB 与合并后的 Gamma 结果中只能有一个成功逻辑结果，且 condition、代币、结果、tick、手续费启用状态和规则值必须一致。全部校验通过后才落盘并密封 `PlatformRuleSnapshot`，随即释放网络能力；同一运行恢复不重新查询，运行中不热刷新，快照外市场须创建新运行。官方响应缺失、非法、重复、查询失败或冲突时失败即关闭；官方 SDK 缺少 `itode`、`feeSchedule.rebateRate` 或字段存在性表达不再阻断此加载器。未消费的 `oas` 只登记响应覆盖情况，不为它扩展实现；该窄型例外不授权通用 REST 客户端、`serde_json::Value` 任意取值、手工签名、影子交易 SDK 或网页值回退。单笔 `post_order` 返回一个模拟/官方对应响应对象；`post_orders` 返回一个批量响应数组封装，逐项允许混合成功/失败且非原子，一项失败不得回滚已接受的其他项。FOK/FAK 市场 BUY 的 `spend_pusd + worst_price_pusd_per_share`、GTC/GTD 限价 BUY 的 `limit_price_pusd_per_share + size_shares`、两种订单各自的 SDK 编码/一致性、实际 maker/taker 角色、可立即成交限价单、`post-only`、平台 dust、`matched-cancel`、普通 `not-found`、模型单笔/全部撤单、平台到期/关闭、运维方紧急撤单、迟到 user-channel 和挂单审计，都有明确订单生命周期语义；CLOB `min_order_size`（shares）边界与可立即成交 GTC/GTD 限价 BUY 的版本化 `$1` 提交本金边界矩阵必须按 `demand/sim-system-demand.md` 的验收标准通过。程序不存在把市价单改写为限价单、把限价单改写为市价单，或按固定经过时长/市场切换自行生成策略性撤单的路径。
  第 6 项所称规则一致性必须按字段归属解释，不能扩大为所有近似字段的硬相等门：CLOB
  `mos/mts` 是最小下单量和初始价格步长的规则源，Gamma `orderMinSize`/
  `orderPriceMinTickSize` 只作诊断；`negRisk` 取 Gamma，CLOB 响应自然含 `nr` 时也只作诊断；
  完整费用规则取 Gamma `feesEnabled + feeSchedule`，费用启用时才要求 CLOB `fd` 存在并核对
  `rate/exponent/takerOnly`。`itode` 缺失按官方合同解释为 false，费用禁用时允许
  `feeSchedule`/`fd` 省略。Gamma 每批目标不超过 100 项；专用 DTO 必须忽略未知附加字段且
  禁止 `deny_unknown_fields`，全部规则数值使用精确十进制解析并禁止先进入 `f64`。除此之外的
  必需语义缺失、非法或冲突才失败即关闭。冷启动详细字段与测试合同以
  `demand/sim-system-demand.md` 第 10.1 节及 `plan/sim-system-plan.md` 任务 1.0 为唯一细节来源，
  本总集成需求不得另立更宽或更窄的规则。
  7. 真实平台和 `sim-system` 的订单/经济原始证据都必须先通过同一套版本化归一化层，其中订单生命周期进入 NormalizedOrderEvent，maker/taker rebate 应计、日结/付款和结算/赎回进入非订单经济归一化合同；消费路径严格分离：`Intent Bridge（模拟后端） -> sim-system -> 本实例归一化层 -> training/offline-evaluation/paper 账本/审计`，`Intent Bridge（实盘后端） -> 官方 SDK/真实平台 -> live 归一化层 -> live 账本/审计`。真实执行反馈经 `feedback-ingestion` 后只能进入后续 training 数据；独立校准证据只能进入 `sim-calibration`；offline-evaluation 只能读取外生市场结果/判定/结算评分证据，并由本次运行生成当前候选执行结果。验收必须证明 `sim-system` 证据不能进入 live 账本、live 审计或 live 状态，并覆盖未知提交、普通 `not-found`、`matched-cancel`、部分挂单、部分终态、平台在 `executed_base_qty<accepted_base_qty` 时发生的 dust 终态、迟到 user-channel、重复终态证据，以及撤单已请求、待处理、已接受、已拒绝、重试、超时、重叠终态和重试预算耗尽；每笔按 base 计量的引擎接受订单满足 `accepted_base=executed_base+dust_base+canceled_base+expired_base+market_closed_base+open_base`，上述数量单位均为份额。按 quote 预算计量的 FOK/FAK 以固定 SDK 编码等价 `takerAmount`（shares）作为 `accepted_base` 审计投影，响应 `takingAmount` 和逐笔撮合成交表达实际执行证据，以 `max(accepted_base-executed_base,0)` 投影 canceled，并覆盖实际执行超过该投影时 canceled 为零；其资金只由独立 quote 等式守恒。被拒绝意图不进入适用守恒域，pUSD 现金独立守恒，实际成交、dust 终态和预留释放均只入账一次。maker rebate 必须覆盖实际 maker/taker 分段、逐市场历史 `feeSchedule`、按市场汇总、逐笔贡献不可应用、未知不完整、应收款单次入账和恢复不重复；taker rebate 必须覆盖实际收取的 taker 手续费乘固定 Silver `8%`、失败/补偿不应计，以及不查询 wV、不实现动态等级和一次性等级奖金。两类付款还必须覆盖 maker/taker 日桶互不合并、`0.9999 pUSD` 不付款、`1.0000 pUSD` 付款、`1.23459 pUSD` 付款 `1.2345 pUSD` 且锁定 `0.00009 pUSD`、低于门槛金额及余数不进入后续日门槛、D 日 taker 在 D+1 `00:10 UTC` 与 maker 在 `00:45 UTC` 的正常现金可用时刻，以及应计增加 NAV 一次、付款使对应应收款减少与现金/可用现金等额增加但 NAV/PnL/RewardContract 奖励不变。正常时刻存在已经归属 D 但内部最终性未闭合的潜在合格贡献时，原日桶必须保持未日结；最后一项闭合后在首个因果可用内部时刻只日结、付款一次，客户端反馈迟到不改变内部付款时刻。普通训练窗口结束或调用方主动截尾本身不生成 `UNKNOWN_INCOMPLETE`；无论是否跨过正常现金可用时刻，尚未闭合的原日桶都保持未日结。只有已经到达应结算边界且既有异常封存/隔离终态证明闭合永久不可能时才使用既有 `UNKNOWN_INCOMPLETE`。付款事件只携带被消费应计的数量和摘要，不复制完整身份集合。不得读取 Liquidity Rewards 字段、硬编码当前逐市场 `feeSchedule` 参数、增加市场类别/slug 校验器、实现第二套待付款状态机或实现历史 USDC.e 迁移分支；疑似 `250 pUSD` 一次性等级奖金和 V2 切换日的旧版 USDC.e maker rebate 均不得进入普通 pUSD 返佣校准。
  8. 验收必须证明 `sim-calibration` 是模拟器版本/产物唯一写入器，使用经审查真实平台证据与独立校准切分、对当前晋级留出集不可达，并记录数据源/规则/参数/代码/配置/版本摘要、验证和原子发布；对冷启动 `itode` 和文档规则 `taker_order_delay_ms=250` 只能记录规则快照/配置身份和验证结果，不能配置、覆盖或重复保存为地域时延参数；失败只能隔离或拒绝发布。TrainingCandidateReceipt、OfflineEvaluationReceipt、PaperQualificationReceipt 及对应运行清单必须绑定同一冻结的 `sim_system_version + sim_calibration_artifact_sha256`；training、offline-evaluation、paper 都只能只读加载，live、评估器、paper 和训练运行时均不能自行校准或就地改写版本。
  9. 每个训练批次必须生成可审计训练批次排名表，记录候选神经网络层级、模型容量、训练参数、随机种子、训练内数据范围、核心质量指标、`sim-system` 回放诊断、两种盈利机制的分别归因及交互项/残差、资源消耗、产物 SHA-256、候选入选理由和淘汰理由；排名表不得读取晋级用时间外留出集，只能作为冻结候选的训练内选优证据，不能进入 shadow/paper/live 或替代 `OfflineEvaluationReceipt`。
  10. 5090 单卡/多卡训练有硬件实测、运行清单、性能分析和资源水位验收；当前 live-trade 服务器性能不得成为训练模型层级或体积上限，超过当前服务器能力的胜出候选必须保留并进入硬件升级门，而不是被静默淘汰或降级。
  11. 实盘交易 SDK-first 边界、部署边界、模型产物边界、钱包边界、small-live/formal-live 授权边界、`data-collect` 运行边界和 live 服务器运行边界全部明确；`data-pipeline(training)` 当前按“可嵌入 Rust 库 + 可独立验收的薄 CLI”验收，`model-train` 按“独立 Python 训练进程 + 本地 PyO3 native 扩展”验收，不要求把整个训练系统封装成单个 Rust 二进制程序。paper/live 真实适配器不属于 `model-train`；必须先完成 `model-train`（模型训练模块，不是 `data-collect-training-collector`）；之后仍只有在用户决定返回 `data-collect` 时，后续 paper/live 适配器阶段才验收三台对应服务器上的三个独立二进制程序/进程，以及 live 热路径固定路径和不由 live-trade 主程序兼任的边界。
  12. 任何不能验证的结论都必须记录具体 `gate_scope`、覆盖范围不完整原因、`gate_verdict` 和范围特定影响，不能冒充通过；只有 paper/live 范围可以映射为 `NO_SUBMIT_PERMISSION`、`CANCEL_ONLY_PERMISSION` 或 `PAUSED_PERMISSION`，training、offline_evaluation、sim_calibration、晋级、`data-collect`、deployment 必须使用各自的隔离、拒绝签发凭据、拒绝校准发布、晋级阻断或部署阻断效果。
  13. 交易决策和风控完全由模型实现，当前动作面全链路只有 BUY 两个结果侧，不存在自有 SELL；live-trade 承载共用桥接器的实盘后端，而该后端是模型意图到官方 SDK/真实平台的唯一执行委托路径。所有模型请求、桥接器映射、硬边界拒绝、真实平台返回和执行反馈都有可审计映射，且不存在 live-trade 改写模型交易或风控意图、绕过官方 `Client` 或回退到 `sim-system` 的路径。
  14. training、offline-evaluation、paper 和 live 的模型决策都由权威模型可见事件触发；触发可以是实际可见的业务/账本/生命周期事件、经 `ObservationContract` 准入并进入共享规范事件带的数据源覆盖范围/`confirmed_gap` 事件、冷启动或恢复时缺少有效待处理唤醒所产生的一次性 `DecisionBootstrapEvent`，也可以是上一轮模型或模型包调度输出安排的稀疏一次性 `DecisionWakeEvent`；仅采集器可见的传输层/订阅诊断，以及运行时脱离上轮调度意图产生的固定频率、固定周期、周期轮询或固定决策节拍，都不是策略决策路径。模型/基线连续因果输出相同档位不单独构成违规，但每个模型调用、交易/风险意图和调度意图都必须追溯到 `trigger_event_id`、`event_available_time`、`decision_ts`、`causal_cutoff_ts`、父决策、运行时归属的决策范围和适用定时器代次；`sim-system` 按规则快照适用的平台内部暂缓与可配置网络/反馈时延必须作为彼此分离的未来事件调度表达，固定间隔 `tick_stream` 只能作为派生产物，不能成为模型决策触发器。
  第 14 项的权威模型可见事件明确包含由共享规范化器从获准的 aggTrade v7 无效证据生成的规范无效事件；原始坏载荷本身不进入模型，仅采集器可见的解析器文本也不成为触发条件。该事件以采集器接纳时间/序号参与稳定排序；合法 WebSocket 成交不因原始 `a` 跳变而等待或在事后回排，当前日事件进入有界队列时取得接纳坐标；跨日激活未完成时，遵循第 5 项的接纳前暂存例外，并在正常排空后才取得该坐标。
  15. `sim-system` 是模拟平台模块的唯一规范名称，并且只有 training、offline-evaluation 和 paper 交易可以经共用桥接器的模拟后端调用；三者通过八个 SDK 对齐方法提交模拟领域 BUY 请求后，`sim-system` 能生成可归一化、可回放、可审计的平台执行、生命周期、maker/taker rebate 应计、日结/付款和结算反馈。调用方不得依赖内部派发类型或第二套公开方法名；live 交易只经实盘后端委托官方 SDK，不能调用或回退到 `sim-system`。
  16. 两种盈利机制均有分别验收：同一已确认配对数量的实际全成本满足 `outcome_up + outcome_down < 1` 的配对闭合，以及基于因果可见信息在结果趋明时增加当时判断正确侧并持有至结算；手续费、maker/taker rebate 应收款及后续付款只是两种机制的经济组成，不能成为第三种策略。两类返佣应收款分别进入 NAV 一次但不进入可用现金，后续与付款相等的部分转现金不重复产生 PnL 或奖励；低于门槛金额和四位截取余数继续作为不可支出应收款但不进入后续门槛。归因必须保留交互项/残差，全部合计与账户级账本、NAV 和已结算 PnL 对账，不存在第三种隐含交易机制。
  17. `sim-system` 按“用户确认并冻结的项目规则假设，而非 Global 官方公开事实”模拟价格优先、同价 FIFO：挂单 BUY 在引擎接受时间必须排在所有更早接受且尚未消耗的同价份额之后；若此前为 `100 shares`、我方为 `5 shares`，则初始 `quantity_ahead_at_acceptance=100`，我方数量区间为 `(100, 105]`。不确定性只能作用于接受前后的不可见事件与通道可见性，不能随机改变我方同价队尾入队顺序；首版后续 L2 变化的队列结果必须按第七节第 8 条唯一使用 `L2_BOOK_DELTA_FIFO_ESTIMATE_V1`，不能再选择另一套情景 `profile`。具体状态结构、样本记录和验收步骤只能在本需求获批后的计划文档中展开。
  18. 训练模块完成当前模型框架不同神经网络层级、容量和训练参数的公平探索；候选不会因当前交易服务器性能被提前排除，较大胜出模型在服务器升级后通过目标硬件一致性、延迟、内存和长运行验收才能部署。
  19. 爱尔兰延迟 `profile` 只能从日期不早于 `2026-07-30` 的合格数据重新计算，并机械绑定数据根、日期下限、精确日期/范围、输入清单及 SHA-256、schema、过滤与加权口径、主机/区域/端点、样本和分位数、缺失/负值、时钟同步证据状态、长尾政策、生成代码及 `profile_id/version/hash`；复算与独立复核完成前，正式配置档保持 `NOT_READY`，训练、离线评估和模拟盘不得取得正式时延就绪。当前 schema 必须以合同规定的接纳时间作为模型因果可用时间，同时保留较早的原始接收/入站时间用于采集与处理延迟分解，不能把同一段时延重复计算。订单/撤单请求与各反馈传输层链路段分别采样、分别记时，且任何产物或报告都不得把该代理 `profile` 写成真实测得的订单出站、RTT、平台处理或撮合延迟。
  20. 使用固定延迟/联合竞态 `profile` 与随机种子，`sim-system` 必须逐事件可复现，并通过原始证据 -> NOE -> 账本对账：创建订单/提交响应先于 user-channel；user-channel 先于响应；两者未到时，暂定意图只进入 `pending_submit_list`，仅使用合法撤单目标/范围，并持续占用 `pending_submit_reserved_pusd`；撤单请求先于原提交到达时返回 `not-found`/`unresolved`，原提交随后仍可 `accepted`，旧撤单不追溯生效；模型可见的 `accepted`/`remaining` 数量为 `5 shares`、平台已成交但尚未通知 `3 shares` 时，模型对可见订单发出整单撤单，平台只取消真实未成交的 `2 shares`。无数量的撤单确认先进入 `cancel_effect_qty_pending_reconciliation`，把预留原额转入 `cancel_reconciliation_reserved_pusd`，不泄露 `2 shares`，也不按本地可见数量释放预留；迟到成交随后确定 `executed=3`、`canceled=2` 并各应用一次；`fully-matched` 与 `venue-dust-terminal` 返回 `matched-cancel`，FAK 终止/GTD 到期等返回各自终态；平台 dust 下的实际成交、dust 终态、预留释放、`cash_balance_pusd` / `total_reserved_pusd` / `available_cash_pusd` 和结算失败/重试分账正确；任一通道迟到、缺失或重复后，均不重复应用预留、成交、现金或持仓。验收必须证明模型只基于当时可见状态决定取消，而撤单结果由到达模拟平台时的真实订单状态决定。
  21. 使用固定的单笔/批量提交测试用例、延迟/联合竞态 `profile` 与随机种子，`sim-system` 必须可重放：单笔提交生成一个响应对象；批量提交生成一个最多 `15` 项的响应数组封装，同一封装内可同时出现 `live`、`matched`、`rejected`、`ambiguous` 项，每项独立归一化且不污染同批其他项。必须分别验收 HTTP 200 混合结果、明确的请求级/逐项拒绝、结果未知的传输层错误/非决定性 non-2xx/丢失/截断/解析失败、有或无决定性错误的空订单 ID/状态、请求项数与响应项数不匹配和无法关联项；截断或无法解析的批量报文主体不得向客户端暴露部分 `Vec`。批量封装只可有一次客户端可见到达，批内每笔订单的平台真实状态和 user-channel 事件仍独立；归一化后的每个项必须共享同一 `response_envelope_id`、摘要和接收时间，并保留原始封装形状与关联证据。验收必须证明不能只凭 `success` 或响应数组位置认定 `accepted`；明确且可安全关联的拒绝释放对应待处理提交预留，无法安全关联或封装不可见时保留 `UnknownSubmitOutcome` 与待处理提交预留，再由后续权威 user-channel/对账证据逐项解析。
  22. Observation 与决策调度一致性必须分两层验收：首先把同一批可关联、被 `ObservationContract` 准入的数据源证据分别编码为已最终化类型化原始数据与实时交接，证明共享语义规范化器产生相同的规范事件 ID、载荷、因果可用性、模型可见覆盖范围/`confirmed_gap` 字段和 `total-order-key` 摘要；仅采集器可见的传输层/订阅健康状态/重连诊断必须由 `data-collect` 独立验收，不能成为仅在线 Observation 字段或触发条件。随后使用同一规范事件带、内容及序号/摘要相同但来自隔离实例的账本事件/投影、相同初始状态及适用角色的相同检查点、因果截止点、处理流水线配置/状态 schema、归一化产物、模型包与调度合同，分别驱动 training 离线回放、offline-evaluation 离线回放、paper 在线流、live 在线流测试路径，证明运行时归属的范围注册表/总上限、冷启动/T-30 引导、适用角色恢复有效待处理唤醒、恢复时无定时器所产生的单次 `DecisionBootstrapEvent`、重置、基准/到期时间、过期后单次迟到触发、缺口/回补、全源静默、唤醒前事件、事件前唤醒、同刻冲突和失效定时器后，仍产生相同的定时器创建/取消/替换/代次、决策次数/顺序、`trigger_event_id`、Observation 非浮点/规范字段，以及按冻结规范化/浮点量化规则计算的规范观察哈希；training 首版只从干净初始账户的轨迹起点确定性重放，不使用中途模拟器/账户/Ledger 检查点。重连测试只比较随后进入规范事件带的数据源证据与状态影响，不伪造未持久化的传输层原因/时段，模型张量浮点字段必须满足冻结容差。任何首个分歧都必须生成审计产物，其中包含数据源/规范输入摘要、账本序号/摘要、初始状态及适用的检查点摘要、核心/配置/状态/产物/调度器身份标识、定时器轨迹和差异字段，并按范围阻止 training 候选有效性、offline-evaluation 凭据、paper 或 live 部署。
  第 22 项第一层一致性还必须覆盖同一批 `trade -> invalid -> invalid -> trade` 及 `trade 100 -> trade 103 -> invalid -> trade 104` 证据：已最终化的 v7 类型化原始数据与后续版本化交接经同一规范化器后，产生相同的记录类型、数据源 `epoch`、连接代次、规范事件 ID、因果准入、无效标记与 `total-order-key` 摘要，且 ID 跳变不产生 REST 回补、待处理、缺口或重排。v5/v6 只能标为旧版，心跳交接的连接内 `event_seq` 不得冒充数据源/接纳坐标；原始字节、哈希和解析器详情不得进入 Observation。该交接一致性不属于 `model-train`；必须先完成 `model-train`（模型训练模块，不是 `data-collect-training-collector`）；之后仍只有在用户决定返回 `data-collect` 时，它才成为后续 paper/live 适配器的验收目标，不是当前心跳二进制程序已实现能力。
  第 22 项第一层一致性还必须覆盖深度快照待处理项的因果边界：同一批快照 v3/更新 v4 证据在已最终化类型化原始数据与版本化交接中，保留相同的数据源 `epoch`、连接代次、深度入站、采集器接纳与 `stitch`/`not-applied` 语义。更新在 `t1` 先被原始观察、快照在 `t2` 返回时，快照必须先因果接纳，缓冲更新随后按入站顺序接纳；离线/在线路径都不得把 `t2` 才形成的连续性判定回写成 `t1` 模型可见。`stale`、`confirmed-hole` 及正常运行中已经接纳的 `snapshot-unavailable` 更新保留原始证据，但不得错误推进快照锚点/`local_u`；可靠运维方停止/运行截止时间之后的未接纳深度待处理项，以及尚无新日许可的跨日暂存，在已最终化类型化原始数据与交接两侧都不存在，不得只在一侧补造行、缺口或未知标记；旧版快照 v2/更新 v3 只能标为排序未知。该一致性要求不授权采集器维护价位级订单簿，也不得形成第二套共享规范化器或模型特征实现。
  23. 验收必须证明 training、offline-evaluation、paper、live 的 data-pipeline 运行时、进程、队列、检查点、状态缓存、运行根和账本实例完全隔离，只共享处理核心代码与版本化合同；处理核心只读取本实例账本投影，模型没有归一化层/Ledger 写入器权限。市场监督标签、执行反馈标签、奖励、未来市场结果和标签成熟度对 paper/live 依赖图及任何角色的模型可见 Observation 不可达；training 可以从模型动作与本训练实例账本序列重算相同奖励，maker/taker rebate 只能先作为平台事实进入 Ledger/NAV，RewardContract 不得再叠加第二笔返佣奖励，后续付款因 NAV 不变也不得产生奖励；变更奖励配置不得改变账本历史或经济事实，offline-evaluation 不计算或消费奖励作为晋级依据。
  24. 两层评估必须可验收地区分：未经独立 offline-evaluation -> paper/live 配对证据校准的训练内性能指标只产生诊断、`WARNING`、排名或研究证据，不能自动提前停止、阻断整个训练批次或删除/禁用合同有效候选产物；首轮表现型提前停止默认禁用。只有数据/因果/合同/数值/账本/RewardContract/产物的确定性无效问题可以按受影响范围硬性失败，预注册的非表现资源预算只能停止对应试验并保留模型权重/优化器检查点；训练中途的模拟器、账户和 Ledger 状态不作为续跑检查点。独立 offline-evaluation 才能签发晋级判定。
  25. offline-evaluation 必须证明评估合同、阈值、基线策略、初始 Ledger 策略和场景/随机种子集合均在解析留出集日期/市场 ID 前冻结，`evaluation_snapshot_cutoff` 由冻结规则自动计算；`OfflineEvaluationDatasetManifest` 必须从截止点内全部合格区间中选择 `end_time` 最新、与训练同源同 schema，但从未进入训练/开发/校准/验证、HPO、选优或任何人工自适应决策的连续时间外留出集。该留出集必须具备完整市场/T-30、最终化状态、能区分 `known-observed` 缺失性与 `unknown_not_observed` 的覆盖范围、外生评分证据成熟度、由实际依赖时长推导的清洗期（purge）/隔离期（embargo）、不重叠、内容摘要和访问/暴露证据。必须以相同场景/随机种子分别验证自然与预注册注入的单源、多源、全源缺失性及恢复；外部业务消息不完整本身不使结构/因果完整区间失格，采集器停机、数据源未启动或非正常未知尾部也不能冒充可评分缺失性。绝对最新但尚未合格的数据不导致失败；无合格区间、未知覆盖范围过大或统计证据不足必须判为 `QUARANTINE`，不能判模型 `FAIL`。
  26. offline-evaluation 必须只读运行本轮唯一最终候选模型包，禁止训练、调参、重拟合归一化、校准模拟器、改写数据集、写 ModelRegistry 或直接授权 paper/live；候选与预注册基线使用同一规范事件带、初始 Ledger/现金/持仓/预留/挂单状态及场景/随机种子。只有按预先冻结、逐项可解释的标准得到 `PASS`，才输出绑定数据集清单、评估报告、运行时和全部合同身份标识的 `OfflineEvaluationReceipt`。晋级报告必须包含 Ledger/NAV/已结算 PnL、风险、执行现实、基线与统计不确定性并可回放，不得用训练奖励替代经济与执行事实；首轮硬性标准只能是合同/因果/账本、BUY-only/权限、现金/数量守恒、不得超支/资不抵债及预先明示的业务安全风险上限。未经独立 offline-evaluation -> paper/live 配对证据校准的利润、Sharpe/DSR/PBO、最大回撤、胜率、成交率、频率和相对表现阈值一律只作建议。结果一经暴露，任何影响输出或判定的身份标识变化都必须等待更新的未见区间；全部身份标识不变的只读审计重放只可验证原结果，不得改变判定或签发新晋级凭据。
  27. 验收必须证明 `TRAINED_CANDIDATE -> OFFLINE_EVALUATION_PASSED -> PAPER_PASSED -> LIVE_SHADOW_PASSED -> SMALL_LIVE_1_PASSED -> SMALL_LIVE_3_PASSED -> FORMAL_LIVE_1H_PASSED` 不可跳级；TrainingCandidate、OfflineEvaluation、PaperQualification、LiveShadow、SmallLive1、SmallLive3、FormalLive1H 凭据均由对应阶段在满足本文完成条件后签发，凭据本体不可变，只有 Promotion Controller 能校验前序凭据、追加 `consume`/`revoke`/`invalidate` 状态事件并签发 PromotionDecision。每个真实资金阶段必须先准入；运行完整结束后先以终态 `FundsAuthorizationStateLog` 事件关闭准入，再签发绑定授权 ID、准入决策、环境/账户身份标识、账本起止点和终态事件的结果凭据；完成决策同时绑定二者后才能推进阶段，再另行签发下一档授权。跨阶段合同身份标识必须一致，运行时/主机/硬件/初始状态等阶段本地身份标识必须分别绑定；真实资金账户身份标识变化时回到 `LIVE_SHADOW_PASSED` 重跑 small-live 1，环境、live 运行时或硬件身份标识变化时回到 `PAPER_PASSED` 重跑 live shadow。失败、`QUARANTINE`、中断、超时、证据不完整或授权撤销均不得签发 `PASS` 凭据或自动推进。
  28. ModelRegistry、晋级阶段、单次运行资金授权与执行权限必须保持分轴并通过负例验收：`active + 无本次运行有效授权`、`candidate/shadow + 有授权`、`active + 错误/跨级阶段`、`active + 资金档/运行/市场/时长不匹配`、`授权/决策/状态事件与实际环境、公开钱包/出资方/地址或稳定账户全局账本身份标识任一不匹配`、`active + 已撤销/过期/属于其他运行的授权`、`同一 (environment,wallet,funder,account_global_ledger_id) 跨任意晋级链存在另一个未关闭准入`、`同一晋级链存在另一个未关闭准入`、`active + 运行时/LiveStageGate 未通过` 均不得产生 `ALLOW_SUBMIT`；live shadow 无论其他字段如何都固定为 `NO_REAL_FUNDS + NO_SUBMIT_PERMISSION`。只有模型状态为 `active`、阶段正确、不可变授权凭据与 `FundsAuthorizationStateLog` 最新状态一致且唯一绑定当前 `active_admission_run_id`，并且运行准入 PromotionDecision 与全部 live 前置检查中的环境/账户身份标识完全相等，才可在授权范围内提交合法 BUY 意图；运行结束后必须追加终态事件、关闭准入，且不自动续权。
  29. 必须通过 RTDS、CLOB/Gamma、Binance aggTrade 和 Binance 深度/快照的单源静默、多源断开、全部传输层不可达和全部传输层存活但业务静默场景，证明 training 采集器保持同一 PID、健康源继续写盘并逐源恢复。普通业务静默或 `stale` 状态保持中性，不生成缺口/告警或触发代次切换；唯一的当前窄例外是 training RTDS 的稳定期 300 秒/首次新鲜数据 60 秒完整运行时替换，且必须严格先断后连（`break-before-make`），不得产生第二个并存连接。每日清单继续由唯一归属方保存 `collection-day` 覆盖范围与既有计数器；跨日切换、重启、终态和容量行为以当前验收矩阵与代码为准，不复活旧 `100-cycle` 或旧部署门禁作为当前完成条件。
  30. 必须通过普通外部 aggTrade 无效输入、解析器实现/类型化 schema 内部失败、队列/写入器/清单/原子发布、序号溢出、证据所有权丢失、磁盘/资源硬边界和内部不变量的分层故障注入证明：上限内完整无效输入逐条进入同一 v7 产物，且不伪装成成交、不更新最近合格状态或合法成交去重状态，并继续运行；只有后述本地完整性错误按冻结范围隔离或失败关闭。超过 512 KiB（524,288 bytes）的输入只能保存真实有界的超限证据，队列接纳后、持久发布前崩溃必须标为未知尾部，不得把任何一种情况伪装成普通有效行情或已发布证据。
  31. 必须证明 training 能从类型化原始数据接收事实、最近合格时间、已知/未知覆盖范围，并使用虚拟逻辑时钟因果重放真实与预注册注入的数据源可用性、缺失性和恢复，而不依赖精确的传输层时段记录；模型可见 Observation 必须明确区分 `fresh`、`stale`、`missing`、无效、`unknown_not_observed` 及独立 `confirmed_gap` 证据。在一个或多个源缺失、全部源缺失及恢复边界上，确定性的单次信息年龄跨阈值、风险承载状态下有界重新设定唤醒（`re-arm`）基线，与离散 `next_wake` 目标候选，必须在同一 Observation、模拟器、账本、奖励、场景/随机种子和资源合同下比较；两者都必须证明运行时归属范围的总上限、冷启动/恢复根触发、基准/到期时间与过期后单次唤醒语义。目标候选还必须证明业务事件按范围抢占、全源静默时持续唤醒、最小/最大档位、anti-Zeno、定时器一致性和决策资源约束均有效。模型可以输出暂停、不提交、撤销 BUY 或 BUY 对侧结果侧的风险中和意图，且风险中和始终按 BUY-only 双侧持仓、总份额、pUSD 占用和成本真实记账，不得被描述或实现成 SELL、释放现金或真实减仓；调度器不得替模型生成这些动作。
  32. 跨模块验收必须用一个固定的小型跨 UTC 日场景证明训练轨迹边界正确：上游投影门先只读取清单准入且已最终化的数据分片，由 `data-pipeline` 在物理枚举顺序之外建立唯一规范全序，并至少产生一条可由 `sim-system` 消费的真实 `ExecutionMarketEvent` 纵向链路；该链路与黄金事件共同构成 `sim-system` 面向真实训练需要的完成条件。模拟器之后的训练闭环门再从固定现金、无历史持仓/订单/预留/返佣应收款/日桶的干净账户开始完整轨迹，时间范围不对齐完整 UTC 日，同一账户状态顺序跨日连续、不同隔离账户可并行。D 日返佣先进入不可支出应收款，只有正常现金可用时刻到达且内部最终性已经闭合才转为现金并可供后续 BUY 使用；付款前结束的对照轨迹保留应收款且不提前付款。中断后从干净轨迹起点确定性重放，不从中途模拟器/账户/Ledger 检查点续跑。验收不得要求每条样本覆盖完整 UTC 日、固定 288 个市场、强制随机打乱或人工快进到付款，也不得新增第二套原始文件排序器、完整日校验器或返佣专用截尾系统。完整归一化反馈、Ledger、Observation 和 RewardContract 输入合同兼容性的闭环不阻断 `sim-system` 开发；RewardContract 的实际计算由 `model-train` 首门验收。任何测试替身都不得取代前述真实上游投影门。
  33. 目标结构验收必须以根 Cargo 工作区的 `cargo metadata`、按发布目标生成的 `cargo tree` 和实际产物为证据：`contracts` 是无行为叶子 crate；`data-pipeline/core` 不依赖 I/O、网络、Python、`sim-system` 或 `ledger` 实现；`execution/normalization` 不依赖具体后端；`paper-trade` 包含模拟后端和 `sim-system` 但不包含实盘后端；`live-trade` 只包含实盘后端及官方 SDK，且不包含 `sim-system`、模拟后端、PyO3、Python 或离线训练专用 Arrow/Parquet 路径。paper/live 各自嵌入在线 `data-pipeline`，而两个 `data-collect-*-hot` 仍是独立进程。未来目录不得在未获批时以空 crate 或占位实现进入工作区。
  34. Python/Rust 边界验收必须证明 PyO3 只作为 `model-train/native` 的 crate 依赖，maturin 只作为 `model-train` 的构建后端与打包工具：同一输入、初始状态、配置和随机种子的纯 Rust 直接调用，与 `Python -> GymnasiumEnv -> PyO3 -> Rust` 调用必须生成逐项相同的规范事件、模拟原始证据、归一化事件、Ledger 序列、Observation、终态和摘要；`reset`/`step` 必须通过 Gymnasium 官方环境检查。还必须覆盖单进程、多训练工作进程的会话隔离，结构化异常而非正常路径 panic，粗粒度边界吞吐和转换开销基准，以及 `maturin build --release --locked` 后在干净目标环境安装、导入和最小轨迹运行。每个 wheel 必须绑定 Python 解释器/SOABI、体系结构、glibc、Rust/PyO3/maturin 版本、根 `Cargo.lock`、`uv.lock`、Git 提交和 SHA-256；不能把本地可导入冒充训练服务器兼容通过，也不得在 live 服务器编译。
