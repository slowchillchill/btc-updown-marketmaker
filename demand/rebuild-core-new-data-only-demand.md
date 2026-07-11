# 新采集数据唯一训练源核心重构需求

事项编号：`rebuild-core-new-data-only`

状态：`基础准备修订中；StrategyActionSurface=BUY_ONLY；FinalDatasetBoundaryStatus=DEFERRED_TO_OBSERVED_COLLECTION；CollectionStartReadinessStatus=SCHEMA_V3_DEPLOYED_AND_VERIFIED；不授权模拟、训练、paper、live或资金操作`

对应执行计划：`/home/chillchill/btc-updown-marketmaker/plan/rebuild-core-new-data-only-plan.md`

核心训练框架：`/home/chillchill/btc-updown-marketmaker/plan/rebuild-core-new-data-only-training-framework.md`

对应 changelog：`/home/chillchill/btc-updown-marketmaker/changelog/rebuild-core-new-data-only-changelog.md`

历史经验来源：`/home/chillchill/btc-updown-marketmaker/mistake-log/`

采集模块权威需求：`/home/chillchill/btc-updown-marketmaker/demand/data-collect-demand.md`

## 1. 文档权威、当前时点与总目标

1. 本文是“完全不使用旧数据集，只使用未来新采集数据训练”决策下的新需求权威，不在 `/home/chillchill/btc-updown-marketmaker/demand/rebuild-core-demand.md` 或旧数据唯一训练源文档上继续修改。
2. 当前事实时点为 `2026-07-10 UTC`。目标 cohort 尚未确定；用户已授权补充 Binance aggTrade `trade_time=T`、立即重新部署采集，并根据新版本实际采集质量另行选择正式训练日期。当前不开始模拟、训练、paper、live 或资金操作。
3. 总目标是建立从新采集市场事实到事件驱动模型、离散事件模拟平台、Model Intent、NormalizedOrderEvent、ledger、reward 和评估的完整可验证链路。策略身份冻结为“现金担保、双 outcome、BUY-only 的配对获取、库存调节与持有至结算策略”，并阻断 `mistake-log` 已记录的时间泄漏、固定 tick、短视状态、动作塌缩、输出未消费、reward 错位、账本不闭合、模拟器过度乐观和 leaderboard 误读。
4. 本文定义需求、不变量、能力门和验收标准；具体模型结构、损失、reward 公式和技术取舍由对应核心训练框架定义；阶段、任务、步骤和验证由对应执行计划定义。
5. `/home/chillchill/btc-updown-marketmaker/mistake-log/`、V2 旧实现和旧报告只作为失败证据与设计约束，不是训练样本来源，也不授权复用旧 feature、label、normalizer、模型 bundle、阈值或 leaderboard。
6. 平台规则、费用、奖励资格、订单状态和 SDK 能力必须在实施时按当前官方资料重新冻结。Polymarket 相关能力继续遵循官方 Rust SDK first；SDK 缺口必须先报告并获得用户批准，不得自行建立 raw REST、manual signing、自建 SDK 或 shadow implementation。

## 2. 数据日期边界合同

用户于 `2026-07-10 UTC` 撤销此前固定方案 B 日期，改为在 Binance aggTrade schema v3 部署并重新采集后，根据实际完整性再选择正式训练日期。`FinalDatasetBoundaryStatus=DEFERRED_TO_OBSERVED_COLLECTION`；任何旧的固定日期或名义 market 数都不能继续作为正式 cohort、split 或训练配置。

| 字段 | 当前值 |
|---|---|
| `date_basis` | `UTC` |
| `market_start_time_start_inclusive` | `TBD_AFTER_SCHEMA_V3_COLLECTION_EVIDENCE` |
| `market_start_time_end_exclusive` | `TBD_AFTER_SCHEMA_V3_COLLECTION_EVIDENCE` |
| 覆盖的完整 UTC 日 | `TBD` |
| UTC 日数 | `TBD`；不再强制 14 日 |
| 名义 5 分钟 market 数 | 由最终冻结的完整 UTC 日机械计算 |
| model development / calibration / holdout | 在最终日期冻结后按时间前推原则生成 |

旧方案 B 的 `[2026-07-11T00:00:00Z, 2026-07-25T00:00:00Z)` 与 4032 个名义 market 只保留为已撤销历史，不得作为默认值回流。

必须把两个 gate 分开：

- `CollectionStartReadinessGate`：当前必须先完成 Binance aggTrade schema v3 的 `trade_time=T`、本地测试、重新部署和真实新段验证；该 gate 的通过时刻只形成候选数据版本起点，不自动冻结正式 cohort。
- `FinalDatasetBoundaryGate`：待新版本运行并积累实际数据后由用户重新选择完整 UTC 日期；在此之前保持 `DEFERRED`，不得生成正式 split 或训练。

日期合同还必须满足：

1. `date_basis = UTC`，所有边界使用半开区间 `[start, end)`，禁止用本地时区或含糊的“到某日”为程序输入。
2. cohort 只按 `market_start_time` 决定；名义 market 数不是实际 eligible market 数。
3. `model_context_halo = 30s`：模型可见 event sequence 从 T-30 开始；首个 cohort market 的 T-30 证据可来自 cohort 起点前 30 秒，但只能作为该 market 的因果上下文，不能成为区间外独立训练 episode。
4. `state_reconstruction_lookback` 与 model context 分离：为在 T-30 得到正确 Binance L2，允许向前回溯到最近有效 snapshot/checkpoint并连续应用全部 updates。T-30 之前的更早新数据只用于生成带 lineage/hash 的 checkpoint，不进入模型可见 30 秒序列或独立 episode。
5. cohort/split 起点若没有可连续重建的 snapshot→T-30 链，必须丢弃边界 market，直到出现完全属于该 split 的有效 checkpoint 链；不得跨 split 复用训练事件构造 validation/holdout 初始状态。
6. `label_maturity_tail` 延伸到 cohort 结束之后，直到最后一个 market 的 outcome、resolution、未结订单和经济 reward 全部成熟或被明确标记为 censored；tail 只用于闭合标签/账户状态，不创建区间外训练 episode。每日归档仍只拉 D+1 执行时的昨天 D，不把正在采集的当天分区并入昨天包。
7. 若用户最终禁止读取 cohort 外的新数据，必须剔除缺少完整 context、state reconstruction 或 maturity 的首尾 market，不能伪造、回填或静默缩短上下文。

## 3. 新数据唯一训练源边界

1. 正式训练、validation、calibration、frozen holdout、normalization、特征、标签、market trajectory 和模型选择所用市场事实，只允许来自最终确认 cohort 及其 context/maturity halo 的新采集数据。
2. 严禁读取或混入：
   - `/home/chillchill/old-datasets`；
   - 5090 `/root/autodl-tmp/` 下的历史日期、`data-v1` 至 `data-v5`；
   - `/home/chillchill/V2` 的旧训练数据或派生缓存；
   - 旧 model bundle、feature schema、normalizer、threshold、reward label 或 split manifest。
3. run manifest 必须记录输入 allowlist、显式 old-data denylist、逐文件 sha256、collector commit、binary sha256、schema/config digest 和实际打开文件清单。发现任何旧数据路径或未冻结输入即 fail closed。
4. 当前本地 `/home/chillchill/new-datasets/2026-07-08` 和 `2026-07-09` 仅可用于 schema、质量门和代码准备验证，不得进入目标 cohort 的训练、模型选择或最终评估。
5. 新数据已经具有采集器记录的 `timestamp_received`。本事项不生成 `simulated_received_time`，也不沿用旧数据延迟回填方案。
6. 官方平台规则和经用户批准的 simulator rule artifact 不属于训练市场数据，但必须单独记录 provenance，不能伪装成新数据观察到的执行事实。
7. 在没有真实本账户 execution feedback 的情况下，queue position、BUY fill、partial fill、cancel race、batch逐项结果和pUSD admission等不可识别量只能是 `assumed` 或 `stress` simulator 参数；由其生成的策略候选默认 `non-promotable`。任何旧执行 artifact 若未来用于校准，必须另行取得用户批准，且不能作为模型直接输入。
8. `price-to-beat` 是 BTC 5 分钟 Up/Down market 的必需状态与最终结果解释依据。用户已批准一个范围受限的 SDK 例外：本地 D+1 日归档任务可从官方 `gamma-api.polymarket.com` 读取已结束目标市场的 `eventMetadata.priceToBeat` 与 `eventMetadata.finalPrice`，不得扩展到网页 internal endpoint、下单路径或影子 SDK。
9. Gamma 日级回填的核心成功条件只有两个：同一目标 slug 的 `priceToBeat` 和 `finalPrice` 都存在并可无精度损失地保存为十进制字符串。`closed`、`acceptingOrders`、`umaResolutionStatus`、winner 或公式一致性只能作为可选诊断字段，缺失或不一致不得阻断这两个目标字段落盘、整日归档或其他市场可用性。
10. 日级回填只在 D+1 UTC 02:00 对昨天 D 执行，不设置 D+2 final audit。TLS handshake timeout、timeout、connection reset 或 `URLError` 这类瞬时传输错误必须在本次 D+1 执行内做有界重试；HTTP 业务响应已到达但缺任一目标字段时不做语义重试，只标记 `fetch_succeeded=false` 与 `diagnostic_error=p2b_final_missing`，不得暂停整日流程。P2B artifact schema 升为 `price-to-beat-gamma-v2`，必须逐 row 保存 `gamma_http_attempt_count`。若已有完整 288-slug D 文件，已成功和语义缺失 rows 必须原样复用；仅允许重请文件中标记为瞬时传输错误的 slug。现有文件不符合该 D 合同时应明确失败，不自动重建全部文件。
11. RTDS Chainlink 仍作为实时价格流与因果时间证据保存，但不得再用 RTDS exact-row、winner、closed/resolved 等额外条件阻断 Gamma 两字段回填。Polymarket 与 Binance 两套基准必须分名：`polymarket_price_to_beat` / `polymarket_final_price` 与 `binance_start_reference_price` / `binance_end_reference_price`。
12. future paper/live hot path 必须显式传输同一 slug 下两个 nullable decimal-string 字段：交易期间可用的 `price_to_beat` 与结束后出现的 `final_price`。缺字段只保持 nullable/pending，不新增 closed/resolved/winner 阻断门；consumer 以本机 runtime receive time 更新状态。当前 data-collect paper/live 二进制只有 heartbeat，尚未实现真实 external market event 或这两个字段，不能宣称已支持。

## 4. Canonical 数据证据与 episode 准入

### 4.1 权威输入

1. canonical 训练证据必须从 `training/typed_raw/utc_day=.../source_kind=...` 下 finalized typed Parquet、同名 segment manifest、run receipt、clock receipt 和 source/writer health evidence 构建；完整处理脚本属于本仓库 `/home/chillchill/btc-updown-marketmaker/data-pipeline`，后续部署到 5090 的 `~/btc-updown-marketmaker/data-pipeline` 执行，不继续膨胀每日归档脚本。
2. 当前日归档中的 `validated_markets` 只能作为诊断候选，不能成为训练真相：它裁剪为 `[market_start, market_end)`、不保留完整 T-30，且物理 row order 已实证可能因 part 文件字典序而倒退。
3. exact identity 模式下，训练 pipeline 必须使用 manifest 中数值化的 hour/part、source row ordinal、`timestamp_received`、event sequence 和 child ordinal 确定性重建顺序；approximate 模式使用第 5 节单独定义的 batch key/order。两种模式都不得依赖 Parquet 文件名字符串顺序或物理 row order。
4. `validation_verdict = Complete` 只说明 segment 写入结束，不自动等于 training PASS。训练准入必须重新计算 effective verdict，至少覆盖 hash、schema、row count、partial artifact、source gaps、sequence continuity、时间边界、clock、health 和版本一致性。
5. 单个坏 market 必须隔离到该 market/capability，不得因为一个稀疏源或一个 5 分钟窗口失败而默认拒绝整个健康 UTC 日。

### 4.2 来源与完整性

目标 episode 的物理来源包括：

- `polymarket_market`
- `polymarket_market_metadata`
- `binance_depth_snapshot`
- `binance_depth_update`
- `binance_aggtrade`
- `polymarket_rtds_chainlink_btc_usd`

其中连续市场源为 Polymarket market、Binance depth update、Binance aggTrade 和 RTDS Chainlink；snapshot 与 metadata 是状态初始化和身份解析辅助证据。

每个 market 必须在 `[market_start-30s, market_end)` 上独立验证模型可见窗口，并用 `state_reconstruction_lookback` 验证 T-30 初始状态：

1. slug、condition ID、Up/Down token、market start/end、price-to-beat 和 metadata lineage 一致；
2. Polymarket market 事件按目标 condition/token 过滤且有可解释初始状态；
3. Binance depth 从最近有效 snapshot 开始连续应用全部 update，并在 T-30 生成可复现 L2 checkpoint；不能只复制最近 snapshot 加 market 内 updates；
4. Binance aggTrade 与 RTDS 在相同因果窗口内具有可解释 coverage、freshness 和 gap 状态；
5. `timestamp_received`、source timestamp、schema、collector/version regime 和 source health 可解释；
6. partial/zero-byte/current-running 文件不得被当作 finalized 输入；
7. outcome/settlement label 已成熟，或该 market 被明确标记为 censored/diagnostic，不得把 unresolved 静默写成负类或零 reward；
8. 逐源准入判定式、阈值、downgrade 和缺失 mask 在目标 outcome 不可见时预注册并 hash：PM 双 token/initial state、depth sequence continuity、aggTrade ID gap、RTDS freshness、clock/reconnect/cutover、boundary coverage 与 label maturity。不得采完后根据收益改阈值。

### 4.3 能力分级

每个 market 必须分别生成版本化的数据训练资格和执行校准资格：

- `policy_training_eligible`：完整 T-30、四连续源、depth checkpoint、exact identity/order、price-to-beat 和 label maturity 全部通过；它只说明市场/标签可用于研究性策略训练；
- `representation_only`：允许非关键辅助源缺失并带 mask，但不得产生正式 policy/reward 结论；
- `diagnostic_only`：只用于数据/运行问题分析；
- `excluded`：无法保证因果顺序、身份、标签或基本完整性。

另设 `promotion_execution_calibrated=true|false`。assumed/stress simulator 不阻断 `policy_training_eligible` 的研究性反事实训练，但必须使 promotion 状态为 false。approximate microbatch 默认最多 `representation_only/non-promotable`；只有 future paper/live 也采用同一版本化 batch 语义、通过 parity 且用户单独批准后，才能另行评估升阶，不能默认等同 exact identity。

稀有事件没有在每个 market 出现不是拒绝理由；无法证明状态连续、因果可见或 label maturity 才是硬阻断。

## 5. 时间、事件身份与因果可见性

1. 模型可见时间权威是 exact contract 的 collector causal-ingress `timestamp_received`；source/platform timestamp 和更早的source-task receive time只作为payload/provenance/延迟诊断，不能让晚进入共享因果队列的事件提前可见。
2. exact mode 必须在官方SDK typed event进入共享 causal queue的单一 ingress allocator内，原子采样/分配 `timestamp_received + collector_run_id + global_ingress_seq`，并验证同run的timestamp不随seq倒退。当前source task更早采样的receive time若保留，必须另名为diagnostic字段，不能冒充全局因果可见时间；该改动未在cohort前生效时只能走approximate能力。
3. 必须保存整个 cohort 的连续 clock/NTP receipt。单次 `NTPSynchronized=yes` 点状快照不能补证完整区间；时钟异常区间必须形成 regime 和 capability 降级。
4. exact identity 模式必须同时保存 source-local transport order 与 collector-wide monotonic `global_ingress_seq`；后者与causal-ingress timestamp原子分配。因果顺序按 `(timestamp_received, global_ingress_seq)`，且global seq只在同time tie-break；timestamp/seq不一致即artifact invalid。connection-local/run-local但非全局可比的sequence不得跨source比较，禁止用source timestamp、source优先级或文件遍历顺序伪造全局顺序。
5. Polymarket 一个 transport message 可展开多个 typed child rows。推荐在 cohort 开始前新增并冻结：
   - `transport_message_id`；
   - `connection_epoch`；
   - source-local monotonic `event_seq`；
   - collector-wide monotonic `global_ingress_seq`；
   - `child_index` 与 `child_count`；
   - 共享 `timestamp_received`；
   - SDK envelope/payload digest 或可审计引用。
6. 若上述身份不能在 cohort 开始前生效，唯一可批准降级是 approximate schema。source内batch key至少包含 `source_kind + timestamp_received_ms + collector_run_id + connection_scope_if_available`，成员由稳定row locator列举但视为simultaneous；若不同source同receive-ms且没有global_ingress_seq，还必须形成`CrossSourceTieGroup`，原子应用全部source batches，不声称组内因果顺序。
7. approximate batch 可能合并同毫秒多个真实 frame。原子应用 batch 后，必须对每个受影响的 active market 分别产生一次 model-visible event；prefetched market只更新persistent encoder state/cache，不产生 submit intent。不得把每个 fan-out child row当成独立决策事件，也不得把全局多 market 动作含糊合成“一次模型调用”。
8. Binance aggTrade typed 数据必须保存官方 `T` 为独立 `trade_time`，不得用 `E` 冒充。用户已选择 schema v3 路线并授权修改后立即重新部署采集；正式训练日期只能从新 schema 实际数据中另行选择，旧 v2 数据是否保留只按实际用途决定。

## 6. 严格事件触发与 persistent state

1. 必须分开 `StateUpdateContract` 与 `DecisionTriggerContract`。所有 canonical 事件严格有序更新状态；每个被合同定义为“有效模型可见事件”的 transport/microbatch、NOE 或 due event 都必须触发一次 intent 评估，不得因为事件密度、material-change filter 或计算预算跳过。
2. 模型训练因果回放、simulator、paper 和 future live 的决策都必须由版本化业务事件触发，不得使用固定频率、固定 decision tick、周期 polling 或用 timer 人工制造行情决策。昂贵 counterfactual 标签可以只在预注册 decision-state 子集生成，但这不改变 runtime 的逐有效事件评估语义。
3. 允许的触发必须写入 `DecisionTriggerContract`，至少区分：
   - 原子市场 transport/microbatch 完成；
   - Binance/RTDS 业务事件；
   - simulator/live 的 NOE、fill、reject、cancel、settlement 等执行事件；
   - 模型先前明确授权的 TTL/expiration due event；
   - market lifecycle、risk/account state 变化事件。
4. 数据物理 chunk、GPU microbatch 和 truncated BPTT 边界只用于计算，不是业务事件、模型 reset 或 decision trigger。
5. market state 从 T-30 开始 warm up，到 market decision terminal 才停止产生新 intent；persistent market encoder state/cache必须按market ID隔离。账户、现金、仓位、open orders 和 ledger state 属于更长的 `AccountRunTrajectory`，不得在 market switch 时无条件清空。
6. metadata 当前可能把 prefetched next market 也写成 active。episode builder 必须确定性派生 prefetched/active role、Up/Down mapping、market lineage 和 reset receipt；prefetched market只 warm up，不得提前 submit。
7. 全局 Binance/RTDS event 对 active 与 prefetched market 的 state fan-out 必须确定性；NOE/account event 同时路由到 owning market 和当前 active market 的风险评估，不能丢失上一 market late fill 对当前账户的影响。
8. persistent encoder state/cache必须由per-market single-writer严格有序更新。每个事件到达入口即分配global monotonic `ingress_seq`；路由后只更新 `relevant_pending_watermark[market_id]`，账户事件另更新account domain并fan-out到受影响active markets。每次推理绑定 `state_version + causal_cutoff + relevant_pending_watermark[market_id] + market_id`；完成时只要该domain有更高relevant ingress已到达，即使尚未被actor消费，该intent也必须判stale、不得执行，并由该relevant event触发重评。仅影响另一个prefetched market且不影响共享账户的event不得无故使当前active intent stale。
9. bounded queue 过载时不得丢 event、临时合批或改固定 tick。系统必须停止新 submit/fail closed、继续有序追赶 state，并记录 backpressure；无法在目标硬件满足全部有效事件评估时，ESAR-v1 当前配置即验收失败，先简化模型/候选集后重测。

## 7. 核心模型与训练方法要求

1. 共享策略与训练框架固定为 `ESAR-v1`：stateful Event-State Encoder，加完整联合候选动作 scorer。标准 GRU 只保留为可部署基线，不是预定主模型；当前不冻结参数量、层数或 hidden size 上限。核心训练框架文档是其技术权威。
2. 必须存在低容量、可解释基线：causal snapshot MLP/逻辑回归和 XGBoost grouped action ranker；神经网络收益必须相对这些基线报告。
3. 必须存在 causal TCN challenger，用来验证 persistent state 是否真的带来收益。不同序列骨干必须使用相同因果数据、动作、simulator、经济评估和生产吞吐/延迟门，但不要求参数量相等。
4. Transformer/TLOB、Mamba/SSM、Neural CDE、Decision Transformer、IQL/CQL、PPO/SAC 不进入 v1 主路径；只有在数据、simulator 和推理证据满足对应前提后，才能作为新事项评估。
5. 若候选使用 GRU，其 recurrent cell 必须是标准 GRU 加显式 delta-time、freshness、mask 和多尺度输入，不是自定义 time-decay GRU；否则标准 ONNX GRU 的导出依据不成立。
6. 模型必须分层输出：
   - market/outcome/多 horizon dynamics；
   - probability calibration、uncertainty 和 return quantiles；
   - simulator execution/fill/adverse-selection proxy；
   - 完整候选 Model Intent 的 return/risk distribution 与 ranking；
   - explicit `hold_existing_buys`。
7. 必须冻结 `ModelOutputConsumerMatrix`，逐字段标记 `training_only_auxiliary`、`runtime_intent_selection`、`risk_utility`、`simulator_diagnostic` 或 `audit_only`。training-only head 不得被包装成 runtime 能力；未参与 runtime 选择的输出不得留在部署接口。
8. alpha AUC、分类准确率或单一 PnL 不得单独授权策略。必须同时验证 calibration、selected-action net EV、downside risk、action support、simulator sensitivity 和 runtime consumer parity。
9. 新采集外部市场数据没有历史 Model Intent、真实 action、NOE、ledger、reward 或 behavior transitions，因此不得直接宣称行为克隆、真实 offline RL 或 Decision Transformer policy training。
10. 执行顺序必须是：先冻结 simulator/NOE/ledger/reward 合同并在非训练小切片闭环；再训练监督式 causal representation；随后由 simulator 生成反事实候选 rollout，训练 grouped action ranking/return distribution，最后做完整 trajectory replay。直接 RL 只能后置。

## 8. 联合动作与 Model Intent 合同

1. 新框架及其模型、ONNX接口、Rust解码、simulator enabled capability和自有订单状态机的主动下单集合严格为 `BUY-only`。`BUY` 是系统常量，不是模型可学习、可切换或可由runtime改写的维度；不得保留自有SELL candidate、命令builder、enum分支、feature flag、占位测试、研究路径或未来恢复路线。
2. 目标新cohort外部市场的bid/ask、ask depth和trade side是完整L2与成交事实，必须保留并使用`market_side/book_side/trade_side`命名空间；它们不属于自有动作。官方Rust SDK adapter中的订单`Side`是单独协议边界，只能固定构造`Side::Buy`，不得进入市场side命名空间或模型动作schema。旧本账户SELL/执行artifact仍须用户单独批准，只能校准simulator且不得作为模型输入。
3. 策略的第一条经济路径是异步形成实际配对：只对confirmed持仓的`paired_inventory_confirmed=min(q_up_confirmed,q_down_confirmed)`计算配对结果，candidate报告必须排除initial fixture、provisional/failed fill并相对common baseline计算增量；成本使用冻结的order-invariant pooled-average confirmed cost basis，只计confirmed fill price、实际fee和已入账且可归因的rebate。不得建立影响动作评分的FIFO配对血缘。下单意图或静态盘口的价格和小于1不等于已经盈利。第二条路径是买入互补outcome降低净方向暴露，或在临近结算且证据充分时买入更可能获胜的一侧；互补BUY消耗现金并增加gross inventory，不能描述为主动减仓或释放资金。
4. 单outcome BUY必须保持合法，因为异步配对和库存调节都需要它；不得按动作来源设置“纯单边赌博”类别惩罚。风险只依据当时账户的未配平暴露、尾部损失、资金占用和最终经济结果计算，相同经济状态必须同分。
5. 动作不能再次拆成互不约束、可能不被runtime消费的quote/offset/size/risk独立heads。一个scorer candidate必须代表一个完整、合法、可执行的Model Intent。v1最大`32`个candidates/state；submit candidate只包含`1..2`笔BUY，每个outcome最多一个价格档，batch最多两项。配置生成超过上限必须fail closed，不能按模型score预筛。
6. v1 candidate surface只覆盖：`hold_existing_buys`、单笔BUY UP、单笔BUY DOWN、联合BUY UP+DOWN、取消指定BUY和cancel-all-buys。v1没有replace、pause或cancel-only candidate；模型先取消旧BUY，cancel NOE更新状态并触发新评估后，才能由新的submit candidate决定是否挂新BUY，禁止自动续提或fallback。持久`PermissionVerdict::NO_SUBMIT/CANCEL_ONLY`只属于系统硬边界，不由模型输出。split、merge、redeem均不是模型或runtime动作。
7. 初版使用版本化、有限离散候选集合控制复杂度。candidate enumerator只能执行平台合法性、tick、min-size、pUSD free/reserved/allowance、BUY open-order上限和用户批准的硬风险约束；allowance/permission verdict只属于hard-boundary audit，不进入模型策略输入。不得要求本地ledger、平台回报和链上状态在同一时刻完全相等；时间差必须投影为confirmed/provisional/unknown并只影响相关BUY submit的保守admission，不得因此暂停整个模型、禁用BUY取消能力或暂停其他market。enumerator不得替模型选择outcome、报价、数量或持仓策略，也不得把失败candidate变形成另一笔订单。
8. `CapitalBudgetContract`必须在训练前冻结账户/单market最大投入、最低free-cash reserve、异步第二腿资金预留规则和batch资金预留语义；这些是现金可执行性约束，不是行为类别奖励或惩罚。
9. batch candidate必须定义逐项accepted/rejected/unknown、partial fill和部分失败语义；batch不是原子成交承诺。单个candidate不混合cancel与submit，因此不存在隐式补偿或依赖顺序。
10. 每个runtime模型输出必须进入versioned Model Intent，并在simulator和future runtime中有唯一consumer。必须逐维干预outcome、单腿/双腿presence、offset、size、TTL和BUY cancel target；任何输出被丢弃、重算、硬编码覆盖或只影响日志都必须阻断候选发布。
11. `hold_existing_buys`是合法经济候选，不得添加人为idle penalty强迫交易；但必须通过counterfactual regret、action diversity和action-surface audit判断是否发生无条件塌缩。

## 9. 离散事件模拟平台要求

1. simulator 是训练的一等模块，不是离线 PnL 脚本。它必须由相同 causal market events 驱动，执行 Model Intent，输出 raw simulation evidence，再走共享 NOE 和 ledger 合同。
2. 必须先冻结 `SimulatorCapabilityMatrix`。v1 只实现已冻结 candidate surface 真正需要的 order type、lifecycle 和会计路径；unsupported capability 必须 fail closed。不得为了“覆盖全部平台”先建设第二套通用交易所、SDK、NOE 或 ledger。
3. 聚合 L2 只能支持 queue/fill proxy，不能声称拥有真实 FIFO queue position。不可观察参数必须以多场景分布表达，至少有 conservative/base/tail-stress，不得只在单一乐观参数点训练和验收。
4. 每个 simulator rule/parameter 必须有 `SimulatorCalibrationManifest`，provenance 只能是 `observed_new_data`、`official_rule`、`approved_external_execution_artifact`、`assumed` 或 `stress`，并记录版本、适用时期、置信度和 promotion capability。
5. 新数据能观察市场演化和版本化 collector host/region/SDK/clock boundary 下的 `collector_visibility_delay`，不能把它称为 wire ingress、纯network或future live主机延迟，也不能观察本账户真实BUY fill/cancel、batch逐项结果或pUSD admission。只有 `assumed/stress` 的execution simulator可以生成研究性counterfactual label，不得支持promotion声明。
6. counterfactual replay 必须冻结 `CounterfactualMarketImpactContract`。v1 结果只在 `small_agent_exogenous_market_path` 假设和预注册 participation/visible-depth/candidate-size 上限内有效；simulator 必须维护 own-order/own-consumption overlay，并定义后续 observed L2 如何对账。超过 impact 上限的 candidate 必须排除或仅进入 non-promotable impact stress，报告不得声称已建模真实市场冲击。
7. 对同一 state 的候选动作必须使用相同未来 market trajectory 与 common random numbers，输出 paired counterfactual outcomes；随机 seed、simulator version 和输入 hash 相同必须逐事件复现。相同 future tape 是方差缩减证据，不是“我方订单不影响市场”的证明。
8. 全部有效模型可见事件仍执行一次模型评估，但昂贵反事实标签只允许在预注册子集生成。`CounterfactualRolloutBudgetContract` 必须在全量 rollout 前冻结每 market ranking-group 上限、每 group candidate 上限、scenario/seed、总 wall-time/disk预算、每 branch 的 max events/max wall duration/max market transitions/max pending-tail、`candidate_effect_terminal`、超限censor/residual-value规则、按 event type/market phase/state novelty/account-order状态的分层抽样、inclusion probability 和训练权重；抽样不得使用 outcome/PnL，也不得变成 runtime 固定 tick。
9. 每个ranking group必须有common-baseline branch和group共享的causal cutoff。正常terminal不是简单的owning-market结束，而是所有纳入比较的branch相对baseline产生的差异订单、reservation、UP/DOWN仓位、cash和continuation-policy下游经济状态均已闭合；重叠market因资金占用产生的差异也必须闭合。达到group硬上限仍未闭合的branch必须标censored，并按ConservativeAccountNavValuationContract保存residual lower/upper bound；涉及该branch的pairwise label必须排除或按预注册bounds处理，禁止不同candidate按不同horizon比较。
10. 完整 event-trigger replay 用于最终策略行为验证，不要求对每个 event 重跑全部候选到 settlement。counterfactual implementation 必须复用一次解码的 future tape、checkpoint 和向量化 branch；纵向切片实测预算超限即先减少预注册 label groups/候选/scenarios，不得无界扩量。
11. simulator private state 不得绕过 raw evidence/NOE 直接写 ledger 或 reward；simulation、paper 和 live authority 必须明确区分。

## 10. Reward、惩罚与标签成熟合同

1. 必须区分`MarketContextEpisode`与`AccountRunTrajectory`：前者从T-30到market decision terminal，拥有persistent market encoder state/cache与市场标签；后者按时间跨连续market保留pUSD cash、UP/DOWN positions、open/pending BUY orders、late fill、resolution、resolved-but-uncredited、`platform_settlement_credit_observed`、`unmatched_settlement_credit_suspense`和ledger，直到所有经济状态闭合。经济reward/closure的唯一权威是AccountRunTrajectory，per-market attribution只是可对账派生分解。
2. 必须冻结`ConservativeAccountNavValuationContract`：逐状态规定free/reserved/provisional pUSD、confirmed/provisional/unsettled UP/DOWN positions、open/pending/unknown BUY orders、one-sided/stale book的只读mark、terminal payout、resolved-but-uncredited winning token、`platform_settlement_credit_observed`、`unmatched_settlement_credit_suspense`和外部现金流如何估值。`suspense_amount>=0`，在NAV中作为contra只扣一次，不减少平台confirmed free cash、不进入external cash flow或capital-at-work；它只属于ledger/valuation/audit，不新增模型输入、动作或风险模式。中间mark只用于会计和风险评估，不是可执行清算承诺，不得变成candidate可复用现金。
3. 经济权威是上述合同下ledger的conservative account NAV变化与终局settled NAV，不是每tick人工reward、spread proxy、分类AUC或重复广播的market PnL。
4. 初始→首决策、决策→决策、末决策→terminal 和 zero-decision market 四类 interval 都必须显式存在；基础 reward 来自：

   `r_economic = conservative_account_NAV_after - conservative_account_NAV_before - external_cash_flow`

5. 必须分开candidate与完整账户回报。ranker只使用同group、同cutoff相对common baseline的`G_candidate = branch_NAV - baseline_NAV - branch_differential_external_cash_flow`；完整policy evaluation与账本闭合才使用：

   `G_account = terminal_settled_NAV - starting_NAV - external_cash_flow_adjustments`

6. fee、slippage、实际入账rebate/reward和settlement一旦进入cash/NAV，就不得在reward shaping中再次加减。`platform_settlement_credit_observed.flow_class=internal_settlement`，是winning token到pUSD的账户内部资产转换，绝不进入`external_cash_flow`扣减；external cash flow只允许账户边界的真实入金、出金等外部资金变化。
7. 初版允许的核心风险惩罚仅限版本化、预注册并做敏感性分析的：
   - `inventory_exposure_integral=integral(abs(q_up_confirmed-q_down_confirmed) dt)`；signed net quantity只作方向状态，UP-heavy与DOWN-heavy镜像状态必须同罚；
   - drawdown/downside tail risk；
   - 仅在现金约束的跨market trajectory仍不能体现机会成本、且在frozen holdout前预注册时，才可加入capital-usage time integral。
8. 默认禁止：idle penalty、固定每事件惩罚、按submit/cancel/reject/fill次数或动作类别打分、把guard/market-end/stale/operator cancel算作模型churn、任意reward clip、未成熟label写零、重复settlement reward、为非法动作设置巨大魔法负数。订单生命周期的实际经济影响只能通过ledger/NAV、风险暴露和资金占用进入效用。
9. 已知非法动作应在硬约束层移出 candidate；真实不可预测 reject、late/stale 和 cancel race 由 simulator lifecycle 产生经济后果并单独报告，避免重复惩罚。
10. 每个trajectory必须保留reward vector，不得只保留scalar：realized/unrealized PnL、fees、eligible rebates/rewards、actual paired quantity/all-in edge、unpaired exposure-time、gross inventory、drawdown、adverse selection、BUY fill/cancel/reject、capital-usage time、cash exhaustion、settlement wait、settlement、censoring和maturity。配对完成、成交次数和类别标签不得作为额外奖励重复计分。
11. candidate scorer 必须学习经济回报分布与非负 downside loss。`q05` 不得错误称为 CVaR；CVaR/expected-shortfall 必须来自足够的 empirical rollout lower tail、足够密集的 tail quantiles 或独立校准 head。risk profile 的单位、归一化、权重、tail alpha 和变更规则必须在 frozen holdout 前冻结。
12. Polymarket outcome label 由已批准的 Gamma 两字段异步回填生成：`polymarket_label_up = polymarket_final_price >= polymarket_price_to_beat`，并记录 source、fetch/maturity timestamp 和 artifact hash。只有缺失或无法解析这两个字段才使 label 未成熟；SDK settlement/winner、RTDS boundary 或 condition/token 交叉检查如果保留只能作独立诊断，不得阻断 Gamma 两字段落盘、label 生成或整日归档。
13. 必须分别记录 exactly-one `market_decision_done`、`market_settlement_done` 和 `account_run_done`。AccountRunTrajectory reward 分量必须与 ledger/NAV 闭合；全部 per-market attribution 加 residual/external-flow bridge 后才允许与 account-run 总额对账。
14. 资金状态必须互斥：`order_reserved_pusd`是真实订单预留，`provisionally_committed_pusd`是尚未确认且已不在order reservation的承诺，`unsettled_position_cost_basis`是已离开cash的未结算仓位成本，`second_leg_headroom`只属于candidate admission预算且不得改写ledger/NAV。`capital_at_work`只加总前三个互斥桶。权威resolution后losing部分立即转为realized loss并退出capital-at-work；winning claim继续占用到`platform_settlement_credit_observed`。losing-only账户不等待不存在的回款即可闭合。`capital_usage_time`不进入NAV haircut；candidate若启用capital权重只使用`delta_capital_usage_time=branch-common_baseline`，完整policy才使用总量。
15. 必须冻结`SettlementLedgerTransition`并持久保存market terminal outcome：权威resolution使losing token资产归零、确认损失且不产生pUSD；winning token原子转为resolved-winning-claim。任何在resolution后才转为confirmed的late fill必须在同一幂等transition中直接归入losing loss或winning claim，不能遗留unsettled token。`platform_settlement_credit_observed`正常顺序下以幂等event key原子注销对应winning claim并增加等额pUSD，`external_cash_flow=0`，禁止claim与cash双计。
16. 平台回款证据若先于本地claim，不得丢弃、拒绝、伪造claim或暂停整个模型；必须把实际观察到的pUSD与等额`unmatched_settlement_credit_suspense`同时入账，后者是内部对账contra而非external cash flow。pUSD是否可提交新BUY仍只由现有平台现金/allowance/admission权威决定，不能因本地归因延迟额外冻结已获平台确认的现金。claim后到时原子注销claim与对应suspense，不得再次增加cash；partial/aggregate credit只能消费可匹配outstanding claim，未归因余额继续留在suspense。非零suspense必须进入branch-differential closure；超过tail仍无法归因时，受影响`G_candidate`、per-market attribution及包含该缺口的`G_account/account_run_done`都保持censored或显式residual bridge，不得冒充成熟终局，但runtime、其他market和BUY cancel继续运行。对应capital release time在归因前只能保存censored bounds，不得继续累加一个伪精确locked-capital标量。用户指定的平台约十分钟自动兑回只建模为该模型外事件，不是本方redeem action；官方协议只保证resolved后可redeem，在取得当前账户级自动回款分布前只能使用`assumed/stress`延迟并保持non-promotable，禁止按固定十分钟伪造现金。

## 11. Split、评估与 promotion 边界

1. 独立统计单位是完整 market 和 UTC 日，不是数亿 event rows。名义事件量不能被描述为同等数量的独立样本。
2. 全部 source、T-30 context、labels、simulator scenarios 和 replicas 必须随同一个 market 进入同一 split；一个 AccountRunTrajectory 也不得跨 split。
3. split 严格按时间前推，禁止 row random split、per-file split 后拼接或按 label/收益挑日期。
4. 最终日期由用户基于新 schema 实际采集情况另行冻结；冻结后最后两个完整 UTC 日作为 calendar-frozen final holdout、倒数第三个完整 UTC 日作为 dedicated calibration，更早日期作为 model development。冻结前不得预看收益选择日期，冻结后 coverage 不足只会使对应数据 gate 失败。
5. model development 使用 anchored expanding-window folds 做架构/超参选择；dedicated calibration 只冻结 probability calibration、selection threshold 和 risk profile；最终 holdout 只允许一次正式验收。
6. 每个split必须从冻结、与其他split独立的cash-only account fixture开始：仅有固定starting pUSD，UP/DOWN positions、open/pending orders、reservation、resolved claim均为零。随后按时间在split内carry account state，所有持仓成本只能由本split的confirmed ledger fills产生。训练policy产生的cash/position/order state不得带入calibration/holdout；split结束若仍有open/pending/unsettled状态，必须延伸maturity tail完成、或隔离受影响market，不能把未闭合状态跨界。
7. split boundary 的 purge/embargo 必须至少覆盖 `max(model_context, state reconstruction dependency, feature dependency, label horizon, action TTL, reward maturity dependency)`；同一原始事件不得同时出现在相邻 split 的训练输入中。若 reconstruction chain 太长，丢弃边界 market直到 split 内 checkpoint可用。
8. normalization、feature statistics、candidate priors 和 calibration 只能在对应 train fold/dedicated calibration 拟合；final holdout 不能参与 feature、simulator、threshold、risk weight 或模型选择。
9. 必须比较`hold_existing_buys`、naive directional BUY、简单paired-BUY/hold-to-settlement规则、XGBoost ranker以及经用户批准并预注册的stateful encoder candidates；causal TCN必须作为序列结构challenger。所有候选使用相同因果数据、split、动作、simulator seeds、经济评估和生产吞吐/延迟门，但不要求参数量相等；报告全部seed、失败trial和selection history。paired counterfactual tie必须基于`ΔJ`的paired-difference interval是否完全落入预注册equivalence band；边际区间重叠不等于tie。
10. 若最终仍只保留两个 UTC day clusters 作为 final，禁止宣称可靠的 day-level bootstrap/cluster confidence interval。必须逐日报告，并用预注册的 contiguous-market block/paired-market interval 作为条件性不确定性；regime稳定性主要来自 development folds。
11. action-surface audit、reward/ledger closure、simulator sensitivity 和 runtime consumption parity 均先于 PnL leaderboard。
12. 首批最终选定日期的数据只能支持开发与候选比较，不足以单独证明跨季节、跨网络、跨平台规则 regime 的 live 稳健性。完成 training bundle、replay 或 frozen holdout 不自动授权 paper、live 或资金操作。
13. final holdout一旦被观察，任何改变dataset/split/simulator/reward/model contract的修复都会使其失去pristine资格；旧两日只能降为diagnostic。重新正式验收必须取得新的未来未读holdout和用户批准，不得在同两日反复重跑挑最好结果。

## 12. 工具、资源与产物边界

1. 数据处理必须 bounded、streaming、sharded，禁止把全部 event rows 一次性载入 RAM/GPU；必须有 peak RSS、GPU memory、I/O、wall time 和失败恢复证据。
2. v1 训练优先复用 PyArrow/Parquet、PyTorch、XGBoost、ONNX 和现有 Rust runtime；不得为通用数据格式、ranker、RNN 或实验台账重复造轮子。
3. 先在单张 5090 上 profile；只有证据表明 GPU 或吞吐是瓶颈，才允许增加 DDP/多 GPU 复杂度。
4. model bundle 至少冻结：dataset/split manifest、PriceToBeatAuthorityContract、StateUpdate/DecisionTriggerContract、feature schema、ModelOutputConsumerMatrix、BUY-only candidate contract、CapitalBudgetContract、SettlementLedgerTransition、CounterfactualRolloutBudget/MarketImpact contracts、simulator capability/calibration manifest、ConservativeAccountNavValuationContract、reward/risk profile、model weights、normalizer、ONNX opset、training code commit、seed、指标和 sha256。
5. training unroll 与在线 single-step 必须使用同一 cell 和数学定义；PyTorch、ONNX 和 Rust consumer 必须逐事件数值与 decoded intent 对账。
6. future live 目标是约 2 vCPU/1.9 GiB 的运行服务器。模型在本地构建和验证，禁止在 live 服务器编译；event-to-intent p50/p95/p99、峰值内存和持续事件密度必须在等价硬件约束下验收。

## 13. 明确非范围

本文及当前执行不授权：

- 除 Binance aggTrade `trade_time=T` schema v3、已批准的 D+1 归档修复和对应重新部署外，扩大修改 data-collect；
- 把重新部署时刻自动解释为正式 cohort 固定起点；
- 除已批准的昨天 D 拉取、基础市场筛选、两字段文件、打包与上传外，处理或复制其他训练数据；仍禁止删除、覆盖或重写 raw/old dataset；
- 构建 simulator、data-pipeline 或模型代码；
- 在新核心系统中实现、保留占位或规划自有SELL能力，以及把split/merge/redeem暴露为模型或runtime动作；该排除是最终能力边界，不是deferred事项；
- 运行 GPU 训练、paper、shadow、live 或资金操作；
- 复用旧数据、旧 bundle 或旧 leaderboard；
- 将已批准的 Gamma 两字段日级回填例外扩大为 raw REST/manual signing/shadow SDK；
- 替用户静默选择固定起止日、14 日或 4032 个 market。

## 14. 验收标准

### 14.1 基础准备文档验收

- [ ] 需求、计划、核心训练框架和 changelog 使用同一事项编号并互相引用。
- [ ] 三类文档明确当前训练仍处于准备阶段，schema v3 与重新部署已完成，D+1 归档按当前合同执行；不声称目标 cohort 已存在或训练已开始。
- [ ] 四文档均记录旧方案 B 已撤销，正式日期等待新 schema 实际采集后由用户重新选择。
- [ ] 完全禁止 old-datasets、旧 normalizer、旧 bundle 和旧 split 进入正式训练。
- [ ] `mistake-log` 中的固定 tick、短状态、action collapse、未消费 head、reward interval、账本和 split 泄漏均有对应阻断门。
- [ ] 历史SELL余额成熟死循环以“新核心主动订单不可表达SELL”闭环，而不是保留sellability overlay、递减重试或未来恢复路径；外部book ask/market side仍作为行情事实保留。

### 14.2 采集后数据准入验收

- [ ] 目标 UTC cohort、model context halo、state reconstruction lookback、label/account tail、collector/version regime 和全部文件 hash 已冻结。
- [ ] canonical builder 从 finalized typed raw + manifests + health/clock evidence 读取，不依赖 `validated_markets` 或物理 row order。
- [ ] D+1 Gamma 日文件将 `priceToBeat` 与 `finalPrice` 无精度损失地保存为十进制字符串；只以这两个字段是否存在判定回填目标，其他诊断字段不阻断归档；瞬时传输错误有界重试，字段缺失不重试，重跑只重请已有 D 文件中的瞬时失败 rows。
- [ ] Polymarket transport identity 已生效；或用户明确批准同毫秒原子 microbatch 降级并接受 `representation_only/non-promotable` 能力。
- [ ] 每个 market 的 T-30、depth checkpoint、四连续源 coverage、source gap、sequence、metadata lineage、price-to-beat 和 label maturity 已形成 `EpisodeEligibilityRecord`。
- [ ] unresolved、partial、clock-unknown、identity-unknown 和 version-mixed episode 未被静默收入 `policy_training_eligible`。
- [ ] 首尾 halo 与 split purge/embargo 无交叉泄漏。

### 14.3 模型、模拟、reward 与评估验收

- [ ] XGBoost/低容量基线、经用户批准并预注册的stateful encoder candidates与causal TCN在同一因果数据、split、动作、simulator seeds、经济评估和生产吞吐/延迟门下比较；不要求参数量相等。
- [ ] 每个 action 维度有非零 consumer effect；raw score、decoded Model Intent、simulator command 与 ledger effect 可追溯。
- [ ] candidate schema没有可学习order side或risk-intent第二权威；ONNX/Rust/simulator只可表达`hold_existing_buys`、BUY UP、BUY DOWN、单腿/双腿BUY和BUY cancel/cancel-all，任何自有SELL、replace、pause、cancel-only或redeem intent/command/enabled capability均使验收失败；系统PermissionVerdict仍独立有效。
- [ ] simulator覆盖双腿batch逐项mixed result、异步单腿、partial fill、第二腿现金不足、BUY cancel、cancel NOE后重新评分、late fill、fee/rebate、resolved-but-uncredited和`platform_settlement_credit_observed`；不把batch当原子成交，不在cancel后自动续提，也不在平台回款前伪造pUSD。
- [ ] `hold_existing_buys`合法且未被idle penalty扭曲；全hold或全BUY submit均触发action-surface诊断而非直接解释为成功。
- [ ] simulator 参数具有 provenance、版本和多场景敏感性；聚合 L2 未被描述为真实 FIFO queue。
- [ ] counterfactual state/candidate/seed/scenario/总资源预算、抽样概率/权重和small-agent market-impact能力已冻结；没有全事件全后缀组合爆炸。
- [ ] MarketContextEpisode 与 AccountRunTrajectory 已分离；split account fixture独立，未闭合经济状态不跨 split。
- [ ] ConservativeAccountNavValuationContract覆盖free/reserved/provisional pUSD、BUY pending/unknown、UP/DOWN unsettled、stale/one-sided只读mark、terminal和资金重新可用；中间mark不冒充可执行清算或可复用现金，account-run reward与ledger/NAV闭合，三类done exactly once，未成熟/censored不写零。
- [ ] settled net PnL、actual paired all-in edge、unpaired exposure-time、gross inventory、capital-usage time、time-to-pair、cash exhaustion和settlement closure均独立报告；fill count或pair标签不冒充利润。
- [ ] `G_candidate`与`G_account`分名且终点一致；UP-heavy/DOWN-heavy镜像暴露同罚；paired指标只累计confirmed baseline-relative增量；capital reservation/headroom/at-work互斥且不在NAV与risk中双扣。
- [ ] closure property覆盖`0.98`完整集成本→`1.00`内部结算回款→`G=+0.02`，以及losing-only、winning-only、完整pair和partial pair；内部结算回款未被误分类为external cash flow，losing-only不等待不存在的credit。
- [ ] SettlementLedgerTransition证明loser归零、winner转claim、平台credit原子注销claim并增加pUSD、重复event幂等且claim/cash不双计；confirm→resolution→credit、resolution→confirm→credit、resolution→credit→delayed-confirm及partial/aggregate/duplicate credit排列最终收敛；断言`NAV=ledger_pUSD_total+valued_assets-suspense-other_liabilities`且suspense只扣一次、不进capital-at-work，非零tail使受影响candidate/account closure保持censored但不全局暂停；全部split从cash-only零仓位fixture开始，初始成本基础无歧义。
- [ ] ModelOutputConsumerMatrix、single-writer/state-version/stale-intent和TBPTT长程影响测试通过。
- [ ] final holdout 未用于调参，所有失败 trial 和 selection history 已保留。
- [ ] holdout若因合同修复失效，旧两日已降为diagnostic；没有把同两日重跑冒充新pristine验收。
- [ ] 单一 AUC、单一 PnL、command exit 0 或 bundle 存在均不能单独通过 promotion。
- [ ] 首批最终选定日期的数据只形成 training/evaluation candidate；paper、live 和资金操作仍需后续独立需求、证据和用户授权。
