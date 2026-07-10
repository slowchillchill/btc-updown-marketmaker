# 新采集数据唯一训练源核心重构需求

事项编号：`rebuild-core-new-data-only`

状态：`基础准备草案；FinalDatasetBoundaryStatus=DECISION_REQUIRED；CollectionStartReadinessStatus=DOCS_ONLY_NOT_EXECUTED；不授权数据处理、模拟、训练、paper、live、部署或资金操作`

对应执行计划：`/home/chillchill/btc-updown-marketmaker/plan/rebuild-core-new-data-only-plan.md`

核心训练框架：`/home/chillchill/btc-updown-marketmaker/plan/rebuild-core-new-data-only-training-framework.md`

对应 changelog：`/home/chillchill/btc-updown-marketmaker/changelog/rebuild-core-new-data-only-changelog.md`

历史经验来源：`/home/chillchill/btc-updown-marketmaker/mistake-log/`

采集模块权威需求：`/home/chillchill/btc-updown-marketmaker/demand/data-collect-demand.md`

## 1. 文档权威、当前时点与总目标

1. 本文是“完全不使用旧数据集，只使用未来新采集数据训练”决策下的新需求权威，不在 `/home/chillchill/btc-updown-marketmaker/demand/rebuild-core-demand.md` 或旧数据唯一训练源文档上继续修改。
2. 当前事实时点为 `2026-07-10 UTC`。目标 cohort 尚未采集完成；本事项当前只准备需求、计划、训练核心框架和 changelog，不开始数据处理、模拟、训练或部署。
3. 总目标是建立从新采集市场事实到事件驱动模型、离散事件模拟平台、Model Intent、NormalizedOrderEvent、ledger、reward 和评估的完整可验证链路，并阻断 `mistake-log` 已记录的时间泄漏、固定 tick、短视状态、动作塌缩、输出未消费、reward 错位、账本不闭合、模拟器过度乐观和 leaderboard 误读。
4. 本文定义需求、不变量、能力门和验收标准；具体模型结构、损失、reward 公式和技术取舍由对应核心训练框架定义；阶段、任务、步骤和验证由对应执行计划定义。
5. `/home/chillchill/btc-updown-marketmaker/mistake-log/`、V2 旧实现和旧报告只作为失败证据与设计约束，不是训练样本来源，也不授权复用旧 feature、label、normalizer、模型 bundle、阈值或 leaderboard。
6. 平台规则、费用、奖励资格、订单状态和 SDK 能力必须在实施时按当前官方资料重新冻结。Polymarket 相关能力继续遵循官方 Rust SDK first；SDK 缺口必须先报告并获得用户批准，不得自行建立 raw REST、manual signing、自建 SDK 或 shadow implementation。

## 2. 数据日期边界决策门

用户原始表达“2026-07-11 到 2026-07-23 合计 14 天”存在客观冲突：UTC 首尾包含实际是 13 个自然日，不是 14 个自然日。

在用户确认前，只允许在文档和工具中使用占位符 `DATASET_COHORT_END_EXCLUSIVE_UTC`，不得把任一候选静默写成最终边界：

| 选项 | 目标 market anchor 半开区间 | UTC 日数 | 名义 5 分钟 market 数 | 保留的用户表达 |
|---|---|---:|---:|---|
| A | `[2026-07-11T00:00:00Z, 2026-07-24T00:00:00Z)` | 13 | 3744 | 保留“截至 7 月 23 日” |
| B | `[2026-07-11T00:00:00Z, 2026-07-25T00:00:00Z)` | 14 | 4032 | 保留“从 7 月 11 日开始采满 14 天” |

本文推荐选项 B，因为它同时保留已明确的开始日和“合计 14 天”的持续时间；但该推荐不是替用户作出的决定。`FinalDatasetBoundaryGate` 在用户确认前保持 `BLOCKED`。

开始日 `2026-07-11T00:00:00Z` 已经明确，因此必须把两个 gate 分开：

- `CollectionStartReadinessGate`：只判断固定开始日前不可事后恢复的 event identity、clock receipt、schema/version、price-to-beat authority 和 canonical raw retention；它不依赖 A/B 结束日，但当前仍因本轮只授权写文档而处于 `NOT_EXECUTED`。
- `FinalDatasetBoundaryGate`：决定 13 日或 14 日的结束边界、最终 cohort、calibration/holdout 日期和 manifest；最迟必须在停止目标采集和生成正式 split 前由用户解决。

日期合同还必须满足：

1. `date_basis = UTC`，所有边界使用半开区间 `[start, end)`，禁止用本地时区或含糊的“到某日”为程序输入。
2. cohort 只按 `market_start_time` 决定；名义 market 数不是实际 eligible market 数。
3. `model_context_halo = 30s`：模型可见 event sequence 从 T-30 开始；首个 cohort market 的 T-30 证据可来自 cohort 起点前 30 秒，但只能作为该 market 的因果上下文，不能成为区间外独立训练 episode。
4. `state_reconstruction_lookback` 与 model context 分离：为在 T-30 得到正确 Binance L2，允许向前回溯到最近有效 snapshot/checkpoint并连续应用全部 updates。T-30 之前的更早新数据只用于生成带 lineage/hash 的 checkpoint，不进入模型可见 30 秒序列或独立 episode。
5. cohort/split 起点若没有可连续重建的 snapshot→T-30 链，必须丢弃边界 market，直到出现完全属于该 split 的有效 checkpoint 链；不得跨 split 复用训练事件构造 validation/holdout 初始状态。
6. `label_maturity_tail` 延伸到 cohort 结束之后，直到最后一个 market 的 outcome、resolution、未结订单和经济 reward 全部成熟或被明确标记为 censored；tail 只用于闭合标签/账户状态，不创建区间外训练 episode。
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
7. 在没有真实本账户 execution feedback 的情况下，queue position、fill、partial fill、cancel race、sellable maturity 等不可识别量只能是 `assumed` 或 `stress` simulator 参数；由其生成的策略候选默认 `non-promotable`。任何旧执行 artifact 若未来用于校准，必须另行取得用户批准，且不能作为模型直接输入。
8. `price-to-beat` 是 BTC 5 分钟 Up/Down market 的必需状态与最终结果解释依据。当前 metadata typed schema 未保存平台显式字段；不得把任意开盘附近 RTDS/Binance tick 猜成 price-to-beat。必须建立独立 `PriceToBeatAuthorityContract`，记录 market/condition/slug、数值、完整精度、官方规则/来源、payload/receive time、row locator、首次可用时间、修订/maturity、collector/schema版本与 hash。
9. 默认 SDK-first 派生规则（由官方市场规则、官方RTDS字段语义和本地多market精确对账共同支持的推断，不冒充平台显式字段）是：使用官方 Rust SDK RTDS `crypto_prices_chainlink` 中 `symbol=btc/usd` 且 `payload_timestamp == market_start_utc_ms` 的 full-precision value。`market_start_utc_ms` 来自 slug epoch或Gamma Event `startTime`，不得误用市场创建时间。模型在该 row 的 collector `timestamp_received` 之前只能看到 `p2b_pending`，收到后才变为 `p2b_derived_observed`；不得把 payload timestamp 回拨为提前可见。`p2b_pending` 时只能更新state并允许no-op/no-submit/cancel/risk intent，hard boundary禁止新submit。
10. 缺 exact start-boundary Chainlink row 时，禁止 nearest、forward-fill、interpolate 或 Binance替代，只能 `p2b_unresolved/censored` 并排除 policy/reward。结束边界同样使用 exact full-precision Chainlink row；`final >= p2b => Up`，否则 Down。
11. maturity 分为 `derived_observed` 与 `official_confirmed`。当前官方 Rust SDK可完成 RTDS派生，但尚不暴露Gamma实际响应中的 `eventMetadata.priceToBeat/finalPrice`；这不阻断有exact row的研究性 policy training，却使平台显式确认/promotion gate 标记 `BLOCKED_SDK_FIELD_GAP`。若 promotion 硬要求该字段，必须等待官方SDK支持/经审查的官方新版，或向用户报告并取得例外批准；不得自行使用raw Gamma REST、网页/internal endpoint或自建反序列化旁路。

## 4. Canonical 数据证据与 episode 准入

### 4.1 权威输入

1. canonical 训练证据必须从 `training/typed_raw/utc_day=.../source_kind=...` 下 finalized typed Parquet、同名 segment manifest、run receipt、clock receipt 和 source/writer health evidence 构建。
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
7. approximate batch 可能合并同毫秒多个真实 frame。原子应用 batch 后，必须对每个受影响的 active market 分别产生一次 model-visible event；prefetched market只更新 hidden，不产生 submit intent。不得把每个 fan-out child row当成独立决策事件，也不得把全局多 market 动作含糊合成“一次模型调用”。
8. Binance aggTrade typed 数据当前只保存 `E`，虽 parser 读取 `T`。保存 `T` 是强烈建议；若不改 schema，必须冻结“模型只使用 receive time 与 E，不宣称拥有 trade occurrence time T”的降级合同，不能事后重解释。

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
5. market state 从 T-30 开始 warm up，到 market decision terminal 才停止产生新 intent；market hidden state 必须按 market ID 隔离。账户、现金、仓位、open orders 和 ledger state 属于更长的 `AccountRunTrajectory`，不得在 market switch 时无条件清空。
6. metadata 当前可能把 prefetched next market 也写成 active。episode builder 必须确定性派生 prefetched/active role、Up/Down mapping、market lineage 和 reset receipt；prefetched market只 warm up，不得提前 submit。
7. 全局 Binance/RTDS event 对 active 与 prefetched market 的 state fan-out 必须确定性；NOE/account event 同时路由到 owning market 和当前 active market 的风险评估，不能丢失上一 market late fill 对当前账户的影响。
8. recurrent state 必须由 per-market single-writer 严格有序更新。每个事件到达入口即分配global monotonic `ingress_seq`；路由后只更新 `relevant_pending_watermark[market_id]`，账户事件另更新account domain并fan-out到受影响active markets。每次推理绑定 `state_version + causal_cutoff + relevant_pending_watermark[market_id] + market_id`；完成时只要该domain有更高relevant ingress已到达，即使尚未被actor消费，该intent也必须判stale、不得执行，并由该relevant event触发重评。仅影响另一个prefetched market且不影响共享账户的event不得无故使当前active intent stale。
9. bounded queue 过载时不得丢 event、临时合批或改固定 tick。系统必须停止新 submit/fail closed、继续有序追赶 state，并记录 backpressure；无法在目标硬件满足全部有效事件评估时，ESAR-v1 当前配置即验收失败，先简化模型/候选集后重测。

## 7. 核心模型与训练方法要求

1. 主框架固定为 `ESAR-v1`：小型 stateful Event-State GRU 编码器，加完整联合候选动作 scorer。核心训练框架文档是其技术权威。
2. 必须存在低容量、可解释基线：causal snapshot MLP/逻辑回归和 XGBoost grouped action ranker；神经网络收益必须相对这些基线报告。
3. 必须存在同等参数/延迟预算的 causal TCN challenger，用来验证“GRU persistent state”是否真的带来收益。
4. Transformer/TLOB、Mamba/SSM、Neural CDE、Decision Transformer、IQL/CQL、PPO/SAC 不进入 v1 主路径；只有在数据、simulator 和推理证据满足对应前提后，才能作为新事项评估。
5. v1 recurrent cell 是标准 GRU 加显式 delta-time、freshness、mask 和多尺度输入，不是自定义 time-decay GRU；否则标准 ONNX GRU 的导出依据不成立。
6. 模型必须分层输出：
   - market/outcome/多 horizon dynamics；
   - probability calibration、uncertainty 和 return quantiles；
   - simulator execution/fill/adverse-selection proxy；
   - 完整候选 Model Intent 的 return/risk distribution 与 ranking；
   - explicit no-action/abstain/risk intent。
7. 必须冻结 `ModelOutputConsumerMatrix`，逐字段标记 `training_only_auxiliary`、`runtime_intent_selection`、`risk_utility`、`simulator_diagnostic` 或 `audit_only`。training-only head 不得被包装成 runtime 能力；未参与 runtime 选择的输出不得留在部署接口。
8. alpha AUC、分类准确率或单一 PnL 不得单独授权策略。必须同时验证 calibration、selected-action net EV、downside risk、action support、simulator sensitivity 和 runtime consumer parity。
9. 新采集外部市场数据没有历史 Model Intent、真实 action、NOE、ledger、reward 或 behavior transitions，因此不得直接宣称行为克隆、真实 offline RL 或 Decision Transformer policy training。
10. 执行顺序必须是：先冻结 simulator/NOE/ledger/reward 合同并在非训练小切片闭环；再训练监督式 causal representation；随后由 simulator 生成反事实候选 rollout，训练 grouped action ranking/return distribution，最后做完整 trajectory replay。直接 RL 只能后置。

## 8. 联合动作与 Model Intent 合同

1. 动作不能再次拆成互不约束、可能不被 runtime 消费的 quote/offset/size/risk 独立 heads。
2. scorer 的一个 candidate 必须代表一个完整、合法、可执行的 Model Intent，可包含零个或多个原子操作。v1 最大 `32` 个 candidates/state、每 candidate 最大 `2` 个 logical quote legs 和 `4` 个有确定顺序的 atomic platform operations；配置生成超过上限必须 fail closed，不能按模型 score 预筛。至少覆盖：
   - no-op/保留现有订单；
   - no-submit/abstain；
   - submit；
   - cancel 指定订单；
   - replace；
   - cancel-all/cancel-only/pause；
   - outcome token、BUY/SELL、price offset、size、TTL/time-in-force、target order reference。
3. 初版允许使用版本化、有限离散候选集合控制复杂度，但 candidate enumerator 只能执行平台合法性、tick、min-size、sellable/reserved/open-order 和用户批准的硬风险约束；allowance/permission verdict 只属于 hard-boundary audit，不进入模型策略输入。enumerator 不得替模型生成方向、报价或持仓策略。
4. `pause/cancel_only` 等跨事件模式必须属于 ModelRiskStateMachine，包含 `effective_from`、作用域、退出/expiry、resume event 和重启恢复；命名空间不得与 `PermissionVerdict::NO_SUBMIT/CANCEL_ONLY` 混用。
5. 多操作 candidate 必须定义执行顺序、per-item accepted/rejected、部分失败和补偿语义。
6. 每个 runtime 模型输出必须进入 versioned Model Intent，并在 simulator 和 future runtime 中有唯一 consumer。任何输出被丢弃、重算、硬编码覆盖或只影响日志都必须阻断候选发布。
7. 必须执行逐维 intervention：改变 side、offset、size、TTL、cancel target 或 risk intent，应产生对应 Model Intent 与 simulator 行为变化；否则该维度视为未实现。
8. no-action 是合法经济候选，不得添加人为 idle penalty 强迫交易；但必须通过 counterfactual regret、action diversity 和 action-surface audit 判断是否发生无条件塌缩。

## 9. 离散事件模拟平台要求

1. simulator 是训练的一等模块，不是离线 PnL 脚本。它必须由相同 causal market events 驱动，执行 Model Intent，输出 raw simulation evidence，再走共享 NOE 和 ledger 合同。
2. 必须先冻结 `SimulatorCapabilityMatrix`。v1 只实现已冻结 candidate surface 真正需要的 order type、lifecycle 和会计路径；unsupported capability 必须 fail closed。不得为了“覆盖全部平台”先建设第二套通用交易所、SDK、NOE 或 ledger。
3. 聚合 L2 只能支持 queue/fill proxy，不能声称拥有真实 FIFO queue position。不可观察参数必须以多场景分布表达，至少有 conservative/base/tail-stress，不得只在单一乐观参数点训练和验收。
4. 每个 simulator rule/parameter 必须有 `SimulatorCalibrationManifest`，provenance 只能是 `observed_new_data`、`official_rule`、`approved_external_execution_artifact`、`assumed` 或 `stress`，并记录版本、适用时期、置信度和 promotion capability。
5. 新数据能观察市场演化和版本化 collector host/region/SDK/clock boundary 下的 `collector_visibility_delay`，不能把它称为 wire ingress、纯network或future live主机延迟，也不能观察本账户真实fill/cancel/sellable。只有 `assumed/stress` 的execution simulator可以生成研究性counterfactual label，不得支持promotion声明。
6. counterfactual replay 必须冻结 `CounterfactualMarketImpactContract`。v1 结果只在 `small_agent_exogenous_market_path` 假设和预注册 participation/visible-depth/candidate-size 上限内有效；simulator 必须维护 own-order/own-consumption overlay，并定义后续 observed L2 如何对账。超过 impact 上限的 candidate 必须排除或仅进入 non-promotable impact stress，报告不得声称已建模真实市场冲击。
7. 对同一 state 的候选动作必须使用相同未来 market trajectory 与 common random numbers，输出 paired counterfactual outcomes；随机 seed、simulator version 和输入 hash 相同必须逐事件复现。相同 future tape 是方差缩减证据，不是“我方订单不影响市场”的证明。
8. 全部有效模型可见事件仍执行一次模型评估，但昂贵反事实标签只允许在预注册子集生成。`CounterfactualRolloutBudgetContract` 必须在全量 rollout 前冻结每 market ranking-group 上限、每 group candidate 上限、scenario/seed、总 wall-time/disk预算、每 branch 的 max events/max wall duration/max market transitions/max pending-tail、`candidate_effect_terminal`、超限censor/residual-value规则、按 event type/market phase/state novelty/account-order状态的分层抽样、inclusion probability 和训练权重；抽样不得使用 outcome/PnL，也不得变成 runtime 固定 tick。
9. candidate branch 的正常 terminal 是：该candidate-originated订单、成交、仓位与直接经济效果在owning market settlement/tail中闭合。达到硬上限仍未闭合时不得把截断值当成熟终局；必须标censored，并按ConservativeNavValuationContract保存residual lower/upper bound或排除ranking label。
10. 完整 event-trigger replay 用于最终策略行为验证，不要求对每个 event 重跑全部候选到 settlement。counterfactual implementation 必须复用一次解码的 future tape、checkpoint 和向量化 branch；纵向切片实测预算超限即先减少预注册 label groups/候选/scenarios，不得无界扩量。
11. simulator private state 不得绕过 raw evidence/NOE 直接写 ledger 或 reward；simulation、paper 和 live authority 必须明确区分。

## 10. Reward、惩罚与标签成熟合同

1. 必须区分 `MarketContextEpisode` 与 `AccountRunTrajectory`：前者从 T-30 到 market decision terminal，拥有 market hidden 与市场标签；后者按时间跨连续 market 保留 cash、positions、open/pending orders、late fill、sellable maturity 和 ledger，直到所有经济状态闭合。经济 reward/closure 的唯一权威是 AccountRunTrajectory，per-market attribution 只是可对账派生分解。
2. 必须冻结 `ConservativeNavValuationContract`：逐状态规定 cash、reserved cash、sellable/immature/unsettled shares、open/pending/unknown orders、one-sided/stale book、可执行深度、fee/slippage haircut、terminal payout 和外部现金流如何估值。mid price 不得默认等于可清算价值。
3. 经济权威是上述合同下 ledger 的 conservative liquidation NAV 变化与终局 settled NAV，不是每 tick 人工 reward、spread proxy、分类 AUC 或重复广播的 market PnL。
4. 初始→首决策、决策→决策、末决策→terminal 和 zero-decision market 四类 interval 都必须显式存在；基础 reward 来自：

   `r_economic = conservative_NAV_after - conservative_NAV_before - external_cash_flow`

5. AccountRunTrajectory 终局经济回报必须满足：

   `G_economic = terminal_settled_NAV - starting_NAV - external_cash_flow_adjustments`

6. fee、slippage、实际入账 rebate/reward 和 settlement 一旦进入 cash/NAV，就不得在 reward shaping 中再次加减。
7. 初版允许的核心风险惩罚仅限版本化、预注册并做敏感性分析的：
   - inventory exposure time integral；
   - drawdown/downside tail risk；
   - 经证据证明具有经济含义的 model-initiated churn。
8. 默认禁止：idle penalty、固定每事件惩罚、把 guard/market-end/stale/operator cancel算作模型 churn、任意 reward clip、未成熟 label 写零、重复 settlement reward、为非法动作设置巨大魔法负数。
9. 已知非法动作应在硬约束层移出 candidate；真实不可预测 reject、late/stale 和 cancel race 由 simulator lifecycle 产生经济后果并单独报告，避免重复惩罚。
10. 每个 trajectory 必须保留 reward vector，不得只保留 scalar：realized/unrealized PnL、fees、eligible rebates/rewards、inventory exposure、drawdown、adverse selection、fill、cancel、reject、capital use、settlement、censoring 和 maturity。
11. candidate scorer 必须学习经济回报分布与非负 downside loss。`q05` 不得错误称为 CVaR；CVaR/expected-shortfall 必须来自足够的 empirical rollout lower tail、足够密集的 tail quantiles 或独立校准 head。risk profile 的单位、归一化、权重、tail alpha 和变更规则必须在 frozen holdout 前冻结。
12. settlement/outcome 必须由官方 SDK 可覆盖的 metadata/settlement 路径或经批准的官方结果来源异步回填，记录 winning token、source、maturity timestamp 和 condition/token 一致性。未成熟或冲突 market 不得进入 policy/reward/holdout 统计。
13. 必须分别记录 exactly-one `market_decision_done`、`market_settlement_done` 和 `account_run_done`。AccountRunTrajectory reward 分量必须与 ledger/NAV 闭合；全部 per-market attribution 加 residual/external-flow bridge 后才允许与 account-run 总额对账。

## 11. Split、评估与 promotion 边界

1. 独立统计单位是完整 market 和 UTC 日，不是数亿 event rows。名义事件量不能被描述为同等数量的独立样本。
2. 全部 source、T-30 context、labels、simulator scenarios 和 replicas 必须随同一个 market 进入同一 split；一个 AccountRunTrajectory 也不得跨 split。
3. split 严格按时间前推，禁止 row random split、per-file split 后拼接或按 label/收益挑日期。
4. 最后两个目标 UTC 日作为 calendar-frozen final holdout；倒数第三个目标日是 dedicated calibration；更早日期才是 model development。具体日期只能在 `FinalDatasetBoundaryGate` 解决后机械派生。coverage 不足只会使 gate 失败，不得看到结果后换日期。
5. model development 使用 anchored expanding-window folds 做架构/超参选择；dedicated calibration 只冻结 probability calibration、selection threshold 和 risk profile；最终 holdout 只允许一次正式验收。
6. 每个 split 必须从冻结、与其他 split 独立的 account fixture 开始，按时间在 split 内 carry account state。训练 policy 产生的 cash/position/order state不得带入 calibration/holdout；split 结束若仍有 open/pending/unsettled 状态，必须延伸 maturity tail 完成、或隔离受影响 market，不能把未闭合状态跨界。
7. split boundary 的 purge/embargo 必须至少覆盖 `max(model_context, state reconstruction dependency, feature dependency, label horizon, action TTL, reward maturity dependency)`；同一原始事件不得同时出现在相邻 split 的训练输入中。若 reconstruction chain 太长，丢弃边界 market直到 split 内 checkpoint可用。
8. normalization、feature statistics、candidate priors 和 calibration 只能在对应 train fold/dedicated calibration 拟合；final holdout 不能参与 feature、simulator、threshold、risk weight 或模型选择。
9. 必须比较 no-action、naive directional、简单规则 maker、XGBoost ranker、ESAR-v1 GRU 和同预算 TCN；报告全部 seed、失败 trial 和 selection history。paired counterfactual tie 必须基于 `ΔJ` 的 paired-difference interval 是否完全落入预注册 equivalence band；边际区间重叠不等于 tie。
10. final 只有两个 UTC day clusters，禁止宣称可靠的 day-level bootstrap/cluster confidence interval。必须逐日报告，并用预注册的 contiguous-market block/paired-market interval 作为条件性不确定性；regime稳定性主要来自 development folds。
11. action-surface audit、reward/ledger closure、simulator sensitivity 和 runtime consumption parity 均先于 PnL leaderboard。
12. 只有约 13/14 个 UTC 日的首批数据只能支持开发与候选比较，不足以单独证明跨季节、跨网络、跨平台规则 regime 的 live 稳健性。完成 training bundle、replay 或 frozen holdout 不自动授权 paper、live 或资金操作。
13. final holdout一旦被观察，任何改变dataset/split/simulator/reward/model contract的修复都会使其失去pristine资格；旧两日只能降为diagnostic。重新正式验收必须取得新的未来未读holdout和用户批准，不得在同两日反复重跑挑最好结果。

## 12. 工具、资源与产物边界

1. 数据处理必须 bounded、streaming、sharded，禁止把全部 event rows 一次性载入 RAM/GPU；必须有 peak RSS、GPU memory、I/O、wall time 和失败恢复证据。
2. v1 训练优先复用 PyArrow/Parquet、PyTorch、XGBoost、ONNX 和现有 Rust runtime；不得为通用数据格式、ranker、RNN 或实验台账重复造轮子。
3. 先在单张 5090 上 profile；只有证据表明 GPU 或吞吐是瓶颈，才允许增加 DDP/多 GPU 复杂度。
4. model bundle 至少冻结：dataset/split manifest、PriceToBeatAuthorityContract、StateUpdate/DecisionTriggerContract、feature schema、ModelOutputConsumerMatrix、candidate/state-machine contract、CounterfactualRolloutBudget/MarketImpact contracts、simulator capability/calibration manifest、ConservativeNavValuationContract、reward/risk profile、model weights、normalizer、ONNX opset、training code commit、seed、指标和 sha256。
5. training unroll 与在线 single-step 必须使用同一 cell 和数学定义；PyTorch、ONNX 和 Rust consumer 必须逐事件数值与 decoded intent 对账。
6. future live 目标是约 2 vCPU/1.9 GiB 的运行服务器。模型在本地构建和验证，禁止在 live 服务器编译；event-to-intent p50/p95/p99、峰值内存和持续事件密度必须在等价硬件约束下验收。

## 13. 明确非范围

本文及当前基础准备不授权：

- 修改或部署 data-collect；
- 实际开始或停止 7 月 11 日采集；
- 处理、复制、删除或重写任何新旧数据；
- 构建 simulator、data-pipeline 或模型代码；
- 运行 GPU 训练、paper、shadow、live 或资金操作；
- 复用旧数据、旧 bundle 或旧 leaderboard；
- 因官方 SDK 缺口自行切换 raw REST/manual signing/shadow SDK；
- 因日期冲突替用户静默选择 13 天或 14 天。

## 14. 验收标准

### 14.1 基础准备文档验收

- [ ] 需求、计划、核心训练框架和 changelog 使用同一事项编号并互相引用。
- [ ] 三类文档明确当前只做准备，不声称目标数据已存在或训练已开始。
- [ ] 日期冲突以 `DECISION_REQUIRED` 硬门表达，未静默选边界。
- [ ] 完全禁止 old-datasets、旧 normalizer、旧 bundle 和旧 split 进入正式训练。
- [ ] `mistake-log` 中的固定 tick、短状态、action collapse、未消费 head、reward interval、账本和 split 泄漏均有对应阻断门。

### 14.2 采集后数据准入验收

- [ ] 目标 UTC cohort、model context halo、state reconstruction lookback、label/account tail、collector/version regime 和全部文件 hash 已冻结。
- [ ] canonical builder 从 finalized typed raw + manifests + health/clock evidence 读取，不依赖 `validated_markets` 或物理 row order。
- [ ] price-to-beat exact boundary SDK-RTDS派生、causal receive maturity、full precision和SDK显式确认缺口已形成 authority artifact；缺row未用nearest/Binance伪造。
- [ ] Polymarket transport identity 已生效；或用户明确批准同毫秒原子 microbatch 降级并接受 `representation_only/non-promotable` 能力。
- [ ] 每个 market 的 T-30、depth checkpoint、四连续源 coverage、source gap、sequence、metadata lineage、price-to-beat 和 label maturity 已形成 `EpisodeEligibilityRecord`。
- [ ] unresolved、partial、clock-unknown、identity-unknown 和 version-mixed episode 未被静默收入 `policy_training_eligible`。
- [ ] 首尾 halo 与 split purge/embargo 无交叉泄漏。

### 14.3 模型、模拟、reward 与评估验收

- [ ] XGBoost/低容量基线、ESAR-v1 GRU 和同预算 TCN 在同一数据/split/simulator seeds 上比较。
- [ ] 每个 action 维度有非零 consumer effect；raw score、decoded Model Intent、simulator command 与 ledger effect 可追溯。
- [ ] no-action 合法且未被 idle penalty 扭曲；全 no-action 或全 submit 均触发 action-surface 诊断而非直接解释为成功。
- [ ] simulator 参数具有 provenance、版本和多场景敏感性；聚合 L2 未被描述为真实 FIFO queue。
- [ ] counterfactual state/candidate/seed/scenario/总资源预算、抽样概率/权重和small-agent market-impact能力已冻结；没有全事件全后缀组合爆炸。
- [ ] MarketContextEpisode 与 AccountRunTrajectory 已分离；split account fixture独立，未闭合经济状态不跨 split。
- [ ] ConservativeNavValuationContract覆盖reserved/pending/unsettled/stale/one-sided/depth/terminal；account-run reward 与 ledger/NAV 闭合，三类 done exactly once，未成熟/censored 不写零。
- [ ] ModelOutputConsumerMatrix、single-writer/state-version/stale-intent和TBPTT长程影响测试通过。
- [ ] final holdout 未用于调参，所有失败 trial 和 selection history 已保留。
- [ ] holdout若因合同修复失效，旧两日已降为diagnostic；没有把同两日重跑冒充新pristine验收。
- [ ] 单一 AUC、单一 PnL、command exit 0 或 bundle 存在均不能单独通过 promotion。
- [ ] 首批 13/14 日结果只形成 training/evaluation candidate；paper、live 和资金操作仍需后续独立需求、证据和用户授权。
