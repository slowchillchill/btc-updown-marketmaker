# 新采集数据唯一训练源核心重构 Changelog

事项编号：`rebuild-core-new-data-only`

状态：`基础准备修订与 D+1 归档验证；FinalDatasetBoundaryStatus=DEFERRED_TO_OBSERVED_COLLECTION；BinanceAggTradeSchemaStatus=V3_DEPLOYED_AND_VERIFIED；未开始模拟或训练`

对应需求：`/home/chillchill/btc-updown-marketmaker/demand/rebuild-core-new-data-only-demand.md`

对应执行计划：`/home/chillchill/btc-updown-marketmaker/plan/rebuild-core-new-data-only-plan.md`

核心训练框架：`/home/chillchill/btc-updown-marketmaker/plan/rebuild-core-new-data-only-training-framework.md`

## 2026-07-10：创建新数据-only基础准备文档族

### 用户决策与当前授权

- 用户决定另建一套完全不使用 old-datasets 的需求、计划和核心训练框架。
- 训练市场数据计划只使用未来新采集 cohort；旧日期、`data-v1` 至 `data-v5`、旧 normalizer、旧 bundle 和旧 split 不进入正式训练。
- 本轮环境上下文与审查时点是 `2026-07-10 UTC`；用户明确原先给出的 7 月 11 日至 7 月 23 日是未来计划区间，需先采集完成再训练。
- 当前只授权基础准备文档，不授权修改/deploy collector、数据处理、simulator/model实现、训练、paper、live、资金或 destructive action。
- `mistake-log` 与 V2 旧代码只用于错误证据和设计审查，不是训练数据来源。

### 日期冲突

- 发现“2026-07-11 到 2026-07-23 合计 14 天”存在客观冲突：首尾包含是 13 个 UTC 日。
- 候选 A：`[2026-07-11T00:00:00Z, 2026-07-24T00:00:00Z)`，13 日，名义 3744 个 5 分钟 market。
- 候选 B：`[2026-07-11T00:00:00Z, 2026-07-25T00:00:00Z)`，14 日，名义 4032 个 5 分钟 market。
- 文档推荐 B，因为保留开始日和“14 天”持续时间，但没有替用户决策；所有 final boundary/split 保持 BLOCKED。
- 拆分 `CollectionStartReadinessGate` 与 `FinalDatasetBoundaryGate`：固定7月11日开始日前不可恢复准备不等待A/B结束日；A/B只阻断停止边界、final manifest和split。当前开始准备仍未获本轮执行授权。
- 进一步区分 target market anchor、T-30 model context、回溯至最近snapshot的state reconstruction lookback、末尾label/account maturity tail；更早证据只生成checkpoint，不是区间外独立训练episode。

### data-collect 与新数据只读事实

- 当前 physical source kinds 为：
  - `polymarket_market`
  - `polymarket_market_metadata`
  - `binance_depth_snapshot`
  - `binance_depth_update`
  - `binance_aggtrade`
  - `polymarket_rtds_chainlink_btc_usd`
- 四类连续源是 Polymarket market、Binance depth update、Binance aggTrade 和 RTDS；snapshot/metadata 是辅助状态和身份来源。
- 因果可见轴是 collector `timestamp_received`，source/platform timestamp 不得让晚到事件提前可见。
- Polymarket `timestamp_received` 当前是在官方 SDK typed event 交付给 collector 后采样的本机时间，不是 wire ingress 或纯网络延迟。
- Binance aggTrade parser 读取 `E` 和 `T`，当前 typed schema 只保存 `E`。新数据-only训练不需要模拟 received time；若不保留 T，必须明确只有 received+E，不能声称有 trade time T。
- 当前本地 `/home/chillchill/new-datasets` 只有 2026-07-08/09 样本；这些样本只用于准备审查，不进入目标 cohort。

### price-to-beat 关键补充与SDK能力

- 第一轮全局审查发现原稿遗漏原核心明确要求的 `price-to-beat/p2b`；当前data-collect metadata typed schema也没有该字段。
- 只读实证 `btc-updown-5m-1783598400`：start边界Chainlink RTDS full-precision value `62720.96267198819`，collector在start后约1.028秒收到；平台关闭后event metadata显示相同priceToBeat。end边界value也与finalPrice完全一致；连续窗口满足下一market p2b等于上一market final。
- 默认 SDK-first 派生冻结为：official Rust SDK RTDS `crypto_prices_chainlink`、`btc/usd`、`payload_timestamp == market_start` 的exact full-precision value；start用slug epoch/Gamma Event startTime，不用market creation date。
- 收到exact row前模型只能看到`p2b_pending`，收到后才是`derived_observed`；不得把payload timestamp回拨成提前可见。
- 缺exact row禁止nearest、forward-fill、interpolate或Binance替代，只能unresolved/censored并排除policy/reward。
- 官方Rust SDK当前覆盖RTDS派生与Gamma event/market基本字段，但不暴露Gamma实际响应中的`eventMetadata.priceToBeat/finalPrice`。因此研究训练可用derived-observed；平台显式`official_confirmed`/promotion保持`BLOCKED_SDK_FIELD_GAP`，不得自行走raw Gamma REST、网页internal endpoint或自建反序列化旁路。

### canonical 输入问题

- 当前 `validated_markets` 不是可直接训练的 canonical truth：日归档只选择 `[market_start, market_end)`，丢失模型需求的 T-30。
- 归档 tar.gz 只含 validation report、accepted/rejected 和 validated bundles，不含完整 typed raw/segment manifests；本地日期目录当前仍有 pulled typed raw，因此后续必须保护完整 raw 日拉取，不能只依赖 tar.gz。
- 当前归档脚本对 `--part=N` 文件使用普通字典序，并在写 selected Parquet 前不重新排序。实测多个 selected PM market 出现约 300 秒 receive-time 倒退。
- 新计划要求 canonical builder 从 finalized typed raw、segment manifests、numeric part、row ordinal 和稳定 event identity 重建顺序，不依赖 physical row order。
- segment 的 `validation_verdict=Complete` 不自动等于训练 PASS；已观察到 Complete manifest 同时存在 non-empty source gaps。训练准入需独立计算 effective verdict。
- 2026-07-08 的 validation PASS 是 75 个 observed candidate 中 74 个 accepted，不代表完整 UTC 日 288 个名义 market 全部存在。

### Polymarket transport identity 问题

- 当前 `price_change` transport frame 可 fan-out 为多条 typed rows，但 schema 未保存 durable transport message ID、connection epoch、child index/count 或 run-local monotonic event sequence。
- 当前样本中同 receive 毫秒存在大量 child rows；逐 child row 触发模型会重复处理同一个业务 transport event。
- 推荐 cohort 前补齐 identity；若未补齐，只能由用户明确批准“同 `timestamp_received` 毫秒为一个 approximate atomic microbatch”的降级，不能宣称 frame exact。
- 新要求继续坚持 SDK-first：identity 必须在官方 SDK typed path 内生成，不允许为此引入 raw WebSocket/REST shadow path。

### T-30、depth 与 market role

- 原始 typed 数据实证包含 next market prestart 事件，但 validated bundle 裁掉它们。
- Binance depth 不能只取最近 snapshot 加 market 内 updates；必须从 snapshot 连续应用全部 updates，并在 T-30 生成 checkpoint。
- next market 在 prestart 时可能仍被 typed metadata 标成 `lifecycle_state=active`；episode builder 必须自行派生 prefetched/active、market lineage、Up/Down mapping 和 reset receipt。
- 重叠 5 分钟 market 的 T-30 会与上一 market 后 30 秒共享全局 Binance/RTDS evidence，因此 split 需要按 market 分组并执行 purge/embargo。

### label maturity 新阻断项

- 当前远端只读盘点的 728 个 Polymarket segment manifests、约 1.81 亿 rows 中，`market_resolved_rows=0`。
- collector 当前通常订阅到 market end 后约 5 秒；未观察到 resolution 时写 unresolved observation，不能据此生成最终 winner/settlement label。
- 采集本身可以继续，label 可在结束后异步回填；但未成熟 market 不能进入 policy/reward/holdout。
- 后续优先通过官方 Rust SDK 可覆盖路径获取最终 outcome。若 SDK 缺能力，必须先报告用户，不自行使用 raw REST/Gamma shadow path。

### 当前运行快照

- 2026-07-10 10:53 UTC 左右只读快照：训练采集 service active/running，`NRestarts=0`，当前 release commit `5c0d8907...`，binary sha256 `8279b338ffbb2f9bf5ec480df458cdea9569c0dad400a0320c63f62bbf53efff`，点状 NTP 状态同步，六类 writer 在推进。
- 该快照只说明当时运行状态，不证明未来 13/14 日数据完整，也不能补证完整区间 clock sync。
- 7 月 9 日 aggTrade T08 事故及 4-byte partial 是旧于目标 cohort 的已知异常；目标 cohort 仍需冻结 release/schema/config，任何中途升级必须形成 cutover/regime。

### 训练框架决策

- 主框架冻结为 `ESAR-v1 = Event-State GRU + Joint Candidate-Action Ranker + Discrete-Event Simulator`。
- 主干初始采用标准小型 stateful GRU加delta-time/freshness/mask/multiscale inputs；hidden 从 T-30 持续到decision terminal，按 market ID 隔离；重叠 market state 并存。
- 分离 `MarketContextEpisode` 与 `AccountRunTrajectory`：market hidden按market隔离；cash/positions/open/pending/late-fill/settlement在account run中跨market持续，经济closure只以account run为权威。
- 强制基线：常数/逻辑回归、小 MLP、XGBoost grouped ranker、no-action、naive directional 和简单规则 maker。
- 强制同预算 challenger：causal TCN。
- 只借鉴 DeepLOB 的小型 book spatial encoder，不照搬其固定股票 LOB 模型。
- Transformer/TLOB、Mamba/SSM、Neural CDE 后置，除非同数据/预算下证明稳定收益和 live latency/memory可接受。
- BC/AWBC、IQL/CQL、Decision Transformer 不作为 v1 主线，因为新数据没有真实 behavior action/reward transition。
- PPO/SAC 后置，因为未校准 simulator 中的 RL 容易利用 simulator 漏洞，且当前 hybrid action 复杂度没有必要。

### 动作合同决策

- 删除“相互独立 quote/offset/size/risk heads 再由 runtime拼接”的路径。
- 一个 scorer candidate 表示完整 Model Intent，可包含 no-op、no-submit、submit、cancel、replace、cancel-all/cancel-only、pause，以及 token/side/offset/size/order type/TTL/target order ref。
- v1 candidate硬上限为32/state、2 logical quote legs、4 ordered atomic operations；超过上限配置fail closed，不按score预筛。
- candidate enumerator 只做当前平台合法性、sellable/reserved/open-order和批准硬风险约束，不生成方向或报价策略；allowance/wallet权限只在不进模型的preflight/audit层。
- 每个模型字段必须有唯一 consumer；逐维 perturbation 必须改变 Model Intent 和 simulator behavior。
- 新增`ModelOutputConsumerMatrix`；training-only辅助head不进入部署接口，也不能被宣传成runtime控制能力。
- 模型持久`Pause/CancelOnly`状态机与权限层`NO_SUBMIT/CANCEL_ONLY`分命名空间；allowance只在permission/preflight，不进入模型账户输入。
- no-action 是正常经济候选，不添加 idle penalty 强迫成交。

### simulator 决策

- simulator 是训练的一等离散事件平台模块，不是 PnL 后处理脚本。
- Model Intent 必须产生 raw simulation evidence，再经过共享 NOE 和 ledger；simulator private state 不得直接写 reward。
- 参数 provenance 分为 `observed_new_data`、`official_rule`、`approved_external_execution_artifact`、`assumed`、`stress`。
- 聚合 L2 只支持 queue/fill proxy，不能声称真实 FIFO position。
- 新数据只测量版本化collector host/region/SDK/clock边界的collector-visibility delay和市场路径，不能冒充wire ingress/future-live延迟，也不能识别本账户fill/cancel/sellable；只有assumed/stress execution参数的候选默认non-promotable。
- 同 state candidate 使用相同 future path 和 common random numbers，比较完整 trajectory，不只看即时 spread。
- 增加`CounterfactualRolloutBudgetContract`：runtime仍逐有效事件评估，但昂贵全候选labels只在与收益无关的分层state子集生成，并冻结groups/candidates/seeds/scenarios/总预算/inclusion weight。
- 增加`CounterfactualMarketImpactContract`：固定future tape只在small-agent上限内有效；维护own-order/consumption overlay，超容量动作排除或non-promotable stress。
- 增加`SimulatorCapabilityMatrix`：v1只实现candidate实际使用能力，unsupported fail closed，避免过度建设通用交易所模拟器。

### reward 决策

- 经济权威为 ledger conservative NAV 差和 terminal settled NAV。
- 新增`ConservativeNavValuationContract`，覆盖reserved/pending/unknown、sellable/immature/unsettled、stale/one-sided book、可执行深度、haircut和terminal payout。
- 明确initial→first-decision、decision intervals、last-decision→terminal和zero-decision四类reward interval；三类done分别是market decision、market settlement和account run。
- reward 先保存完整 vector，再用版本化 risk profile 选择 candidate。
- 核心 utility 只对经济回报、inventory exposure、drawdown 和 downside tail risk做预注册权衡。
- reject/late/stale/churn 默认先作为诊断和 hard gate；未经经济证据不随意塞入 scalar，避免 double count。
- 禁止 idle penalty、固定 tick penalty、破坏 terminal signal 的 clip、重复 settlement、未成熟写零和把 guard cancel计作模型 churn。

### split 与首批能力判断

- 日期确认后，最后两个 calendar UTC 日固定为 final holdout；倒数第三日为 calibration；更早日期做 anchored expanding development folds。
- split 以完整 market/day 为统计单元；同 market 的 source、T-30、labels、candidate 和 simulator replicas 同 split。
- final holdout 只允许一次正式验收，不因 coverage或收益难看换日期。
- 每split使用独立冻结account fixture，一个AccountRunTrajectory不跨split；final只有两个day clusters，不能宣称稳健day-level bootstrap CI。
- holdout若因合同修复作废，已观察两日只能降为diagnostic；新正式验收需要未来未读holdout和用户批准。
- 13/14 日即使有数亿 rows，独立 calendar regime 和 market 数仍有限；首批结果只可形成 training/evaluation candidate，不能单独证明长期 live 稳健性。

### 外部证据核验

- [DeepLOB](https://arxiv.org/abs/1808.03668)：CNN处理 LOB 空间结构、LSTM处理时间依赖；作为小型 book encoder 参考。
- [TCN 系统比较](https://arxiv.org/abs/1803.01271)：causal convolution是必要 challenger 依据。
- [TLOB](https://arxiv.org/abs/2502.15757)：复杂模型收益需与简单 MLP 和交易成本现实对照。
- [ONNX GRU](https://onnx.ai/onnx/operators/onnx__GRU.html)：标准 hidden-state I/O 支持 single-step runtime 合同。
- [XGBoost Learning to Rank](https://xgboost.readthedocs.io/en/stable/tutorials/learning_to_rank.html)：支持按 query/group训练 pairwise/LambdaMART ranker。
- [ABIDES](https://arxiv.org/abs/1904.12066) 与 [Queue-Reactive](https://arxiv.org/abs/1312.0563)：只借鉴离散事件、消息/延迟和状态依赖 intensity 思想，不移植第二套交易所/ledger。
- [Polymarket Market Channel](https://docs.polymarket.com/market-data/websocket/market-channel)：验证一个 price_change message 可含多个 child changes。
- [Polymarket Order Lifecycle](https://docs.polymarket.com/concepts/order-lifecycle)：验证 post-only、partial fill未成交部分、订单/交易状态需要独立 lifecycle。
- [Polymarket RTDS](https://docs.polymarket.com/market-data/websocket/rtds)：验证Chainlink `btc/usd` 的payload timestamp/value语义，作为p2b exact-boundary SDK-first派生依据。
- [Polymarket SDK清单](https://docs.polymarket.com/api-reference/clients-sdks) 与 [官方Rust SDK](https://github.com/Polymarket/rs-clob-client-v2)：核对RTDS/Gamma覆盖和eventMetadata字段缺口。
- CQL/IQL/Decision Transformer论文仅用于判断数据前提，不构成当前采用理由。

### 文档落盘

- 新建 `demand/rebuild-core-new-data-only-demand.md`。
- 新建 `plan/rebuild-core-new-data-only-plan.md`。
- 新建 `plan/rebuild-core-new-data-only-training-framework.md`。
- 新建 `changelog/rebuild-core-new-data-only-changelog.md`。
- 四份文档使用相同事项编号并互相引用。
- 原核心需求和旧数据-only文档不作为本事项写入目标；本事项开始时记录 hash：
  - `demand/rebuild-core-demand.md`：`23cc0f9c61941218a36648d5c30fd1d4293d05c75e036b92ea59bfad9746f36a`
  - `demand/rebuild-core-old-data-only-demand.md`：`ebefb4a301a77cc11e89f1edc183658a4047c8227ddf2a927a368eeaf39f841b`
  - `plan/rebuild-core-old-data-only-plan.md`：`4f36f339d0c19519c9489084756e28a3ecc16e0ea3b67764d7a6f441cd51dd44`
  - `plan/rebuild-core-old-data-only-dataset-processing-plan.md`：`7f58039e8a9b217d099c9cae3ad920a56667e025f385029f1ef85132e63d5200`
  - `changelog/rebuild-core-old-data-only-changelog.md`：`d12c58b0f9374a079868dc3d94249d1305ef78cd4570669410a6c3e5f018ce8f`

### 未执行事项

- 未修改任何 collector/schema/runtime/model/simulator代码。
- 未启动、停止或重启远端 service。
- 未处理、删除、重写或上传任何数据。
- 未运行训练、paper、shadow、live 或资金操作。
- 未把当前 active service 或 7 月 8/9 样本解释为目标 cohort 已完成。

### 仍待后续决策

1. 若 cohort 开始前无法补 transport identity，是否接受同 receive 毫秒 atomic microbatch 的 approximate 降级；推荐补 identity，不推荐长期依赖降级。
2. Binance aggTrade `T` 是否在 cohort 前补入 typed schema；框架可在只有 receive+E 时工作，但必须接受无法使用真实 trade occurrence time 的限制。

### 审查记录

- 初版机械自审通过：事项编号/互链、Markdown fence、尾随空白、关键合同断言和旧文档hash均正常。
- 第一轮数据合同审查：`0 Critical / 3 High / 3 Medium`，结论NEEDS_FIXES。主要问题是30秒halo不足以重建depth、正式纵切过晚、A/B错误阻断start readiness、准入阈值未预注册、approx schema/order不自洽、数据资格与promotion校准混合。
- 第一轮训练方法审查：`2 Critical / 6 High / 5 Medium`，结论NEEDS_FIXES。主要问题是全事件全后缀rollout组合爆炸、market/account经济episode缺失、market-impact/live并发/TBPTT/output consumer/candidate状态机/valuation不闭合，以及tie/continuation/TCN/calibration/过度模拟问题。
- 第一轮全局对抗审查：`3 Critical / 8 High / 4 Medium`，结论NEEDS_FIXES。额外发现核心p2b遗漏、权限材料混入模型、Training Stage与主计划阶段命名歧义、source arrival措辞和holdout被观察后的处理不完整。
- 已按最小修复方向同步四文档：start/final gate拆分、p2b exact SDK-RTDS合同、state reconstruction、pre-cohort非训练纵切、exact/approx能力、两层episode/account fixture、rollout budget、market impact、simulator capability、valuation、output consumer、single-writer/stale intent、TBPTT长程测试、paired ΔJ、continuation hash、dedicated calibration与holdout失效规则。
- 第二轮数据合同定点复审：`0 Critical / 0 High / 0 Medium`，PASS；context/reconstruction、start/final gate、pre-cohort slice、预注册准入、exact/approx、training/promotion和p2b全部闭合。
- 第二轮训练复审仍发现`0 Critical / 2 High / 2 Medium`：T0/T1映射倒置、single-branch无horizon、stale缺ingress watermark、partial-fill market措辞错误；均已修复。
- 第三轮训练复审仍发现`0 Critical / 2 High / 1 Medium`：single-candidate terminal与AccountRun terminal冲突、global watermark会被无关prefetched event误stale、changelog未同步；已统一candidate_effect_terminal/hard-limit censor，改为per-domain relevant pending watermark并补本记录。
- 跨source同receive-ms另补collector-wide global ingress exact序列；缺失时使用simultaneous CrossSourceTieGroup并保持representation-only/non-promotable，禁止拿connection-local sequence伪造跨源顺序。
- 需求权威补入holdout一旦观察后合同修复即降为diagnostic、重新正式验收必须使用未来未读holdout并取得用户批准。
- 第四轮训练定点复审：`0 Critical / 0 High / 0 Medium`，PASS。
- 第二轮全局复审曾发现`0 Critical / 1 High / 4 Medium`：causal receive与global ingress分配可能乱序、stale watermark作用域过宽、主时序图漏画模拟器冻结、split边界重建归属不清和changelog待同步。已将causal receive/run/sequence改为共享causal-ingress临界区原子分配并断言时间单调，watermark改为market/domain相关pending，补齐模拟器步骤，并把重建前缀限定为split独占且从相邻split purge。
- 最终全局对抗复审：`0 Critical / 0 High / 0 Medium`，PASS；确认当前仅授权基础文档、13/14日边界仍为`BLOCKED`且start readiness独立、全链路new-data-only，并确认event触发、p2b、阶段顺序、holdout失效、权限命名空间、causal ingress、stale watermark与branch terminal/horizon合同闭环。

## 2026-07-10：用户确认方案 B

### 日期决定

- 用户明确选择推荐方案 B，原 `FinalDatasetBoundaryStatus=DECISION_REQUIRED` 更新为 `RESOLVED_OPTION_B`。
- 正式 target market anchor 半开区间冻结为 `[2026-07-11T00:00:00Z, 2026-07-25T00:00:00Z)`，覆盖 `2026-07-11` 至 `2026-07-24` 共 14 个完整 UTC 日，名义 4032 个 5 分钟 market。
- split 日期机械冻结为：model development=`2026-07-11..2026-07-21`，dedicated calibration=`2026-07-22`，frozen final holdout=`2026-07-23..2026-07-24`。
- 候选 A 与 `DATASET_COHORT_END_EXCLUSIVE_UTC` 占位符不再是当前配置；后续正式 manifest、split 与训练配置若使用其他边界必须 fail closed。

### 授权边界不变

- 本次决定只解决日期边界，不代表目标 cohort 已形成，也不代表数据完整性或训练准入已 PASS。
- `CollectionStartReadinessStatus` 仍为 `DOCS_ONLY_NOT_EXECUTED`；未授权修改/deploy collector、数据处理、模拟器实现、训练、paper、live 或资金操作。
- 原审查记录中“13/14 日边界 BLOCKED”描述的是方案 B 确认前的审查时状态，由本节决定正式取代。

### 决策同步审查

- 第一轮限定复核：`0 Critical / 0 High / 1 Medium`；发现计划主时序图仍写“解决结束日”，可能重新打开已冻结选择。
- 已改为“应用已解决的方案 B 边界并冻结 dataset manifest/eligibility/split”。
- 最终定点复核：`0 Critical / 0 High / 0 Medium`，PASS；确认日期、名义 market 数、split、四文档状态和未授权边界一致。

## 2026-07-10：撤销方案 B、收紧 Gamma 回填并授权 schema v3 立即重新部署

### 本节取代的旧决定

- 用户明确撤销固定 `[2026-07-11T00:00:00Z, 2026-07-25T00:00:00Z)`、14 个完整 UTC 日和 4032 个名义 market 的方案 B 权威。
- 新状态为 `FinalDatasetBoundaryStatus=DEFERRED_TO_OBSERVED_COLLECTION`：先完成 Binance aggTrade schema v3 重新部署并持续观察，再由用户根据实际数据完整性选择训练日期。
- 旧方案 B 在本文上一节仅作历史决策记录，不再是配置、cohort、split 或 holdout 默认值。

### Gamma 回填过度校验修正

- 用户否决了必须同时满足 closed、acceptingOrders=false、UMA resolved、winner one-hot 和公式一致性才视为 MATURE 的方案；该方案会把非核心诊断放大成数据不可用或整日终止风险。
- 新核心条件只有同一 slug 的 `eventMetadata.priceToBeat` 和 `eventMetadata.finalPrice` 同时存在。closed/resolved/winner 字段如果保留只能作诊断，不得阻断两个目标字段落盘。
- 回填仅在 D+1 UTC 02:00 对昨天 D 执行；不增加 D+2、不扩展到网页 internal endpoint。用户随后明确更正：TLS handshake timeout 这类瞬时传输错误应当重试；只有 HTTP 已到达但 `priceToBeat/finalPrice` 缺失才不做语义重试。因此实现改为瞬时传输错误最多 3 attempts，已有 D 文件只重请瞬时失败 rows，不覆盖已成功或语义缺失 rows。P2B artifact 升为 `price-to-beat-gamma-v2`，逐 row 记录 `gamma_http_attempt_count`。

### hot path 实现审计

- 当前 `runtime.rs` 的 `PaperHot/LiveHot` 只启动 heartbeat/status handoff server，`handoff.rs` 的 frame 不包含 `price_to_beat` 或 `final_price`；paper/live 目录也没有真实市场事件生产路径。
- future hot path 需求已冻结为两个 nullable decimal-string 字段，缺失不阻断 handoff，不增加 closed/resolved/winner 门。本轮不将此文档需求冒充为已实现热路径。

### Binance 两类价格证据与 schema 决定

- Polymarket 平台字段保持命名为 `polymarket_price_to_beat/polymarket_final_price`；Binance aggTrade 边界价另名为 `binance_start_reference_price/binance_end_reference_price`，不冒充 Chainlink/Polymarket 结算权威。
- Binance 边界选取规则冻结为 `T <= boundary` 的最后一笔；同 `T` 取 aggregate trade ID 最大者。起止价生成独立 Binance Up/Down label，与 Polymarket label 不一致时保留 disagreement，不改写平台 settlement/reward。
- 旧 `binance_aggtrade_v2` 仅将 `E` 写为 `event_time`，尽管 parser 已读取 `T`。用户选择方案 1 并授权立即实现 `trade_time=T`、升级 `binance_aggtrade_v3`、重新部署与重启采集。

### D+1 归档和 data-pipeline 边界

- 远端 2026-07-09 T08 四字节 `*-running.parquet` 仍物理存在；旧日归档脚本会因发现任意 partial 全局退出，因此即使采集源 bug 已修复，该历史产物仍会阻断昨天归档。用户已批准该归档修复。
- 修正语义：只拉取昨天 `typed_raw/utc_day=D` 及其同名 segment manifests，不拉当天分区或整个当前 `run_manifests/`；partial 只写报告且绝不读取，市场是否保留交由现有逐 market gap 规则。v2/v3 混合日改为逐文件读取后按字段并集拼接，旧 v2 rows 的 `trade_time=null`，新 v3 rows 保留真实 `T`。
- 云端同名昨天包必须按当前本地/远端 SHA1 核对，不能只看名字就报告成功。
- 完整 canonical/feature/label/trajectory 处理脚本属于本仓库 `data-pipeline/`，后续部署到 5090 的 `~/btc-updown-marketmaker/data-pipeline` 执行；不继续把每日归档脚本扩成训练 pipeline。

### schema v3 提交、部署与真实数据验收

- 首次提交/push 为 `3a1b7254fc1cef00ab950acb906181ad7cbd1e3d`；从该 commit 干净 worktree 本地构建 release `3a1b7254fc1c-aggtrade-v3-20260710T143830Z`，training binary SHA256=`78c59f31ce8a9803d72c791c1ff066ced6660f7072137e6719b0dd9570c3058e`。
- 数据采集服务器已完成整包 SHA 验证、`current` 切换和受控重启。当前 PID `951266`、进程 SHA 一致、service `active/running`、`NRestarts=0`，6 个 writer 全部打开。
- 真实 `binance_aggtrade_2026-07-10T14--part=1.parquet` 为 `binance_aggtrade_v3`、`Complete`、`1298` rows、E/T 都无 null、`source_gaps=[]`；`566` rows 的 E/T 不同，差值范围 `0..3ms`。因此 `CollectionStartReadinessStatus` 已更新为 `SCHEMA_V3_DEPLOYED_AND_VERIFIED`，但这不自动冻结正式 cohort 起点。
- 本地 D+1 timer 已启用为 UTC `02:00`。首次 Persistent 补触发读取的是昨天 `2026-07-09`；在打包上传前因用户更新“TLS 瞬时错误应重试”而受控停止，没有上传该次未完整结果。
- 瞬时重试修正已由提交 `4863baaa73d2698dd50ef21a84f15bc95bde6fcb` 推送。重新执行同一 D 日时复用既有 artifact，只请求原 TLS timeout row `1` 次并成功；逐行对比确认其余 287 rows 的核心值未变化。最终 `price-to-beat-gamma-v2` 为 `288/288` 成功、attempt count 无 null，P2B artifact SHA256=`aad4052de1d00f945e23d458019a828ef997fd600aa202da39962c6d84a26bd4`。
- D=`2026-07-09` 实际归档以 status `0/SUCCESS` 完成：名义/观察市场 `288/288`、accepted=`96`、rejected=`192`；192 个拒绝全部且仅因已知 T08 事故后的 Binance aggTrade 无 rows，不是 Gamma P2B/finalPrice 缺失。802 MiB tar SHA256=`b3a97269adbbe97d0fa0c4555dd36cfca95dcde0bc6d8f6da9a9c859712b2352`；AliyunPan 同名文件 SHA1=`46C20AAA47DEA139EC4438D0178F27A3EE3DECE3` 与本地一致，tar 不含仍在采集的 2026-07-10 数据。timer 保持 enabled/active/waiting，下一次触发为 `2026-07-11T02:00:00Z`。
- 本次实际归档 wall time `9m29.844s`、memory peak `79.4GiB`。该值只记录为后续运维性能观察，不改变当前数据准入、P2B 双字段目标或训练 cohort 决策；正式 cohort 仍等待 schema v3 后续采集质量由用户另行冻结。

### 当前授权边界

- 已授权：文档对齐、schema v3 最小实现与测试、D+1 昨天归档/Gamma 两字段修正、commit/push、本地 release build、数据采集服务器重新部署和重启采集。
- 未授权：future paper/live 真实 market handoff 实现或部署、`data-pipeline` 实现、simulator/model 实现、训练、paper/live 交易和资金操作。
