# 旧数据唯一训练源核心重构执行计划

事项编号：`rebuild-core-old-data-only`

状态：`草案，待用户审查；本文不授权执行`

对应需求文档：`/home/chillchill/btc-updown-marketmaker/demand/rebuild-core-old-data-only-demand.md`

对应 changelog：`/home/chillchill/btc-updown-marketmaker/changelog/rebuild-core-old-data-only-changelog.md`

旧数据处理专项计划：`/home/chillchill/btc-updown-marketmaker/plan/rebuild-core-old-data-only-dataset-processing-plan.md`

历史参考文档：`/home/chillchill/btc-updown-marketmaker/demand/rebuild-core-demand.md`

## 目标和执行边界

本计划把“旧历史数据是唯一训练市场数据源，新数据只生成延迟校准 artifact”的需求拆成可验证阶段。计划遵循先冻结合同、再小样本纵向验证、再冻结 canonical schema、最后全量迁移和训练的顺序。

本计划目前只作为审查材料，不授权：

- 修改 5090 历史归档；
- 修改或重启 data-collect；
- 执行全量数据转换；
- 启动训练、paper、live 或资金操作；
- 修改原 `demand/rebuild-core-demand.md`；
- 使用新数据 market rows 参与训练或模型选择。

## 成功标准总览

1. 旧数据边界、平台时期和 capability tier 可追溯到文件 hash。
2. 新数据只有 latency-calibration artifact 能进入训练配置；训练 loader 无新数据读取路径。
3. collector-recorded 与 simulated available time 分离，同版本/seed 可确定性重放。
4. 模型调用严格由业务事件触发，不存在固定 decision tick。
5. 模拟平台、NOE、ledger、reward 和 settlement 在小样本端到端闭合。
6. canonical schema 只在跨版本纵向切片通过后冻结。
7. 全量转换和训练 bounded/sharded，可恢复，不复现历史内存与串行 replay 问题。
8. action-surface audit 先于 leaderboard；训练完成不自动授权 paper/live。
9. latency 与 simulator-rule calibration 的质量和 promotion capability 可审计；历史 prestart coverage 与 runtime 完整 T-30 的差异经过独立 gate。

---

## 阶段 0：冻结文档权威与不可变基线

- [ ] 任务 0.1：登记新旧核心文档权威
  - [ ] 步骤 0.1.1：记录原 `demand/rebuild-core-demand.md` 的 sha256、git 状态和路径。
    - 验证：新事项完成时重新计算 sha256，必须完全一致。
  - [ ] 步骤 0.1.2：确认新 demand、plan、changelog 使用 `rebuild-core-old-data-only` 同一事项编号并互相引用。
    - 验证：demand、主计划、旧数据处理专项计划和 changelog 路径存在、引用无断链，旧核心文档只被标为历史参考。
  - [ ] 步骤 0.1.3：把新事项状态保持为“待用户审查/未授权实施”。
    - 验证：本事项全部文档均不存在已授权训练、部署或资金操作的文字。

- [ ] 任务 0.2：保存当前工作树边界
  - [ ] 步骤 0.2.1：保存 `git status --short --branch`、tracked diff name-only 和 untracked scope。
    - 验证：后续只 stage 新事项 demand、主计划、专项计划和 changelog 四个路径，不使用 `git add .` 或 `git add -A`。
  - [ ] 步骤 0.2.2：确认现有 data-collect、旧核心需求和其他用户改动不被本事项覆盖。
    - 验证：`git diff -- <existing paths>` 不出现本事项新增改动。

### 阶段 0 完成门

- 原核心需求 hash 已冻结；
- 新事项 demand、主计划、专项计划和 changelog 对齐；
- 没有执行授权或范围外改动；
- 自审通过后由独立审查确认权威边界。

---

## 阶段 1：历史数据与新延迟参考数据盘点

- [ ] 任务 1.1：创建历史训练数据 inventory
  - [ ] 步骤 1.1.1：只读枚举 5090 `/root/autodl-tmp/` 中日期归档和 `data-v1` 至 `data-v5`。
    - 验证：记录文件路径、大小、sha256、可读性、日期和归档成员清单；不解包覆盖原目录。
  - [ ] 步骤 1.1.2：按历史 schema/source kind 建立版本族，而不是按文件名直接视为同一种格式。
    - 验证：每个版本族至少抽样首、中、末日期，确认字段类型和 footer metadata。
  - [ ] 步骤 1.1.3：识别 CLOB/platform regime、来源覆盖、T-30、received time、source sequence 和 raw evidence 能力。
    - 验证：每个 artifact 生成 capability tier 和明确缺失原因；明确回填 Binance aggTrades 与 Polymarket 历史盘口均无逐事件采集接收时间；量化 `raw_json`/`extra_json` column chunk 体积；每个 episode 记录 `context_start_available_ts`、`prestart_coverage_seconds`、missing mask 和 task eligibility。

- [ ] 任务 1.2：冻结 OldDataBoundaryManifest schema
  - [ ] 步骤 1.2.1：定义 artifact、source、coverage、platform regime、capability、split-state 和 approval 字段。
    - 验证：schema 能表达 `unassigned/train/validation/frozen_holdout/diagnostic_only/excluded`。
  - [ ] 步骤 1.2.2：禁止 canonical-ready 自动变成 training-approved。
    - 验证：缺少显式 approval 的 artifact 无法被正式 training loader 发现。
  - [ ] 步骤 1.2.3：为全部旧 artifact 生成只读 inventory draft。
    - 验证：总文件数、总大小与源目录核对；重复、损坏、缺源分别列出。
  - [ ] 步骤 1.2.4：冻结证据层 hash 与 row locator 合同。
    - 验证：每个源文件记录相对路径、sha256、schema fingerprint、row count 和稳定 row ordinal；没有写保护或版本化备份证据时，不得宣称 archive immutable。

- [ ] 任务 1.3：创建新数据 latency reference inventory
  - [ ] 步骤 1.3.1：只读枚举 `/home/chillchill/new-datasets` 中 `polymarket_market` 与 `binance_aggtrade` 的 finalized typed raw、schema、source time、receive time、health/gap 和已知事故窗口。
    - 验证：running/tmp/4-byte、缺 schema、source gap 和 writer 异常文件全部排除默认校准集合。
  - [ ] 步骤 1.3.2：按 source/event type 确认 timestamp 的业务含义和 measurement boundary。
    - 验证：旧 aggTrades `timestamp_ms=T` 与新 `event_time=E` 不混用；Polymarket platform timestamp、collector-generated control event 和 fallback provenance 分离；不能解释的差值只能 quarantine 或记录为 visibility-delay uncertainty。
  - [ ] 步骤 1.3.3：确认新数据文件不会被 training loader、feature builder、label builder 或 split builder 直接发现。
    - 验证：负向测试向配置注入新数据路径时必须拒绝。
  - [ ] 步骤 1.3.4：冻结 LatencyCalibration48hGate 与 CalibrationCoverageManifest。
    - 验证：定义半开区间 `[start_ms,end_ms)`、每源 eligible interval union，以及 `intersection(union(PM eligible intervals), union(agg eligible intervals))` 得到的 joint intervals；毫秒 duration sum 是唯一 gate 计数权威，完整 UTC hour 只是派生审计桶；同时定义累计/最长连续 duration、partial-hour 精确计数条件、事故排除、clock evidence、source/writer health、Polymarket transport identity、Binance `T` measurement boundary 和版本兼容 receipt；wall span 不能计入 gate。

### 阶段 1 完成门

- 旧数据 inventory 与 capability tier 完整；
- 新数据默认校准集合只含健康、finalized、时间语义可解释窗口；
- 旧训练边界和新校准边界物理、配置和 manifest 隔离；
- 独立审查通过后才能进入合同冻结。

---

## 阶段 2：冻结决定数据形态的核心合同

- [ ] 任务 2.1：冻结事件身份与时间合同
  - [ ] 步骤 2.1.1：定义稳定 `canonical_event_id`，不得依赖模拟 received time。
    - 验证：改变 latency scenario 后基础事件 ID 完全不变。
  - [ ] 步骤 2.1.2：定义 source event time、collector-recorded time、simulated received time、available time 和 provenance。
    - 验证：`observed_received_time_ms` 不被冒充为历史原字段；任何 simulated value 写入 collector-recorded 字段必须被 schema/validator 拒绝。
  - [ ] 步骤 2.1.3：定义 source 内排序、跨源 available-time merge、同时间 tie-break 和 clock uncertainty。
    - 验证：同 artifact/calibration/scenario/seed 重放顺序一致。
  - [ ] 步骤 2.1.4：分开定义 `RecordedTimeValidityGate` 与 `TargetTopologyComparabilityGate`。
    - 验证：可信旧 `recv_time_utc` 可用于 historical-as-recorded replay；host、region、clock 与 measurement boundary 无法证明和目标拓扑兼容时，混合时间线标记 mixed-topology/provisional/non-promotable，不得宣称同一 target runtime 的真实跨源 causal order，也不得把 recorded fact 改成 null。

- [ ] 任务 2.2：冻结 DecisionTriggerContract 和 persistent-state contract
  - [ ] 步骤 2.2.1：定义 raw event、normalized event、model-visible event 和非触发控制帧。
    - 验证：无业务事件时模型调用次数为零。
  - [ ] 步骤 2.2.2：定义事件启动 causal microbatch、in-flight policy、最大 causal lag 和高优先级 lifecycle/NOE lane。
    - 验证：不存在固定周期启动的策略 microbatch；每次调用可追溯到 trigger event。
  - [ ] 步骤 2.2.3：定义 market state reset、账户状态延续和旧市场 late-event 隔离。
    - 验证：market switch 不泄漏 feature state，也不丢失旧订单 settlement。

- [ ] 任务 2.3：冻结 logical input、action 和 Model Intent
  - [ ] 步骤 2.3.1：冻结逻辑输入字段、缺失/新鲜度 mask、ragged/padded/runtime 映射和 normalization authority。
    - 验证：不同物理表示还原到相同逻辑状态。
  - [ ] 步骤 2.3.2：冻结 submit/cancel/no-submit/pause、side、price、size、order type、post-only、TIF、TTL、slippage 和 decoder。
    - 验证：每个输出头都能映射到 simulator/runtime consumer；未映射字段阻断 bundle。
  - [ ] 步骤 2.3.3：解决 TTL authority。
    - 验证：TTL 只能来自预授权 intent，或 TTL-due 事件触发的新模型决策；runtime 无影子 TTL。

- [ ] 任务 2.4：冻结 NOE、ledger、settlement 和 reward 合同
  - [ ] 步骤 2.4.1：分离 order lifecycle、trade settlement 和 ledger economic effect。
    - 验证：MATCHED→CONFIRMED 和 MATCHED→FAILED 两条路径均有正反测试设计。
  - [ ] 步骤 2.4.2：定义 UnknownSubmitOutcome、order-local recovery、reconciliation observation 和 correction event。
    - 验证：timeout 后不能重复下单，也不能让 query 直接覆盖 ledger。
  - [ ] 步骤 2.4.3：分离 market outcome、simulation execution、policy reward 和 accounting truth。
    - 验证：run-level cumulative PnL 不能作为逐决策标签。

- [ ] 任务 2.5：冻结 transition/trajectory 和 split contract
  - [ ] 步骤 2.5.1：确认哪些训练阶段需要 event trajectory、state/action/reward/next-state/done 或市场结果表。
    - 验证：没有真实 action 的旧数据不会被标成 observed BC/offline-RL transition。
  - [ ] 步骤 2.5.2：冻结 strict time-forward、market grouping、platform regime、purge/embargo 和 frozen holdout。
    - 验证：同 market 不跨 split，holdout 不参与 rerank。

- [ ] 任务 2.6：冻结 SimulatorRuleCalibrationManifest
  - [ ] 步骤 2.6.1：定义 submit/accept/reject/unknown、queue/fill、cancel、user-channel、fee/rebate、sellable 和 settlement/resolution 参数的来源合同。
    - 验证：每个参数均记录 source artifact、hash、采样时间、平台时期、适用范围、置信度、provenance 和质量状态；未登记参数不能被 simulator 使用。
  - [ ] 步骤 2.6.2：定义 `approved/provisional/assumed/quarantined` 质量等级及其 promotion capability。
    - 验证：`provisional` 或 `assumed` 参数只能用于探索和压力测试，不得支撑正式 policy candidate；例外必须由用户按参数和范围显式批准。
  - [ ] 步骤 2.6.3：冻结历史盘口证据的能力边界。
    - 验证：历史 order book 只证明市场状态，不被当作本账户真实 queue position、order lifecycle 或 fill truth。

- [ ] 任务 2.7：冻结 EvaluationCandidate 与 PromotionContract
  - [ ] 步骤 2.7.1：定义 `exploratory/non-promotable`、`paper_evaluation_candidate` 和 `promotion_eligible` 的状态转换与所需证据。
    - 验证：后续训练、排行榜或 paper 结果不能自行改变 capability；每次转换均要求明确 gate receipt。
  - [ ] 步骤 2.7.2：在看到 paper/shadow 结果前冻结 truncated-history 与 full-T-30 对比合同。
    - 验证：预先定义 reference cohort、state distance、action disagreement、size/price/TTL drift、risk-limit breach、聚合方式、置信区间、通过阈值和缺证据判定；合同生成 version 与 sha256。
  - [ ] 步骤 2.7.3：冻结阈值变更规则。
    - 验证：paper/shadow 开始后不得静默修改指标或阈值；必要变更必须新版本、说明原因、使既有结果失效并重跑，且写入 changelog。

### 阶段 2 完成门

- 所有决定数据形态的合同均版本化；
- evaluation/promotion capability 和完整 T-30 对比指标、阈值已预冻结并带 hash；
- 尚未锁定的模型 backbone/超参数不影响 canonical 语义；
- 合同覆盖历史 mistake-log 的 action collapse、泄漏、NOE、ledger 和资源问题；
- 独立审查无事实冲突后才能写 adapter。

---

## 阶段 3：构建并验证 latency 与 simulator-rule calibration artifacts

- [ ] 任务 3.1：实现校准数据准入
  - [ ] 步骤 3.1.1：只读取阶段 1 冻结的两源 eligible calibration segments。
    - 验证：两源各自 cumulative eligible healthy duration 和 joint eligible duration 均不少于 `172800000ms`；事故/gap/running/schema-drift/clock-unknown 区间不能进入 baseline 样本。
  - [ ] 步骤 3.1.2：按 source/event type 计算 `source_to_collector_visibility_delay_ms`，记录 clock uncertainty 和筛选原因。
    - 验证：正式字段命名为 `source_to_collector_visibility_delay_ms`；负值和异常值不被静默 clamp；所有排除都有 reason；新数据业务内容不进入延迟模型参数。
  - [ ] 步骤 3.1.3：输出数据范围、hash、样本量、分位数、条件维度和生成代码版本。
    - 验证：artifact 可独立复算且训练期间不可漂移；明确 reference topology、target topology 和 provisional/final 状态。

- [ ] 任务 3.2：构建 baseline 和 stress 分布
  - [ ] 步骤 3.2.1：优先构建逐 transport event empirical CDF；只有缺少原始样本时才使用标记为 approximation 的连续分位插值。
    - 验证：Polymarket typed child rows 不重复计权；默认分布不在少数 quantile 点形成离散尖峰；存在 burst/自相关时使用 block bootstrap、regime model 或等价方法，IID 简化必须有验证依据。
  - [ ] 步骤 3.2.2：输出 baseline、high-latency、tail-stress scenario。
    - 验证：pMAX 只进入诊断/尾部场景，不作为默认普通样本。
  - [ ] 步骤 3.2.3：定义 source/event type 无合格样本时的阻断行为。
    - 验证：系统不得从其他 source 偷用分布；相关 latency-dependent policy 训练保持 blocked。

- [ ] 任务 3.3：验证可重现采样
  - [ ] 步骤 3.3.1：使用 calibration version、scenario、event ID 和 seed 生成 `simulated_received_time_ms`。
    - 验证：随机键逐事件确定性派生；重复运行逐事件结果一致；更换 worker/shard/resume 不改变同 seed 结果；更换 seed 只改变模拟时间，不改变基础事件内容、身份或 split。
  - [ ] 步骤 3.3.2：旧数据存在已验证 `recv_time_utc` 时优先使用 collector-recorded time。
    - 验证：覆盖 collector-recorded 字段的测试必须失败；Polymarket 历史盘口和回填 aggTrades 必须走 simulated 分支；`RecordedTimeValidityGate` 决定 recorded fact 是否可用，`TargetTopologyComparabilityGate` 只决定目标拓扑声明、任务 capability 和 promotion，不得反向抹掉有效 recorded time。
  - [ ] 步骤 3.3.3：验证 transport identity、连接顺序和 late arrival。
    - 验证：同 transport child rows 共享 availability；同连接事件不逆序；模拟时间不早于 source time；推到 market end 后的事件不被前移。

- [ ] 任务 3.4：构建并验证 simulator-rule calibration artifact
  - [ ] 步骤 3.4.1：盘点官方平台规则和经用户批准的历史 execution evidence，按阶段 2 manifest 归档。
    - 验证：规则、观测证据、工程假设和 stress override 分栏记录；过时规则按平台时期隔离。
  - [ ] 步骤 3.4.2：只将有批准来源和适用范围的参数标记为 promotion-capable。
    - 验证：假设值、固定概率和无证据 queue/fill 参数只能标为 `assumed` 或 `provisional`，promotion gate 拒绝它们。
  - [ ] 步骤 3.4.3：发布版本化 SimulatorRuleCalibrationManifest 并接入 fail-closed 校验设计。
    - 验证：simulator 对缺版本、缺 hash、范围不匹配或 quarantined 参数拒绝运行；provisional/assumed 参数强制产物保持 non-promotable。

### 阶段 3 完成门

- LatencyCalibrationGate 与 SimulatorContractGate 的校准部分通过；
- baseline 与 stress 场景可重现、可解释；
- 新数据只留下 calibration artifact 引用，不进入市场训练表；
- latency 或 simulator-rule calibration 仍为 `provisional/assumed` 时，只允许 non-promotable 纵向切片和探索训练；后续只有经用户单独授权，才可把该产物用于 non-promotable paper/no-submit shadow 以解除不确定性，且不得 promotion 或进入 live；
- 独立审查确认没有把 arrival delta 夸大为纯网络 latency。

---

## 阶段 4：跨版本小样本纵向切片

- [ ] 任务 4.1：选择代表性样本
  - [ ] 步骤 4.1.1：至少选择早期 V1/旧 schema、CLOB V2 后、具有 depth、缺少 depth、具有 collector-recorded time、缺少 receive time、正常和缺源样本。
    - 验证：样本矩阵覆盖主要 capability tier 和 prestart coverage 分层，不只选择最干净日期，也不补造缺失的 T-30 事件。
  - [ ] 步骤 4.1.2：冻结样本路径、hash 和预期能力。
    - 验证：纵向切片结果可从原归档重建。

- [ ] 任务 4.2：实现 L0→L1 旧数据 adapter
  - [ ] 步骤 4.2.1：按版本族读取旧证据层，生成不含 `raw_json` 的 typed-slim 候选并保留 typed/extra、row locator、未知字段和缺失语义。
    - 验证：独立 validator 对字段存在性、类型、数值、嵌套数组、顺序和 extra 做全量语义对账；现有 consumer 未完成 typed-only 切换前不得替换正式输入。
  - [ ] 步骤 4.2.2：生成稳定 canonical event 和 capability metadata。
    - 验证：不同 adapter 版本不能静默改变已发布语义。

- [ ] 任务 4.3：构建模拟 availability 和 causal replay
  - [ ] 步骤 4.3.1：把 calibration overlay 应用于缺少合格 `collector_recorded_time` 的事件。
    - 验证：所有 simulated event 都有 provenance/version/seed/scenario。
  - [ ] 步骤 4.3.2：按 available time 更新 persistent state 并生成 model-visible triggers。
    - 验证：模型看不到 cutoff 后事件；无事件无决策；同 seed 顺序相同；跨源同毫秒未知先后只使用版本化 causal bucket/tie-break；旧 recorded 与新 simulated 拓扑不兼容时仍可做明确标记的 historical-as-recorded/mixed-boundary exploratory replay，但保持 provisional/non-promotable，target-topology counterfactual 任务 blocked；episode 的 coverage、missing mask 和 eligibility 全程传递到 state、transition 和报告。

- [ ] 任务 4.4：贯通 simulator→NOE→ledger→reward
  - [ ] 步骤 4.4.1：用最小 Model Intent baseline 驱动离散事件模拟平台。
    - 验证：模拟平台先写 raw evidence，不能直写 ledger/reward。
  - [ ] 步骤 4.4.2：覆盖 accepted/rejected/unknown、no-fill/full/partial、cancel、matched-cancel、late fill、settlement success/failure。
    - 验证：正反例 ledger 可回放，FAILED 产生补偿事件。
  - [ ] 步骤 4.4.3：生成分命名空间标签和报告。
    - 验证：simulation label 与 historical market label 不混列，累计 PnL 只出现在 accounting。

### 阶段 4 完成门

- 跨版本、跨 capability 的端到端 vertical slice 通过；
- 发现的 schema/合同冲突回到需求或计划决策，不得在 adapter 中静默绕过；
- 资源和吞吐满足放大估算；
- 独立阶段审查通过后才能冻结 canonical schema。

---

## 阶段 5：冻结 canonical contract、验收 consumer、全量迁移并协调激活

本阶段涉及 L0→L1、consumer migration 和全量发布的详细顺序，以旧数据处理专项计划的阶段 6、7 为执行权威；主计划不保留另一套相反顺序。

- [ ] 任务 5.1：冻结分层 canonical schema
  - [ ] 步骤 5.1.1：冻结 L0 immutable reference、L1 normalized event、L2 causal state/decision、L3 simulation transition/reward、L4 split/run artifact。
    - 验证：L0 证据层保留 `raw_json`；L1 typed-slim 禁止 `raw_json` 并可用 file hash + row ordinal 回源；每层 authority、输入、输出和禁止字段明确。
  - [ ] 步骤 5.1.2：发布 schema version、adapter matrix 和 compatibility policy。
    - 验证：未知版本 fail closed to quarantine，不做猜测式解析。

- [ ] 任务 5.2：先执行专项计划阶段 6 的 typed-only consumer 实现与纵向切片验收
  - [ ] 步骤 5.2.1：迁移 Python data pipeline、Rust replay 和 recording/export，并禁止 silent fallback to `raw_json`。
    - 验证：legacy 与 typed-only 的 event、label、feature、replay、state 和 ordered digest 满足预冻结等价合同。
  - [ ] 步骤 5.2.2：发布 `ConsumerCompatibilityManifest`。
    - 验证：`scope=vertical_slice` 为 PASS、typed candidate 的 `raw_json` 依赖为零后，才允许启动全量迁移；active dataset pointer 仍保持 legacy。

- [ ] 任务 5.3：再执行专项计划阶段 7 的 bounded/sharded 全量迁移与发布
  - [ ] 步骤 5.3.1：先通过峰值磁盘容量与保留水位门，再按 archive/day/source/market streaming 转换。
    - 验证：容量不足时 fail closed，禁止提前删除 L0 腾空间；中断恢复不重复、不丢失，峰值 RSS 不随总数据量线性增长。
  - [ ] 步骤 5.3.2：逐 shard 执行全量 semantic parity、row count、coverage、event identity、hash、locator 和 causal validation。
    - 验证：新 schema/event family 回到合同和 consumer 验收；正式发布默认 `required_shard_quarantine_count = 0`，任何改变训练 boundary/split 的排除均阻断并请求用户批准。
  - [ ] 步骤 5.3.3：生成 inactive candidate 的 boundary/split/calibration/simulator manifests，并执行恢复和 scenario 审计；full-dataset consumer status 暂为 pending。
    - 验证：新数据路径只出现在 calibration provenance；抽样 100% 可回到 L0；基础市场事实不随 latency scenario 改变；不得提前签发 full-dataset consumer PASS。

- [ ] 任务 5.4：执行 full-dataset consumer gate 并协调原子激活
  - [ ] 步骤 5.4.1：实际 Python、Rust replay 和 recording/export typed reader 对全部 shard生成 ordered canonical-event digest，并与 legacy audit reader 对账；再做风险分层的 label/feature/replay 深层比较。
    - 验证：`ConsumerCompatibilityManifest(scope=full_dataset)` 为 PASS，converter/reconstructor 不能代替实际 consumer。
  - [ ] 步骤 5.4.2：协调切换 versioned dataset pointer 与匹配 consumer config。
    - 验证：dataset/schema/consumer hash 预检通过才原子激活；失败回滚旧 pointer/config，不出现半切换。

### 阶段 5 完成门

- canonical schema 与 adapter matrix 冻结；
- `ConsumerCompatibilityManifest(scope=full_dataset)` 为 PASS，active 正式 consumer 已 typed-only；
- 全量转换可恢复、bounded、可追溯；
- split 和 holdout 已冻结；
- 独立审查通过并形成 commit/push 后才允许训练阶段。

---

## 阶段 6：训练、行为验证与候选选择

- [ ] 任务 6.1：建立 market representation/alpha 基线
  - [ ] 步骤 6.1.1：只使用旧数据 market facts 和 market outcome labels。
    - 验证：lineage 中无新数据 market artifact、simulation execution label 或 holdout leakage。
  - [ ] 步骤 6.1.2：比较候选 temporal backbone，但保持逻辑 input/state/event contract 不变。
    - 验证：模型选择不迫使重写 canonical 数据合同。
  - [ ] 步骤 6.1.3：按 prestart coverage 分层执行 variable-warmup 训练和评估。
    - 验证：截断历史候选只能声明实际 coverage 能力；未见完整 T-30 的训练结果不得标记为已训练完整 T-30。

- [ ] 任务 6.2：训练 simulator execution/action 候选
  - [ ] 步骤 6.2.1：使用旧市场轨迹驱动 simulator 生成明确标记的 synthetic execution trajectory。
    - 验证：不得称为 observed BC/offline-RL data。
  - [ ] 步骤 6.2.2：优先验证 execution prediction、action ranking 或受控 policy baseline，再决定是否需要 offline RL。
    - 验证：训练方法选择有 action support、reward maturity 和 simulator sensitivity 证据。

- [ ] 任务 6.3：action-surface audit
  - [ ] 步骤 6.3.1：检查 raw output、decoded action entropy、threshold sweep、alpha sensitivity 和 control separation。
    - 验证：动作塌缩候选不得进入 leaderboard。
  - [ ] 步骤 6.3.2：确认每个 intent 字段被 simulator/runtime consumer 实际消费。
    - 验证：未消费输出阻断 bundle。

- [ ] 任务 6.4：评估候选与 capability 分级
  - [ ] 步骤 6.4.1：在同一 boundary、split、calibration、simulator 和 scenario 下比较候选。
    - 验证：baseline/high/tail sensitivity、frozen holdout、rolling worst 和资源指标完整；报告分别保留 latency arrival 与 simulator execution-rule calibration 的质量和 promotion capability。
  - [ ] 步骤 6.4.2：生成 evaluation-candidate report 和 capability 建议。
    - 验证：`exploratory/non-promotable`、`paper_evaluation_candidate` 与后续可能的 `promotion_eligible` 不合并成一个 candidate 状态；排行榜不能直接改变 ModelRegistry active 状态。
  - [ ] 步骤 6.4.3：形成 paper/no-submit shadow 准入申请材料。
    - 验证：provisional/assumed 产物只能申请经用户单独授权的 non-promotable paper/no-submit shadow；即使 calibration 已 approved，也必须等待阶段 7 的完整 T-30 gate，不能在本阶段标记 promotion-eligible。

### 阶段 6 完成门

- 训练数据 lineage 证明旧数据唯一市场源；
- action surface 分化且所有 action 可执行；
- latency/simulator 敏感性没有被单点结果掩盖；
- 评估候选具有明确的 calibration quality、coverage 和 capability；
- 独立审查通过并 commit/push 后，才能向用户申请 paper/no-submit shadow；本阶段不能授予 promotion 或 live 资格。

---

## 阶段 7：paper/shadow 与 future live 授权门

- [ ] 任务 7.1：paper/shadow 独立验证
  - [ ] 步骤 7.1.1：paper 使用共享 simulator/NOE/ledger；shadow 使用实时市场上下文但不提交订单。
    - 验证：训练、paper、shadow 的逻辑合同一致，authority source 分离。
  - [ ] 步骤 7.1.2：验证 runtime T-30、事件触发、persistent state、market switch、资源和 accounting。
    - 验证：paper/shadow runtime 必须提供完整 T-30；按预冻结阈值比较截断历史与完整 T-30 下的 state、action 和 risk 差异，未通过不得 promotion；历史训练缺失的 coverage 不被报告伪装成已训练事实。
  - [ ] 步骤 7.1.3：汇总 promotion capability。
    - 验证：只有完整 T-30 差异 gate 通过，且 latency 与 simulator-rule calibration 均为 approved 或存在用户批准的有限例外时，才可提出 promotion 建议；provisional/assumed 候选即使完成 paper/shadow 也保持 non-promotable。

- [ ] 任务 7.2：future live preflight
  - [ ] 步骤 7.2.1：确认当前 V2 官方 Rust SDK、production host、market capability、Model Intent、NOE、ledger 和 cancel preservation。
    - 验证：不存在 SDK 旁路、影子策略或 fail-closed 同时关闭必要 cancel 的路径。
  - [ ] 步骤 7.2.2：单独向用户申请 small-live/live 授权。
    - 验证：没有明确授权时不得执行任何真实下单、撤单或资金操作。

### 阶段 7 完成门

- paper/shadow 只能提供 promotion 证据，不能自动授权 live；
- 完整 T-30、approved calibration、frozen holdout、action surface、风险和资源 gate 全部满足后，才可提出 promotion 建议；
- live 必须使用真实平台，不使用模拟平台作为账户或订单权威；
- 用户明确授权、部署 receipt 和资金边界全部满足后才能进入后续 live 计划。

---

## 阶段审查、提交与推进规则

1. 每个任务完成后先自审 diff、需求一致性、测试证据、边界情况和剩余风险。
2. 每个阶段完成后必须派独立审查，覆盖需求验收、正确性、影子路径、过度设计、遗漏和文档同步。
3. 审查问题必须修复并复审；只有需要用户决策的真实冲突才能暂停。
4. 阶段审查通过后再创建范围明确的 commit 并 push；不得混入当前工作树其他用户改动。
5. 任何代码、数据、schema、平台或环境事实与本文已批准需求冲突时，必须先记录事实、影响和选项并请求用户决策，不得静默改方向。
