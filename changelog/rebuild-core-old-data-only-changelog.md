# 旧数据唯一训练源核心重构 changelog

事项编号：`rebuild-core-old-data-only`

对应需求文档：`/home/chillchill/btc-updown-marketmaker/demand/rebuild-core-old-data-only-demand.md`

对应计划文档：`/home/chillchill/btc-updown-marketmaker/plan/rebuild-core-old-data-only-plan.md`

## 2026-07-10

### 用户决策

- 用户决定正式训练只使用 5090 `/root/autodl-tmp/` 中已经存在的历史日期数据和 `data-v1` 至 `data-v5`。
- 用户决定新采集数据当前不稳定，不作为训练市场数据；新数据只作为旧数据缺失 received time 时的网络到达延迟参考。
- 用户要求创建新的核心需求文档，不继续修改原 `demand/rebuild-core-demand.md`。

### 新文档权威边界

- 新建 `demand/rebuild-core-old-data-only-demand.md`，事项编号为 `rebuild-core-old-data-only`。
- 原 `demand/rebuild-core-demand.md` 保留为历史参考；本事项创建时记录的当前 sha256 为 `23cc0f9c61941218a36648d5c30fd1d4293d05c75e036b92ea59bfad9746f36a`，本事项未修改该文件。
- 按项目文档对齐规则，新建同 slug 的计划和 changelog；需求只保留需求与验收，详细阶段、任务、步骤和验证写入计划。
- 三份新文档当前均为待用户审查状态，不授权代码、数据转换、训练、paper、live、部署或资金操作。

### 训练数据边界

- 旧历史数据被定义为唯一训练市场事实来源。
- 新数据中的 price、book、trade、outcome、market trajectory、feature 和 label 不得进入 train、validation、frozen holdout、normalization、reward 或模型选择。
- 新数据对训练的唯一允许影响是生成冻结、带 hash、样本范围、时钟不确定性、reference topology 和适用 source/event type 的 latency-calibration artifact。
- 新数据当前未完成多日稳定性、时钟和全部 source 长跑验证时，只能标记为 `provisional_reference`，不得用来宣称已还原历史真实 received time 或已证明 live 网络延迟。
- 本次只读核对时，本地新数据日期目录只有 `2026-07-08`、`2026-07-09`，479 个可读 typed-raw 文件抽样记录的 collector region 均为 `eu-west-1`；当前 live envelope 仍把 host clock sync 标为 unavailable，因此尚不具备跨日期、跨 topology 的已批准延迟真值资格。
- 历史数据缺失完整 T-30 时只保留真实 coverage 和 missing mask，不补造市场事件；future paper/live 的 T-30 目标保留，但必须通过独立 promotion gate 验证历史训练与 runtime 上下文差异。

### simulated received time 语义修正

- 没有接受“把随机值写成历史 observed received time”的做法。
- 旧数据真实存在的接收时间保留在 `observed_received_time_ms`；缺失时单独生成 `simulated_received_time_ms`。
- 因果回放使用 `event_available_time_ms`，并强制记录 `received_time_provenance`、calibration version、scenario、seed、condition key、reference/target topology 和平台时期。
- 原始历史归档保持 immutable；模拟时间只写入新的派生 artifact，不原地回写旧归档。
- p50、p85、p90、p95、p99 用于报告和场景定义；pMAX 只用于诊断/tail stress。默认分布禁止在少数 quantile 点上离散随机。
- 延迟样本存在 burst、自相关或 reconnect regime 时，需求要求 block bootstrap、regime model 或等价相关性保留方法；IID 采样必须先证明不会实质改变结果。
- 新数据校准输入被限制为时间、source/event、topology、health、payload/burst 和 provenance；价格、数量、盘口和市场结果不得通过延迟模型间接进入训练。
- 补充 transport identity、child-row 共享 availability、同连接顺序、late-arrival、逐事件确定性 RNG 和 split-before-simulation 合同，防止并发、分片或重跑改变因果轨迹。

### 训练与模拟平台合同

- 继续保留模型严格事件触发要求：无业务事件时不得产生策略决策，固定 tick 只能用于统计或兼容索引。
- 明确训练框架、DecisionTriggerContract、persistent state、action、reward、trajectory、simulator 和 runtime consumer 必须先于全量旧数据转换冻结。
- 模拟平台被定义为训练和 paper 的一等离散事件模块；live 只能连接真实平台。
- 旧市场数据没有真实 action、NOE、ledger 和 reward 时，不得宣称可直接进行 observed behavior cloning 或真实 offline RL；policy 反馈必须明确标记为 simulation authority。
- partial fill 改为由累计成交量和 accepted size 判断，删除“1 段 full、2 至 5 段 partial”的错误定义；固定 partial 概率、segment、dust、TTL 和延迟数值不得作为无证据平台真值。

### 执行事实合同修正

- Model Intent 补齐 order type、post-only、time-in-force、TTL/expiration、slippage/worst-price 和 decoder；TTL 必须由模型预授权或由 TTL-due 事件重新触发模型。
- NOE 必须覆盖 UnknownSubmitOutcome 和 order-local recovery。
- 分离 OrderLifecycleState、TradeSettlementState 和 LedgerEconomicEffectState；MATCHED 只产生 provisional effect，CONFIRMED/FAILED 决定 finality 或补偿事件。
- 分离 market outcome label、simulation execution label、policy reward 和 accounting truth；累计 PnL 只作为 run-level accounting/evaluation。

### 计划顺序

- 阶段 0：冻结文档权威和原核心需求 hash。
- 阶段 1：盘点旧训练数据与新 latency reference 数据。
- 阶段 2：冻结事件、状态、输入、动作、NOE、ledger、reward、trajectory 和 split 合同。
- 阶段 3：构建并验证 latency 与 simulator-rule calibration artifacts。
- 阶段 4：执行跨版本、跨 capability 小样本纵向切片。
- 阶段 5：纵向切片通过后冻结 canonical schema，再做 bounded/sharded 全量迁移。
- 阶段 6：按 market representation/alpha、simulator execution/policy、action-surface audit、评估候选与 capability 分级顺序训练。
- 阶段 7：paper/shadow 独立验证，future live 仍需用户单独授权。

### 当前验证

- 已确认三个新事项路径此前不存在，本次均为新文件。
- 已确认当前工作树在本事项开始前已有多项 data-collect、旧核心需求和 mistake-log 改动；本事项不修改、覆盖或回滚这些现有改动。
- 已执行需求专项只读审查；审查确认旧数据能力分级、训练框架前置、事件触发、模拟平台、NOE/settlement、observed/simulated provenance 和明确非范围均需进入新需求。
- 已执行延迟 provenance 专项只读审查；审查确认当前新数据只能作为 provisional arrival-delay reference，必须增加 reference/target topology、时钟不确定性、条件分布和相关性保持要求。

### 最终独立审查第一轮与修复

- 第一轮对抗审查结论为 `FAIL`，未发现 Critical，发现 3 项 High：provisional latency calibration 未明确绑定 non-promotable、simulator execution 参数缺少版本化来源/校准 manifest、历史 T-30 缩短与 future runtime 完整 T-30 的差异缺少闭环 gate。
- 新需求增加 provisional calibration 仅允许探索性 non-promotable 使用的硬边界；正式候选要求 approved calibration，例外只能由用户按范围显式批准。
- 新需求和计划增加 `SimulatorRuleCalibrationManifest`：记录 submit、queue/fill、cancel、user-channel、fee/rebate、sellable、settlement/resolution 等规则或参数的来源、hash、平台时期、适用范围、置信度、provenance、质量等级和 promotion capability。历史盘口不得冒充本账户 queue/fill 真值。
- 新需求和计划增加 episode prestart coverage、missing mask、task eligibility、variable-warmup 与完整 T-30 promotion gate；paper/shadow 必须提供完整 T-30，并按预冻结阈值比较截断历史与完整上下文下的 state、action 和 risk 差异。

### 最终独立审查第二轮与修复

- 第二轮对抗审查确认上一轮 3 项 High 已关闭，但发现 1 项新 High：把 paper/shadow 的完整 T-30 结果列为开始全量训练前提会形成“无候选不能 paper、未 paper 不能训练”的循环依赖；同时发现 provisional calibration 若一律禁止进入 paper，会妨碍用 paper/no-submit shadow 解除不确定性。
- 新需求把验收拆成三个能力门：进入全量转换和可扩展 exploratory 训练、发布 paper/shadow 评估候选、paper/shadow 后允许 promotion 或申请 live 授权。后续 gate 的证据不再反向阻断产生评估候选所需的训练。
- provisional/assumed calibration 仍然只能产生 non-promotable 产物；经用户单独授权后，可进入 non-promotable paper/no-submit shadow 收集验证证据，但不得 promotion 或进入 live。
- 计划把阶段 6 改为 evaluation candidate 与 capability 分级；阶段 7 同时要求完整 T-30 差异 gate 和 approved calibration，才允许提出 promotion 建议。promotion 仍不自动授权 live。

### 最终独立审查第三轮与补强

- 第三轮对抗审查结论为 `PASS`，未发现 Critical 或 High；确认三层能力 gate 已消除循环依赖，provisional 权限边界一致，approved calibration 与完整 T-30 只在 promotion gate 汇合。
- 审查另发现 1 项 Medium：需求要求预冻结 promotion contract，但阶段 2 尚未单独落项完整 T-30 对比指标和阈值，存在看到 paper 结果后选择阈值的风险。
- 计划阶段 2 增加 `EvaluationCandidate 与 PromotionContract`：在 paper/shadow 前冻结 capability 状态转换、reference cohort、state/action/risk 指标、聚合方式、置信区间、通过阈值、缺证据判定、版本、hash 和变更失效规则。
- 最终定点复核结论为 `PASS`（0 Critical、0 High、0 Medium）；确认 Task 2.7 已闭环且未重新引入循环依赖。
- 文档一致性断言、Markdown fence、whitespace、未暂存状态和原核心需求 sha256 校验全部通过；三份新文档可以进入用户审查。

### 旧数据集处理新决策

- 用户明确历史 Binance aggTrades 是 REST 回填数据，不是实时采集数据，因此与 Polymarket 历史盘口一样没有逐事件采集器接收时间，必须使用新采集延迟参考生成模拟接收时间。
- 用户决定等待新采集数据累计 48 小时，再使用 `polymarket_market` 和 `binance_aggtrade` 两源到采集服务器的延迟分布；新数据仍不得作为训练 market rows、features、labels、trajectory 或 holdout。
- 用户发现旧 typed Parquet 同时保留 `raw_json`，要求分析空间影响、typed 转换正确性以及删除条件，并创建旧数据集处理专项计划。

### received-time 字段事实修正

- `observed_received_time_ms` 不是旧数据原字段，也不是新数据 source-specific Parquet 的真实字段名；旧数据实际只有 Binance orderbook 和 RTDS Chainlink 存在 `recv_time_utc`，新数据物理字段名为 `timestamp_received`。
- 新需求改用可空 `collector_recorded_time_ms` 表示经验证的旧 `recv_time_utc` 映射，并与 `simulated_received_time_ms`、`event_available_time_ms` 和 provenance 分离。
- Polymarket 历史盘口与回填 Binance aggTrades 的 collector-recorded time 必须为 null；不得把平台 timestamp、批量下载 started/finished time 或模拟值冒充采集器记录时间。

### 48 小时校准阻断项

- 当前本地 `/home/chillchill/new-datasets` 只覆盖 2026-07-08 和 2026-07-09。只读统计中，Polymarket candidate wall span 约 `30.26h`、aggTrade 约 `14.26h`、两源共同完整 UTC 小时候选最多约 `14h`；严格 eligible healthy coverage 因 clock、health 和 transport identity 缺口仍为 `0h`。
- Binance aggTrade 事故 `DC-INCIDENT-20260709-AGGTRADE-T08` 在修复前后形成约 `22.37h` 断层；截至 2026-07-10 08:16 UTC 的审查快照，修复后新代码只累计约 `1.9h`，不得与事故前数据拼成 approved 48 小时。
- 发现新的 measurement-boundary 冲突：旧回填 aggTrades `timestamp_ms` 来自 Binance `T` trade time，而新 `binance_aggtrade_v2.event_time` 持久化的是 `E` event time。正式 48 小时计时前必须持久化 `T` 或由用户明确批准并量化 `E-T` transfer assumption。
- 当前 Polymarket typed child rows 没有 durable transport event ID；按 child row 统计会重复加权包含多个 price changes 的同一 message。approved 48 小时要求 fan-out 前 transport identity、child index/count 和共享 receive time。
- 当前启动证据把 host clock sync 标为 unavailable；事后单次同步快照不能补证过去区间。48 小时必须按两源各自 eligible healthy interval union 和 joint interval 交集累计实际 duration；完整 UTC hour 作为默认审计桶，证据完整的 partial hour 才能按精确毫秒计入，wall-clock span 不能替代。

### raw_json 空间与语义审查

- 本地 2026-06-30 解包目录共有 `312` 个 Parquet、`49,543,942` 行、Parquet 文件合计 `5.825 GiB`；`raw_json` compressed column chunks 为 `3.054 GiB`，占 `52.44%`。同时存在 `6,109,241,264` bytes 的 tar.gz，因此 `/home/chillchill/old-datasets` 总占用约 `12G`。用户关于体积来源的判断成立。
- 用户指定 4 小时窗口共 `52` 个 Parquet、`1.053 GiB`，`raw_json` compressed column chunks 为 `594,042,272` bytes，占 `52.526%`。
- Binance orderbook `14,400` 行和 RTDS `14,074` 行完成全量 typed/parsed-object 对账，mismatch 为 `0`；Polymarket 覆盖 48 文件/152 row groups 的 1,549 行分层样本并全量覆盖 140 条 tick-size change 和 48 条 market-resolved，mismatch 为 `0`。该证据不能替代所有日期、版本和未知事件的全量 parity gate。
- 当前 `raw_json` 是 JSON parse 后 sorted-key compact 重写的语义镜像，不是 wire-frame exact bytes。typed 转换仍可能折叠 absent/null/empty、改变 known 字段原始类型；现有 coverage validator 只比较文件/row count，现有 6 个 migration tests 不验证 typed-only round trip。
- Python data pipeline、Rust replay 和 recording/export 仍直接读取 `raw_json`；因此当前不能从正式 mirror 原地删列。
- 原归档尚无正式 file hash/row locator manifest，抽查文件 owner/group 可写且无 immutable flag。删除门保持关闭。

### 新专项计划与最终方向

- 新建 `plan/rebuild-core-old-data-only-dataset-processing-plan.md`，事项编号继续使用 `rebuild-core-old-data-only`，显式引用当前需求、主计划和本 changelog。
- 目标分为 L0 历史证据层、L1 typed-slim、L2 availability overlay 和 L3 training/replay。L0 永久保留 `raw_json` 并冻结 hash/locator；L1 不含 `raw_json`，只保留 approved typed/extra/provenance 和回源 locator。
- 计划要求先解决 Binance `T`、Polymarket transport identity、clock evidence 和 source/writer supervision，再开始正式 48 小时；旧归档 inventory/parity 可与采集并行，随后在纵向切片汇合。
- 任何 data-collect schema/deployment、远端转换、训练或 destructive cleanup 仍需用户另行授权。当前专项计划待独立审查和用户批准。

### 旧数据处理专项独立审查第一轮与修复

- 对抗审查结论为 `FAIL`（`0 Critical / 3 High / 4 Medium`）。High 包括：主计划先全量迁移而专项计划要求先切 consumer 的顺序冲突；允许 `E-T transfer assumption` 但后续只实现 `timestamp_received-T` 的悬空分支；旧 collector-recorded 与新 simulated 时间可能混合不同采集拓扑。
- 主计划阶段 5 已改为显式委托专项计划顺序：纵向切片通过后，先完成 Python/Rust/recording 的 typed-only 切换并发布 `ConsumerCompatibilityManifest PASS`，再启动全量迁移。
- 删除 `E-T` 作为 approved 备用路径。正式 48 小时必须逐事件持久化 Binance `T`；`E-T` 只保留为诊断统计，缺 `T` 时 gate 持续 blocked。
- 新增 `collector_recorded_topology_version` 与 `RecordedTopologyCompatibilityGate`。旧 `recv_time_utc` 保留为 recorded fact，但 host/region/clock/measurement boundary 与目标回放拓扑无法证明兼容时，相关跨源 episode 只能 provisional/non-promotable 或从 latency-sensitive 任务排除。
- 将阶段 2 的全库 parity 改为 validator 冻结加代表性/高风险/稀有事件验证；全量逐行 semantic parity 与阶段 7 每个 shard 的实际转换同遍执行，避免 schema 修订导致重复扫描全库，同时不降低最终删列门。
- 增加全量迁移峰值磁盘容量、保留水位和 fail-closed 门；禁止为腾空间提前删除 L0。
- 修正 joint coverage 术语为 `intersection(union(PM eligible intervals), union(agg eligible intervals))`；明确 duplicate-key 仅可做 synthetic negative test，L0 只能恢复已保存的 canonical semantic mirror，不能恢复历史 wire bytes 或已丢失 duplicate keys。
- 同步修正“四个新事项路径”、`collector_recorded_time` 与 CausalReplayGate provenance 术语。修复后等待独立复审。

### 延迟与 raw_json 定点复核及第二轮修复

- 延迟定点复核发现 `RecordedTopologyCompatibilityGate` 把 recorded-time 是否可信与是否代表 target topology 过度耦合，可能让 topology unknown 的旧 depth/RTDS 没有可定义的 `event_available_time_ms`。现拆为 `RecordedTimeValidityGate` 与 `TargetTopologyComparabilityGate`：可信 recorded fact 可用于 historical-as-recorded replay；topology unknown 只限制 target-runtime 声明、counterfactual capability 和 promotion。
- 48 小时区间明确为半开 `[start_ms,end_ms)`；合并去重后的 millisecond duration sum 是唯一 gate 权威，完整 UTC hour/full-hour count 只是派生审计视图，禁止与 partial duration 重复相加。
- Polymarket transport identity 收紧到官方 SDK 最早可见 transport message 接收边界、业务解析和 child fan-out 之前；绑定 connection epoch、connection-local ordinal/source sequence、SDK envelope 或 payload digest/reference、child index/count 和共享 receive time，不引入 raw REST、manual WebSocket 或影子 SDK。正式计时门显式要求 source-timestamp provenance 已部署。
- raw_json 定点复核发现 consumer “实现”与 active “切换”混为一体、阶段 7 缺实际 consumer 全量对账、quarantine 可静默缩减训练 boundary。阶段 6 现只实现 typed-only consumer 并在纵向切片验收，legacy active pointer 保持不变；阶段 7 生成 inactive full candidate 后，由实际 Python/Rust/recording reader 对全部 shard 做 event-envelope 对账和风险分层深层验证，再协调原子切换 dataset pointer 与 consumer config，并提供 rollback receipt。
- 正式 L1 默认要求 `required_shard_quarantine_count = 0`。任何改变正式市场集合、日期、regime、source coverage、split 或 holdout 的排除都阻断发布、输出影响、重新冻结并请求用户批准；新 schema/event family 回到 schema/consumer 合同，不得永久 quarantine 后继续宣称完成。
- 磁盘门改为逐 filesystem 计算，覆盖归档解包/流式缓存、staging、失败/quarantine shard、checkpoint、并发任务、双版本窗口和恢复余量，并在每个 shard 启动前重检。
- L0 不可变门不再允许单一 `chmod` 副本通过；最低同时要求 file-hash manifest、active 只读/immutable、独立可恢复 versioned copy/只读快照/对象副本和实际恢复演练。上述修复等待定点复审。

### 旧数据处理专项最终复审

- 延迟定点复审结论为 `PASS`（`0 Critical / 0 High / 0 Medium`）：确认逐事件 `T`、官方 SDK transport identity/source-timestamp start gate、半开 eligible intervals、唯一 millisecond duration 权威，以及 recorded-time validity/target-topology comparability 解耦均已闭合。
- raw_json/consumer 定点复审结论为 `PASS`（`0 Critical / 0 High / 0 Medium`）：确认阶段 6 只实现并在纵向切片验收，阶段 7 按 inactive candidate、全量实际 consumer gate、协调原子激活和 rollback 顺序闭合；required quarantine、逐 filesystem 容量和 L0 独立恢复副本门均正确。
- 最终全局对抗复审结论为 `PASS`（`0 Critical / 0 High / 0 Medium`）：四份新文档在需求、主计划、专项计划和 changelog 之间一致，没有新的循环、静默语料缩减、过度拓扑阻断或未授权执行路径。
- 原 `demand/rebuild-core-demand.md` sha256 复核仍为 `23cc0f9c61941218a36648d5c30fd1d4293d05c75e036b92ea59bfad9746f36a`；本事项未继续修改旧核心需求文档。
