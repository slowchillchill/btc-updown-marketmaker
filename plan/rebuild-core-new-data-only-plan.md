# 新采集数据唯一训练源核心重构执行计划

事项编号：`rebuild-core-new-data-only`

状态：`基础准备计划；当前只执行文档阶段 0；FinalDatasetBoundaryStatus=DECISION_REQUIRED；CollectionStartReadinessStatus=DOCS_ONLY_NOT_EXECUTED`

对应需求：`/home/chillchill/btc-updown-marketmaker/demand/rebuild-core-new-data-only-demand.md`

核心训练框架：`/home/chillchill/btc-updown-marketmaker/plan/rebuild-core-new-data-only-training-framework.md`

对应 changelog：`/home/chillchill/btc-updown-marketmaker/changelog/rebuild-core-new-data-only-changelog.md`

## 1. 当前授权边界与成功标准

当前 UTC 日期是 `2026-07-10`，目标数据尚未采完。本轮只允许完成：

- 新数据-only需求文档；
- 本执行计划；
- 核心训练框架；
- 同事项 changelog；
- 只读代码、数据、服务和官方资料核对；
- 文档自审、机械检查与独立审查。

当前不允许修改/部署 collector，不允许处理目标数据、实现 simulator/model、启动训练、paper/live 或资金操作。阶段 1 之后均是未来执行计划，必须另行获得用户授权；与结束日无关的开始准备不需要等待A/B决定，但不能把“计划可执行”误写成“本轮已授权”。

本轮成功标准：四份文档同 slug 对齐；事实、推断和建议分开；训练框架、数据额外需求、simulator、reward、split 和 `mistake-log` 闭环在采集/训练实现前冻结；没有把未来数据写成已完成事实。

## 2. 为什么按这个顺序执行

```text
需求/训练合同
    ↓
固定开始日 readiness + 非训练准备纵切
    ↓
完成目标 cohort + label/account tail
    ↓
解决结束日并冻结 dataset boundary/eligibility/split
    ↓
正式 cohort 小切片复验 canonical event → simulator → NOE → ledger → reward
    ↓
全量 bounded pipeline
    ↓
冻结 simulator capability/calibration/impact/valuation/rollout budget
    ↓
baselines/representation
    ↓
counterfactual action ranking
    ↓
frozen holdout
```

顺序收益：

1. 先冻结训练合同并用 7月8-9日新样本做非训练准备纵切，避免数据采完才发现没有 transport identity、p2b、T-30、winner label 或所需 action semantics。
2. 采集前只处理不可事后可靠恢复的字段；label maturity 可异步回填，不把所有未来工作挤进 collector。
3. cohort 完成后先冻结 boundary/split，防止看到收益后换日期、换 market 或调整 holdout。
4. 先做纵向切片，能用少量 market 暴露 event order、账本、reward 和 simulator 错误，避免全量扫描后返工。
5. simulator/ledger 闭合后再训练策略，防止高 AUC 或漂亮 PnL 建立在错误执行现实上。
6. 先强基线再主模型，防止用复杂网络掩盖数据泄漏或标签无效。
7. action-surface、consumer 和 sensitivity 先于 leaderboard，直接阻断旧 MPV2 的“输出存在但没有控制订单”问题。

## 3. 阶段总览

| 阶段 | 目标 | 当前状态 | 进入条件 | 退出证据 |
|---|---|---|---|---|
| 0 | 基础文档与框架冻结 | 当前执行 | 用户提出准备任务 | 四文档审查通过，结束日期冲突显式BLOCKED |
| 1 | 固定开始日前可训练性准备与非训练纵切 | 未授权/未开始 | 开始日已固定、用户另行授权；不依赖A/B结束日 | identity/p2b/clock/schema/retention与准备纵切receipt |
| 2 | 目标 cohort 采集与日级保护 | 未开始 | 阶段 1 PASS 或批准降级 | 每日 finalized raw/manifest/hash/health evidence |
| 3 | cohort boundary、maturity 与 split 冻结 | 未开始 | cohort + tail 完成 | DatasetBoundary/EpisodeEligibility/Split manifests |
| 4 | 端到端纵向切片 | 未开始 | 阶段 3 boundary 冻结 | canonical→simulator→NOE→ledger→reward closure |
| 5 | bounded 全量 canonical pipeline | 未开始 | 阶段 4 PASS | 全量 features/labels/trajectories 与资源报告 |
| 6 | simulator capability/calibration 与 rollout预算冻结 | 未开始 | 阶段 4 PASS、阶段 5必要输入可用 | capability/calibration/impact/valuation/budget manifests |
| 7 | baselines、GRU、TCN 与 action ranker | 未开始 | 阶段 5/6 PASS | trial ledger、action-surface、模型比较 |
| 8 | trajectory aggregation 与 frozen holdout | 未开始 | 阶段 7 gate PASS | 一次性 holdout report、candidate bundle |
| 9 | 后续 paper/shadow 决策 | 非本事项授权 | 用户另立需求并批准 | 独立文档和证据；不自动进入 live |

## 阶段 0：基础准备与合同冻结

### 任务 0.1：盘点权威资料与实际边界

- 步骤 0.1.1：逐项读取核心需求、data-collect demand/plan/changelog、`mistake-log`、V2 训练失败实现和当前新数据 schema。
  验证：每个关键判断能指向真实文件、字段、样本或服务证据。
- 步骤 0.1.2：只读核对 `/home/chillchill/new-datasets` 与采集服务器当前长跑状态。
  验证：明确当前样本只用于准备审查，不能冒充目标 cohort。
- 步骤 0.1.3：检索模型、LOB、序列建模、action ranking、offline RL、simulation 和当前 Polymarket 官方规则。
  验证：技术选择优先引用论文、官方文档和成熟工具，而不是凭记忆。

### 任务 0.2：解决或显式保留日期冲突

- 步骤 0.2.1：向用户呈现：
  - A：`[2026-07-11, 2026-07-24)`，13 日；
  - B：`[2026-07-11, 2026-07-25)`，14 日。
- 步骤 0.2.2：用户确认前统一使用 `DATASET_COHORT_END_EXCLUSIVE_UTC`。
  验证：所有文档均为 `FinalDatasetBoundaryStatus=DECISION_REQUIRED`，没有机械 split 或训练命令使用未确认结束日。

### 任务 0.3：冻结技术方向

- 步骤 0.3.1：确定 `ESAR-v1` 主模型、XGBoost/MLP 基线、TCN challenger 和后置路线。
  验证：框架文档解释数据前提、模型/动作/损失、推理状态和 rejected alternatives。
- 步骤 0.3.2：冻结严格事件触发、p2b、T-30/state reconstruction、MarketContextEpisode/AccountRunTrajectory、joint candidate、budgeted simulator labels、valuation/ledger reward 和 time-forward split。
  验证：不存在固定 decision tick、全事件全后缀rollout、直接BC/IQL/DT、跨split账户状态或independent unused action heads。
- 步骤 0.3.3：建立 `mistake-log` 逐项闭环表。
  验证：每个已知高影响失败都有阻断测试和剩余风险，不宣称架构自动解决不可观察事实。

### 任务 0.4：文档对齐与审查

- 步骤 0.4.1：创建同事项编号的 demand、plan、framework、changelog。
  验证：顶部互相引用；需求写需求/验收，框架写技术权威，计划写阶段/测试，changelog 写事实和决策变化。
- 步骤 0.4.2：执行 Markdown fence、whitespace、链接路径、事项编号、状态和 old-doc hash 检查。
  验证：`git diff --check` 通过；旧核心和旧数据-only文档 hash 不变。
- 步骤 0.4.3：派独立审查分别检查数据合同、训练方法和全局一致性。
  验证：Critical/High 清零；Medium 要么修复，要么有明确不影响当前基础准备的理由。
- 步骤 0.4.4：按项目阶段规则，只在确认提交范围仅包含本事项文档且不卷入用户其他脏改动后，创建独立 commit/push；若提交范围与用户现有改动无法安全隔离，则停止并报告冲突。
  验证：commit 只包含四份新文档；push 结果和 commit hash 可追溯。

阶段 0 退出条件：四文档审查通过；FinalDatasetBoundaryGate仍可保持显式 BLOCKED，但不能被遗忘。退出阶段 0 不授权阶段 1。

## 阶段 1：cohort 开始前可训练性准备

> 本阶段涉及代码/schema/deploy和非训练纵切，当前未授权。开始日已固定为7月11日，本阶段不等待A/B结束日；若开始前未完成，必须选择批准降级或承认受影响数据不具备 `policy_training_eligible` 能力，不能事后伪造证据。

### 任务 1.1：拆分开始 readiness 与最终边界

- 步骤 1.1.1：冻结 `CollectionStartReadinessGate`：start=`2026-07-11T00:00:00Z`、model context=T-30、state reconstruction lookback、identity/p2b/clock/schema/retention。
  验证：A/B尚未决定也能判断开始前不可恢复项；当前文档状态仍为NOT_EXECUTED。
- 步骤 1.1.2：保留 `FinalDatasetBoundaryGate`：用户确认A/B后才冻结end、名义market数、dedicated calibration与最后两日holdout。
  验证：结束日未确认时禁止停止计划、final manifest和split，但不阻断固定开始日采集。
- 步骤 1.1.3：冻结 collector git commit、release binary sha256、systemd unit、CLI/config digest、schema versions、host/region 和官方 SDK version。
  验证：startup receipt 与实际进程 `/proc`/systemd 配置一致。
- 步骤 1.1.4：中途升级策略设为显式 cutover/regime，不允许覆盖同路径或静默混训。
  验证：synthetic version-change test 能把前后 episode 分开。

### 任务 1.2：补齐不可恢复的 Polymarket event identity

- 步骤 1.2.1：首选在官方SDK typed event最早可见边界、业务parse/fan-out前生成`transport_message_id + connection_epoch + source-local event_seq`；进入共享causal queue时由单一allocator原子分配causal-ingress`timestamp_received + collector_run_id + global_ingress_seq`。
  验证：一个多-child frame只有一个transport ID，child index/count连续、causal receive共享；同run timestamp随seq不倒退，顺序按(timestamp,global seq tie-break)。
- 步骤 1.2.2：typed schema、manifest 和 tests 保存 identity、SDK envelope/payload digest/reference 与 row locator。
  验证：跨source抢占调度、同毫秒tie、clock rollback、rollover/reconnect/重启正反例通过；更早source-task receive另名diagnostic；identity不碰撞，不能通过raw REST/manual WebSocket旁路实现。
- 步骤 1.2.3：若用户不批准 schema 改动，冻结 fallback：同 `timestamp_received` 毫秒原子 microbatch。
  验证：source batch key含source/receive-ms/run/connection scope；无global ingress的跨source同毫秒形成simultaneous CrossSourceTieGroup；原子apply后对每个受影响active market各触发一次，prefetched只update；同batch同asset/price多次冲突更新property test通过；能力默认representation_only/non-promotable。

### 任务 1.3：冻结 PriceToBeatAuthorityContract

- 步骤 1.3.1：用官方 Rust SDK RTDS Chainlink类型定义精确派生：`btc/usd` 且 `payload_timestamp == market_start` 的full-precision value；start取slug epoch/Gamma Event startTime。
  验证：收到前为p2b_pending且hard boundary禁止新submit，因果可见时间只用collector receive；禁止nearest/forward-fill/interpolate/Binance替代。
- 步骤 1.3.2：用7月8-9日准备样本对照 exact start/end RTDS rows、市场规则、winner与已有官方event结果，不把它们用于目标训练/选模。
  验证：完整精度、condition/slug、row locator和hash一致；缺exact row负例变unresolved。
- 步骤 1.3.3：记录SDK能力：RTDS派生可用；Gamma eventMetadata显式p2b/final字段当前SDK缺失。
  验证：derived_observed允许研究训练；official_confirmed/promotion标记BLOCKED_SDK_FIELD_GAP，不自行使用raw Gamma REST或网页/internal endpoint。

### 任务 1.4：冻结时间、逐源准入与 source semantics

- 步骤 1.4.1：将 `timestamp_received` 明确为 SDK typed event 交付后的 collector 本机采样时间。
  验证：文档、schema metadata 和 causal builder 不称其为 wire ingress/纯 network latency。
- 步骤 1.4.2：部署连续 clock/NTP receipt 与异常/cutover记录。
  验证：故意注入 clock status unknown 时，episode capability 降级而不是静默 PASS。
- 步骤 1.4.3：决定 Binance aggTrade `T`：推荐新增 typed 列/sidecar；若不做，明确模型只使用 receive+E。
  验证：两条路线都不能把 E 重命名成 T，且这一项不单独阻断 collector 启动。
- 步骤 1.4.4：在目标outcome不可见时预注册并hash逐源准入式和阈值：PM双token/initial state、depth continuity、aggTrade ID gap、RTDS freshness/exact p2b row、clock/reconnect/cutover、capability downgrade。
  验证：7月8-9日只用于运行正反例，不能根据目标cohort收益重调阈值。

### 任务 1.5：保护 canonical raw 与日级拉取

- 步骤 1.5.1：确认本地日归档每天拉取完整 `typed_raw/utc_day=...` 与 segment manifests，而不是只保留压缩 archive 中的 `validated_markets`。
  验证：远端 50GB retention 删除前，本地 finalized raw、manifest、sha256 和 row counts 已落盘并校验。
- 步骤 1.5.2：冻结 remote retention、local free-space 和 archive failure alert。
  验证：日拉取失败时阻断远端对应 day 的安全删除判断；不得假定 tar.gz 包含 canonical raw。
- 步骤 1.5.3：为 14 日容量和双版本 staging 计算磁盘水位。
  验证：保留 raw、derived、failed staging、simulator labels 和恢复余量，不因空间不足提前删证据。

### 任务 1.6：pre-cohort 非训练合同纵向切片

- 步骤 1.6.1：只使用明确允许作准备验证的7月8-9日新样本，不访问目标outcome做选模，打通numeric part/order、exact/approx atomicity、T-30与snapshot→checkpoint重建。
  验证：row reorder、gap、missing snapshot、multi-child和multi-market负例fail closed。
- 步骤 1.6.2：用最小stub验证p2b_pending→derived_observed、完整candidate→simulator raw evidence→NOE→AccountRunTrajectory ledger→reward/valuation closure与output consumer。
  验证：这不是训练、leaderboard或目标cohort验收；只验证合同可实现及不可恢复字段是否齐全。
- 步骤 1.6.3：profile有效事件密度、single-writer/state-version、candidate生成和最小rollout吞吐。
  验证：形成未来CounterfactualRolloutBudgetContract的保守初值，不改变逐有效事件runtime评估语义。

### 任务 1.7：阶段审查

- 自审：schema diff、SDK-first、identity/p2b/timestamp、逐源准入、retention和非训练纵切。
- 独立审查：判断哪些字段不可事后恢复，fallback 是否被错误写成 exact/policy能力。
- 验证：Critical/High 清零；阶段 commit/push 后才允许进入阶段 2。

## 阶段 2：目标 cohort 采集与日级保护

### 任务 2.1：启动前 gate

- 步骤 2.1.1：核对 UTC 当前时间、unit、release hash、NTP、六类 writer、磁盘和 retention。
  验证：启动 receipt 完整，不能用前一天 snapshot 代替。
- 步骤 2.1.2：确认 T-30 首日 halo 已开始记录。
  验证：首个目标 market 的 `[start-30s,start)` 可见序列和snapshot→T-30 reconstruction chain都能从finalized raw重建；否则该market预标记ineligible，直到split内checkpoint可用。

### 任务 2.2：持续采集而不提前训练

- 步骤 2.2.1：长跑采集；监测 service、source/writer progress、queue、RSS、disk、clock、reconnect、gaps 和 segment finalize。
  验证：每个 UTC 日形成完整 health timeline；当前 active/zero restart 只是一个信号，不是数据 PASS。
- 步骤 2.2.2：事故形成精确 `[start,end)` interval、影响 source/market、binary/schema regime 和修复 commit。
  验证：事故前后不被拼成无缝 coverage；坏 market 隔离，不默认丢整日。
- 步骤 2.2.3：不在 cohort 未完成时启动正式 feature fitting、split、simulator leaderboard 或模型训练。
  验证：training run inventory 为空；任何 exploratory probe 明确不写正式 artifact。
- 步骤 2.2.4：逐market记录exact start/end Chainlink boundary row是否到达及collector receive time；缺失只标p2b/final unresolved，不在线猜补。
  验证：每日报告区分p2b_pending、derived_observed、official-confirmation pending和censored。

### 任务 2.3：每日 canonical 拉取与校验

- 步骤 2.3.1：按 UTC 日拉取完整 typed raw、segment manifests、run/clock/health receipts 到本地。
  验证：远端/本地 sha256、size、row count、source kind 和 finalized 集合一致。
- 步骤 2.3.2：生成日级 provisional coverage，不以 `validated_markets PASS` 代替。
  验证：显示实际 observed markets、missing nominal markets、source gaps、partial和version regimes。
- 步骤 2.3.3：校验 selected bundles 只作诊断；发现 row order倒退不修改 raw。
  验证：canonical rebuild test按 numeric part/ordinal恢复单调因果顺序。

### 任务 2.4：阶段审查

- 自审：每日 hash、事故、缺口和 retention 删除边界。
- 独立审查：随机抽取首/中/末日及事故日，重建 T-30 和 source state。
- 退出：用户已在停止前解决结束边界，目标anchor interval、model context、state reconstruction和必要label/account tail均已保存；否则阶段不完成。

## 阶段 3：Boundary、标签成熟与 split 冻结

### 任务 3.1：冻结 DatasetBoundaryManifest

- 步骤 3.1.1：逐文件记录 path、sha256、size、row count、time range、source kind、schema、collector/config regime、health/clock refs。
  验证：allowlist 之外不能打开；old-data denylist access test fail closed。
- 步骤 3.1.2：记录名义 market 与实际 observed/eligible/excluded 差异。
  验证：不会把 74/75 个观测 market 的 PASS 写成完整 288/日。
- 步骤 3.1.3：冻结 model context halo、state reconstruction lookback 和 label/account tail 的用途。
  验证：更早新数据只生成checkpoint、没有区间外独立episode；最后market与AccountRunTrajectory已成熟闭合或明确censored。

### 任务 3.2：重建 per-market episode

- 步骤 3.2.1：从 raw manifests 按稳定 identity/order 构造 canonical events。
  验证：同输入多次构建 event IDs/order/hash 一致。
- 步骤 3.2.2：从有效 Binance snapshot 连续应用全部 depth updates，在 T-30 生成 L2 checkpoint。
  验证：sequence gap、out-of-order、snapshot mismatch 负例阻断 `policy_training_eligible`；split边界chain不跨split。
- 步骤 3.2.3：确定性派生 prefetched/active、Up/Down token mapping、market lineage 和 reset receipts。
  验证：重叠 market state 能并存，typed `lifecycle_state=active` 不被盲信。
- 步骤 3.2.4：生成 `EpisodeEligibilityRecord`。
  验证：policy_training_eligible/representation_only/diagnostic/excluded与独立promotion_execution_calibrated原因可重现；approx默认representation_only。

### 任务 3.3：异步 outcome/settlement maturity

- 步骤 3.3.1：优先使用官方 Rust SDK 可覆盖路径查询/订阅最终 outcome；SDK能力不足时停下报告，不擅自用 raw REST/Gamma shadow path。
  验证：exact start/end SDK-RTDS full precision、winner token、condition、outcome、source和maturity time一致；eventMetadata显式确认缺口单列。
- 步骤 3.3.2：记录 unresolved、conflict、late maturity 与 censoring。
  验证：未成熟 market 不进入 policy/reward/holdout；不会默认写零。

### 任务 3.4：先 split 再拟合/模拟

- 步骤 3.4.1：按确认日期机械派生 development、calibration、last-two-day holdout。
  验证：日期未确认则命令拒绝生成 split。
- 步骤 3.4.2：按 market group 和严格时间前推分配全部 source/replica。
  验证：market ID、source row locator和AccountRunTrajectory跨 split交集为零。
- 步骤 3.4.3：计算 purge/embargo dependency horizon。
  验证：T-30、state reconstruction、shared global events、label horizon、TTL和reward maturity不跨 split泄漏；必要时丢弃边界market。
- 步骤 3.4.4：冻结 final holdout access control。
  验证：日常 training account/process 无法读取 holdout labels/outcomes。
- 步骤 3.4.5：为development、dedicated calibration和holdout分别冻结独立account fixture，split内按时间carry，split末尾延伸tail直到订单/结算闭合。
  验证：train policy产生的cash/position/order state不进入calibration/holdout；account-run总额是经济closure权威。

### 任务 3.5：阶段审查

- 审查 boundary、eligibility、maturity、split、purge/embargo 和 old-data denylist。
- 退出：manifests hash 冻结；任何后续 boundary 修改都会使下游 artifact 失效并回到本阶段。

## 阶段 4：正式 cohort 端到端纵向切片

### 任务 4.1：选择不看收益的代表性切片

- 步骤 4.1.1：按预注册条件选择低/中/高事件密度、可见流动性、source reconnect和边界coverage等market regime，再由simulator注入full/partial-fill stress scenario。
  验证：不把外部行情误称为本账户fill证据；选择依据不含模型PnL或outcome方向。

### 任务 4.2：打通 causal state

- 步骤 4.2.1：从 T-30 replay 至 terminal，验证多 market overlap、transport atomicity 和 hidden reset。
  验证：p2b pending/maturity、state reconstruction、source timestamp future-shift、row reorder、child duplicate 和 reconnect 负例均被捕获。
- 步骤 4.2.2：构造最小 ESAR single-step stub 与 candidate contract，不训练正式模型。
  验证：每个有效模型可见event触发一次，prefetched只warmup；single-writer/state-version/stale intent/overload和physical chunk语义明确。

### 任务 4.3：打通 simulator、NOE、ledger、reward

- 步骤 4.3.1：实现最小平台 lifecycle，先覆盖 no-action、submit、accepted/rejected、partial/full fill、cancel、settlement。
  验证：property tests 覆盖现金、仓位、订单 remaining、settlement守恒。
- 步骤 4.3.2：simulator raw evidence 进入共享 NOE/ledger。
  验证：禁止 private-state-to-reward shortcut。
- 步骤 4.3.3：计算 reward vector 与 ledger closure。
  验证：ConservativeNavValuation覆盖reserved/pending/unsettled/stale/depth；AccountRunTrajectory sum event ΔNAV = terminal economic return，三类done exactly once。
- 步骤 4.3.4：同 state 两个 candidate common-random-number paired rollout。
  验证：seed/version/input hash一致时轨迹逐事件相同；small-agent/own-order overlay和paired ΔJ equivalence语义通过。

### 任务 4.4：阶段审查

- 审查 correctness、简洁性、SDK-first、模型/平台 authority、参数 provenance 和错误注入。
- 只有纵向切片 PASS 才允许全量 pipeline；失败时在小切片修合同，不进入全量迁移。

## 阶段 5：bounded 全量 canonical pipeline

### 任务 5.1：流式 feature/label builder

- 步骤 5.1.1：PyArrow predicate pushdown、按 day/source/market sharding，禁止 full load。
  验证：peak RSS/GPU、I/O、wall time 和 resume checkpoint 记录完整。
- 步骤 5.1.2：仅在 train fold fit normalizer/statistics；validation/calibration/holdout只 transform。
  验证：故意把 holdout统计注入 fit 时测试失败。
- 步骤 5.1.3：生成 supervised heads 的 mature/mask labels。
  验证：unresolved/censored/null语义一致，不出现默认零。

### 任务 5.2：训练序列与 single-step parity fixture

- 步骤 5.2.1：按 market 构造 T-30→terminal unroll，physical chunks carry/detach hidden。
  验证：冻结chunk/burn-in/loss window；随机chunk由前序no-grad burn-in重建；不同chunk size前向输出、full-unroll/no-detach、早期event干预和remove-hidden/multiscale ablation均报告。
- 步骤 5.2.2：保存 event-state fixture 供 PyTorch/ONNX/Rust 对账。
  验证：训练与runtime不是两套 feature/normalizer。

### 任务 5.3：阶段审查

- 扫描 old-data access、split leakage、full-load、row-order假设和 nondeterminism。
- 退出：全量 canonical artifact 与 resource report hash 冻结。

## 阶段 6：simulator capability/calibration 与 rollout 预算冻结

### 任务 6.1：冻结 SimulatorCalibrationManifest

- 步骤 6.1.1：先冻结SimulatorCapabilityMatrix，只启用candidate实际需要的order/lifecycle/accounting路径；逐规则/参数记录official_rule、observed_new_data、assumed/stress provenance。
  验证：unsupported fail closed；聚合L2 queue proxy不显示为observed FIFO；未建设第二套通用exchange/SDK/ledger。
- 步骤 6.1.2：当前官方 order types、post-only、fee、rebate/reward eligibility 和 settlement 规则复核。
  验证：动态 market metadata 优先，禁止旧固定 fee/250ms/TTL/partial概率常数。
- 步骤 6.1.3：建立 conservative/base/high-latency/tail/adverse-fill scenarios。
  验证：单一乐观 scenario 不能标记 promotable。
- 步骤 6.1.4：冻结CounterfactualMarketImpactContract和ConservativeNavValuationContract。
  验证：small-agent假设、size/visible-depth/volume上限、own-order overlay、impact stress和stale/one-sided/pending估值正反例通过。

### 任务 6.2：counterfactual pilot 与预算冻结

- 步骤 6.2.1：仅在阶段4正式纵切market上做counterfactual pilot；冻结CounterfactualRolloutBudgetContract：按event type/market phase/state novelty/account-order状态抽样与收益无关的label states，记录inclusion probability/weight、每market groups、每group candidates、seeds/scenarios、总branch/wall-time/disk，以及每branch max-events/duration/market-transitions/pending-tail上限。
  验证：runtime仍逐有效事件评估；没有固定tick抽样或全事件全后缀组合爆炸。
- 步骤 6.2.2：按candidate contract枚举完整合法Model Intents，上限32 candidates、2 logical legs、4 atomic ops。
  验证：超限配置fail closed；enumerator只做合法性/硬风险，不按score预筛或生成策略。
- 步骤 6.2.3：在pilot同state/path/seed paired rollout到candidate_effect_terminal，或达到branch hard limit后censor；保存continuation_policy_hash/iteration。只有完整policy evaluation运行到AccountRunTrajectory terminal。
  验证：超限未闭合branch标censored并保存valuation residual bounds或排除ranking；pilot group、paired ΔJ、reward/ledger refs齐全；本阶段不生成全量ranker dataset。
- 步骤 6.2.4：运行 simulator sensitivity、market-impact和 exploit probes。
  验证：只在一个参数点盈利、异常 churn、无风险套利或 private-state leakage 均阻断。

### 任务 6.3：阶段审查

- 审查 lifecycle closure 与 economic closure 分离、unknown outcome、cancel race、late fill、fee double count、maturity。
- 退出：仅 `assumed/stress` execution参数时，artifact 明确 `non-promotable`。

## 阶段 7：baselines、ESAR-v1 与 action ranking

### 任务 7.1：先跑最低基线

- 步骤 7.1.1：常数、逻辑回归、小 MLP、naive directional 和简单规则 maker。
  验证：同 split、同 costs、同 simulator scenarios。

### 任务 7.2：监督 representation

- 步骤 7.2.1：训练 ESAR GRU outcome/dynamics/calibration heads。
  验证：anchored folds、Brier/ECE/quantile coverage、seed variance，AUC不单独晋级。
- 步骤 7.2.2：同参数/延迟预算训练 TCN。
  验证：相同输入/参数上限/live p99，另报wall-clock receptive field、streaming cache和峰值内存；不只看一个validation分数。

### 任务 7.3：生成预算化 counterfactual groups 与 XGBoost ranker

- 步骤 7.3.1：representation和simulator合同均冻结后，按CounterfactualRolloutBudgetContract生成全量训练用grouped candidate dataset。
  验证：state抽样与收益无关、inclusion weight正确、continuation hash分层、总branch/wall-time/disk未超预算。
- 步骤 7.3.2：先用同一groups训练XGBoost grouped action ranker基线。
  验证：query group完整，candidate rows不跨split或随机拆分。

### 任务 7.4：joint candidate scorer

- 步骤 7.4.1：训练 grouped ranking + quantile + masked execution losses。
  验证：所有loss maturity/抽样weight正确；tie基于paired ΔJ interval完全落入equivalence band，不把不确定当等价。
- 步骤 7.4.2：action-surface audit。
  验证：ModelOutputConsumerMatrix、raw/decoded/consumer、逐维intervention、control separation、support和entropy全部报告；training-only head不伪装runtime能力。
- 步骤 7.4.3：runtime parity。
  验证：PyTorch/ONNX/Rust hidden carry、score、selected intent对账；single-writer、global ingress ID/`relevant_pending_watermark[market_id]`与account fan-out、state-version/causal-cutoff、queued relevant-event stale拒绝与重评、unrelated prefetched event不误stale、no-submit和burst overload通过。

### 任务 7.5：阶段审查

- 先审 action surface，再审 PnL；全 no-action须判断是否标签支持，所有candidate相同或输出未消费则直接FAIL。
- 所有 run，包括失败 seed/超参，写入 trial ledger。

## 阶段 8：trajectory aggregation 与 frozen holdout

### 任务 8.1：simulator dataset aggregation

- 步骤 8.1.1：使用 no-action、规则、分层随机和当前 policy 收集状态。
  验证：只在更小预注册subset内、遵守rollout budget；inventory/open-order/cancel states coverage增加，不被单一teacher垄断。
- 步骤 8.1.2：每轮冻结 policy/dataset/simulator/continuation-policy hash。
  验证：能够回放任一ranking group；不同continuation labels分层/条件化，不能隐式在线漂移或混为同一真值。

### 任务 8.2：预注册 final evaluation

- 步骤 8.2.1：冻结 primary metric、valuation、risk weights、expected-shortfall alpha、paired-equivalence band、baseline、seeds 和 scenario set。
  验证：holdout结果不可见时完成并签 hash。
- 步骤 8.2.2：冻结 runtime budget 与 promotion capability状态。
  验证：只有 assumed simulator参数的候选预先标记 non-promotable。

### 任务 8.3：只运行一次 frozen holdout

- 步骤 8.3.1：在最后两个 calendar UTC 日运行。
  验证：不换日期、不删难看 market、不调 threshold。
- 步骤 8.3.2：报告 calibration、net EV、downside/expected-shortfall、drawdown、inventory、action support、regime、scenario sensitivity、CPU/memory/event-to-intent。
  验证：逐日报告、contiguous-market block与paired-market条件性interval；明确只有两个day clusters，禁止宣称稳健day-level CI，不把event rows当IID。
- 步骤 8.3.3：生成 bundle 与 `training/evaluation_candidate` 状态。
  验证：bundle存在、exit 0、单一AUC/PnL均不是promotion证据。

### 任务 8.4：阶段审查与停止点

- 独立审查 dataset/split/simulator/reward/model/runtime完整链路。
- 修复会改变 frozen contract 的问题时，holdout验收作废并回到相应阶段；已观察的两日只能降为diagnostic，不能再次称为pristine holdout。重新正式验收必须取得新的未来未读holdout和用户批准，不得偷偷重跑并只保留最好结果。
- 本计划在 candidate bundle 处停止；paper/shadow/live 必须另立需求和授权。

## 阶段 9：后续 paper/shadow 决策门

本阶段仅声明未来门，不属于当前计划的已授权交付：

- 先决定是否需要收集真实 execution feedback 以校准 fill/cancel/sellable；
- simulator-only、assumed参数候选不得直接 promotion；
- paper/no-submit shadow 必须有新的 demand/plan/changelog；
- live 仍需单独资金、安全、SDK、部署和回滚批准；
- live binary 在本地构建，模型从本地已验证 artifact 直接部署到 `/home/ubuntu/live-trade`，不经 GitHub，不在 live 主机编译。

## 全局阻断条件

任一条件成立即停止对应阶段：

1. 13/14 日边界未解决却需要生成 final cohort/split；
2. 目标数据实际 schema、collector/runtime行为与已批准合同冲突；
3. 需要官方 Rust SDK 不支持的能力且尚未获得用户批准替代方案；
4. canonical raw 在远端 retention 前未安全拉回或 hash 不一致；
5. transport identity 不存在且用户未批准 approximate microbatch；
6. exact price-to-beat/final boundary row、label maturity或condition-token winner冲突；
7. old-data/旧 bundle 被访问；
8. fixed tick、random-row split、holdout调参、full-load或physical-row-order假设出现；
9. simulator/NOE/AccountRunTrajectory ledger/reward/valuation不闭合；
10. action输出无consumer或被runtime改写；
11. counterfactual没有预算/抽样权重、continuation hash或超出small-agent impact能力；
12. simulator只有assumed参数或SDK显式p2b确认缺口未解决却要宣称promotion/live能力；
13. 用户当前授权只到文档准备，却要开始后续实现/训练。

## 文档与阶段同步规则

1. 每阶段开始前复核需求验收条目和核心训练框架对应合同。
2. 每任务完成后自审 diff、证据、边界和剩余风险。
3. 每阶段完成后派独立审查；Critical/High 未清零不得宣布完成。
4. 现实与已批准文档冲突时先写 changelog 的事实、影响和选项，向用户请求决定，不擅自改语义。
5. 每阶段改动 demand/plan/framework/changelog 任一项时，同事项文档同步；技术选择变化必须记录为何原结论失效。
6. 阶段 commit 只包含该阶段明确范围，不卷入用户现有脏改动；模型和数据 artifact 不进入 GitHub。
