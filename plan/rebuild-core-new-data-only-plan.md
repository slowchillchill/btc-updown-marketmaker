# 新采集数据唯一训练源核心重构执行计划

事项编号：`rebuild-core-new-data-only`

状态：`基础准备修订与 D+1 归档验证；StrategyActionSurface=BUY_ONLY；FinalDatasetBoundaryStatus=DEFERRED_TO_OBSERVED_COLLECTION；CollectionStartReadinessStatus=SCHEMA_V3_DEPLOYED_AND_VERIFIED`

对应需求：`/home/chillchill/btc-updown-marketmaker/demand/rebuild-core-new-data-only-demand.md`

核心训练框架：`/home/chillchill/btc-updown-marketmaker/plan/rebuild-core-new-data-only-training-framework.md`

对应 changelog：`/home/chillchill/btc-updown-marketmaker/changelog/rebuild-core-new-data-only-changelog.md`

## 1. 当前授权边界与成功标准

当前 UTC 日期是 `2026-07-10`，正式目标日期尚未选择。本轮允许完成：

- 新数据-only需求文档；
- 本执行计划；
- 核心训练框架；
- 同事项 changelog；
- 只读代码、数据、服务和官方资料核对；
- Binance aggTrade `trade_time=T` schema v3、测试、release 构建、数据采集服务器重新部署与新段验证；
- 只拉昨天 D 的 D+1 UTC 02:00 日归档、Gamma `priceToBeat/finalPrice` 两字段文件、历史 partial 不读但不整日阻断、同名云文件幂等修复；
- 文档自审、机械检查与独立审查。

当前不允许实现 simulator/model、启动训练、paper/live 或资金操作；也不允许把本轮 schema 重新部署后的任意日期自动命名为正式 cohort。完整 canonical/data-pipeline 处理脚本后续写在本仓库 `data-pipeline/` 并部署到 5090 执行，不塞入每日归档脚本。

本轮成功标准：四份文档同slug对齐；事实、推断和建议分开；自有动作严格BUY-only且没有deferred/影子SELL路径，外部bid/ask仍是完整行情；训练框架、数据额外需求、simulator、reward、split和`mistake-log`闭环在训练实现前冻结；Binance aggTrade v3本地门通过后从干净commit构建并重新部署，真实finalized segment证明`E/T`双列；D+1脚本仅处理昨天并通过Gamma/partial/云端同名包合同测试；没有把未来cohort写成已完成事实。

## 2. 为什么按这个顺序执行

```text
需求/训练合同
    ↓
schema v3 readiness + 非训练准备纵切
    ↓
按实际质量选择并完成目标 cohort + label/account tail
    ↓
由用户冻结最终边界并生成 dataset manifest/eligibility/split
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

1. 先冻结训练合同并用 7月8-9日新样本做非训练准备纵切，避免数据采完才发现没有 transport identity、p2b/finalPrice、T-30、成熟 outcome label 或所需 action semantics。
2. 采集前只处理不可事后可靠恢复的字段；label maturity 可异步回填，不把所有未来工作挤进 collector。
3. cohort 完成后先冻结 boundary/split，防止看到收益后换日期、换 market 或调整 holdout。
4. 先做纵向切片，能用少量 market 暴露 event order、账本、reward 和 simulator 错误，避免全量扫描后返工。
5. simulator/ledger 闭合后再训练策略，防止高 AUC 或漂亮 PnL 建立在错误执行现实上。
6. 先强基线再主模型，防止用复杂网络掩盖数据泄漏或标签无效。
7. action-surface、consumer 和 sensitivity 先于 leaderboard，直接阻断旧 MPV2 的“输出存在但没有控制订单”问题。

## 3. 阶段总览

| 阶段 | 目标 | 当前状态 | 进入条件 | 退出证据 |
|---|---|---|---|---|
| 0 | 基础文档与框架冻结 | 修订中 | 用户更新采集与日期要求 | 四文档记录日期 deferred、两套 P2B、最小 Gamma 回填与 schema v3 授权 |
| 1 | schema v3 可训练性准备与非训练纵切 | 部分已授权/执行中 | 用户批准 T 字段、归档修复与重新部署 | T typed schema、两字段归档、release/deploy 与新段 receipt |
| 2 | 候选数据持续采集与日级保护 | 待阶段 1 部署后继续 | 阶段 1 部署验证通过 | 每日 finalized raw/manifest/hash/health evidence |
| 3 | cohort boundary、maturity 与 split 冻结 | 未开始 | cohort + tail 完成 | DatasetBoundary/EpisodeEligibility/Split manifests |
| 4 | 端到端纵向切片 | 未开始 | 阶段 3 boundary 冻结 | canonical→simulator→NOE→ledger→reward closure |
| 5 | bounded 全量 canonical pipeline | 未开始 | 阶段 4 PASS | 全量 features/labels/trajectories 与资源报告 |
| 6 | simulator capability/calibration 与 rollout预算冻结 | 未开始 | 阶段 4 PASS、阶段 5必要输入可用 | capability/calibration/impact/valuation/budget manifests |
| 7 | baselines、预注册序列候选与 action ranker | 未开始 | 阶段 5/6 PASS | trial ledger、action-surface、模型比较 |
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

### 任务 0.2：撤销旧方案 B 并延后日期冻结

- 步骤 0.2.1：记录用户撤销固定方案 B，改为 schema v3 重新部署并观察实际数据后另行选择正式日期。
- 步骤 0.2.2：清除旧日期、14 日和 4032 个 market 的默认配置权威；保留历史记录但不得回流。
  验证：所有文档均为 `FinalDatasetBoundaryStatus=DEFERRED_TO_OBSERVED_COLLECTION`，正式 manifest/split 在用户再次选择前不能生成。

### 任务 0.3：冻结技术方向

- 步骤 0.3.1：冻结 `ESAR-v1` 共享 Event-State Encoder、联合候选 scorer、XGBoost/MLP 基线和生产验收合同；不预定某个序列骨干或容量为主模型。
  验证：框架文档解释数据前提、模型/动作/损失、推理状态和 rejected alternatives，且不存在参数量、层数或 hidden size 硬上限。
- 步骤0.3.2：冻结严格事件触发、p2b、T-30/state reconstruction、MarketContextEpisode/AccountRunTrajectory、BUY-only joint candidate、CapitalBudget、budgeted simulator labels、valuation/ledger reward和time-forward split。
  验证：BUY是系统常量，candidate只表达UP/DOWN单腿或双腿BUY及BUY订单管理；不存在自有SELL deferred路径、固定decision tick、全事件全后缀rollout、直接BC/IQL/DT、跨split账户状态或independent unused action heads。
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

阶段 0 退出条件：四文档同步记录 schema v3、最小两字段回填和日期 deferred。退出阶段 0 只授权本轮明确列出的 schema/归档/重新部署，不自动授权训练或其他阶段实现。

## 阶段 1：cohort 开始前可训练性准备

> 本阶段中的 Binance aggTrade `trade_time=T`、D+1 归档修复和重新部署已获授权；其他 identity/simulator/data-pipeline 实现仍需按计划后续执行。正式 cohort 起点不预设为部署时刻。

### 任务 1.1：拆分开始 readiness 与最终边界

- 步骤 1.1.1：冻结 `CollectionStartReadinessGate`：Binance aggTrade v3 必须保存 T、本地测试通过、远端新 release 生效并产生可读 v3 finalized segment。
  验证：记录 commit、binary sha256、切换时间、新 segment schema/row count/hash；只形成候选数据版本起点。
- 步骤 1.1.2：保持 `FinalDatasetBoundaryGate=DEFERRED`。
  验证：未出现固定 end、14 日、4032 或预选 calibration/holdout 日期。
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

- 步骤 1.3.1：D+1 UTC 02:00 对昨天 D 的目标 slug 调用经用户批准的官方 Gamma JSON 路径，将 `priceToBeat` 与 `finalPrice` 无精度损失地保存为十进制字符串。
  验证：只有这两个字段是否存在决定该市场回填成功；closed/resolved/winner 等诊断信息不阻断字段落盘或整日流程。
- 步骤 1.3.2：不增加 D+2 或网页 internal endpoint。TLS handshake timeout/timeout/connection reset/`URLError` 使用最多 3 次、单次等待不超过 5 秒的传输重试；HTTP 响应已到达但缺字段时不做语义重试，只写 `fetch_succeeded=false` 与 `diagnostic_error=p2b_final_missing`。P2B schema 使用 `price-to-beat-gamma-v2`，每 row 写 `gamma_http_attempt_count`。已有完整 D 文件时只重请其中的瞬时传输失败 rows；现有文件不匹配 D 合同时明确失败。
  验证：故意断开首次连接时同 slug 第二次成功；只缺 `finalPrice` 的 slug 仍只有一次 HTTP 请求；已有 v1 D 文件中只重请瞬时失败 slug，迁移后 288 rows 全部为 v2、attempt count 非空且其他价格/状态不变；把 max-attempt override 设为 100 仍只发起 3 attempts。
- 步骤 1.3.3：冻结 cold/hot 分界。cold 日文件用于训练参考；future paper/live handoff v2 必须传两个 nullable decimal-string 字段并按 runtime receive time 更新，当前 heartbeat-only 实现明确为未完成。
  验证：缺 closed/resolved/winner 仍能传字段；cold fetch time 不冒充 hot receive time。

### 任务 1.4：冻结时间、逐源准入与 source semantics

- 步骤 1.4.1：将 `timestamp_received` 明确为 SDK typed event 交付后的 collector 本机采样时间。
  验证：文档、schema metadata 和 causal builder 不称其为 wire ingress/纯 network latency。
- 步骤 1.4.2：部署连续 clock/NTP receipt 与异常/cutover记录。
  验证：故意注入 clock status unknown 时，episode capability 降级而不是静默 PASS。
- 步骤 1.4.3：新增 Binance aggTrade `trade_time=T` 必填 typed 列并升级为 `binance_aggtrade_v3`，立即重新部署采集。
  验证：E/T 分列写入真实 parquet；旧 v2 不被伪装成 v3；新 release 产生可读非空 v3 finalized segment。
- 步骤 1.4.4：在目标outcome不可见时预注册并hash逐源准入式和阈值：PM双token/initial state、depth continuity、aggTrade ID gap、RTDS freshness、clock/reconnect/cutover、capability downgrade。Gamma 两字段回填只按任务 1.3 判定，不得被这些逐源诊断阈值阻断。
  验证：7月8-9日只用于运行正反例，不能根据目标cohort收益重调阈值。

### 任务 1.5：保护 canonical raw 与日级拉取

- 步骤 1.5.1：确认本地日归档在 D+1 只拉昨天 D 的完整 `typed_raw/utc_day=...` 与 segment manifests，不拉仍在采集的当天分区，也不只保留压缩 archive 中的 `validated_markets`。
  验证：远端 50GB retention 删除前，本地 finalized raw、manifest、sha256 和 row counts 已落盘并校验。
- 步骤 1.5.2：冻结 remote retention、local free-space 和 archive failure alert。
  验证：日拉取失败时阻断远端对应 day 的安全删除判断；不得假定 tar.gz 包含 canonical raw。
- 步骤 1.5.3：按最终待选日期范围和双版本 staging 计算磁盘水位。
  验证：保留 raw、derived、failed staging、simulator labels 和恢复余量，不因空间不足提前删证据。
- 步骤 1.5.4：归档发现历史 `running/digesting/partial` 时绝不读取该文件，但不因孤立历史 partial 整日退出；将其写入报告并由现有逐 market gap 规则决定受影响市场。上传同名昨天包时验证新内容而非只验证旧文件名存在。
  验证：已知 T08 四字节事故文件不进入 parquet reader且不阻断其他健康市场；云端旧同名包不能造成假成功。
- 步骤 1.5.5：完整 canonical/feature/label/trajectory 处理只在本仓库 `data-pipeline/` 实现并部署到 5090 的 `~/btc-updown-marketmaker/data-pipeline` 执行。
  验证：日归档脚本只负责拉取、两字段文件、基础市场筛选、打包和上传，不膨胀为训练 pipeline。

### 任务 1.6：pre-cohort 非训练合同纵向切片

- 步骤 1.6.1：只使用明确允许作准备验证的7月8-9日新样本，不访问目标outcome做选模，打通numeric part/order、exact/approx atomicity、T-30与snapshot→checkpoint重建。
  验证：row reorder、gap、missing snapshot、multi-child和multi-market负例fail closed。
- 步骤 1.6.2：用最小 stub 分别验证 cold `p2b_final_missing→Gamma backfilled` 和 future hot `null→runtime received`的 availability 语义，再验证完整 candidate→simulator raw evidence→NOE→AccountRunTrajectory ledger→reward/valuation closure 与 output consumer。
  验证：cold fetch time 不冒充 hot receive time，缺 closed/resolved/winner 不影响两字段状态转换；这不是训练、leaderboard 或目标 cohort 验收。
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
- 步骤 2.2.4：每日 D+1 文件记录 Gamma `priceToBeat/finalPrice` 两字段是否取得；RTDS 继续保留为独立实时价格证据，不作为这两个字段回填的额外阻断门。
  验证：报告区分两字段 present/missing；不加入 closed/resolved/winner gate。

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

- 步骤 3.3.1：从已批准的 D+1 Gamma 文件读取 `polymarket_price_to_beat` 与 `polymarket_final_price`，按 `polymarket_label_up = polymarket_final_price >= polymarket_price_to_beat` 生成 Polymarket outcome label，并记录 source、fetch/maturity time 和 artifact hash。
  验证：只以这两个字段存在及可解析判定该 label 成熟；SDK settlement/winner、RTDS boundary 或公式交叉检查如果后续保留只是独立诊断，不是 Gamma 两字段或 label 的准入门。
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
- 步骤3.4.5：为development、dedicated calibration和holdout分别冻结独立cash-only account fixture：只有固定starting pUSD，positions/open/pending/reservation/resolved-claim均为零；split内按时间carry，split末尾延伸tail直到订单/结算闭合。
  验证：全部持仓pooled cost只来自本split confirmed fills；train policy产生的cash/position/order state不进入calibration/holdout；account-run总额是经济closure权威。

### 任务 3.5：阶段审查

- 审查 boundary、eligibility、maturity、split、purge/embargo 和 old-data denylist。
- 退出：manifests hash 冻结；任何后续 boundary 修改都会使下游 artifact 失效并回到本阶段。

## 阶段 4：正式 cohort 端到端纵向切片

### 任务 4.1：选择不看收益的代表性切片

- 步骤 4.1.1：按预注册条件选择低/中/高事件密度、可见流动性、source reconnect和边界coverage等market regime，再由simulator注入full/partial-fill stress scenario。
  验证：不把外部行情误称为本账户fill证据；选择依据不含模型PnL或outcome方向。

### 任务 4.2：打通 causal state

- 步骤 4.2.1：从 T-30 replay 至 terminal，验证多 market overlap、transport atomicity 和 persistent encoder state/cache reset。
  验证：p2b pending/maturity、state reconstruction、source timestamp future-shift、row reorder、child duplicate 和 reconnect 负例均被捕获。
- 步骤 4.2.2：构造最小 ESAR single-step stub 与 candidate contract，不训练正式模型。
  验证：每个有效模型可见event触发一次，prefetched只warmup；single-writer/state-version/stale intent/overload和physical chunk语义明确。

### 任务 4.3：打通 simulator、NOE、ledger、reward

- 步骤4.3.1：实现最小BUY-only平台lifecycle，覆盖`hold_existing_buys`、BUY UP、BUY DOWN、双腿BUY batch、accepted/rejected/unknown、partial/full fill、BUY cancel/cancel-all、resolution和模型外`platform_settlement_credit_observed`事件。
  验证：batch逐项mixed result与异步单腿不被伪造成原子成交；cancel NOE触发新评估且不自动续提，因此没有replace candidate；本地ledger/平台回报/链上状态延迟只形成confirmed/provisional/unknown并限制相关BUY submit，不暂停整个模型或BUY cancel；property tests覆盖pUSD free/reserved、UP/DOWN仓位、订单remaining、capital usage和settlement守恒；simulator enabled capability不能表达自有SELL、replace、split、merge或redeem命令。
- 步骤 4.3.2：simulator raw evidence 进入共享 NOE/ledger。
  验证：禁止 private-state-to-reward shortcut。
- 步骤4.3.3：计算reward vector与ledger closure。
  验证：ConservativeAccountNavValuation覆盖free/reserved/provisional pUSD、BUY pending/unknown、UP/DOWN unsettled、stale/one-sided只读mark、resolved-but-uncredited、`platform_settlement_credit_observed`、非负`unmatched_settlement_credit_suspense`与资金重新可用；断言`NAV=ledger_pUSD_total+valued_assets-suspense-other_liabilities`，suspense只扣一次、不减少平台confirmed free cash、不进入external flow或capital-at-work。SettlementLedgerTransition持久保存terminal outcome，使loser归零、winner转claim、resolution后confirmed的fill直接进入loss/claim，平台credit以`flow_class=internal_settlement`原子消费claim并增加pUSD且`external_cash_flow=0`。credit先到时以cash加等额suspense保留平台事实和归因缺口；claim后到时清除claim/suspense且不重复加cash。confirm→resolution→credit、resolution→confirm→credit、resolution→credit→delayed-confirm、partial/aggregate/duplicate credit均幂等收敛；非零tail使受影响`G_candidate/per-market/G_account/account_run_done`保持censored或显式residual bridge、capital release time保存bounds，但不全局暂停；中间mark不冒充主动清算或可用现金；`paired_inventory_confirmed`只累计confirmed baseline-relative增量并保留结算前审计累计量；`G_candidate`与`G_account`分名且各自终点一致，成熟AccountRunTrajectory sum event ΔNAV = `G_account`，三类done exactly once。
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

- 步骤 5.2.1：按 market 构造 T-30→terminal unroll，physical chunks carry候选所需的persistent encoder state/cache；detach只适用于recurrent candidate。
  验证：冻结candidate-specific chunk/context、burn-in、loss window和cache重建合同；recurrent candidate的随机chunk由前序no-grad burn-in重建hidden，并报告不同chunk size前向输出、full-unroll/no-detach、早期event干预和remove-hidden/multiscale ablation；其他candidate报告等价的context/cache完整性测试。
- 步骤 5.2.2：保存 event-state fixture 供 PyTorch/ONNX/Rust 对账。
  验证：训练与runtime不是两套 feature/normalizer。

### 任务 5.3：阶段审查

- 扫描 old-data access、split leakage、full-load、row-order假设和 nondeterminism。
- 退出：全量 canonical artifact 与 resource report hash 冻结。

## 阶段 6：simulator capability/calibration 与 rollout 预算冻结

### 任务 6.1：冻结 SimulatorCalibrationManifest

- 步骤6.1.1：先冻结SimulatorCapabilityMatrix，只启用candidate实际需要的BUY order/lifecycle/accounting路径；逐规则/参数记录official_rule、observed_new_data、assumed/stress provenance。
  验证：UP/DOWN BUY、最多两项batch逐项结果、pUSD reservation、BUY cancel/cancel-all、cancel后新评估和`platform_settlement_credit_observed`完整；自有SELL、replace及split/merge/redeem命令不作为disabled/deferred capability存在；聚合L2 queue proxy不显示为observed FIFO；未建设第二套通用exchange/SDK/ledger。
- 步骤 6.1.2：当前官方 order types、post-only、fee、rebate/reward eligibility 和 settlement 规则复核。
  验证：动态 market metadata 优先，禁止旧固定 fee/250ms/TTL/partial概率常数。
- 步骤 6.1.3：建立 conservative/base/high-latency/tail/adverse-fill scenarios。
  验证：加入resolved-but-uncredited、`platform_settlement_credit_observed` delay和tail内始终未回款场景；用户指定的约十分钟平台自动兑回在取得当前账户级分布前只能标记assumed/stress，单一乐观scenario不能标记promotable或伪造pUSD复用。
- 步骤6.1.4：冻结CapitalBudgetContract、CounterfactualMarketImpactContract和ConservativeAccountNavValuationContract。
  验证：account/单market最大投入、最低free-cash、第二腿headroom、batch reservation、small-agent假设、size/visible-depth/volume上限、own-order overlay、impact stress和stale/one-sided/pending估值正反例通过；order reservation、provisional commitment、unsettled cost basis互斥，second-leg headroom不进ledger/NAV，capital-time不与NAV haircut双扣。

### 任务 6.2：counterfactual pilot 与预算冻结

- 步骤 6.2.1：仅在阶段4正式纵切market上做counterfactual pilot；冻结CounterfactualRolloutBudgetContract：按event type/market phase/state novelty/account-order状态抽样与收益无关的label states，记录inclusion probability/weight、每market groups、每group candidates、seeds/scenarios、总branch/wall-time/disk，以及每branch max-events/duration/market-transitions/pending-tail上限。
  验证：runtime仍逐有效事件评估；没有固定tick抽样或全事件全后缀组合爆炸。
- 步骤6.2.2：按candidate contract枚举完整合法BUY-only Model Intents，上限32 candidates；submit candidate含1..2笔BUY、每outcome一个价格档、batch最多两项；cancel与submit不混在同一candidate。
  验证：candidate schema无order-side、replace或risk-intent维度，BUY由SDK adapter固定为`Side::Buy`；`hold_existing_buys`唯一表达保留订单且不操作；模型不能表达pause/cancel-only，系统PermissionVerdict仍独立生效；超限配置fail closed；enumerator只做合法性/现金硬风险，不按score预筛或生成策略。
- 步骤6.2.3：在pilot同state/path/seed运行candidate与common-baseline branch，使用group共享causal cutoff，直到全部branch-differential订单、reservation、仓位、cash和continuation下游状态闭合，或达到group hard limit后censor；保存continuation_policy_hash/iteration。只有完整policy evaluation运行到AccountRunTrajectory terminal。
  验证：ranker label使用同cutoff的`G_candidate`；完整policy使用`G_account`。重叠market差异、branch-differential settlement suspense未闭合、所需`platform_settlement_credit_observed`未出现或其他超限状态必须censor并保存residual bounds；涉及censored branch的pairwise label排除或按预注册bounds处理，不同candidate不得使用不同horizon，也不得把owning-market结束冒充effect terminal。losing-only branch在resolution归零后无需等待credit。
- 步骤 6.2.4：运行 simulator sensitivity、market-impact和 exploit probes。
  验证：只在一个参数点盈利、异常 churn、无风险套利或 private-state leakage 均阻断。

### 任务 6.3：阶段审查

- 审查lifecycle closure与economic closure分离、batch逐项unknown outcome、异步单腿、cancel race、late fill、fee double count、capital lock、resolved-but-uncredited和`platform_settlement_credit_observed`；property tests覆盖`0.98→1.00→+0.02`、losing-only、winning-only、完整pair和partial pair。
- 退出：仅 `assumed/stress` execution参数时，artifact 明确 `non-promotable`。

## 阶段 7：baselines、预注册序列候选与 action ranking

### 任务 7.1：先跑最低基线

- 步骤7.1.1：常数、逻辑回归、小MLP、naive directional BUY和简单paired-BUY/hold-to-settlement规则。
  验证：同 split、同 costs、同 simulator scenarios。

### 任务 7.2：监督 representation

- 步骤 7.2.1：训练经用户批准并预注册的 stateful encoder candidates 及其 outcome/dynamics/calibration heads。
  验证：anchored folds、Brier/ECE/quantile coverage、seed variance，AUC不单独晋级。
- 步骤 7.2.2：训练 causal TCN challenger。
  验证：使用相同因果输入、动作、simulator、经济评估和生产吞吐/延迟门，另报参数量、wall-clock receptive field、streaming cache和峰值内存；不以参数量相等代替公平性，也不只看一个validation分数。

### 任务 7.3：生成预算化 counterfactual groups 与 XGBoost ranker

- 步骤 7.3.1：representation和simulator合同均冻结后，按CounterfactualRolloutBudgetContract生成全量训练用grouped candidate dataset。
  验证：state抽样与收益无关、inclusion weight正确、continuation hash分层、总branch/wall-time/disk未超预算。
- 步骤 7.3.2：先用同一groups训练XGBoost grouped action ranker基线。
  验证：query group完整，candidate rows不跨split或随机拆分。

### 任务 7.4：joint candidate scorer

- 步骤 7.4.1：训练 grouped ranking + quantile + masked execution losses。
  验证：所有loss maturity/抽样weight正确；tie基于paired ΔJ interval完全落入equivalence band，不把不确定当等价。
- 步骤 7.4.2：action-surface audit。
  验证：ModelOutputConsumerMatrix、raw/decoded/consumer、逐维intervention、control separation、support和entropy全部报告；UP-heavy与DOWN-heavy镜像confirmed暴露的inventory penalty相同；不存在selected-risk-intent第二权威，training-only head不伪装runtime能力。
- 步骤 7.4.3：runtime parity。
  验证：PyTorch/ONNX/Rust persistent encoder state/cache carry、score、selected intent对账；single-writer、global ingress ID/`relevant_pending_watermark[market_id]`与account fan-out、state-version/causal-cutoff、queued relevant-event stale拒绝与重评、unrelated prefetched event不误stale、`hold_existing_buys`和burst overload通过。

### 任务 7.5：阶段审查

- 先审action surface，再审PnL；全`hold_existing_buys`须判断是否标签支持，所有candidate相同或输出未消费则直接FAIL。
- 所有 run，包括失败 seed/超参，写入 trial ledger。

## 阶段 8：trajectory aggregation 与 frozen holdout

### 任务 8.1：simulator dataset aggregation

- 步骤8.1.1：使用`hold_existing_buys`、规则、分层随机和当前policy收集状态。
  验证：只在更小预注册subset内、遵守rollout budget；inventory/open-order/cancel states coverage增加，不被单一teacher垄断。
- 步骤 8.1.2：每轮冻结 policy/dataset/simulator/continuation-policy hash。
  验证：能够回放任一ranking group；不同continuation labels分层/条件化，不能隐式在线漂移或混为同一真值。

### 任务 8.2：预注册 final evaluation

- 步骤8.2.1：冻结primary metric、valuation、risk weights、expected-shortfall alpha、capital-usage晋级规则、paired-equivalence band、baseline、seeds和scenario set。
  验证：holdout结果不可见时完成并签 hash。
- 步骤 8.2.2：冻结 runtime budget 与 promotion capability状态。
  验证：只有 assumed simulator参数的候选预先标记 non-promotable。

### 任务 8.3：只运行一次 frozen holdout

- 步骤 8.3.1：在最后两个 calendar UTC 日运行。
  验证：不换日期、不删难看 market、不调 threshold。
- 步骤8.3.2：报告calibration、settled net EV、downside/expected-shortfall、drawdown、actual paired all-in edge、unpaired exposure-time、gross inventory、capital-usage time、time-to-pair、cash exhaustion、settlement closure、action support、regime、scenario sensitivity、CPU/memory/event-to-intent。
  验证：逐日报告、contiguous-market block与paired-market条件性interval；明确只有两个day clusters，禁止宣称稳健day-level CI，不把event rows当IID。
- 步骤 8.3.3：生成 bundle 与 `training/evaluation_candidate` 状态。
  验证：bundle存在、exit 0、单一AUC/PnL均不是promotion证据。

### 任务 8.4：阶段审查与停止点

- 独立审查 dataset/split/simulator/reward/model/runtime完整链路。
- 修复会改变 frozen contract 的问题时，holdout验收作废并回到相应阶段；已观察的两日只能降为diagnostic，不能再次称为pristine holdout。重新正式验收必须取得新的未来未读holdout和用户批准，不得偷偷重跑并只保留最好结果。
- 本计划在 candidate bundle 处停止；paper/shadow/live 必须另立需求和授权。

## 阶段 9：后续 paper/shadow 决策门

本阶段仅声明未来门，不属于当前计划的已授权交付：

- 先决定是否需要收集真实execution feedback以校准BUY fill/cancel、batch逐项结果、pUSD admission和`platform_settlement_credit_observed` delay；
- simulator-only、assumed参数候选不得直接 promotion；
- paper/observe-only shadow必须有新的demand/plan/changelog；
- live 仍需单独资金、安全、SDK、部署和回滚批准；
- live binary 在本地构建，模型从本地已验证 artifact 直接部署到 `/home/ubuntu/live-trade`，不经 GitHub，不在 live 主机编译。

## 全局阻断条件

任一条件成立即停止对应阶段：

1. 在用户重新选择前任一正式 cohort/split 写入固定日期、14 日、4032 或旧方案 B 默认值；
2. 目标数据实际 schema、collector/runtime行为与已批准合同冲突；
3. 需要官方 Rust SDK 不支持的能力且尚未获得用户批准替代方案；
4. canonical raw 在远端 retention 前未安全拉回或 hash 不一致；
5. transport identity 不存在且用户未批准 approximate microbatch；
6. 缺失训练所需的 `priceToBeat/finalPrice` 或 label maturity；Gamma 回填不得因 closed/resolved/winner 或公式交叉验证被额外阻断；
7. old-data/旧 bundle 被访问；
8. fixed tick、random-row split、holdout调参、full-load或physical-row-order假设出现；
9. simulator/NOE/AccountRunTrajectory ledger/reward/valuation不闭合；
10. action输出无consumer或被runtime改写；
11. candidate、ONNX、Rust consumer或simulator enabled capability能够表达自有SELL，或保留自有SELL的deferred/research/feature-flag路径；外部book ask/trade side不属于本条禁止范围；
12. 模型candidate能够表达replace、pause、cancel-only或risk-intent第二权威，或官方SDK order Side没有在adapter固定为Buy；
13. counterfactual没有预算/抽样权重、continuation hash或超出small-agent impact能力；
14. simulator只有assumed参数或SDK显式p2b确认缺口未解决却要宣称promotion/live能力；
15. 用户当前授权只到文档准备，却要开始后续实现/训练。

## 文档与阶段同步规则

1. 每阶段开始前复核需求验收条目和核心训练框架对应合同。
2. 每任务完成后自审 diff、证据、边界和剩余风险。
3. 每阶段完成后派独立审查；Critical/High 未清零不得宣布完成。
4. 现实与已批准文档冲突时先写 changelog 的事实、影响和选项，向用户请求决定，不擅自改语义。
5. 每阶段改动 demand/plan/framework/changelog 任一项时，同事项文档同步；技术选择变化必须记录为何原结论失效。
6. 阶段 commit 只包含该阶段明确范围，不卷入用户现有脏改动；模型和数据 artifact 不进入 GitHub。
