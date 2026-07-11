# online-feedback 线上问题与平台潜规则汇总实施计划

> **给后续代理工作者：** 必须使用中文执行；复杂或长时间执行时按本文件的“阶段 -> 任务 -> 步骤”推进，并在每个阶段完成后做独立审查。本文是计划与首轮交叉验证结论，不授权实盘、paper runtime、训练、部署或资金操作。

**目标：** 建立一个持续更新的 online-feedback 复盘体系，系统收集 Polymarket BTC Up/Down 交易机器人在真实线上、paper 和平台交互中遇到的常见问题、平台明规则、潜在规则、影子规则、社区 issue、changelog 变化、相似机器人实现经验，并转化为可验证的开发门禁。

**架构：** 以 `sdk-live-trade` 的 SDK-first 真实订单经验、`V2` 的训练/paper/runtime 经验为本地证据底座；外部资料分为官方文档/changelog、GitHub issue/PR、量化交易/开发者社区、公开相似 bot 代码四类。每个关键规则都必须标明证据等级、影响路径、验证方式和工程对策。

**技术栈：** Polymarket CLOB / Gamma / WebSocket / Data API、官方 SDK 或本项目 SDK-first Rust 路径、V2 maker policy / paper artifacts、JSONL/TSV evidence ledger、artifact replay、只读外部调研。

---

## 状态与边界

- 生成日期：2026-06-28（Asia/Tokyo）。
- 产物目录：`/home/chillchill/btc-updown-marketmaker/mistake-log/online-feedback/`。
- 本次已创建：
  - `2026-06-28-online-feedback线上问题与平台潜规则汇总计划.md`
  - `外部资料交叉验证清单.tsv`
  - `相似机器人实现观察清单.tsv`
- 本次没有执行 SSH、训练、replay、paper-runtime、dashboard、部署、下单、撤单、资金查询或数据删除。
- 当前 `btc-updown-marketmaker` 目录不是 git 仓库，因此本次以文件级校验交付，不创建 commit。

## 成功标准

1. `mistake-log/online-feedback/` 存在，且计划文件、外部资料清单、相似机器人观察清单都已落盘。
2. 至少覆盖七类输入：官方文档/changelog、GitHub issue/PR、公开相似机器人实现、本地 `sdk-live-trade` 与 `V2` 复盘、模型推理/训练资料、HFT/做市研究、Linux/systemd 长时间运行资料。
3. 每个高优先级平台规则都标注证据等级：`官方明规则`、`官方变更规则`、`issue/社区强信号`、`本地 artifact 证据`、`公开 bot 实现习惯`、`待验证推断`。
4. 每个关键点都给出工程对策：runtime gate、adapter 合同、artifact 字段、测试或 replay 校验方式。
5. 不把公开 bot 的收益宣传当成事实；只提取其实现模式、风险控制和缺口。

## 首轮交叉验证结论

### 总结判断

我们后续的 online-feedback 体系不应只是“错误日志”。它应该成为一个交易前置规则库：凡是会影响下单、撤单、成交确认、余额成熟、费用/返佣、结算、训练标签、paper/live 迁移和模型 action 的线上规则，都必须先进入这里，再反向约束 SDK adapter、paper replay、V2 训练和 live gate。

首轮外部检索与本地复盘交叉后，优先级最高的结论是：

1. **平台状态机比单个 API response 更重要。** matching-engine restart、timeout 后实际成交、post response blank id、late user-channel、CLOB/orderbook snapshot、Data API positions 都可能互相延迟或冲突。对策是统一 NormalizedOrderEvent 和 artifact-backed reconciliation。
2. **5 分钟 crypto 市场不能沿用旧“无费用/低延迟”假设。** 官方 changelog 已多次调整 5m crypto 的 taker fee、maker rebate、taker delay、rate limits 和 batch cancel 限制。对策是每轮读取当前 market info/fee/reward 字段，不用固定常数。
3. **SELL 可卖余额成熟是核心影子规则。** 本地 `sdk-live-trade` 与 GitHub issue 均显示，用户通道或本地账本认为已成交，不等于平台 sellable balance 立即可用。对策是 SELL reject 不能直接覆盖本地 ledger，也不能跨 tick suppression，必须做成熟度分布和 warning/audit-only 分层。
4. **公开 bot 大多只解决一部分问题。** 做市型 bot 依赖 post-only、库存偏斜、取消重挂、合并头寸、风险冷却；5m Up/Down 尾盘型 bot 依赖时间窗、价格带、外部 BTC price feed、Gamma/CLOB 轮询、赎回。很多公开实现只是模拟或缺少真实 fill/queue/settlement 对账，不能直接作为我们的 live path。
5. **V2 训练必须把 online feedback 前置。** 如果 fill、queue、fee/rebate、settlement、order lifecycle 和 paper artifact 不能闭合，模型训练的 selected EV、replay PnL、ranking label 和 reward 都会学到错误规则。
6. **第二轮检索确认：我们的问题不是单点 API 问题，而是“模型训练 + HFT 执行 + 混合链上/链下平台 + 长周期运维”的交叉系统问题。** 因此后续方案必须同时覆盖模型预加载、目标市场发现、市场切换、线程/内存/磁盘治理、供应链安全和回测过拟合治理。

### 关键规则与对策矩阵

| 编号 | 规则/问题 | 证据等级 | 影响路径 | 工程对策 |
| --- | --- | --- | --- | --- |
| OF-01 | matching engine restart 期间下单/撤单可能失败，平台维护者在 issue 中明确说这是预期行为，不是普通 bug。 | 官方文档 + GitHub issue | live submit、cancel、retry、fail-close | 加 `platform_service_state` 事件；MARKET_NOT_READY/Service not ready 只允许有限 retry；retry 前后必须查 order/user-channel，避免重复落单。 |
| OF-02 | request timeout 不等于订单未成交，issue 报告过 5s timeout 后多次重试均实际成交。 | GitHub issue 强信号 | FOK/FAK、retry、重复下单、ledger | timeout 后进入 `UnknownSubmitOutcome`，先查 order/trades/user-channel，再决定补单；禁止盲目 retry。 |
| OF-03 | POST /orders transport 成功、item success、accepted 是三层不同语义；blank orderID/UNKNOWN/error text 不能进入 accepted path。 | 本地 artifact + 官方 API 语义 | SDK adapter、ledger reservation、risk gate | accepted classifier 必须同时要求非空 platform order id、无拒单错误、状态可解释、scope 对应。 |
| OF-04 | SELL balance/allowance reject 可能是平台可卖余额成熟或服务端缓存问题，不等于链上余额或本地 ledger 错。 | 本地 artifact + GitHub issue | SELL、inventory、next tick account_state | SELL reject 作为 order-local warning/audit-only；记录 maturity probe；不覆盖 local ledger，不跨 tick suppression。 |
| OF-05 | CLOB balance/allowance、proxy wallet、safe/deposit wallet、signature type 容易出现“链上正确但 CLOB 缓存或参数不正确”。 | 官方账户模型 + GitHub issue | wallet bootstrap、API key、balance gate | 启动前分 EOA/proxy/safe/deposit wallet 做 source map；同时验证 chain allowance、CLOB balance-allowance、SDK signature_type/funder。 |
| OF-06 | tick size 可能变更或 SDK local cache 过期；0.999 等价格可能被 SDK/client-side validator 拒绝。 | 官方 WebSocket event + GitHub issue | quote rounding、action decoding、submit reject | 订阅/处理 `tick_size_change`；submit 前刷新 market metadata；所有模型输出先过 tick/min/max quantizer。 |
| OF-07 | 官方 changelog 显示 CLOB V2 迁移、订单 hash/order version/pUSD 等变更会让旧 SDK 或旧签名逻辑失效。 | 官方 changelog + GitHub issue | SDK version、order signing、live deploy | live 前固定 SDK version smoke；遇到 `order_version_mismatch` 直接 fail closed 并升级 SDK/签名路径，不绕 raw REST。 |
| OF-08 | CLOB V2 迁移曾取消所有开放订单；类似平台变更期间 open orders 不能只依赖本地状态。 | 官方 changelog | startup recovery、run resume、stale order | 每次启动执行 open-order inventory audit；平台 changelog 重大变更后强制 cancel-all/reconcile gate。 |
| OF-09 | 5m crypto 官方引入/调整 taker fees、maker rebates、taker delay；不能使用旧 fee/rebate 常数。 | 官方 changelog + fees/rewards 文档 | EV、PnL、reward、ranking label | 每个 market 读取当前 fee/reward info；artifact 记录 `fee_rate_bps`、rebate 口径、source URL/version。 |
| OF-10 | 官方 order lifecycle 已披露 taker 有 250ms 延迟；这会影响尾盘抢单和 marketable order 的成交解释。 | 官方 order lifecycle | FOK/FAK、late-window strategy、slippage | simulator 和 live gate 纳入 taker delay；尾盘策略必须用更保守的 cut-off 与 latency budget。 |
| OF-11 | WebSocket 是必要实时输入，但可能 stale、断线、缺 snapshot 或只收到 top update。 | 官方 WSS + 公开 bot 实现 | orderbook、quote freshness、no-quote gate | market channel 需要 heartbeat、snapshot requirement、stale fallback REST、hash/age 记录；缺 snapshot 不允许交易。 |
| OF-12 | user-channel late event、synthetic fill、CLOB query 可能重复表达同一成交。 | 本地 artifact + 公开 bot reconciliation | ledger、PnL、validator | 同一 order/fill 用 alias/covered-by-synthetic 规则归一化；validator 只校验 normalized facts，不重新解释业务。 |
| OF-13 | rate limits 和 batch cancel 上限在 changelog 中变动，issue 中仍有人不清楚 fixed/rolling window。 | 官方 rate limits + GitHub issue | submit cadence、cancel-all、market scan | 按 endpoint 建 token bucket；DELETE /orders 分批；遇 429 使用 jitter/backoff 并记录 bucket 状态。 |
| OF-14 | liquidity rewards 不是“挂单就有”，通常受 min size、spread、scoring、heartbeat/order scoring status 影响。 | 官方 market-maker docs +公开 maker bot | maker reward、quote placement、training reward | reward path 单独建 `reward_eligibility_gate`；不把 rebate/reward 混入 adjusted PnL 主账。 |
| OF-15 | 5m Up/Down 相似 bot 常避开开盘/尾盘极端区间，或只做尾盘 favorite，不等于适合我们做市。 | 公开 bot 代码 | strategy design、training objective | 将做市、尾盘狙击、方向下注分成不同策略族；训练/评估不能混用标签和 PnL。 |
| OF-16 | 公开做市 bot 强调库存偏斜、反向头寸合并、post-only、风险冷却和 emergency flatten。 | 公开 bot 代码 +本地 live 经验 | rule maker、model action head | 路径 3 的规则 maker 必须有 inventory skew、merge/redeem、post-only guard、panic/cancel-all。 |

## 第二轮扩展检索与交叉结论

### 扩展范围

第二轮补充检索覆盖：

- Polymarket 混合架构：官方 Trading Overview、Order Lifecycle、Deposit Wallets、Gamma Markets、CLOB Market Info、CTF Overview/Split/Merge/Redeem、Resolution、WSS、Rate Limits、Changelog；第三方架构资料不能替代官方证据。
- 模型训练与交易：V2 训练复盘、online learning/feature store 公开实现、回测过拟合与 Deflated Sharpe/PBO 论文、训练标签与执行反馈闭环。
- 高频交易与做市：Avellaneda-Stoikov 库存风险框架、Queue-Reactive order book 模型、公开 bot 的 post-only/库存偏斜/市场切换/延迟指标。
- 模型预加载与推理：ONNX Runtime SessionOptions、线程池、graph optimization、memory arena、启动 warmup、hot swap 约束。
- 长时间运行硬件治理：Linux cgroup v2、PSI、systemd resource control、journald 空间限制、公开 bot 的 `--max-old-space-size=512`、PM2 `max_memory_restart`、systemd hardening。
- 目标市场发现与切换：Gamma slug/keyset/active/closed、CLOB market info/token id、5m/15m slug 时间窗推断、公开 bot 的 prefetch/switch/clear stale orderbook。
- 供应链安全：Polymarket bot 针对性恶意 npm/GitHub 仓库、私钥与 `.env` 窃取、第三方前端依赖攻击新闻。

### 新增关键规则与对策矩阵

| 编号 | 规则/问题 | 证据等级 | 影响路径 | 工程对策 |
| --- | --- | --- | --- | --- |
| OF-17 | Polymarket 是混合架构：链下 CLOB/撮合/API 与链上 CTF/Exchange/赎回各自有权威边界，任何单一接口都不能代表全局真相。 | 官方明规则 | order state、position state、reconciliation、训练标签 | 明确四层状态：`offchain_order_state`、`onchain_position_state`、`data_api_projection`、`local_ledger`；artifact 必须记录每层来源和时间戳。 |
| OF-18 | 目标市场发现不能只靠 slug 推断；slug 只能生成候选，最终必须由 Gamma/CLOB 的 active、closed、acceptingOrders、conditionId、clobTokenIds、tick/min size、fee/reward 字段确认。 | 官方文档 + 公开 bot 源码 | market scan、token id、action routing、no-quote gate | 建 `MarketDiscoveryGate`；没有完整 UP/DOWN token id、未来 expiry、可交易状态和 fee/tick/min_size 快照时禁止交易。 |
| OF-19 | 目标市场切换必须清理旧市场状态；序列特征、orderbook cache、未完成订单、库存和 no-quote warmup 不能跨 `market_id` 泄漏。 | 本地 artifact 证据 + 公开 bot 源码 | V2 seq buffer、maker quote、市场轮换 | 切换流程固定为：预发现下一市场、旧市场 cancel/audit、清空 per-market feature/orderbook buffer、新市场订阅 snapshot、满足 seq_len warmup 后再报价。 |
| OF-20 | 5m 市场应做 next-market prefetch，但 prefetch 只能准备数据和模型状态，不可提前把订单或 ledger 指向下一市场。 | 公开 bot 源码 + 本地复盘 | latency、市场切换、重复订单 | `prefetched_market_context` 与 `active_market_context` 分离；只有 switch gate 成功后才允许 submit path 使用新 token。 |
| OF-21 | 模型不能在第一个 tick 到来时才加载；模型 bundle、feature schema、normalizer、ONNX session、线程配置和 warmup inference 都属于启动前置门禁。 | ONNX 官方文档 + 本地 V2 经验 | inference latency、first-tick miss、schema drift | 启动时加载全部候选模型并校验 sha256、input/output name、seq_len、feature_order；创建 ORT session 后跑固定 warmup，记录 warmup latency 分布。 |
| OF-22 | 多模型 hot-swap 必须 side-by-side，不能原地替换正在使用的 session；新模型先 shadow 产出，验证 schema/latency/calibration 后再切换。 | 本地 V2 经验 + 公开 online-learning 实现 | live action、模型漂移、回滚 | 增加 `ModelRegistry`：active、shadow、retired 三态；切换前 no-quote 或 drain；每个模型单独 ledger、latency、resource budget。 |
| OF-23 | ORT 线程池默认可能使用所有物理核心，在线小服务器上会挤压 WebSocket、签名、网络和日志线程。 | ONNX 官方文档 + 本地服务器约束 | CPU、tail latency、长时间运行 | live 配置显式限制 intra/inter op thread；小模型默认顺序执行；禁止在 2 vCPU live host 上构建或跑长训练；线程调整必须通过 profile 证明。 |
| OF-24 | ORT thread spinning 能降低推理延迟但增加 CPU/power；长时间在线交易不能只看均值延迟，必须看空闲 CPU、p95/p99 和 WSS 心跳。 | ONNX 官方文档 | CPU、WSS stale、heartbeat miss | 在 live host 上默认保守 spin；如果启用 spinning，必须记录 CPU steal/load、event loop lag、feed age 和 inference p99。 |
| OF-25 | HFT 做市不能只训练“价格方向正确”；真实 maker EV 由 fill hazard proxy、adverse selection、费用/返佣、cancel/repost 成本共同决定。Polymarket WSS 只有聚合 book，不应声称可直接观测真实 FIFO 排队位置。 | HFT 论文 + 公开 bot 源码 | training objective、quote placement、maker policy | 训练标签拆成 direction、fill/no-fill、post-fill return、同价位 size-ahead 估计、queue_proxy/fill_hazard、net EV；不通过 execution reality gate 的样本不得进 reward label。 |
| OF-26 | Avellaneda-Stoikov 只能提供库存风险和报价距离框架，不能直接替代 Polymarket 5m 二元市场的 fee/rebate、尾盘跳变和结算风险。 | 论文 + 平台规则 | strategy family、risk model | 把它作为 baseline，不作为收益保证；参数必须由本市场 artifact 校准，并加入二元 pay-off、expiry、fee/rebate、position cap。 |
| OF-27 | Queue-Reactive 类模型提示我们 orderbook state 本身决定订单流强度；Polymarket WSS 无严格全局单调序列且只暴露聚合 book 时，只能构造 queue proxy/fill hazard，不能把它当作真实队列位置。 | 论文 + Polymarket WSS + polyfill-rs | orderbook replay、feature freshness | 记录 book timestamp、recv_ts、hash、snapshot_seen、depth_limit、stale_age、own_order_id、place/cancel/replace ts、same-price size-ahead estimate、后续 trade/depth delta、censoring reason；缺 snapshot 或 hash 断层时 no-quote。 |
| OF-28 | 回测过拟合是 V2 继续训练的核心风险；多批模型训练结果“不及预期”不应简单归咎于模型弱，可能是特征泄漏、选择偏差、purge/embargo 不足、样本被执行现实污染或未登记失败试验造成的幸存者偏差。 | PBO/DSR 论文 + 本地 V2 经验 | leaderboard、model selection、训练经验总结 | 固定 walk-forward、purged/embargo split、冻结最终 holdout；建立 `experiment_trial_ledger.jsonl` 登记所有模型/超参/特征/切分/费用假设/失败试验；报告 PBO/DSR/多重试验次数；paper/live 不允许当作超参搜索器。 |
| OF-29 | online feedback 进入训练前必须分层并保留因果上下文：平台错误样本、执行失败样本、可成交但负 EV 样本、no-quote/no-submit 机会样本、结算后 PnL 样本不能混成同一个 label。 | 本地 V2 + SDK 复盘 | supervised/ranking/reward label | 设计 `execution_feedback_labels.jsonl`：outcome、decision_id、paper_or_live、market_id、token_id、side/price/size、TTE、order_type、model_version、feature_schema_version、feature_hash、causal_cutoff_ts、book_hash_at_decision、label_maturity_ts、authority_source、settlement_source、censoring_reason 分开入库。 |
| OF-30 | 目标市场发现要同时支持确定性候选和全量 active scan；只用当前时间戳 slug 会漏掉平台创建延迟、时钟偏差、slug 变体和 closed 默认行为变化。 | 官方 Market Data + 公开 bot 源码 | market discovery、clock drift | 采用候选 slug + active events/keyset scan 双路径；校验 server time/NTP drift；记录 discovery miss rate 和 fallback 命中率。 |
| OF-31 | 长时间运行的内存增长不能简单等同泄漏；Linux cgroup v2 的 file cache、anon、memory.events、PSI 才是判断依据。 | Linux 官方文档 + 本地 V2 replay | memory、OOM、误清理 | 运行采样必须包含 memory.current/stat/events、pressure、anon/file/slab；file cache 增长且 oom/high=0 时只记录不干预。 |
| OF-32 | 磁盘比内存更容易成为隐形故障：JSONL、journal、artifact、临时文件、训练日志和 core dump 都可能让 live host 在无人值守时失效。 | systemd/journald 文档 + 本地经验 | artifact writer、runtime halt、数据丢失 | 建 disk watermark gate；JSONL 分卷压缩；journal 限额；artifact manifest 原子写；剩余空间低于阈值时 no-submit/cancel-only。 |
| OF-33 | CPU 与 I/O 资源门禁必须覆盖 event loop lag、WSS feed age、REST backlog、签名耗时、submit latency、inference latency，而不是只看系统 load。 | HFT 实践 + 本地 live 经验 | latency budget、tail risk | 每个决策记录 stage latency：feed->feature->inference->decision->sign->post->user-channel；超过 TTL 的 action 直接作废。 |
| OF-34 | 公开 Polymarket bot 已成为恶意软件目标；任何克隆仓库、npm/pip 依赖、脚本和 `.env` 都必须按供应链风险处理。 | 安全社区强信号 + 新闻 | private key、API key、research workflow | 外部 bot 只读 clone；`SupplyChainGate` 未通过时禁止安装、运行或接触真实密钥；研究环境无资金、无密钥、网络出站可审计。 |
| OF-35 | 平台官方文档本身在 2026 年持续变化；online-feedback 不是一次性文件，必须记录 `last_checked_date`、文档 URL、关键字段和破坏性变更。 | 官方 changelog | live gate、SDK version、训练 replay | 每次 paper/live/training report 绑定 online rule version；高风险 changelog 后强制刷新门禁并重新审查。 |

### 跨域判断

1. **训练技术路径应先收敛到“执行现实建模”，再尝试更复杂模型。** 继续训练时优先补齐 fill/queue/latency/fee/rebate/settlement 标签，先做校准、排序和规则 maker baseline，再考虑 RL 或大规模模型组合。
2. **模型预加载与目标市场切换是同一类问题。** 5m 市场窗口很短，模型、feature buffer、orderbook snapshot 和 token id 都必须提前准备；但切换生效必须由 market gate 原子完成。
3. **高频交易思路可借鉴，不能照搬。** Polymarket 的 off-chain CLOB 具备 HFT 延迟和排队问题，但 payoff、手续费、结算、赎回、平台状态机都与传统股票/期货不同。
4. **硬件治理要按机器分层。** 5090/训练机适合 replay 和训练，live 小服务器只适合轻量推理与执行；同一套脚本不能默认跨机器运行。
5. **公开 bot 的最有价值部分是失败边界。** 市场发现、订单簿 stale 清理、post-only、阶段停手、systemd/内存上限、panic cancel-all 都值得吸收；收益截图、README 宣传和未闭合模拟不进入硬规则。

## 相似 BTC Up/Down bot 的实现观察

已检查十一个公开仓库或相似实现，详见 `相似机器人实现观察清单.tsv`。提取出的可复用思想如下：

- **Polymarket 官方 `poly-market-maker`**：按固定 `sync_interval` 拉 midpoint，计算目标订单，比较 open orders，先 cancel 后 place；适合作为最小做市生命周期参考，但年代较早，未覆盖 CLOB V2/5m crypto 最新规则。
- **`pm-maker-bot`**：BTC Up/Down 做市，强调双边 bid、库存偏斜、post-only、toxic flow detection、风险黑名单、最后阶段 cancel all；可参考风险控制，但 README 不是完整可验证 live evidence。
- **`polymarket-arbitrage-bot`**：5m crypto 尾盘 favorite 策略，250-290 秒进场、0.97-0.99 买入；必须拆成 `trading_exit`、`hold_to_resolution`、`resolved_detected`、`redeem_after_resolved` 四个状态理解，redeem 只能发生在市场 resolved 之后。它主要是模拟逻辑，不是完整 live maker。
- **`polysagabot`**：短周期 BTC Up/Down，具备多价格源、WSS orderbook cache、REST fallback、dry-run fill haircut、activity reconciliation、confidence model shadow mode；这是最接近我们“paper/live/artifact + model shadow”的公开结构，但仍需要我们自己的 SDK-first 和账本闭合。
- **`poly-maker`**：公开作者明确提醒当前市场竞争下该 bot 可能不盈利；它仍提供 reward scanning、Google Sheet 参数化、open positions merge、不同 IP 更新器等工程线索。
- **`manhashed/polymarket-maker`**：README 与源码均体现目标市场发现、市场预取、`market_switch`、切换时 cancel-all、fee rate 刷新、Polymarket/Binance WebSocket、systemd/PM2 内存上限；适合参考 lifecycle，但它仍不是我们的 Rust SDK-first 路径。
- **`humanplane/updownarena`**：纸面比赛框架，把 Gamma 发现、CLOB token 校验、WSS orderbook、过期市场 orderbook 清理和 orderbook walk 撮合分开；适合作为 paper execution realism 的参考。
- **`floor-licker/polyfill-rs`**：强调 warmed hot path、零分配边界、connection pre-warming、depth limit 和 snapshot/hash 处理；提示我们 Rust 热路径优化必须明确“热路径成立条件”，不能把冷启动和首次市场插入也称为零分配。
- **`skharchikov/polymarket-bot`**：在线学习 ADR 强调下注时精确保存特征、resolved bet 后 warm-start retrain、typed predict API；可验证我们“交易反馈进入训练前先固化 schema”的方向。

结论：相似 bot 的共识不是“某个模型更强”，而是线上可靠性来自执行门禁、状态对账、库存控制、保守时间窗和可审计账本。

## 阶段计划

### 阶段 1：证据结构与资料登记

目标：把 online-feedback 从普通 Markdown 总结变成可复核 evidence ledger。

- 任务 1.1：建立资料索引
  - 步骤 1.1.1：维护 `外部资料交叉验证清单.tsv`，每行包含 source_id、source_type、url、证据摘要、关联规则、下一步。
  - 步骤 1.1.2：维护 `相似机器人实现观察清单.tsv`，每行包含 repo、commit、策略族、实现模式、可参考点、不可采信点。
  - 验证：`wc -l` 至少大于 20 行；每条 OF 规则至少能反查一个 source_id。

- 任务 1.2：建立 online issue schema
  - 输出字段：`issue_id`、`rule_id`、`source_ids`、`evidence_level`、`surface`、`symptom`、`platform_rule`、`hidden_rule_hypothesis`、`engineering_guard`、`verification_command_or_artifact`、`status`。
  - 验证：任何新增规则必须能区分 `官方明规则` 与 `待验证推断`。

### 阶段 2：官方规则与 changelog 追踪

目标：官方文档和 changelog 是规则底座，不能被社区经验替代。

- 任务 2.1：建立 Polymarket 官方规则快照
  - 步骤 2.1.1：抓取或人工登记 `Changelog`、`Order Lifecycle`、`Matching Engine`、`Rate Limits`、`WSS Market/User Channel`、`Fees`、`Maker Rebates`、`Liquidity Rewards`、`Post Multiple Orders`、`Fetching Markets`、`markets/keyset`、`markets/slug`、`clob-markets/{condition_id}`、`CTF Overview/Split/Merge/Redeem`。
  - 步骤 2.1.2：对每个页面记录检索日期、URL、影响路径、是否可能破坏当前 SDK-first 实现。
  - 验证：每个官方页面都有 `last_checked_date=2026-06-28` 或更新日期。

- 任务 2.2：把 changelog 变更转成门禁
  - CLOB V2、5m crypto fee/rebate、taker delay、DELETE /orders 上限、rate limit 调整都必须进入 runtime preflight。
  - 验证：preflight checklist 中每个重大变更都有明确“通过/阻断/仅提示”语义。

### 阶段 3：GitHub issue / 开发者社区问题归类

目标：把 issue 当作线上风险样本，不当作最终事实。

- 任务 3.1：归类 py-clob-client issue
  - `#273`：timeout 但 matched/filled。
  - `#286`：Service is not ready / matching engine restart。
  - `#287`、`#290`、`#297`、`#342`：balance/allowance、proxy wallet、sellable/cache。
  - `#122`、`#242`、`#272`、`#282`：tick size / price validation。
  - `#335`、`#336`、`#337`、`#340`：order_version_mismatch / CLOB V2。
  - `#147`：rate limit burst vs throttle。
  - 验证：每个 issue 只给“风险信号”，必须再与官方文档或本地 artifact 交叉验证后才能写成硬规则。

- 任务 3.2：社区与量化交易资料复核
  - 检查 Polymarket-specific 社区文章、公开 bot issue、量化社区关于 market making 的库存/排队/延迟/手续费讨论。
  - 对社区声明标注 `community_claim`，除非有官方文档或 artifact 验证，不进入 hard gate。

### 阶段 4：相似机器人实现对比

目标：提取可复用工程结构，剥离不可采信收益叙事。

- 任务 4.1：把公开 bot 分成三族
  - 做市族：post-only、双边报价、库存偏斜、合并头寸、风险冷却。
  - 尾盘狙击族：固定时间窗、价格带、favorite buy、`trading_exit`、`hold_to_resolution`、`resolved_detected`、`redeem_after_resolved`。
  - 混合/研究族：WSS + REST fallback、activity reconciliation、confidence model shadow mode。
  - 验证：每个 repo 只进入一个主策略族，避免把尾盘方向下注当成 maker policy。

- 任务 4.2：映射到我们当前开发路径
  - 对 `sdk-live-trade`：只采纳 order lifecycle、SDK adapter、artifact gate、wallet/sellable maturity 的工程经验。
  - 对 `V2`：只采纳 execution reality、action surface、ranking/calibration、rule maker 参数头的训练约束。
  - 验证：任何公开 bot 模式进入训练/执行前，都必须写清是否经过 fee/rebate、fill、queue、settlement 对账。

### 阶段 5：工程门禁落地

目标：把 online feedback 转成开发前检查，而不是事后复盘。

- 任务 5.1：新增 Online Feedback Gate
  - 检查项：官方 changelog freshness、SDK version、CLOB V2 compatibility、rate limit bucket、fee/rebate source、market metadata/tick size、WSS freshness、user-channel auth、wallet signature_type/funder、sellable maturity、settlement/redeem readiness。
  - 验证：每次 paper/live 前输出一份 gate report，字段缺失即 fail closed。

- 任务 5.2：新增 Platform Shadow Rule Ledger
  - 每条影子规则必须包含：触发条件、证据来源、可信度、是否 hard gate、是否只 warning、如何复现、如何关闭。
  - 验证：`SELL balance/allowance reject`、`timeout-after-submit`、`tick-size-stale`、`matching-engine-restart` 四条必须先入 ledger。

- 任务 5.3：更新 V2 训练前置约束
  - execution reality gate 必须消费 online-feedback 中的 fee/rebate、fill/queue、latency、order lifecycle、settlement 和 sellable maturity 规则。
  - 验证：任何 replay leaderboard 或 training report 必须引用对应 online rule version。

### 阶段 6：持续维护

目标：让 online-feedback 变成活文档。

- 任务 6.1：每次平台 changelog 或 SDK issue 高风险更新后，新增一条 `online_feedback_update` 记录。
- 任务 6.2：每次真实资金或 paper incident 后，补充 `source_ids`、artifact path、sha256、结论层级。
- 任务 6.3：每次复盘完成后扫描中文输出，避免混入英文段落。
- 验证：计划、清单和后续台账互相能反查；没有只存在聊天里的结论。

### 阶段 7：交叉架构与模型运行门禁

目标：把模型训练、HFT 执行、Polymarket 混合架构和硬件治理合成一个上线前门禁。

- 任务 7.1：建立 `online_rule_version`
  - 步骤 7.1.1：把 OF-01 到 OF-35 转成机器可读的 `platform_shadow_rules.jsonl`。
  - 步骤 7.1.2：每条规则写入证据来源、hard/warning/audit-only 级别、影响模块、验证命令或 artifact 字段。
  - 验证：任一 live/paper/training report 都能反查本次规则版本。

- 任务 7.2：建立模型预加载门禁
  - 步骤 7.2.1：定义 `model_bundle_preflight`：sha256、feature_order、seq_len、input/output name、normalizer、ORT session options。
  - 步骤 7.2.2：启动时执行 warmup inference，并记录 p50/p95/p99、线程数、RSS、CPU、first-run latency。
  - 验证：未通过 bundle/schema/warmup 的模型不得进入 active；只能 shadow 或 fail closed。

- 任务 7.3：建立硬件分层门禁
  - 步骤 7.3.1：定义 `HardwareTierGate` 字段：`host_class`、hostname、vCPU、MemTotal、disk_total/free、swap、GPU、允许 workload、禁止 workload、ORT intra/inter op 上限、active/shadow model 上限。
  - 步骤 7.3.2：默认分层：`live_exec` 只允许运行轻量推理/执行/日志，不允许 build、训练、长 replay、全量 backtest；`paper_runtime` 允许 paper 和 dashboard，不允许 GPU 训练；`training_gpu` 允许训练/replay，但不得持有真实资金密钥；`research_sandbox` 只读外部仓库，不持有真实 `.env`。
  - 验证：`live_exec` 若检测到 build/train/replay/GPU workload、vCPU < 2、MemTotal < 1800 MiB、disk_free < 10 GiB 或 ORT intra/inter op 超过配置上限，直接 fail closed。

- 任务 7.4：建立训练反馈门禁
  - 步骤 7.4.1：设计 `execution_feedback_labels.jsonl`，把 reject、unknown submit、partial/full fill、cancel、trading_exit、hold_to_resolution、resolved_detected、redeem_after_resolved、settled PnL、no-quote/no-submit opportunity 分层。
  - 步骤 7.4.2：每条标签必须包含因果上下文：`decision_id`、`paper_or_live`、`market_id`、`token_id`、side/price/size、TTE、order_type、model_version、feature_schema_version、feature_hash、causal_cutoff_ts、book_hash_at_decision、label_maturity_ts、authority_source、settlement_source、censoring_reason。
  - 步骤 7.4.3：建立 `experiment_trial_ledger.jsonl`，登记所有模型、超参、特征、split_id、final_holdout_id、费用/返佣假设、cost_assumption_version、online_rule_version、失败试验和放弃原因。
  - 步骤 7.4.4：V2 训练报告必须引用 purged/embargo split、PBO/DSR 或等价过拟合防护。
  - 验证：leaderboard 不再只看 replay PnL；必须同时报告执行现实覆盖率、标签质量、trial_count、split_id、feature_version、online_rule_version、成本口径和冻结 final holdout 结果。

### 阶段 8：目标市场发现、切换与长运行演练

目标：把 5m 目标市场发现/切换和长时间硬件治理放到同一套演练中验证。

- 任务 8.1：目标市场发现演练
  - 步骤 8.1.1：实现候选 slug、Gamma active scan、CLOB market info 三方校验清单。
  - 步骤 8.1.2：记录 discovery latency、miss rate、fallback 命中、server time/NTP drift。
  - 验证：连续跨多个 5m 窗口时，不出现 token id 缺失、closed market、过期 market 或 slug 错配进入 trading path。

- 任务 8.2：市场切换演练
  - 步骤 8.2.1：演练旧市场 cancel/audit、feature buffer 清空、新市场 snapshot/warmup、no-quote 解锁。
  - 步骤 8.2.2：验证所有 per-market 状态都带 `market_id`，并在切换后不能被旧市场数据更新。
  - 验证：切换窗口内的订单、特征、账本、orderbook、模型输入均能按 `market_id` 追踪。

- 任务 8.3：长时间运行资源演练
  - 步骤 8.3.1：采样 memory.current/stat/events、PSI、CPU、event loop lag、disk free、journal usage、artifact backlog。
  - 步骤 8.3.2：注入 WSS stale、REST backlog、disk watermark、model latency spike、clock drift。
  - 验证：资源风险只允许触发 no-submit、cancel-only 或 fail-closed，不允许继续正常下单。

#### ResourceWatermarkGate 默认采集与阈值

采集命令必须进入 gate report：

- `systemctl show <unit> -p MemoryCurrent -p MemoryMax -p CPUUsageNSec -p TasksCurrent -p RestartUSec -p NRestarts`
- `cat /sys/fs/cgroup/<unit>/memory.current /sys/fs/cgroup/<unit>/memory.stat /sys/fs/cgroup/<unit>/memory.events /sys/fs/cgroup/<unit>/memory.pressure`
- `cat /sys/fs/cgroup/<unit>/cpu.pressure /sys/fs/cgroup/<unit>/io.pressure`
- `df -P <artifact_dir> <log_dir> /`、`journalctl --disk-usage`、`du -sb <artifact_dir>`
- `timedatectl show -p NTPSynchronized -p TimeUSec -p NTPSyncUSec`

默认状态映射：

| 信号 | warning | no-submit | cancel-only / fail-closed |
| --- | --- | --- | --- |
| `memory.events` | `high` 增加但 feed/inference 正常 | `high` 增加且 `memory.current > 85% MemoryMax` 或 anon 占比 > 70% | `oom` 或 `oom_kill` 增加，或 artifact writer 因内存失败 |
| PSI | memory/cpu/io `avg10 > 2%` | `avg10 > 5%` 且 feed age 或 event loop lag 变差 | `avg10 > 10%` 且连续 3 个采样窗口恶化 |
| 磁盘 | free < 20% 或 < 15 GiB | free < 10% 或 < 5 GiB | free < 5% 或 < 2 GiB，或无法原子写 manifest |
| journal/log | journal 接近配置上限 80% | journal + artifact backlog 持续增长且磁盘 warning | 日志写入失败或 log rotation 失败 |
| 时钟 | NTP 不同步一次或 drift > 250 ms | NTP 不同步连续 2 次或 drift > 500 ms | drift > 1 s，5m 窗口禁止交易 |
| 延迟 | event loop lag p95 > 100 ms | feed age、REST backlog、inference p99 任一超过策略 TTL 的 50% | action age 超过 TTL，当前 action 作废并 fail closed |

#### SupplyChainGate 默认字段

每个外部仓库或依赖进入研究清单前必须记录：

- `repo_url`、`pinned_commit`、`clone_path`、`source_id`、`review_only=true`。
- `lockfile_path`、`lockfile_sha256`、`dependency_audit_status`、`provenance_status`。
- `has_real_env=false`、`has_wallet_key=false`、`has_api_key=false`、`network_egress_mode=blocked_or_logged`。
- `install_allowed=false`、`run_allowed=false`，除非用户另行批准且在隔离环境执行。

验证：未知代码需要安装依赖、运行脚本、读取 `.env`、访问钱包/API key 或打开未审计网络出站时，一律 fail closed。

## 优先执行清单

P0 必须先做：

- 建立正式 `online_issues.jsonl` 与 `platform_shadow_rules.jsonl`。
- 对 `OF-01` 到 `OF-35` 做 SDK adapter / runtime gate / training gate 映射。
- 把 `timeout-after-submit`、`SELL balance/allowance reject`、`tick_size_change`、`matching-engine-restart` 加入正负例 replay 或 artifact test。
- 每次 live/paper 前刷新 changelog、rate limits、fee/rebate 与 market metadata。
- 建立 `model_bundle_preflight`、`MarketDiscoveryGate`、`MarketSwitchGate`、`ResourceWatermarkGate`、`HardwareTierGate`、`SupplyChainGate` 六个 P0 门禁。
- 建立 `execution_feedback_labels.jsonl` 和 `experiment_trial_ledger.jsonl` 的 schema；缺少因果字段、no-quote/no-submit opportunity 或 trial 登记时，训练报告不得进入 leaderboard。

P1 紧随其后：

- 将公开 bot 的 post-only、库存偏斜、风险冷却、REST fallback、activity reconciliation 映射到我们的 rule maker baseline。
- 对尾盘 favorite 策略单独建策略族，不混入 market maker 训练标签。
- 建立 `current_market_info_snapshot.jsonl`，记录每轮 market 的 tick/min size/fee/reward/rebate/closed/resolved 状态。
- 将 `execution_feedback_labels.jsonl` 接入训练流水线，让模型训练消费分层后的成交、拒单、撤单、退出、resolved 后赎回和结算反馈。
- 建立 ORT session/thread/profile 基准，区分训练机、paper 机和 live 小服务器。

P2 后续维护：

- 监控 py-clob-client / js-clob-client / Polymarket docs changelog。
- 追踪 GitHub issue 是否被官方修复或关闭，并记录修复版本。
- 每月复查公开 bot 的 commit 与 README，避免使用过时实现。
- 每月复查供应链风险报告；外部 bot 研究环境不得接触真实 `.env`、钱包或 API key。

## 不能做的事

- 不能因为某个公开 bot 声称盈利，就跳过我们的 execution reality gate。
- 不能因为官方文档没有写清楚，就把 issue 经验升级成 hard gate；必须先落为假设并设计验证。
- 不能未经批准把 SDK-first 改成 raw REST、manual signing 或平行 shadow adapter。
- 不能把 no-live、dry-run、模拟 P/L、dashboard freshness 或 README 截图当作 live success。
- 不能把训练负结果、paper accounting 问题和平台线上规则混为一个结论。

## 本轮资料来源

详见：

- `外部资料交叉验证清单.tsv`
- `相似机器人实现观察清单.tsv`
- 本地既有复盘：`mistake-log/sdk-live-trade/sdk-live-trade错误记录汇总.md`、`mistake-log/sdk-live-trade/sdk-live-trade高维复盘报告.md`、`mistake-log/V2/V2高维复盘报告.md`、`mistake-log/V2/2026-06-28-V2后续训练技术路径判断.md`
