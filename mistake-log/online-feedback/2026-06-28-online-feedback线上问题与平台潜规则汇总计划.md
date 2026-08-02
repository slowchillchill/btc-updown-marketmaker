# online-feedback 线上问题与平台潜规则汇总实施计划

> **给后续代理工作者：** 必须使用中文执行；复杂或长时间执行时按本文件的“阶段 -> 任务 -> 步骤”推进，并在每个阶段完成后做独立审查。本文是计划与首轮交叉验证结论，不授权实盘、模拟盘运行、训练、部署或资金操作。

**目标：** 建立一个持续更新的 `online-feedback` 复盘体系，系统收集 Polymarket BTC Up/Down 交易机器人在真实线上、模拟盘和平台交互中遇到的常见问题、平台明规则、潜在规则、影子规则、社区议题、变更记录和相似机器人实现经验，并转化为可验证的开发门禁。

**架构：** 以 `sdk-live-trade` 的 SDK 优先真实订单经验、`V2` 的训练、模拟盘和运行时经验为本地证据底座；外部资料分为官方文档与变更记录、GitHub 议题与拉取请求、量化交易与开发者社区、公开相似机器人代码四类。每个关键规则都必须标明证据等级、影响路径、验证方式和工程对策。

**技术栈：** Polymarket CLOB、Gamma、WebSocket、Data API，官方 SDK 或本项目 SDK 优先 Rust 路径，V2 做市策略与模拟盘产物，JSONL 或 TSV 证据台账、产物重放和只读外部调研。

---

## 状态与边界

- 生成日期：2026-06-28（Asia/Tokyo）。
- 产物目录：`/home/chillchill/btc-updown-marketmaker/mistake-log/online-feedback/`。
- 本次已创建：
  - `2026-06-28-online-feedback线上问题与平台潜规则汇总计划.md`
  - `外部资料交叉验证清单.tsv`
  - `相似机器人实现观察清单.tsv`
- 本次没有执行 SSH、训练、重放、模拟盘运行、看板、部署、下单、撤单、资金查询或数据删除。
- 当前 `btc-updown-marketmaker` 目录不是 Git 仓库，因此本次以文件级校验交付，不创建提交。

## 成功标准

1. `mistake-log/online-feedback/` 存在，且计划文件、外部资料清单、相似机器人观察清单都已落盘。
2. 至少覆盖七类输入：官方文档与变更记录、GitHub 议题与拉取请求、公开相似机器人实现、本地 `sdk-live-trade` 与 `V2` 复盘、模型推理与训练资料、HFT 与做市研究、Linux 与 systemd 长时间运行资料。
3. 每个高优先级平台规则都标注证据等级：`官方明规则`、`官方变更规则`、议题或社区强信号（`issue/社区强信号`）、本地产物证据（`本地 artifact 证据`）、公开机器人实现习惯（`公开 bot 实现习惯`）、`待验证推断`。
4. 每个关键点都给出工程对策：运行时门禁、适配器合同、产物字段、测试或重放校验方式。
5. 不把公开机器人的收益宣传当成事实；只提取其实现模式、风险控制和缺口。

## 首轮交叉验证结论

### 总结判断

我们后续的 `online-feedback` 体系不应只是“错误日志”。它应该成为一个交易前置规则库：凡是会影响下单、撤单、成交确认、余额成熟、费用或返佣、结算、训练标签、模拟盘或实盘迁移和模型动作的线上规则，都必须先进入这里，再反向约束 SDK 适配器、模拟盘重放、V2 训练和实盘门禁。

首轮外部检索与本地复盘交叉后，优先级最高的结论是：

1. **平台状态机比单个 API 响应更重要。** 撮合引擎重启、超时后实际成交、提交响应中的空标识、迟到用户通道、CLOB 或订单簿快照、Data API 头寸都可能互相延迟或冲突。对策是统一 `NormalizedOrderEvent` 和由产物支撑的对账。
2. **5 分钟加密资产市场不能沿用旧“无费用或低延迟”假设。** 官方变更记录已多次调整 5 分钟加密资产市场的吃单费用、做市返佣、吃单延迟、速率限制和批量撤单限制。对策是每轮读取当前市场信息、费用和奖励字段，不使用固定常数。
3. **卖出可用余额成熟是核心影子规则。** 本地 `sdk-live-trade` 与 GitHub 议题均显示，用户通道或本地账本认为已成交，不等于平台可卖余额立即可用。对策是卖出拒绝不能直接覆盖本地账本，也不得形成跨后续决策周期持续生效的抑制状态；必须做成熟度分布和警告、仅审计分层。
4. **公开机器人大多只解决一部分问题。** 做市型机器人依赖仅挂单、库存偏斜、取消重挂、合并头寸和风险冷却；5 分钟 Up/Down 尾盘型机器人依赖时间窗、价格带、外部 BTC 价格源、Gamma 或 CLOB 轮询和赎回。很多公开实现只是模拟，或缺少真实成交、队列和结算对账，不能直接作为我们的实盘路径。
5. **V2 训练必须把线上反馈前置。** 如果成交、队列、费用或返佣、结算、订单生命周期和模拟盘产物不能闭合，模型训练的已选 EV、重放 PnL、排序标签和奖励都会学到错误规则。
6. **第二轮检索确认：我们的问题不是单点 API 问题，而是“模型训练 + HFT 执行 + 混合链上/链下平台 + 长周期运维”的交叉系统问题。** 因此后续方案必须同时覆盖模型预加载、目标市场发现、市场切换、线程/内存/磁盘治理、供应链安全和回测过拟合治理。

### 关键规则与对策矩阵

| 编号 | 规则/问题 | 证据等级 | 影响路径 | 工程对策 |
| --- | --- | --- | --- | --- |
| OF-01 | 撮合引擎重启期间下单或撤单可能失败，平台维护者在议题中明确表示这是预期行为，不是普通缺陷。 | 官方文档和 GitHub 议题 | 实盘提交、撤单、重试、失败即关闭 | 增加 `platform_service_state` 事件；`MARKET_NOT_READY` 或 `Service not ready` 只允许有限重试；重试前后必须查询订单和用户通道，避免重复落单。 |
| OF-02 | 请求超时不等于订单未成交，议题报告过 5 秒超时后多次重试均实际成交。 | GitHub 议题强信号 | FOK/FAK、重试、重复下单、账本 | 超时后进入 `UnknownSubmitOutcome`，先查订单、成交和用户通道，再决定补单；禁止盲目重试。 |
| OF-03 | `POST /orders` 传输成功、逐项成功、`accepted` 是三层不同语义；空订单标识、`UNKNOWN` 或错误文本不能进入接受路径。 | 本地产物和官方 API 语义 | SDK 适配器、账本预留、风险门禁 | 接受分类器必须同时要求非空平台订单标识、无拒单错误、状态可解释且范围对应。 |
| OF-04 | 卖出余额或授权额度拒绝可能由平台可卖余额成熟度或服务端缓存引起，不等于链上余额或本地账本错误。 | 本地产物和 GitHub 议题 | 卖出、库存、下一决策周期的账户状态 | 卖出拒绝作为订单局部警告或仅审计事件；记录成熟度探测；不覆盖本地账本，也不得形成跨后续决策周期持续生效的抑制状态。 |
| OF-05 | CLOB 余额或授权额度、代理钱包、安全钱包或充值钱包、签名类型容易出现“链上正确但 CLOB 缓存或参数不正确”。 | 官方账户模型和 GitHub 议题 | 钱包启动、API 密钥、余额门禁 | 启动前分别为 EOA、代理、安全和充值钱包建立数据源映射；同时验证链上授权、CLOB 余额与授权额度、SDK 的 `signature_type` 和出资方。 |
| OF-06 | 最小价格变动可能变化，或 SDK 本地缓存可能过期；0.999 等价格可能被 SDK 或客户端校验器拒绝。 | 官方 WebSocket 事件和 GitHub 议题 | 报价取整、动作解码、提交拒绝 | 订阅并处理 `tick_size_change`；提交前刷新市场元数据；所有模型输出先经过最小价格变动、最小值和最大值量化器。 |
| OF-07 | 官方变更记录显示，CLOB V2 迁移、订单哈希、订单版本和 pUSD 等变更会让旧 SDK 或旧签名逻辑失效。 | 官方变更记录和 GitHub 议题 | SDK 版本、订单签名、实盘部署 | 实盘前固定 SDK 版本做冒烟测试；遇到 `order_version_mismatch` 直接失败即关闭并升级 SDK 或签名路径，不绕行原始 REST。 |
| OF-08 | CLOB V2 迁移曾取消所有开放订单；类似平台变更期间，开放订单不能只依赖本地状态。 | 官方变更记录 | 启动恢复、运行恢复、过期订单 | 每次启动执行开放订单库存审计；平台发生重大变更后强制执行全部撤单和对账门禁。 |
| OF-09 | 5 分钟加密资产市场官方引入或调整吃单费用、做市返佣和吃单延迟；不能使用旧费用或返佣常数。 | 官方变更记录和费用、奖励文档 | EV、PnL、奖励、排序标签 | 每个市场读取当前费用和奖励信息；产物记录 `fee_rate_bps`、返佣口径、数据源 URL 和版本。 |
| OF-10 | 官方订单生命周期已披露吃单方有 250ms 延迟；这会影响尾盘抢单和可成交订单的成交解释。 | 官方订单生命周期 | FOK/FAK、尾盘策略、滑点 | 模拟器和实盘门禁纳入吃单延迟；尾盘策略必须使用更保守的截止时间和延迟预算。 |
| OF-11 | WebSocket 是必要实时输入，但可能过期、断线、缺少快照或只收到最优档更新。 | 官方 WSS 和公开机器人实现 | 订单簿、报价新鲜度、禁止报价门禁 | 市场通道需要心跳、快照要求、过期时回退 REST，并记录哈希和时效；缺少快照时不允许交易。 |
| OF-12 | 用户通道迟到事件、合成成交和 CLOB 查询可能重复表达同一成交。 | 本地产物和公开机器人对账 | 账本、PnL、校验器 | 同一订单或成交使用别名和 `covered-by-synthetic` 规则归一化；校验器只校验归一化事实，不重新解释业务。 |
| OF-13 | 速率限制和批量撤单上限会随变更记录调整，议题中仍有人不清楚固定窗口与滚动窗口。 | 官方速率限制和 GitHub 议题 | 提交节奏、全部撤单、市场扫描 | 按端点建立令牌桶；分批执行 `DELETE /orders`；遇到 429 时使用抖动和退避，并记录桶状态。 |
| OF-14 | 流动性奖励不是“挂单就有”，通常受最小数量、价差、计分、心跳和订单计分状态影响。 | 官方做市文档和公开做市机器人 | 做市奖励、报价放置、训练奖励 | 奖励路径单独建立 `reward_eligibility_gate`；不把返佣或奖励混入调整后 PnL 主账。 |
| OF-15 | 5 分钟 Up/Down 相似机器人常避开开盘或尾盘极端区间，或只做尾盘热门方向，不等于适合我们做市。 | 公开机器人代码 | 策略设计、训练目标 | 将做市、尾盘狙击和方向下注分成不同策略族；训练和评估不能混用标签与 PnL。 |
| OF-16 | 公开做市机器人强调库存偏斜、反向头寸合并、仅挂单、风险冷却和紧急平仓。 | 公开机器人代码和本地实盘经验 | 规则做市器、模型动作头 | 路径 3 的规则做市器必须有库存偏斜、合并或赎回、仅挂单保护和紧急全部撤单。 |

## 第二轮扩展检索与交叉结论

### 扩展范围

第二轮补充检索覆盖：

- Polymarket 混合架构：官方交易概览、订单生命周期、充值钱包、Gamma 市场、CLOB 市场信息、CTF 概览或拆分或合并或赎回、市场判定、WSS、速率限制和变更记录；第三方架构资料不能替代官方证据。
- 模型训练与交易：V2 训练复盘、在线学习或特征存储的公开实现、回测过拟合与 Deflated Sharpe 或 PBO 论文、训练标签与执行反馈闭环。
- 高频交易与做市：Avellaneda-Stoikov 库存风险框架、Queue-Reactive 订单簿模型、公开机器人的仅挂单、库存偏斜、市场切换和延迟指标。
- 模型预加载与推理：ONNX Runtime 的 `SessionOptions`、线程池、图优化、内存池、启动预热和热切换约束。
- 长时间运行硬件治理：Linux cgroup v2、PSI、systemd 资源控制、journald 空间限制、公开机器人的 `--max-old-space-size=512`、PM2 `max_memory_restart` 和 systemd 加固。
- 目标市场发现与切换：Gamma 的 `slug`、键集、活跃与关闭状态，CLOB 市场信息与代币标识，5 分钟或 15 分钟 `slug` 时间窗推断，以及公开机器人的预取、切换和清理过期订单簿做法。
- 供应链安全：针对 Polymarket 机器人的恶意 npm 或 GitHub 仓库、私钥与 `.env` 窃取、第三方前端依赖攻击新闻。

### 新增关键规则与对策矩阵

| 编号 | 规则/问题 | 证据等级 | 影响路径 | 工程对策 |
| --- | --- | --- | --- | --- |
| OF-17 | Polymarket 是混合架构：链下 CLOB、撮合、API 与链上 CTF、Exchange、赎回各自有权威边界，任何单一接口都不能代表全局真相。 | 官方明规则 | 订单状态、头寸状态、对账、训练标签 | 明确四层状态：`offchain_order_state`、`onchain_position_state`、`data_api_projection`、`local_ledger`；产物必须记录每层来源和时间戳。 |
| OF-18 | 目标市场发现不能只靠 `slug` 推断；`slug` 只能生成候选，最终必须由 Gamma 或 CLOB 的 `active`、`closed`、`acceptingOrders`、`conditionId`、`clobTokenIds`、最小价格变动、最小数量、费用和奖励字段确认。 | 官方文档和公开机器人源码 | 市场扫描、代币标识、动作路由、禁止报价门禁 | 建立 `MarketDiscoveryGate`；没有完整 UP、DOWN 代币标识、未来到期时间、可交易状态，以及费用、最小价格变动和最小数量快照时禁止交易。 |
| OF-19 | 目标市场切换必须清理旧市场状态；序列特征、订单簿缓存、未完成订单、库存和禁止报价预热不能跨 `market_id` 泄漏。 | 本地产物证据和公开机器人源码 | V2 序列缓冲区、做市报价、市场轮换 | 切换流程固定为：预发现下一市场、旧市场撤单和审计、清空逐市场特征与订单簿缓冲区、新市场订阅快照、满足 `seq_len` 预热后再报价。 |
| OF-20 | 5 分钟市场应预取下一市场，但预取只能准备数据和模型状态，不可提前把订单或账本指向下一市场。 | 公开机器人源码和本地复盘 | 延迟、市场切换、重复订单 | 分离 `prefetched_market_context` 与 `active_market_context`；只有切换门禁成功后，提交路径才允许使用新代币。 |
| OF-21 | 模型不能等到首个行情事件到来时才加载；模型包、特征 schema、归一化器、ONNX 会话、线程配置和预热推理都属于启动前置门禁。 | ONNX 官方文档和本地 V2 经验 | 推理延迟、首个行情事件遗漏、schema 漂移 | 启动时加载全部候选模型并校验 SHA-256、输入输出名称、`seq_len`、`feature_order`；创建 ORT 会话后运行固定预热，并记录预热延迟分布。 |
| OF-22 | 多模型热切换必须并行并存，不能原地替换正在使用的会话；新模型先以影子模式产出，验证 schema、延迟和校准后再切换。 | 本地 V2 经验和公开在线学习实现 | 实盘动作、模型漂移、回滚 | 增加 `ModelRegistry` 的活跃、影子和退役三态；切换前禁止报价或排空；每个模型单独记录账本、延迟和资源预算。 |
| OF-23 | ORT 线程池默认可能使用所有物理核心，在线小服务器上会挤压 WebSocket、签名、网络和日志线程。 | ONNX 官方文档和本地服务器约束 | CPU、尾部延迟、长时间运行 | 实盘配置显式限制算子内和算子间线程；小模型默认顺序执行；禁止在 2 vCPU 实盘主机上构建或运行长训练；线程调整必须由性能分析证明。 |
| OF-24 | ORT 线程自旋能降低推理延迟但会增加 CPU 和功耗；长时间在线交易不能只看平均延迟，必须看空闲 CPU、p95、p99 和 WSS 心跳。 | ONNX 官方文档 | CPU、WSS 过期、心跳遗漏 | 在实盘主机上默认采用保守自旋配置；如果启用自旋，必须记录 CPU 被窃取时间或负载、事件循环延迟、数据源时效和推理 p99。 |
| OF-25 | HFT 做市不能只训练“价格方向正确”；真实做市 EV 由成交风险代理、逆向选择、费用或返佣、撤单或重挂成本共同决定。Polymarket WSS 只有聚合订单簿，不应声称可直接观测真实 FIFO 排队位置。 | HFT 论文和公开机器人源码 | 训练目标、报价放置、做市策略 | 训练标签拆成方向、成交或未成交、成交后收益、同价位前方数量估计、`queue_proxy` 或 `fill_hazard`、净 EV；未通过执行现实门的样本不得进入奖励标签。 |
| OF-26 | Avellaneda-Stoikov 只能提供库存风险和报价距离框架，不能直接替代 Polymarket 5 分钟二元市场的费用或返佣、尾盘跳变和结算风险。 | 论文和平台规则 | 策略族、风险模型 | 把它作为基线，不作为收益保证；参数必须由本市场产物校准，并加入二元收益、到期时间、费用或返佣和头寸上限。 |
| OF-27 | Queue-Reactive 类模型提示订单簿状态本身决定订单流强度；Polymarket WSS 无严格全局单调序列且只暴露聚合订单簿时，只能构造队列代理或成交风险，不能把它当作真实队列位置。 | 论文、Polymarket WSS 和 `polyfill-rs` | 订单簿重放、特征新鲜度 | 记录订单簿时间戳、`recv_ts`、哈希、`snapshot_seen`、`depth_limit`、`stale_age`、`own_order_id`、下单或撤单或改单时间戳、同价位前方数量估计、后续成交或深度变化和删失原因；缺快照或哈希断层时禁止报价。 |
| OF-28 | 回测过拟合是 V2 继续训练的核心风险；多批模型训练结果“不及预期”不应简单归咎于模型弱，可能是特征泄漏、选择偏差、清洗期（`purge`）或隔离期（`embargo`）不足、样本被执行现实污染，或未登记失败试验造成的幸存者偏差。 | PBO/DSR 论文和本地 V2 经验 | 榜单、模型选择、训练经验总结 | 固定滚动前推及清洗期（`purge`）与隔离期（`embargo`）切分，冻结最终保留集；建立 `experiment_trial_ledger.jsonl` 登记所有模型、超参数、特征、切分、费用假设和失败试验；报告 PBO、DSR 和多重试验次数；模拟盘和实盘不允许充当超参数搜索器。 |
| OF-29 | 线上反馈进入训练前必须分层并保留因果上下文：平台错误样本、执行失败样本、可成交但负 EV 样本、禁止报价或禁止提交机会样本、结算后 PnL 样本不能混成同一个标签。 | 本地 V2 和 SDK 复盘 | 监督、排序、奖励标签 | 设计 `execution_feedback_labels.jsonl`，将 `outcome`、`decision_id`、`paper_or_live`、`market_id`、`token_id`、方向、价格、数量、TTE、`order_type`、`model_version`、`feature_schema_version`、`feature_hash`、`causal_cutoff_ts`、`book_hash_at_decision`、`label_maturity_ts`、`authority_source`、`settlement_source`、`censoring_reason` 分开入库。 |
| OF-30 | 目标市场发现要同时支持确定性候选和全量活跃扫描；只用当前时间戳 `slug` 会漏掉平台创建延迟、时钟偏差、`slug` 变体和关闭状态默认行为变化。 | 官方市场数据和公开机器人源码 | 市场发现、时钟漂移 | 采用候选 `slug` 加活跃事件或键集扫描双路径；校验服务器时间和 NTP 漂移；记录发现遗漏率和回退命中率。 |
| OF-31 | 长时间运行的内存增长不能简单等同泄漏；Linux cgroup v2 的文件缓存、匿名内存、`memory.events` 和 PSI 才是判断依据。 | Linux 官方文档和本地 V2 重放 | 内存、OOM、误清理 | 运行采样必须包含 `memory.current`、`memory.stat`、`memory.events`、压力、匿名内存、文件缓存和 slab；文件缓存增长且 `oom` 或 `high` 为 0 时只记录不干预。 |
| OF-32 | 磁盘比内存更容易成为隐形故障：JSONL、日志、产物、临时文件、训练日志和核心转储都可能让实盘主机在无人值守时失效。 | systemd 或 journald 文档和本地经验 | 产物写入器、运行时停止、数据丢失 | 建立磁盘水位门禁；JSONL 分卷压缩；限制日志空间；原子写入产物清单；剩余空间低于阈值时禁止提交或仅撤单。 |
| OF-33 | CPU 与 I/O 资源门禁必须覆盖事件循环延迟、WSS 数据源时效、REST 积压、签名耗时、提交延迟和推理延迟，而不是只看系统负载。 | HFT 实践和本地实盘经验 | 延迟预算、尾部风险 | 每个决策记录分阶段延迟：数据源到特征、推理、决策、签名、提交、用户通道；超过 TTL 的动作直接作废。 |
| OF-34 | 公开 Polymarket 机器人已成为恶意软件目标；任何克隆仓库、npm 或 pip 依赖、脚本和 `.env` 都必须按供应链风险处理。 | 安全社区强信号和新闻 | 私钥、API 密钥、研究流程 | 外部机器人仓库只读克隆；`SupplyChainGate` 未通过时禁止安装、运行或接触真实密钥；研究环境无资金、无密钥，网络出站可审计。 |
| OF-35 | 平台官方文档本身在 2026 年持续变化；`online-feedback` 不是一次性文件，必须记录 `last_checked_date`、文档 URL、关键字段和破坏性变更。 | 官方变更记录 | 实盘门禁、SDK 版本、训练重放 | 每次模拟盘、实盘或训练报告绑定线上规则版本；高风险变更记录出现后，强制刷新门禁并重新审查。 |

### 跨域判断

1. **训练技术路径应先收敛到“执行现实建模”，再尝试更复杂模型。** 继续训练时优先补齐成交、队列、延迟、费用或返佣、结算标签，先做校准、排序和规则做市基线，再考虑 RL 或大规模模型组合。
2. **模型预加载与目标市场切换是同一类问题。** 5 分钟市场窗口很短，模型、特征缓冲区、订单簿快照和代币标识都必须提前准备；但切换生效必须由市场门禁原子完成。
3. **高频交易思路可借鉴，不能照搬。** Polymarket 的链下 CLOB 具备 HFT 延迟和排队问题，但收益结构、手续费、结算、赎回和平台状态机都与传统股票或期货不同。
4. **硬件治理要按机器分层。** 5090 训练机适合重放和训练，实盘小服务器只适合轻量推理与执行；同一套脚本不能默认跨机器运行。
5. **公开机器人的最有价值部分是失败边界。** 市场发现、订单簿过期清理、仅挂单、阶段停手、systemd 或内存上限、紧急全部撤单都值得吸收；收益截图、README 宣传和未闭合模拟不进入硬规则。

## 相似 BTC Up/Down 机器人的实现观察

已检查十一个公开仓库或相似实现，详见 `相似机器人实现观察清单.tsv`。提取出的可复用思想如下：

- **Polymarket 官方 `poly-market-maker`**：按固定 `sync_interval` 拉取中间价，计算目标订单，比较开放订单，先撤单后下单；适合作为最小做市生命周期参考，但年代较早，未覆盖 CLOB V2 和 5 分钟加密资产市场的最新规则。
- **`pm-maker-bot`**：BTC Up/Down 做市，强调双边买价、库存偏斜、仅挂单、有毒流量检测、风险黑名单和最后阶段全部撤单；可参考风险控制，但 README 不是完整可验证的实盘证据。
- **`polymarket-arbitrage-bot`**：5 分钟加密资产尾盘热门方向策略，在 250 至 290 秒进场、以 0.97 至 0.99 买入；必须拆成 `trading_exit`、`hold_to_resolution`、`resolved_detected`、`redeem_after_resolved` 四个状态理解，赎回只能发生在市场判定完成之后。它主要是模拟逻辑，不是完整实盘做市器。
- **`polysagabot`**：短周期 BTC Up/Down，具备多价格源、WSS 订单簿缓存、REST 回退、试运行成交折扣、活动对账和置信模型影子模式；这是最接近我们“模拟盘或实盘产物加模型影子”的公开结构，但仍需要我们自己的 SDK 优先路径和账本闭合。
- **`poly-maker`**：公开作者明确提醒当前市场竞争下该机器人可能不盈利；它仍提供奖励扫描、Google Sheet 参数化、开放头寸合并和不同 IP 更新器等工程线索。
- **`manhashed/polymarket-maker`**：README 与源码均体现目标市场发现、市场预取、`market_switch`、切换时全部撤单、费用率刷新、Polymarket 或 Binance WebSocket、systemd 或 PM2 内存上限；适合参考生命周期，但它仍不是我们的 Rust SDK 优先路径。
- **`humanplane/updownarena`**：纸面比赛框架，把 Gamma 发现、CLOB 代币校验、WSS 订单簿、过期市场订单簿清理和订单簿逐档撮合分开；适合作为模拟盘执行现实性的参考。
- **`floor-licker/polyfill-rs`**：强调已预热热路径、零分配边界、连接预热、深度上限和快照或哈希处理；提示我们 Rust 热路径优化必须明确“热路径成立条件”，不能把冷启动和首次市场插入也称为零分配。
- **`skharchikov/polymarket-bot`**：在线学习 ADR 强调在下注时精确保存特征、在下注结果判定后进行热启动重新训练，并使用类型化预测 API；可验证我们“交易反馈进入训练前先固化 schema”的方向。

结论：相似机器人的共识不是“某个模型更强”，而是线上可靠性来自执行门禁、状态对账、库存控制、保守时间窗和可审计账本。

## 阶段计划

### 阶段 1：证据结构与资料登记

目标：把 `online-feedback` 从普通 Markdown 总结变成可复核证据台账。

- 任务 1.1：建立资料索引
  - 步骤 1.1.1：维护 `外部资料交叉验证清单.tsv`，每行包含 `source_id`、`source_type`、`url`、证据摘要、关联规则和下一步。
  - 步骤 1.1.2：维护 `相似机器人实现观察清单.tsv`，每行包含仓库、提交、策略族、实现模式、可参考点和不可采信点。
  - 验证：`wc -l` 至少大于 20 行；每条 OF 规则至少能反查一个 `source_id`。

- 任务 1.2：建立线上议题 schema
  - 输出字段：`issue_id`、`rule_id`、`source_ids`、`evidence_level`、`surface`、`symptom`、`platform_rule`、`hidden_rule_hypothesis`、`engineering_guard`、`verification_command_or_artifact`、`status`。
  - 验证：任何新增规则必须能区分 `官方明规则` 与 `待验证推断`。

### 阶段 2：官方规则与变更记录追踪

目标：官方文档和变更记录是规则底座，不能被社区经验替代。

- 任务 2.1：建立 Polymarket 官方规则快照
  - 步骤 2.1.1：抓取或人工登记 `Changelog`、`Order Lifecycle`、`Matching Engine`、`Rate Limits`、`WSS Market/User Channel`、`Fees`、`Maker Rebates`、`Liquidity Rewards`、`Post Multiple Orders`、`Fetching Markets`、`markets/keyset`、`markets/slug`、`clob-markets/{condition_id}`、`CTF Overview/Split/Merge/Redeem` 等官方页面。
  - 步骤 2.1.2：对每个页面记录检索日期、URL、影响路径、是否可能破坏当前 SDK-first 实现。
  - 验证：每个官方页面都有 `last_checked_date=2026-06-28` 或更新日期。

- 任务 2.2：把变更记录转成门禁
  - CLOB V2、5 分钟加密资产费用或返佣、吃单延迟、`DELETE /orders` 上限和速率限制调整都必须进入运行时预检。
  - 验证：预检清单中每个重大变更都有明确的“通过、阻断、仅提示”语义。

### 阶段 3：GitHub 议题与开发者社区问题归类

目标：把议题当作线上风险样本，不当作最终事实。

- 任务 3.1：归类 py-clob-client 议题
  - `#273`：超时但已匹配或已成交。
  - `#286`：`Service is not ready` 或撮合引擎重启。
  - `#287`、`#290`、`#297`、`#342`：余额或授权额度、代理钱包、可卖余额或缓存。
  - `#122`、`#242`、`#272`、`#282`：最小价格变动或价格校验。
  - `#335`、`#336`、`#337`、`#340`：`order_version_mismatch` 或 CLOB V2。
  - `#147`：速率限制的突发请求与节流。
  - 验证：每个议题只给“风险信号”，必须再与官方文档或本地产物交叉验证后才能写成硬规则。

- 任务 3.2：社区与量化交易资料复核
  - 检查 Polymarket 专项社区文章、公开机器人议题、量化社区关于做市的库存、排队、延迟和手续费讨论。
  - 对社区声明标注 `community_claim`，除非有官方文档或产物验证，否则不进入硬门禁。

### 阶段 4：相似机器人实现对比

目标：提取可复用工程结构，剥离不可采信的收益叙事。

- 任务 4.1：把公开机器人分成三族
  - 做市族：仅挂单、双边报价、库存偏斜、合并头寸、风险冷却。
  - 尾盘狙击族：固定时间窗、价格带、热门方向买入、`trading_exit`、`hold_to_resolution`、`resolved_detected`、`redeem_after_resolved`。
  - 混合或研究族：WSS 加 REST 回退、活动对账、置信模型影子模式。
  - 验证：每个仓库只进入一个主策略族，避免把尾盘方向下注当成做市策略。

- 任务 4.2：映射到我们当前开发路径
  - 对 `sdk-live-trade`：只采纳订单生命周期、SDK 适配器、产物门禁、钱包和可卖余额成熟度的工程经验。
  - 对 `V2`：只采纳执行现实、动作面、排序或校准、规则做市器参数头的训练约束。
  - 验证：任何公开机器人模式进入训练或执行前，都必须写清是否经过费用或返佣、成交、队列和结算对账。

### 阶段 5：工程门禁落地

目标：把线上反馈转成开发前检查，而不是事后复盘。

- 任务 5.1：新增线上反馈门禁（`Online Feedback Gate`）
  - 检查项：官方变更记录新鲜度、SDK 版本、CLOB V2 兼容性、速率限制桶、费用或返佣来源、市场元数据和最小价格变动、WSS 新鲜度、用户通道认证、钱包签名类型和出资方、可卖余额成熟度、结算或赎回就绪状态。
  - 验证：每次模拟盘或实盘前输出一份门禁报告，字段缺失即失败即关闭。

- 任务 5.2：新增平台影子规则台账（`Platform Shadow Rule Ledger`）
  - 每条影子规则必须包含：触发条件、证据来源、可信度、是否为硬门禁、是否仅警告、如何复现和如何关闭。
  - 验证：`SELL balance/allowance reject`、`timeout-after-submit`、`tick-size-stale`、`matching-engine-restart` 四条必须先进入台账。

- 任务 5.3：更新 V2 训练前置约束
  - 执行现实门必须消费 `online-feedback` 中的费用或返佣、成交或队列、延迟、订单生命周期、结算和可卖余额成熟度规则。
  - 验证：任何重放榜单或训练报告必须引用对应线上规则版本。

### 阶段 6：持续维护

目标：让 `online-feedback` 变成活文档。

- 任务 6.1：每次平台变更记录或 SDK 议题出现高风险更新后，新增一条 `online_feedback_update` 记录。
- 任务 6.2：每次真实资金或模拟盘事故后，补充 `source_ids`、产物路径、SHA-256 和结论层级。
- 任务 6.3：每次复盘完成后扫描中文输出，避免混入英文段落。
- 验证：计划、清单和后续台账互相能反查；没有只存在聊天里的结论。

### 阶段 7：交叉架构与模型运行门禁

目标：把模型训练、HFT 执行、Polymarket 混合架构和硬件治理合成一个上线前门禁。

- 任务 7.1：建立 `online_rule_version`
  - 步骤 7.1.1：把 OF-01 到 OF-35 转成机器可读的 `platform_shadow_rules.jsonl`。
  - 步骤 7.1.2：每条规则写入证据来源、`强制/警告/仅审计`（`hard/warning/audit-only`）级别、影响模块、验证命令或产物字段。
  - 验证：任一实盘、模拟盘或训练报告都能反查本次规则版本。

- 任务 7.2：建立模型预加载门禁
  - 步骤 7.2.1：定义 `model_bundle_preflight`：`sha256`、`feature_order`、`seq_len`、输入/输出名称、归一化器和 ORT 会话选项。
  - 步骤 7.2.2：启动时执行预热推理，并记录 p50、p95、p99、线程数、RSS、CPU 和首次运行延迟。
  - 验证：未通过模型包、schema 或预热的模型不得进入活跃状态；只能进入影子状态或失败即关闭。

- 任务 7.3：建立硬件分层门禁
  - 步骤 7.3.1：定义 `HardwareTierGate` 字段：`host_class`、主机名、vCPU、`MemTotal`、磁盘总量与可用空间、交换空间、GPU、允许负载、禁止负载、ORT 算子内或算子间线程上限、活跃或影子模型上限。
  - 步骤 7.3.2：默认分层：`live_exec` 只允许运行轻量推理、执行和日志，不允许构建、训练、长重放或全量回测；`paper_runtime` 允许模拟盘和看板，不允许 GPU 训练；`training_gpu` 允许训练和重放，但不得持有真实资金密钥；`research_sandbox` 只读外部仓库，不持有真实 `.env`。
  - 验证：`live_exec` 若检测到构建、训练、重放或 GPU 负载，或 vCPU 小于 2、`MemTotal` 小于 1800 MiB、`disk_free` 小于 10 GiB、ORT 算子内或算子间线程超过配置上限，直接失败即关闭。

- 任务 7.4：建立训练反馈门禁
  - 步骤 7.4.1：设计 `execution_feedback_labels.jsonl`，把拒绝、未知提交、部分或完全成交、撤单、`trading_exit`、`hold_to_resolution`、`resolved_detected`、`redeem_after_resolved`、结算后 PnL、禁止报价或禁止提交机会分层。
  - 步骤 7.4.2：每条标签必须包含因果上下文：`decision_id`、`paper_or_live`、`market_id`、`token_id`、方向、价格、数量、TTE、`order_type`、`model_version`、`feature_schema_version`、`feature_hash`、`causal_cutoff_ts`、`book_hash_at_decision`、`label_maturity_ts`、`authority_source`、`settlement_source`、`censoring_reason`。
  - 步骤 7.4.3：建立 `experiment_trial_ledger.jsonl`，登记所有模型、超参数、特征、`split_id`、`final_holdout_id`、费用或返佣假设、`cost_assumption_version`、`online_rule_version`、失败试验和放弃原因。
  - 步骤 7.4.4：V2 训练报告必须引用清洗期（`purge`）与隔离期（`embargo`）切分、PBO 或 DSR，或等价过拟合防护。
  - 验证：榜单不再只看重放 PnL；必须同时报告执行现实覆盖率、标签质量、`trial_count`、`split_id`、`feature_version`、`online_rule_version`、成本口径和冻结最终保留集结果。

### 阶段 8：目标市场发现、切换与长运行演练

目标：把 5 分钟目标市场的发现与切换，以及长时间硬件治理放到同一套演练中验证。

- 任务 8.1：目标市场发现演练
  - 步骤 8.1.1：实现候选别名（`slug`）、Gamma 活跃市场扫描、CLOB 市场信息三方校验清单。
  - 步骤 8.1.2：记录发现延迟、遗漏率、后备路径命中和服务器时间/NTP 偏移。
  - 验证：连续跨多个 5 分钟窗口时，不出现 `token_id` 缺失、已关闭市场、过期市场或别名错配进入交易路径。

- 任务 8.2：市场切换演练
  - 步骤 8.2.1：演练旧市场撤单与审计、特征缓冲区清空、新市场快照与预热，以及解除禁止报价（`no-quote`）状态。
  - 步骤 8.2.2：验证所有市场级状态都带 `market_id`，并在切换后不能被旧市场数据更新。
  - 验证：切换窗口内的订单、特征、账本、订单簿和模型输入均能按 `market_id` 追踪。

- 任务 8.3：长时间运行资源演练
  - 步骤 8.3.1：采样 `memory.current`、`memory.stat`、`memory.events`、PSI、CPU、事件循环延迟、磁盘可用空间、systemd 日志用量和产物积压。
  - 步骤 8.3.2：注入 WSS 过时、REST 积压、磁盘水位超限、模型延迟突增和时钟偏移。
  - 验证：资源风险只允许触发禁止提交（`no-submit`）、仅撤单（`cancel-only`）或失败即关闭（`fail-closed`），不允许继续正常下单。

#### 资源水位门禁（`ResourceWatermarkGate`）默认采集与阈值

采集命令必须进入门禁报告：

- `systemctl show <unit> -p MemoryCurrent -p MemoryMax -p CPUUsageNSec -p TasksCurrent -p RestartUSec -p NRestarts`
- `cat /sys/fs/cgroup/<unit>/memory.current /sys/fs/cgroup/<unit>/memory.stat /sys/fs/cgroup/<unit>/memory.events /sys/fs/cgroup/<unit>/memory.pressure`
- `cat /sys/fs/cgroup/<unit>/cpu.pressure /sys/fs/cgroup/<unit>/io.pressure`
- `df -P <artifact_dir> <log_dir> /`、`journalctl --disk-usage`、`du -sb <artifact_dir>`
- `timedatectl show -p NTPSynchronized -p TimeUSec -p NTPSyncUSec`

默认状态映射：

| 信号 | 警告（`warning`） | 禁止提交（`no-submit`） | 仅撤单/失败即关闭（`cancel-only` / `fail-closed`） |
| --- | --- | --- | --- |
| `memory.events` | `high` 增加，但数据流和推理正常 | `high` 增加，且 `memory.current > 85% MemoryMax` 或匿名内存占比 > 70% | `oom` 或 `oom_kill` 增加，或产物写入器因内存失败 |
| PSI | 内存/CPU/输入输出的 `avg10 > 2%` | `avg10 > 5%`，且数据流时延或事件循环延迟变差 | `avg10 > 10%`，且连续 3 个采样窗口恶化 |
| 磁盘 | 可用比例 < 20% 或可用空间 < 15 GiB | 可用比例 < 10% 或可用空间 < 5 GiB | 可用比例 < 5% 或可用空间 < 2 GiB，或无法原子写入产物清单 |
| systemd 日志/应用日志（`journal/log`） | systemd 日志用量接近配置上限 80% | systemd 日志和产物积压持续增长，且磁盘已触发警告 | 日志写入失败或轮转失败 |
| 时钟 | NTP 不同步一次或偏移 > 250 ms | NTP 不同步连续 2 次或偏移 > 500 ms | 偏移 > 1 s，5 分钟窗口禁止交易 |
| 延迟 | 事件循环延迟 p95 > 100 ms | 数据流时延、REST 积压、推理 p99 任一项超过策略 TTL 的 50% | 动作时延超过 TTL，当前动作作废并失败即关闭 |

#### 供应链门禁（`SupplyChainGate`）默认字段

每个外部仓库或依赖进入研究清单前必须记录：

- `repo_url`、`pinned_commit`、`clone_path`、`source_id`、`review_only=true`。
- `lockfile_path`、`lockfile_sha256`、`dependency_audit_status`、`provenance_status`。
- `has_real_env=false`、`has_wallet_key=false`、`has_api_key=false`、`network_egress_mode=blocked_or_logged`。
- `install_allowed=false`、`run_allowed=false`，除非用户另行批准且在隔离环境执行。

验证：未知代码需要安装依赖、运行脚本、读取 `.env`、访问钱包或 API 密钥，或打开未审计网络出站时，一律失败即关闭（`fail-closed`）。

## 优先执行清单

P0 必须先做：

- 建立正式 `online_issues.jsonl` 与 `platform_shadow_rules.jsonl`。
- 对 `OF-01` 到 `OF-35` 做 SDK 适配器、运行时门禁和训练门禁映射。
- 把 `timeout-after-submit`、`SELL balance/allowance reject`、`tick_size_change`、`matching-engine-restart` 加入正反例重放或产物测试。
- 每次实盘或模拟盘前刷新变更记录、速率限制、费用或返佣和市场元数据。
- 建立 `model_bundle_preflight`、`MarketDiscoveryGate`、`MarketSwitchGate`、`ResourceWatermarkGate`、`HardwareTierGate`、`SupplyChainGate` 六个 P0 门禁。
- 建立 `execution_feedback_labels.jsonl` 和 `experiment_trial_ledger.jsonl` 的模式；缺少因果字段、禁止报价/禁止提交机会（`no-quote/no-submit opportunity`）或试验登记时，训练报告不得进入排行榜。

P1 紧随其后：

- 将公开机器人的仅挂单（`post-only`）、库存偏斜、风险冷却、REST 后备路径（`fallback`）和活动对账（`activity reconciliation`）映射到我们的规则做市基线（`rule maker baseline`）。
- 对尾盘高概率一侧（`favorite`）策略单独建策略族，不混入做市（`market maker`）训练标签。
- 建立 `current_market_info_snapshot.jsonl`，记录每轮市场的价格变动单位、最小数量、费用、奖励、返佣、关闭状态和市场判定状态（`tick/min size/fee/reward/rebate/closed/resolved`）。
- 将 `execution_feedback_labels.jsonl` 接入训练流水线，让模型训练消费分层后的成交、拒单、撤单、退出、市场判定（`resolved`）后赎回，以及结算反馈。
- 建立 ORT 会话、线程和性能分析（`session/thread/profile`）基准，区分训练机、模拟盘机器和实盘小服务器。

P2 后续维护：

- 监控 `py-clob-client`、`js-clob-client` 和 Polymarket 文档变更记录。
- 追踪 GitHub 议题是否被官方修复或关闭，并记录修复版本。
- 每月复查公开机器人的提交记录与 README，避免使用过时实现。
- 每月复查供应链风险报告；外部机器人研究环境不得接触真实 `.env`、钱包或 API 密钥。

## 不能做的事

- 不能因为某个公开机器人声称盈利，就跳过我们的执行现实门禁。
- 不能因为官方文档没有写清楚，就把议题经验升级成强制门禁；必须先落为假设并设计验证。
- 不能未经批准把 SDK 优先改成原始 REST（`raw REST`）、手工签名（`manual signing`）或平行影子适配器（`shadow adapter`）。
- 不能把未实盘（`no-live`）、试运行（`dry-run`）、模拟盈亏（P/L）、看板新鲜度（`dashboard freshness`）或 README 截图当作实盘成功（`live success`）。
- 不能把训练负结果、模拟盘核算（`paper accounting`）问题和平台线上规则混为一个结论。

## 本轮资料来源

详见：

- `外部资料交叉验证清单.tsv`
- `相似机器人实现观察清单.tsv`
- 本地既有复盘：`mistake-log/sdk-live-trade/sdk-live-trade错误记录汇总.md`、`mistake-log/sdk-live-trade/sdk-live-trade高维复盘报告.md`、`mistake-log/V2/V2高维复盘报告.md`、`mistake-log/V2/2026-06-28-V2后续训练技术路径判断.md`
