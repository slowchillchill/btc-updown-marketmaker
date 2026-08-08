# sim-system 变更记录

> 事项编号：`SIM-SYSTEM-20260731`
>
> 生命周期：`IMPLEMENTATION_CHANGELOG`
>
> 当前执行 authority：`none`
>
> 对应需求：`demand/sim-system-demand.md`
>
> 对应计划：`plan/sim-system-plan.md`
>
> `data-collect` 当前权威入口：`data-collect/docs/CURRENT.md`
>
> 执行授权：`LOCAL_IMPLEMENTATION_ONLY`
>
> 部署、远端操作和真实资金授权：`none`
>
> 状态：`只记录需求演进、冲突、决定与实施事实；不承担当前执行权威`

本记录按发生时间保存需求演进与实施事实。较早条目只说明当时发生了什么；如果与后续用户
决定冲突，以后续明确取代条目和当前需求、计划正文为准。本记录本身不产生实施、提交、推送、
部署、远端操作、钱包/凭据、真实下单或真实资金权限；本事项的当前执行权威和授权以需求及
计划首屏为准。

## 2026-07-31：建立独立 sim-system 需求事项

### 用户纠正

- 前一版无权威草稿把订单簿写成不接收外部市场状态的静态冻结场景。用户明确纠正：训练和
  模拟盘撮合必须由与模型 Observation 同源、同因果边界的订单簿事件驱动。
- `data-pipeline` 成为 `ExecutionMarketEvent` 的唯一生产语义归属方；`sim-system` 只消费
  版本化规范事件，不能读取类型化原始数据、Parquet、实时交接或原始传输帧，也不能建立
  第二套排序、去重、缺口或因果实现。
- 首版操作范围增加批量提交、全撤、订单查询和成交查询；单笔响应、分页查询、订单、成交
  和结算反馈必须分离但可稳定关联。
- 用户冻结合格 `MarketResolved` 在规范可用时同一逻辑时点原子自动赎回，不模拟人为等待、
  失败或重试。

### 架构结果

- 模拟器会话拥有可演进的外部影子订单簿、本账户订单叠加层、队列估计、账户和时间状态；
  调用方不能随订单注入行情或内部状态。
- 训练历史回放与模拟盘实时流复用同一 `ExecutionMarketEvent` 合同，但会话可变状态、账户、
  RNG、游标和反馈必须隔离。
- 黄金事件用于组件语义和边界测试；真实训练适用仍须由真实管道事件验证。

### 当前事实

- 本事项编号冻结为 `SIM-SYSTEM-20260731`。
- 当日只建立和修订需求草案，没有代码实施、测试通过结论或运行授权。

## 2026-08-01：冻结 L2 队列推定和平台小额余量账户边界

### 用户纠正

- 平台小额余量只属于本模拟账户自身已接受订单，不能作用于外部影子价格档位、前后队列或
  其他参与者挂单。
- 小额余量只能在合格撮合或确定性差额成交推定已经形成正的本账户已撮合数量后，以本账户
  精确未成交余量严格位于 `(0, 0.01 shares)` 且同价队列耗尽为条件形成；禁止发生率、随机
  抽样、随机数量或从外部盘口借量。
- 有效 `book`/价位净缩量在消除同一底层变化重复证据后，必须确定性地推进队头；有效新
  `book` 同时重校准外部盘口并成为后续基准。

### 冻结合同

- 唯一 maker 队列配置档为 `L2_BOOK_DELTA_FIFO_ESTIMATE_V1`，证据类别是
  `PROJECT_ASSUMPTION`，不能冒充真实逐订单 FIFO。
- 外部订单簿、本账户叠加层和派生聚合盘口分别维护所有权和数量；外部变化和账户队列推进
  是同一证据的分层投影，不能相加为两次变化。
- `VENUE_DUST_TERMINAL` 不生成成交、份额、手续费或现金流，只关闭精确账户小余量并幂等
  释放对应活动资金占用。
- 缺口、epoch 切换、陈旧或缺少真实逐订单 ID 本身不把既有 maker 订单隔离；下一份合同
  有效完整 `book` 仍按唯一净差额规则重校准。

### 当前事实

- 这些语义只进入需求草案；没有实现、校准、真实平台证明或部署发生。

## 2026-08-02：固定市场范围、250ms 暂缓和返佣边界

### 市场与 itode 决定

- 整个项目固定只支持 Polymarket Global CLOB 的 `btc-updown-5m`，不建设通用多市场
  模拟器或体育/赛事秒级延迟兼容能力。
- `sim-system` 只从唯一集中代码常量
  `BTC_UPDOWN_5M_TAKER_HOLD_MS=250` 取得可立即成交订单的内部 taker 暂缓；调用方、
  `ExecutionMarketEvent`、时延配置档和 `sim-calibration` 都不能配置或覆盖。
- 上游采集模块不为 `itode` 新增专用查询或类型化字段，`data-pipeline` 和 Observation
  不传播 `itode`/秒级延迟。原始源证据自然包含同名键时仍可在原始边界保留，但不能成为
  模拟器输入。

### 返佣与结算决定

- 用户纠正首版遗漏 maker rebate 的问题；maker rebate 是模拟平台经济事实，不是
  Liquidity Rewards 或训练奖励。
- maker 应计按最终成功且实际为 maker 的成交、撮合时逐市场 `feeSchedule` 和官方公式的
  机械推导计算，先形成不可支出应收款；付款只是应收款转现金，不重复产生 PnL 或奖励。
- 返佣规则、证据分类、固定精度、日桶、付款时刻和未知/不支持语义必须版本化并可追溯，
  不得硬编码当前逐市场费率或从 slug/市场类别猜测。

### 批量与撤单证据

- 批量撤单上限的官方页面存在 3,000 与 1,000 的冲突；用户冻结首版保守使用 1,000，并
  要求保留冲突，后续只能发布新规则版本。
- “matched orders can't be canceled” 只证明一次撤单未成功，不能据此创造撮合数量、
  平台小额余量或账户经济变化。

### 当前事实

- 250ms 的语义与本项目全部准入市场固定适用分别记录为官方平台规则和项目假设；没有把
  项目假设升级成官方永久保证。
- 本轮仍未实施代码、运行模拟器、连接真实平台或执行任何资金操作。

## 2026-08-03：SDK 对齐方法面、开发顺序和双返佣闭环

### 共用桥接器与方法命名

- 用户冻结四类业务运行时先经过共用 `Intent Bridge / Execution Adapter`。模拟后端把
  模拟领域请求路由到隔离 `sim-system`；实盘后端只委托官方 Rust SDK 和真实平台，不能
  跨后端回退。
- 唯一公开操作词汇冻结为：
  `post_order`、`post_orders`、`cancel_order`、`cancel_orders`、
  `cancel_all_orders`、`order`、`orders`、`trades`。
- 模拟路径复用官方 SDK 的方法词汇，但不接受 `SignedOrder`、签名、认证、网络客户端或
  官方具体响应类型；不存在第二套 `Submit*`、`QueryOrder`、`List*` 方法面。
- 固定 SDK 实现和当前官方 API 都按项返回批量提交结果并允许混合成功/失败，因此
  `post_orders` 固定为逐项、非原子语义；不保留此前错误的批量整体结果描述。

### 开发顺序修订

- 用户纠正“先独立完成 sim-system，再做 data-pipeline(training)”的旧顺序。当前顺序为：
  DPT 共享合同与 Gate A -> sim-system 黄金组件验收及真实管道纵向验收 -> DPT Gate B
  训练闭环 -> model-train。
- 组件黄金测试可以在共享合同冻结后与 DPT Gate A 实现并行；真实纵向只能在合格 Gate A
  回执后进行。黄金事件不能替代真实纵向。
- 完整归一化层、Ledger 和 RewardContract 不作为模拟器组件开发前置；它们的实际端到端
  消费属于模拟器之后的训练闭环，RewardContract 实际计算属于 `model-train` 首门。

### taker rebate 和两类付款

- 用户进一步确认首版同时实现 maker 与 taker rebate 的应计、独立日桶、日结和付款。
- taker 只按最终成功实际 taker 成交已收取的手续费乘固定 Silver `8%` 形成应计；首版不
  累计或校验 wV、不查询等级、不实现等级变化或一次性奖金。
- D 日 taker 与 maker 的正常现金可用时刻分别冻结为 D+1 `00:10 UTC` 和 `00:45 UTC`；
  同类日应计低于 `1 pUSD` 不付款且不并入后续日期，达到门槛时向下截取至四位小数，余数
  继续锁定且不参与后续门槛。这些执行处理是观察证据支持、用户冻结的项目假设，不是官方
  永久保证。
- 正常时刻仍有内部最终性未闭合贡献时不提前日结；最后一项闭合后只日结和付款一次。
  普通训练窗口截尾不产生未知终态或提前付款。

### 当前事实

- 本轮只继续修订需求和跨模块边界；没有创建 plan/changelog、代码或测试产物，也没有
  实施外部桥接器、归一化层、Ledger 或 model-train。

## 2026-08-04：数据下限、时延配置档失效、批准门和三件套补齐

### DPT 决定来源与反向引用

本节同步下列 DPT 活动草案中的用户决定，但不改变其事项归属：

- `demand/data-pipeline-training-demand.md`
- `plan/data-pipeline-training-plan.md`
- `changelog/data-pipeline-training-changelog.md` 的“2026-08-04：冻结数据下限并修正批准门和
  计划闭环”条目

### 数据资格和真实纵向入口

- 当前训练数据谱系根冻结为 `/home/chillchill/new-datasets`，UTC 日下限冻结为
  `2026-07-30`。更早采集数据不用于读取、迁移、回填、统计、拟合、训练、校准或评估。
- 2026-07-30 至 2026-08-02 是首批已核实验收基线，不是永久输入全集或独立未见留出集；
  后续日期仍须通过清单准入和版本化切分。
- `sim-system` 不直接读取原始目录或检查日期。真实纵向只消费由 DPT Gate A 覆盖、谱系
  合格且日期不早于下限的 `ExecutionMarketEvent`。
- DPT 局部使用 `tape_assessment=PASS|PARTIAL|FAIL`；只有全部必需能力通过且
  `tape_assessment=PASS` 才签发 `REAL_EXECUTION_TAPE_READY`。PARTIAL 诊断不能启动
  `sim-system` 正式真实纵向。本事项不定义它与 rebuild 全局 `gate_verdict` 的映射。

### 旧时延配置档失效

- 以 2026-07-10 数据计算的旧爱尔兰时延配置档不再具有当前权威。
- 正式配置档只能使用日期合格数据重新计算，并绑定输入、口径、时钟证据、代码和
  `profile_id/version/hash`；重新计算、门禁和独立复核前状态为 `NOT_READY`。
- 固定合成配置档仍可用于明确标记的模块组件符合性测试，但不能替代正式训练、
  offline-evaluation 或模拟盘配置档。
- `sim-calibration` 是正式配置档的独立后续写入方。其回执放在 DPT Gate B 前，还是
  Gate B 后但正式 model-train 前，仍待用户决定；本轮不替用户选择。身份未形成时，受
  影响的正式运行判定和 model-train 保持阻断。
- 两条候选路径的联合验收语义已经机械拆开：门 B 前置方案要求正式配置档回执先存在；
  推荐的门 B 后置方案允许阶段 7 使用明确标识的固定符合性配置档，并同时绑定正式状态
  `NOT_READY` 与后续 `sim-calibration` 门，把联合证据交付 DPT。后者不使正式配置档、
  `SIM-AC-023` 正式运行条件或 model-train 提前通过。

### 批准语义和阶段关闭

- 用户删除“需求批准 + 计划批准 + 第三次通用实施授权”的重复门：需求批准冻结范围，
  计划批准才赋予计划范围内的本地代码、测试和不可变本地测试产物游标。
- 当前需求和计划仍是草案，因此当前 authority 和执行授权继续为 `none`；上述批准语义
  不表示本轮已经批准需求或计划。
- 每个任务完成后必须自审，每个阶段关闭前必须独立审查；发现问题后修复并重复受影响
  测试和审查。推送、发布、部署、远端操作、钱包/凭据、真实下单和真实资金继续需要
  各自具体授权。

### 本轮三件套补齐

- 创建 `plan/sim-system-plan.md` 草案：
  - 组件黄金实现和测试可与 DPT Gate A 建设并行；
  - 真实纵向只在 `REAL_EXECUTION_TAPE_READY` 后开始；
  - 外部归一化/Ledger 的实际闭环位于模拟器之后；
  - `SIM-AC-001` 至 `SIM-AC-048` 逐项映射到具体阶段、测试和证据；
  - 需求第 6.2 节 19 项非目标进入机械审查矩阵；
  - 资源预算先以合成负载实测冻结，再由真实规范事件重新定标，不预填未经验证的数值。
- 创建本变更记录，补齐需求、计划、changelog 的同一事项编号和相互引用。
- 需求正文允许在批准前创建明确无权威的 plan/changelog 草案；本轮创建它们不改变需求
  生命周期，不产生实施游标。

### 当前未实施事实

截至本记录创建时：

- 没有创建或修改 `sim-system` 代码、Cargo 依赖、schema 或运行配置；
- 没有运行组件符合性、真实管道纵向、训练闭环、性能分析或配置档复算；
- 没有签发任何模拟器、DPT、配置档、训练、模拟盘或实盘通过回执；
- 没有连接真实 Polymarket，没有读取钱包、密钥或 API 凭据，没有提交真实订单；
- 没有远端操作、部署、提交、推送或发布；
- 需求、计划和本记录仍为草案，当前执行 authority 和执行授权均为 `none`；
- 下一步仅为用户审核当前需求和计划，不是进入开发。

## 2026-08-04：新运行冷启动规则快照取代采集字段与固定常量

### 用户决定

- 每个新的 `sim-system` 运行必须在首个业务请求前，通过官方 Polymarket Rust SDK 按本次
  有界 `run_market_set` 的每个 `condition_id` 加载一次最新低频平台规则，校验后密封为
  本次运行不可变 `PlatformRuleSnapshot`。
- “一次”指一次冷启动加载阶段，不限制为一个 HTTP 请求；每个新运行新建不跨运行复用的
  CLOB/Gamma 客户端，并对每个目标市场只接受一个成功逻辑结果。SDK 内部传输重试不算第二
  个逻辑结果，加载器不自建重试器；成功后不得重查。运行中不轮询或热刷新；同一运行恢复
  读取已落盘原快照，快照之外的新市场必须创建新运行。
- 该决定取代 2026-08-02 “全部目标市场无条件使用
  `BTC_UPDOWN_5M_TAKER_HOLD_MS=250`”以及“低频规则由 `data-collect` 补齐”的旧方案。
  `itode=true` 时才应用配置中的 `taker_order_delay_ms=250`；false 时不应用。

### 配置与模块边界

- 仓库目标文件为 `sim-system/config/platform-rules-v1.toml`，保存无法查询的文档规则、
  项目/账户规则和查询合同；每个运行产物保存 `platform-rule-snapshot.json`，合并
  `queried_market_rules`、`documented_platform_rules`、`project_account_rules` 与
  `provenance`。
- 查询项包括市场/代币身份、`mos/mts/nr/fd.r/e/to`、`feesEnabled`、完整
  `feeSchedule.rebateRate` 和 `itode`；其中 `mts` 只作为启动基线，运行中当前 tick 仍由
  `ExecutionMarketEvent.tick_size_change` 推进。`oas` 只有字段标签而没有首版消费者和足够
  行为语义，只登记为未消费的 SDK 覆盖缺口，不扩展实现或单独阻断加载器。
- 文档规则配置还冻结批量提交/撤单上限、GTD 边界、手续费与 maker 返佣公式版本、精度、
  UTC 日桶和最低付款门槛；项目/账户区冻结可立即成交限价 BUY 的观测本金门、平台小额
  余量、Silver `8%`、正常现金可用时刻及付款截取规则。价格上下界由运行时当前 tick
  推导，精度和编码复用固定官方 SDK，不再复制为会漂移的第二套参数。
- 官方文档对批量撤单上限存在 1,000 与 3,000 的冲突；用户已于 2026-08-02 决定首版保守
  使用 `cancel_orders_max_items=1000` 并保留两侧证据，本轮不重新打开该已决事项。
- `ExecutionMarketEvent` 不再携带 `min_order_size`、`feeSchedule`、`itode` 或 `oas`；
  `data-pipeline` 只保留精确市场身份和动态市场状态。确定性核心只读已验证快照且无网络，
  冷启动加载器是唯一官方 SDK 网络边界。

### SDK 与存储事实

- 当前固定版及截至本日的官方 Rust SDK 能读出 `mos/mts/nr/fd.r/e/to` 和 Gamma
  `feesEnabled`，但 `mos/nr/fd.r/e/to` 的默认反序列化无法证明字段存在；SDK 也尚未类型化
  暴露 `itode`、`oas`、`feeSchedule.rebateRate`。首版必需字段的存在性与 `itode`、
  `feeSchedule.rebateRate` 取值使正式加载器保持阻断；`oas` 只登记覆盖缺口。实现前须由用户
  批准官方 SDK 更新或受审补丁，禁止 REST、手动 JSON、影子 SDK 和网页值旁路。
- `Client::version()` 会缓存首次成功结果，因此明确每个新运行必须新建 CLOB 客户端；同一
  运行恢复不得创建客户端或联网。
- 四日合格数据中当前 v5 没有规则专用列；删除收益为 0 字节。Gamma 原始证据 JSON 的十个
  相似键全部删除也只节省约 9,317 个压缩字节，占总数据约 0.000101%，并破坏证据摘要。
  因此 `data-collect` 不增列、不删键、不重写既有数据。

本次仅更新需求、计划和变更记录草案及 `data-collect` 当前文档；没有编写实现、创建配置
文件、修改 schema/数据、连接真实订单端点、部署、远端操作、提交或推送。事项仍为草案，
执行 authority 和执行授权仍为 `none`。

## 2026-08-04：窄型公开 HTTP 冷启动取代同日 SDK 冷启动决定

### 用户决定与取代范围

- 用户确认 `sim-system` 新运行冷启动读取公开低频平台规则时，可以不经过官方 Rust SDK，
  改由私有 `PlatformRuleHttpLoader` 直接读取固定的官方公开 HTTPS 端点。该决定取代上文同日
  “冷启动必须通过官方 Polymarket Rust SDK”以及“SDK 字段缺口阻断正式冷启动”的决定；
  上文关于每个新运行加载一次、形成不可变 `PlatformRuleSnapshot`、同一运行恢复不联网、
  运行中不热刷新以及 data-collect 不增删规则字段的其余决定继续有效。
- 这只是 `sim-system` 冷启动公开规则读取的窄例外。`live-trade` 的真实交易、签名、认证、
  订单与账户调用，以及 `data-collect` 的既有 Polymarket 接入继续遵守官方 Rust SDK 边界；
  不得借此创建交易 REST 旁路、手动签名或影子 SDK。

### 固定端点与最小实现

- 每个目标 `condition_id` 只通过 CLOB
  `GET https://clob.polymarket.com/clob-markets/{condition_id}` 读取一个所需成功逻辑结果；
  该端点没有批量版本。
- Gamma 只使用 `GET https://gamma-api.polymarket.com/markets/keyset`，每批不超过 100 项；
  `condition_ids` 必须编码为重复参数，禁止使用会被服务端静默忽略的
  `condition_ids[]=id`。加载器分别请求 `closed=false` 与 `closed=true`，再按
  `conditionId` 合并；响应中的重复、缺失和任何非目标市场都失败，不因 `next_cursor` 存在
  而扫描无关全市场，也不使用未来将弃用的旧 `/markets`。
- 实现优先复用工作区锁定的 `reqwest`、`serde`、`serde_json`、`sha2`、`rust_decimal`，并
  只为静态配置直接声明成熟的 `toml`；使用专用窄 DTO，不建立通用 Polymarket REST 客户端。
  响应先对原始字节计算 SHA-256，再进行
  强类型解析；Gamma 的 `outcomes`、`clobTokenIds` 必须二次强类型解析并按索引核对。未知
  附加字段允许忽略，禁止 `deny_unknown_fields`、`serde_json::Value` 任意探测和先转 `f64`。
- 每个新运行只创建一个无凭据 HTTP 客户端，固定 HTTPS 主机、拒绝重定向并设置连接/请求
  超时与响应体上限。只读 GET 的瞬时传输错误或官方列明的 `500/503` 在同一冷启动总时限内
  最多额外重试一次；其他错误不重试。全部目标通过后才原子密封快照，随后确定性核心必须
  在断网、无钱包和无凭据条件下运行。

### 失败即关闭语义与当前授权

- 官方 Rust SDK 未暴露 `itode`、`oas`、完整 `feeSchedule.rebateRate` 或不能证明旧 DTO
  字段存在的缺口仍是技术事实，但不再阻断上述私有 HTTP 冷启动。`itode` 缺失按官方合同
  解释为 false；`oas` 缺失且首版不消费时不阻断；费用禁用时允许 `fd`/`feeSchedule`
  省略。
- 任一请求最终失败，或必需市场/代币/结果身份、`mos/mts`、`feesEnabled`、启用费用时完整
  `feeSchedule`/`fd` 缺失、非法或跨端点冲突，仍必须失败即关闭；不得以 SDK、旧 Gamma
  字段、静态常量、网页值、其他端点或静默零值回退。
- 本次决定只更新审核中的需求、计划与变更记录，没有批准或实施代码、依赖、配置、数据、
  远端、部署、提交、推送或发布。当前执行 authority 和执行授权继续为 `none`。

### 独立终审纠正

- 终审发现计划此前只写“跨端点冲突”，不足以防止把所有近似字段做硬相等校验。现已在任务
  1.0 明确字段归属负例：CLOB `mos/mts` 与 Gamma `negRisk`/完整 `feeSchedule` 各自为规则源，
  CLOB `nr` 和 Gamma `orderMinSize`/`orderPriceMinTickSize` 只作诊断；费用启用时只核对
  `feeSchedule.rate/exponent/takerOnly` 与 CLOB `fd.r/e/to`。
- 终审还发现“费用启用但规则字段不完整时冷启动拒绝快照”与核心返佣“缺规则字段降级为
  `UNKNOWN_INCOMPLETE`”不能同时成立。现已删除后者：无效或不完整规则快照在会话/恢复进入
  核心前直接拒绝；`UNKNOWN_INCOMPLETE` 只保留给规则完整有效、但异常输入终态使潜在合格
  成交的运行时经济最终性永远无法闭合的情况。
- rebuild-core 三件套已补齐 Gamma 每批 100 项上限、未知附加字段兼容、禁止
  `deny_unknown_fields`、精确十进制和禁止先进入 `f64`，并明确以本需求第 10.1 节和对应计划
  任务 1.0 为冷启动细节唯一来源，避免总集成文档另建第二套规则。

## 2026-08-04：tick 从模拟器冷启动规则移回上游规范事件链

本节取代此前把 CLOB `mts` 写入 `PlatformRuleSnapshot`、并与动态 tick 交叉校验的决定；旧
段落只保留当时的审查历史，不再提供当前语义。

- 用户确认 tick 是每个独立五分钟市场、逐 outcome 随极端价格分化发生的市场状态，不是
  `sim-system` 新运行可从当前端点还原的低频静态规则。初始 tick 来自 `data-collect` 冷路径
  保存的时点证据，动态变化来自热/冷路径市场事件，统一经 `data-pipeline` 的
  `ExecutionMarketEvent` 传入模拟器。
- `CURRENT_QUERY` 冷启动只查询 `min_order_size`、`feeSchedule`、`itode` 等真正低频规则；
  历史运行只加载适用时点合法的 `HISTORICAL_AS_OF` 同 schema 快照。两种模式都不包含、查询
  或交叉校验 tick，当前平台响应也不得回填历史。
- 关联合同同步保留三个独立可空市场交易状态、允许未知旧 tick、来源可辨的公开成交身份；
  对固定 BTC 二元市场不再要求非 1/0 兑付向量。250ms 暂缓的 `delayed` 可见性只按冻结情景
  与真实证据表达，不再无证据断言一定出现或一定不出现。
- 用户进一步批准删除 `InitialL2SnapshotV1` 的初始/重同步用途分支。该载荷只用于逐资产首次
  完整盘口装载；后续 `book` 状态相同只确认，有净差额只经
  `BOOK_RESYNC_DELTA_INFERENCE` 转换应用，缺口和重同步边界继续使用质量合同，不建立第二条
  完整快照应用路径。
- 本次只同步审核中文档；`sim-system` 仍无代码实施、部署、远端、钱包、真实下单、提交或
  推送授权。

## 2026-08-04：需求、计划与变更记录职责迁移

### 用户批准的职责边界

- 需求文档改为产品经理视角的简洁业务目标、范围和宏观验收结果，不再承载技术框架、逐字段
  合同、执行阶段或测试清单。
- 计划文档成为当前业务语义和技术实施的唯一规范来源；它吸收仍然有效的技术合同，但草案
  状态和 `authority=none` 不变，不因此产生代码实施、部署或远端操作授权。
- 本变更记录继续只保存真实发生的事实、用户决定和取代关系。文件开头以及早期条目中把
  “当前需求正文”、需求第 10.1 节、需求第 15 节或其他旧需求章节称为当前技术权威的表述，
  自本条起均被取代，只作为历史记录保留。

### 实际修改

- `demand/sim-system-demand.md` 已收敛为业务目标、产品范围、模块边界、业务要求、非目标和
  10 项宏观验收标准；删除了迁移历史、技术框架矩阵、逐字段实现和测试登记式内容。
- `plan/sim-system-plan.md` 已纠正“只把需求转换为步骤”“需求批准后冻结当前语义”的旧开场，
  并承接当前有效的规则冷启动、规范事件消费、动态 CLOB、订单与账户、结算、产物、恢复、
  资源边界和外部闸门合同。
- 上游 tick 合同保持不变：初始与动态 tick 都只来自 `ExecutionMarketEventV1`；冷启动规则
  快照不含 tick；首次完整快照不含 `purpose`；后续 `book` 只通过
  `BookResyncDeltaInference` 应用净差额；`active`、`closed`、`accepting_orders` 独立；
  `trade_id_source` 保持可辨。
- 阶段 6 继续负责 DPT/sim 联合真实管道纵向验收，并直接使用既有联合不可变清单的内容身份
  与 SHA-256 作为唯一联合真实纵向回执；不新增登记系统或重复回执产品。阶段 7 继续负责外部
  训练闭环，并核对 `SimulationRunManifest` 的内容身份与 SHA-256。
- 跨模块说明只引用归属计划及公共合同类型、版本和内容身份，不复制 DPT 字段清单或阶段叙事；
  未新增文档守卫、读取 Markdown 的 Rust 测试、测试登记表或逐字段理论异常矩阵。
- 本轮只修改上述需求、计划与本变更记录；没有修改代码或其他文档，也没有提交或推送。

### 当前状态

- 需求仍为 `DRAFT_AWAITING_USER_REVIEW`，计划仍为
  `DRAFT_PLAN_AWAITING_DEMAND_APPROVAL`，当前执行 authority 与执行授权仍为 `none`。
- 后续动作仍是用户审查需求和计划；在两者获批前，不得开始代码实施、依赖变更、远端操作、
  部署或发布。

## 2026-08-05：驳回过度收紧的 tick 诊断审查项

用户驳回要求进一步收紧 Gamma `orderPriceMinTickSize` 解析边界的审查项，并决定保留
`plan/sim-system-plan.md` 第 169 至 172 行的当前语义，不修改 sim-system 计划。

- Gamma `orderPriceMinTickSize` 可以由窄型响应 DTO 解析并作为诊断证据可见，但它不是当前或
  历史 tick 的权威来源，不写入 `PlatformRuleSnapshot`，也不参与跨端点一致性门。
- 初始与动态 tick 的唯一权威仍是上游 `ExecutionMarketEventV1`；诊断字段不能查询、替代、
  回填或交叉校验模拟器 tick。
- 先前审查把“诊断可见”错误扩大成“不得解析”，超出了当前计划已经冻结的权威与消费边界，
  属于过度收紧，因此不采纳该审查项。
- 本轮只追加本用户决定和取代关系；没有修改需求、sim-system 计划、DPT 文档、代码或测试，
  没有提交或推送，当前执行 authority 与执行授权继续为 `none`。

## 2026-08-06：模拟平台时间与交易侧可见时间分离

- 用户纠正此前把模拟器事件与模型 Observation 放在同一时间截止线上的描述。当前决定是：
  训练和 offline-evaluation 的 `sim-system` 隐藏平台状态、订单撮合及相关平台响应，按
  Polymarket orderbook 数据的数据源事件发生时间推进；模型公开数据只在采集应用已经取得并
  解析消息的 `timestamp_received` 起可见。
- 完整的数据源事件时间隐藏回放只允许进入训练和 offline-evaluation；paper 只能使用本机
  已经接收的公开事件，live 不使用模拟器。私有订单响应可以先于对应公开市场消息对交易侧
  可见，但此时只能暴露响应自身及其明确携带的订单/账户事实，不能泄漏尚未接收的公开消息、
  盘口或隐藏平台状态。
- 受隐藏平台回放能力覆盖的事件缺少数据源事件时间时，模拟器必须失败或按已冻结范围隔离，
  不得用采集端接收时间或接纳坐标补齐平台时间。
- 审核中的需求与计划已直接改为双时间消费合同、四个请求/响应时间坐标、响应先到隔离及对应
  组件/真实纵向验收。DPT 当前因该公共合同变化定向重开阶段 0R，旧 V1 公共合同不得静默
  改义；本 sim-system 需求仍为 `DRAFT_AWAITING_USER_REVIEW`，计划仍为
  `DRAFT_PLAN_AWAITING_DEMAND_APPROVAL`，当前 authority 与执行授权均为 `none`。
- DPT 已按本轮决定实现双时间 V2 公共合同并通过受影响回归，正在等待独立阶段审查；本事项
  只同步需求、计划和本变更记录，没有创建或修改 `sim-system` 代码、Cargo 依赖、配置或运行
  产物。需求与计划仍处于审核草案，authority 和执行授权仍为 `none`。

### DPT 阶段 0R 后续关闭事实

- DPT 阶段 0R 经问题修正和第二轮完整独立复审后以
  `Approved / PASS`、`Critical 0 / Important 0 / Minor 0` 关闭；双时间 V2 公共合同现可作为
  本事项后续审核和获批实施后的外部输入合同。
- 该外部前置关闭不改变本事项状态：`sim-system` 需求和计划仍为审核草案，authority 与执行
  授权仍为 `none`；本轮没有创建或修改 `sim-system` 代码、依赖、配置或运行产物。

## 2026-08-06：阶段 6 双索引角色措辞收口

- 对 DPT 阶段 2 前置复审确认，同一 training 或 offline-evaluation 运行同时包含隐藏平台状态
  路径和交易侧公开视图路径；前者只消费 `EventTimeIndexEntryV2`，后者只消费
  `VisibilityIndexEntryV2`，两者通过同一 `event_id` 引用唯一事件，互不重建。paper 仍只能
  消费本机已经到达的可见路径。
- 阶段 6 入口已把容易误解为“每个运行只取得一种索引”的措辞改为上述路径职责，并明确门 A
  覆盖唯一事件存储及四份 manifest。该修改只消除审核草案歧义，不改变需求、公共 V2 合同或
  `sim-system` 的 `authority=none`、执行授权 `none` 状态，也不构成 `sim-system` 实施。

## 2026-08-06：同步 DPT ExecutionMarketEventV3 来源追溯合同

- 用户在 DPT 事项中批准方案 2：市场事件升级为 `ExecutionMarketEventV3`，删除各载荷内
  `source_event_ids`，只由顶层 `EventLineageV2.source_evidence` 保存直接来源证据；同时以
  `CanonicalEventRecordV1` 封闭市场事件和全局参考事件。
- 本事项需求与计划已机械同步新的公共合同版本。模拟器仍只消费
  `ExecutionMarketEventV3` 市场事件；`CanonicalReferenceEventV1` 只供 DPT 构建模型公开
  Observation，不进入模拟撮合、隐藏平台状态或私有响应。
- `EventLineageV2.source_evidence` 对模拟器保持不透明，只用于审计；平台推进顺序继续由
  DPT 事件时间投影和已冻结的同毫秒归约顺序决定，模拟器不得根据谱系内容、物理文件位置或
  `event_id` 二次排序、去重或归约。
- 本次只同步审核中需求、计划与本变更记录，没有实施 `sim-system` 代码，也不改变
  `authority=none`、执行授权 `none` 或草案生命周期。

## 2026-08-06：冻结市场结束至合格判定的订单与资金语义

- 用户纠正当前计划中“市场结束后先关闭本账户活动订单并释放资金占用”的旧描述。该描述把
  `end_at` 操作门与合格 `MarketResolved` 订单终态错误合并，本节决定及同步后的需求、计划
  取代该旧语义。
- 当前冻结为用户确认的平台行为：有效处理时点大于或等于 `end_at` 的新订单在创建订单、资金
  预留或成交前拒绝；`end_at` 至合格 `MarketResolved` 期间，已有开放订单保持可查询、可撤并
  继续保留未成交余量预留，但不再参与撮合。
- 合格 `MarketResolved` 按数据源事件时间到达时，届时仍开放的未成交余量转为官方订单状态
  合同已有的原始状态 `ORDER_STATUS_CANCELED_MARKET_RESOLVED`，并用独立幂等变化只释放该余量
  的资金预留。`end_at` 本身不写入该终态，也不释放该预留；对应订单终态反馈仍须经过适用
  返回延迟后才能被交易侧看到。
- 已成交并完成经济应用的资金已经转换为结果代币持仓，不属于开放订单资金预留。判定关闭
  订单余量后，仍须先满足成交、补偿和返佣结算屏障，再以另一账户变化身份对实际结果代币
  余额应用唯一 `AutoRedeemApplied`；订单余量释放不得冒充结果代币兑付。
- 独立复审发现，若笼统禁止私有反馈“暗示”尚未接收的公开状态，会同时禁止先到反馈自身的
  `ORDER_STATUS_CANCELED_MARKET_RESOLVED` 或结算事实，与既有双时间合同冲突。当前计划因此
  明确：响应或反馈到达后可以暴露自身携带的私有事实，但不得连带暴露对应公开消息、盘口或
  其他隐藏平台状态。
- `active`、`closed`、`accepting_orders` 继续作为互相独立的平台来源事实保留，不由
  `end_at` 或判定阶段改写，也不单独充当盘前、盘中、盘后或结算后的判断条件。DPT 现有
  `MarketIdentityV1.window.end_at_utc_ms` 与 `MarketResolutionTerminalV3::Resolved` 已经提供
  所需公共事实，因此本决定不要求修改 DPT 合同、计划或实现，也不向 DPT 增加模拟订单状态、
  权限、资金预留或自动赎回职责。
- 本次只同步 `demand/sim-system-demand.md`、`plan/sim-system-plan.md`、本变更记录，以及
  `plan/rebuild-core-plan.md` 与 `changelog/rebuild-core-changelog.md` 的跨模块验收记录；没有
  实施 `sim-system` 代码、修改 Rust 合同或运行测试，也没有改变草案生命周期、
  `authority=none`、执行授权 `none`，没有提交或推送。

## 2026-08-06：清除 DPT 陈旧任务游标副本

- 用户批准删除 sim-system 当前游标中复制的“DPT 当前任务 2.2”陈旧状态。当前计划只保留
  DPT 已升级 `ExecutionMarketEventV3`、统一事件记录、参考事件、Observation 和门 A 回执的
  稳定合同事实；DPT 当前实施游标必须回到其归属计划读取。
- 本次不改变 sim-system 需求、双时间消费语义、草案生命周期、`authority=none` 或执行授权，
  没有实施 sim-system 代码、修改公共 Rust 合同或运行其测试。

## 2026-08-06：删除等待耗尽的外部生命周期分支

- 用户批准 DPT 推荐方案：采集等待耗尽只属于 DPT typed_raw 隔离审计，不是公共平台终态、
  模型可见质量或模拟器外部输入。公共
  `MarketResolutionTerminalV3::WaitingExhaustedQuarantine` 已删除，不增加第三控制索引。
- 当前计划同步删除 `EXHAUSTED_QUARANTINED` 外部生命周期分支。输入结束仍缺少合格
  `MarketResolved` 时，模拟器只形成内部运行失败封存和隔离；该状态不能伪造公共市场终态、
  关闭订单、释放资金、结算、赎回或查询空页。
- 本次只同步审核中的 sim-system 计划和本变更记录，没有实施 sim-system 代码，也不改变
  需求、草案生命周期、`authority=none` 或执行授权。

## 2026-08-09：Gate A V2 外部前置与阶段 6 公共读取边界复核

### 已核实事实

- DPT 已正式签发成功 `GateAReceiptV3`，其绑定当前运行清单 V2、
  `CanonicalEventRecordV1`、`ExecutionMarketEventV3`、双索引 V2 和
  `MarketObservationCausalShellV3`。该事实满足未来阶段 6 的 Gate A 外部前置，但不代表
  sim-system 组件、真实纵向或训练闭环已经完成。
- DPT 当前公开 `PlatformReplayCursor` 按事件时间索引顺序返回
  `CanonicalEventRecordV1`，其中可能同时包含市场事件和参考事件；公开
  `VisibilityIndexCursor` 只返回 `VisibilityIndexEntryV2`；公开
  `ExecutionMarketEventCursor` 只按唯一事件存储的发射顺序返回市场事件。当前没有现成、
  对称、受测的模型可见索引解析市场事件游标。
- 现有公开原语仍足以形成保守的阶段 6 联合适配：事件时间路径保持
  `PlatformReplayCursor` 顺序并过滤参考事件；模型可见路径保持 `VisibilityIndexCursor`
  顺序，以有界索引页和 `ExecutionMarketEventCursor` 的完整市场事件扫描解析当前页，再按
  原索引位置输出。该组合不需要从一个索引重建另一个索引，也不需要在内存复制完整事件带，
  但可能增加扫描成本，必须由阶段 6 真实资源复核决定是否合格。

### 文档决定与边界

- 用户同意把上述实际读取边界写入计划。阶段 0 至 5 的黄金组件入口和模拟器确定性核心只接收
  有界、强类型、仅含市场事件的 `ExecutionMarketEventV3` 流；阶段 6 的 DPT/联合适配层位于
  sim-system 公共输入边界之前，只组合 DPT 已验证公开读取器，并绑定实现身份、页上限、完整性
  和资源证据。
- 若现有公开原语无法在真实输入下同时满足正确性和资源硬上限，阶段 6 必须停止并回到 DPT
  公共读取合同决策；不得让模拟器直接读 Parquet、重排、重建索引或复制完整事件带。该缺口
  不阻断阶段 0 至 5，也不要求用新代码追溯改写现有 Gate A 回执。
- 组件符合性门封存的组件运行清单内容身份与文件 SHA-256 直接作为 DPT 消费的组件回执
  ID/SHA，不新增第二回执类型或登记系统。
- 需求正文的产品目标、范围和验收标准已经覆盖这些边界，无需修改。本次只修正审核中计划的
  技术实施语义并记录事实；没有实施 sim-system 代码、修改 Cargo 依赖、运行组件测试、提交、
  推送、部署或执行远端操作。需求与计划仍为草案，当前 `authority=none`、执行授权 `none`；
  下一步仍是用户批准需求与计划后，从阶段 0 开始，不得因 Gate A 已存在而直接跳到阶段 6。

## 2026-08-09：需求、计划批准与正式实施根变更

- 用户明确批准 `demand/sim-system-demand.md` 与 `plan/sim-system-plan.md`，同意按阶段 0 至 5
  完成组件符合性，再由 DPT 阶段 3 与 sim-system 阶段 6 运行联合真实纵向的顺序开始执行。
- 用户明确把本事项的文档、代码、本地构建、测试和验收根改为当前工作树
  `/home/chillchill/sim-system`，不再以 `/home/chillchill/btc-updown-marketmaker` 作为本事项
  实施根。该路径决定不改变其他事项或部署拓扑。
- 需求生命周期更新为 `APPROVED_REQUIREMENTS`，计划生命周期更新为
  `APPROVED_ACTIVE_PLAN`；当前执行 authority 为计划，执行授权为
  `LOCAL_IMPLEMENTATION_ONLY`，当前游标进入 `阶段 0 -> 任务 0.1`。
- 批准不包含提交、推送、发布、部署、远端操作、钱包或凭据访问、真实下单和真实资金变化。
  阶段 0 开始时当前工作区尚不存在 `sim-system` crate、模拟器专用依赖、配置或运行产物；该
  事实是实施基线，不是缺陷，也不得通过预建后续阶段空壳来掩盖。

## 2026-08-09：阶段 0 `toml` 依赖现状冲突与用户决定

- 阶段 0 任务 0.1 的机械基线复核发现，已批准计划曾把 `toml` 与 `reqwest`、
  `serde/serde_json`、`sha2`、`rust_decimal` 一并表述为“正式工作区已锁定”；真实
  `Cargo.toml`、`cargo metadata --locked --offline`、`Cargo.lock` 和
  `cargo tree --workspace --locked --offline -i toml` 证明当前工作区没有 `toml` 包。
- 该事实触发方案不一致决策门。主代理停止新增依赖和技术试验，向用户列出修正文档现状、
  现在向无消费者的工作区加入依赖、删除成熟 TOML 解析路线三个选项；用户同意保守的最小
  修正方案。
- 当前计划据此明确：现有四类工具依赖已经锁定；`toml` 是静态配置的已选成熟工具，但尚未
  进入工作区。待阶段 1 创建真实目标 crate 时，先核验当时版本、许可证、安全公告和 Rust
  兼容性，再只由该 crate 精确声明并更新锁文件。不得把依赖塞入无关 crate，也不改为手写
  TOML 解析。
- 本次决定只纠正计划中的技术现状并记录决策，不改变需求业务语义、阶段顺序、授权范围或
  模块边界，也没有新增 Cargo 依赖、Rust 代码、配置、运行产物、提交或推送。
- 修正后重新执行 `cargo metadata --format-version 1 --locked --offline --no-deps`、计划措辞
  检索和 `git diff --check` 均通过。任务 0.1 自审确认：正式根、Git 身份、四个既有工作区
  成员和禁止边界一致；不存在第二个模拟器、模拟/实盘回退、影子规范化或未获准宿主接线。
  当前游标因此进入任务 0.2；这不是阶段 0 完成结论。

## 2026-08-09：固定 SDK 批量提交注释冲突与用户决定

- 任务 0.2 复核发现，固定 SDK 的 `post_orders` 文档注释曾宣称批量订单原子处理、任一订单
  校验失败即返回整体错误；这与已批准计划的逐项非原子语义不能同时成立。
- 当前官方 `POST /orders` 文档明确规定每请求最多 15 项、并行处理并逐项返回结果，示例在
  同一响应中同时包含 `success=true` 与 `success=false`；固定 SDK 的真实返回类型同样是
  `Vec<PostOrderResponse>`，每项分别携带 `success` 和 `error_msg`。因此冲突归因于 SDK
  注释陈旧，不是计划或运行实现的原子语义。
- 用户同意最小修正：只把注释改为并行处理、逐项结果和单项失败不回滚成功项，并明确只有
  请求整体失败才返回方法错误；未修改方法签名、请求、响应解析或任何可执行行为。

## 2026-08-09：sim-system 自有合同首版标识冻结

- 任务 0.2 发现，计划虽然要求错误合同版本在改变状态前拒绝，但尚未给出请求/响应、规则
  快照、三类生命周期反馈和运行清单的精确首版标识；当前工作树和完整 Git 历史均不存在这些
  sim-system schema 的既有 V1 或 V2。
- 用户指出正式样本使用 V2，要求核实为何拟冻结为 V1。机械复核确认 Gate A 样本不是统一
  版本：DPT 运行清单为 V2、回执和市场事件为 V3、双索引为 V2、统一事件记录及部分其他
  合同为 V1。版本号属于各自独立合同，不是整条链路的统一发布代号。
- 用户同意保守建议：sim-system 首次创建的执行合同、规则快照、订单/成交/结算反馈和运行
  清单分别从 V1 开始。`simulation_run_manifest_v1` 在真实纵向中原样绑定 DPT 的实际 V1/V2/V3
  合同身份，不伪装成 DPT 运行清单的后续版本，也不虚构不存在的 sim-system V0/V1 历史。
- 本次只冻结计划中的精确首版标识，不创建 Rust 类型、schema 文件、运行产物或兼容层；八个
  方法继续使用方法特定请求和响应，共用执行合同版本不增加公开 `SimRequest` 总派发类型。

## 2026-08-09：阶段 0 任务 0.2 公共合同与正式样本复核完成

- 正式输入固定为 Gate A 回执
  `bc2fdeddd1ea9a6c931823be498591ccaf32dfad61ed969f86c92b16ae3cc574`；其回执 V3 绑定
  DPT 运行清单 V2、统一事件记录 V1、市场事件 V3、参考事件 V1、Observation V3 和双索引
  V2。各合同独立版本与 sim-system 新冻结的自有 V1 合同可以同时成立。
- 使用本机 `pyarrow 23.0.1` 以每批 256 行、只读取 `payload` 列的方式有界扫描正式唯一事件
  存储；未读取 `typed_raw`、未复制完整事件带、未生成新产物。213,458 条记录中有 200,675
  条市场事件和 12,783 条参考事件；市场事件包含 2 条首次 L2 快照、2 条动态 tick、362 条
  `BOOK_RESYNC_DELTA_INFERENCE`、1 条同时保留三个独立布尔的交易状态以及 1,918 条公开成交。
- 两条首次快照的初始 tick 均来自市场事件且为正，载荷没有旧 `purpose` 字段；真实动态 tick
  见证从 `0.01` 变为 `0.001`。正式范围的 1,918 条成交均使用
  `DERIVED_SOURCE_EVIDENCE`；另一种 `PLATFORM_TRANSACTION_HASH` 由既有黄金合同正例覆盖，
  不伪造正式范围中不存在的平台哈希样本。
- 执行
  `cargo test -p data-pipeline-core --test phase0_contract --test phase0_dual_time_contract --locked --offline`
  返回 0：两个测试目标分别为 50/50 和 1/1 通过。最小异常证据固定为实际解码损坏代表
  `nonpositive_tick_is_rejected_at_decode_boundary`，已发生兼容事故回归固定为
  `initial_l2_snapshot_rejects_legacy_purpose`；不另建 DPT 逐字段重复测试。
- 任务自审及独立只读复核均未发现剩余问题，分级为严重 0、重要 0、次要 0。任务 0.2 完成，
  当前游标进入任务 0.3；该结论不代表阶段 0、模拟器组件或真实纵向已经完成。

## 2026-08-09：阶段 0 任务 0.3 成熟依赖审计与临时技术试验

### 依赖事实与风险

- 工作区当前解析 `reqwest 0.13.4`、`serde 1.0.228`、`serde_json 1.0.150`、
  `sha2 0.10.9`、`rust_decimal 1.42.1`、`rustix 1.1.4` 和固定 SDK `0.7.0`；本机
  `rustc/cargo 1.95.0` 高于这些依赖已声明的最低 Rust 版本。固定 SDK 的真实来源是官方
  `0.7.0` 基线与根 `[patch.crates-io]` 指向的唯一 `0.7.0-p1` 供应商目录，不能描述成未修改
  的注册表包。
- `toml` 尚未进入工作区。截至 2026-08-09 的候选为注册表身份
  `1.1.4+spec-1.1.0`，只需 `parse/serde/std`；正式目标 crate 创建时仍须重新核验并精确
  入锁，不在阶段 0 向无消费者的既有 crate 塞入依赖。
- 定向核验没有发现上述目标包版本的适用安全公告；该结论不是一次完整自动化锁文件审计。
  既有 `data-collect` 因 SDK `rtds/ws` 激活的 `backoff 0.4.0` 被 RustSec
  `RUSTSEC-2025-0012` 标记为停止维护、且无修复版本；它不是已知漏洞，也不是 sim-system
  所需 `clob` 路径引入的依赖。本事项只记录该中等级现存风险并禁止模拟器启用 `rtds/ws`，
  不越界替换采集重试库或扩大 SDK 补丁。
- `rust_decimal 1.42.1` 的通用 Serde 实现即使启用任意精度相关特性仍保留 `visit_f64` 路径，
  不能满足“绝不先进入 `f64`”的证明。当前路线改为专用强类型读取 `RawValue` 原始数字词法，
  字符串只解码为 `String`，两者再交给 `Decimal::from_str_exact/from_scientific`，并拒绝其他
  JSON 类型；不使用 `serde_json::Value` 任意探测。

### 一次性技术试验

- 试验根为
  `/home/chillchill/sim-system/.sim-system-phase0-tech-trial.15e1YQ`，包含一次性独立 crate、
  独立锁文件、独立 `target`、密封目录和零字节 `network.trace` 证据文件。它不是工作区成员，
  也没有创建正式 `sim-system` crate；根 `Cargo.toml` 与 `Cargo.lock` SHA-256 始终分别为
  `31c915cf967fad063582122b8c0f1866a632eef1354d69013c919f11f4f98955` 和
  `dde8bcd1426182b2c55e8da49b3d38be964f8deaa3faa5863edf7669f7634c20`。
- 临时清单、源码和最终锁文件 SHA-256 分别为
  `c47f3a2d2e2cbdbb1218edb3714235257294b7f64a208656cd8f0451bf7ccd56`、
  `c67c929eb0a649e48ef124ca97010036797d107c1817d16269647bb2a373d467`、
  `1d0760c5a6c8a9a4d5c176a16a51b98dc0cf4c5f410f353a5c977b84c08183e0`。独立生成锁文件时
  五个传递包最初选择了本机缓存中的更新补丁版本；试验在运行前用 `cargo update --precise`
  收敛到根锁文件版本，最终 140 个依赖包的名称、版本和来源全部是根锁图子集。
- 试验只构建两个固定 Gamma `markets/keyset` 请求而不发送，证明两个重复
  `condition_ids` 保持顺序、`closed=false/true` 分离、没有 `condition_ids[]` 或逗号拼接，
  并使用禁重定向客户端。固定 CLOB/Gamma 原始响应先分别形成 SHA-256，再进入忽略未知字段
  的窄 DTO；数字和字符串两种线形态精确得到同一 28 位 `Decimal`，JSON 编码的
  `outcomes/clobTokenIds` 再解析为强类型列表。
- 同一父目录候选在写入与同步后通过 `RENAME_NOREPLACE` 密封并同步父目录；第二个不同候选
  撞击同名目标得到 `AlreadyExists`，已密封文件 SHA-256 保持
  `fe21700e92b4a324873bb21ab714bb5599aa2a88dfccfe6dcbef74c56a652e36`，碰撞候选 SHA-256 为
  `81029ca79330220705e5c8a25993b5b63189c964042c0a6b02a047e21a5e1dd8`。
- 最终程序唯一标准输出为
  `phase0_tech_trial=PASS query=PASS raw_sha=PASS dto=PASS decimal=PASS seal=PASS collision=PASS`，
  退出码为 0；`strace -f -e trace=network` 记录文件为 0 字节。试验前后
  `git status --porcelain=v1 -z` 摘要均为
  `762bebddba8490f9974b40576ec0b84031b25a2e107f8293ba9e3178d701864b`。
- 证据收齐后只把经过非空、非符号链接、固定父目录和固定前缀核验的精确临时根移入回收站；
  原路径已经不存在，操作可从桌面回收站恢复。没有网络请求、正式规则快照、生产实现、根锁
  文件变更、提交或推送。

## 2026-08-09：阶段 0 任务 0.4 资源测量预登记

- 当前正式 Gate A 范围再次证明来源计数不能互换：6 个所选 `typed_raw` 文件共有 1,677,687
  行，生产路径在接收时间和市场范围过滤后解码 122,498 行，生成 200,675 个
  `ExecutionMarketEventV3`、12,783 个参考事件、213,458 行唯一事件存储、213,454 行事件
  时间索引和 213,458 行模型可见索引。这些是 DPT 来源与产物事实，不是模拟器容量。
- 计划据此预登记 `COMPONENT_GOLDEN` 与 `REAL_VERTICAL` 两种负载，冻结唯一事件与双路径投递、
  八方法调用与批量项、唯一成交与三类反馈、查询分页与返回行、至少两个完整保留滚动周期的
  独立计数边界。模拟器核心的参考事件计数必须为零；所有计数必须能由输入和运行证据重算。
- 性能口径只使用单调墙钟，分开记录吞吐、p50/p95/p99/最大执行延迟、整次墙钟、CPU、RSS、
  cgroup、文件描述符、读写字节、产物字节以及各内部队列和状态高水位；模拟逻辑时间和正式
  配置档时延都不能冒充执行性能。
- 阶段 0 只冻结负载单位、计数边界、测量方法和证据身份，不冻结任何吞吐下限、延迟上限、
  并发量、资源配额、保留期长度、负载倍率或重复次数。阶段 4 必须先密封确切黄金负载再查看
  正式测量并冻结数值；阶段 6 使用成功 Gate A 覆盖的真实范围单次重新定标，不循环或复制
  真实事件，并将联合适配层与模拟器核心资源分栏记录。
- 任务 0.4 自审和独立只读复核均通过，分级为严重 0、重要 0、次要 0。阶段 0 的四个任务已
  完成，当前只进入阶段完成门自审与独立阶段审查；尚未宣布阶段 0 完成，也未进入阶段 1。

## 2026-08-09：阶段 0 完成门自审与跨模块来源说明冲突

- 完成门自审把当前计划中遗留的类型化原始数据字段名 `timestamp_received` 收口为模拟器实际
  消费的公共事件字段 `received_at_utc_ms`，并把本变更记录首屏从草案期同步为实施记录；这两项
  只纠正当前文档身份和公共字段名，不改变双时间业务语义、DPT 原始输入 schema 或历史条目。
- 当前分支 `sim-system` 的 `HEAD` 与 `origin/sim-system` 均为
  `2ea4e28eee5332ef537e365a11665ef28e760933`，分歧为 `0/0`。根 `Cargo.toml` 与 `Cargo.lock`
  SHA-256 仍分别为 `31c915cf967fad063582122b8c0f1866a632eef1354d69013c919f11f4f98955` 和
  `dde8bcd1426182b2c55e8da49b3d38be964f8deaa3faa5863edf7669f7634c20`，两者均无差异；工作区
  仍只有四个既有成员，没有 `sim-system` crate、专用配置、schema 或生产产物。
- `cargo fmt --all -- --check` 与固定 SDK 独立清单的 `cargo fmt --manifest-path ... -- --check`
  均退出 `0`；后者只输出稳定工具链不支持 `group_imports` 的既有警告，没有格式差异。
  `cargo check --workspace --all-targets --locked --offline` 和
  `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` 均退出 `0`。
- 仓库规定的全工作区回归
  `cargo test --workspace --lib --bins --tests --examples --locked --offline -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second`
  连续两次退出 `0`。具名 UTC 时钟夹具测试按命令明确跳过，没有写成通过；已有需要本地完整
  数据集的 `ignored` 测试仍按各自注解忽略，也没有写成通过。
- `git diff --check` 退出 `0`；发现下述冲突前只有七个预期的已跟踪文件差异，没有暂存或未
  跟踪文件。阶段 0 尚未完成，也未进入阶段 1、提交或推送。
- 后续依赖来源与完整差异辅助复核发现：`data-collect/docs/current/contracts.md` 仍明确写固定
  SDK 相对 crates.io 0.7.0 的生产 `src/` 差异横跨四个文件；当前已批准的
  `clob/client.rs` 批量非原子语义注释修正使机械比较结果变为五个差异文件。该第五项只有
  文档注释差异，不改变方法签名、方法体、返回类型或运行行为，但“横跨四个文件”与真实源码
  范围已经不能同时成立。
- 该事实触发方案不一致决策门。全新独立阶段审查未启动，阶段 0 完成门停止在用户决定前；
  当前未修改 `data-collect` 派生参考，也未自行撤销已批准的 SDK 注释修正。保守选项是把该参考
  收口为“四个可执行行为补丁文件，另有一个 `clob/client.rs` 文档注释修正”，明确不扩大运行
  补丁语义；是否采用须由用户决定。
- 用户批准上述保守最小方案。`data-collect/docs/current/contracts.md` 现准确记录五个生产
  `src/` 差异文件，并明确区分四个可执行行为补丁文件与一个只修正 `post_orders` 批量语义
  注释的 `clob/client.rs`；后者不改变方法签名、方法体、请求、响应解析或运行行为。该派生
  参考同步不改变 data-collect 需求、计划、schema、测试或运行语义，因此没有扩展到该模块的
  三件套或当前外部磁盘事项。阶段 0 停止点解除，进入全新独立阶段审查。

## 2026-08-09：阶段 0 独立阶段审查通过

- 全新 `gpt-5.6-sol/ultra` 独立阶段审查结论为 `PASS`，分级为严重 0、重要 0、次要 0。
  审查独立核对当前八个已跟踪差异、无暂存或未跟踪文件、根 Cargo 清单与锁文件无差异、四个
  既有工作区成员和不存在阶段 1 模拟器实现；并核对需求、计划、本变更记录、AGENTS 路由、
  DPT 外部前置和 data-collect 当前派生参考可以同时成立。
- 审查机械确认正式 Gate A V2 的回执、版本、16 项能力和产物计数，三种公开游标与阶段 6
  适配边界，以及官方注册表 SDK 与供应商 `src/` 恰有五个差异文件。其中四个含可执行行为
  补丁，`clob/client.rs` 只有已批准的注释修正；`post_orders` 的真实返回类型和逐项结果字段
  支持该注释，未形成第五条运行补丁语义。
- 独立执行 `cargo test -p data-pipeline-core --test phase0_contract --test phase0_dual_time_contract --locked --offline`
  取得 50 项与 1 项通过；执行
  `cargo test -p data-collect --test polymarket_sdk_patch_contract --test polymarket_sdk_runtime_contract --locked --offline`
  取得 21 项与 20 项通过。`git diff --check`、`cargo metadata --locked --offline --no-deps` 和
  相关 Git/Cargo 边界核对均退出 0。此前固定全工作区回归的两次退出 0 继续作为阶段证据；
  具名 UTC 时钟测试仍明确记为跳过，不写成通过。
- 阶段 0 的当前合同、成熟依赖路线、实施边界、测量方法、任务自审和独立阶段审查均满足完成
  门，阶段 0 至此关闭。剩余非阻断风险是阶段 6 适配器尚未实现、`toml` 仍须在实际引入前重新
  核验、既有 `backoff 0.4.0` 维护风险仍存在，以及 Gate A 仍正确保持
  `training_dataset_ready=false`；这些边界均已由后续阶段或外部门拥有，不反向重开阶段 0。
- 当前没有提交或推送，也没有进入阶段 1。按阶段关闭规则，须等待用户对阶段 0 当前完整差异
  的提交与推送作出另行授权；该授权完成前不得开始阶段 1。
