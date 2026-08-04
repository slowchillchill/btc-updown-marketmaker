# sim-system 变更记录

> 事项编号：`SIM-SYSTEM-20260731`
>
> 生命周期：`DRAFT_REQUIREMENTS_CHANGELOG`
>
> 当前执行 authority：`none`
>
> 对应需求：`demand/sim-system-demand.md`
>
> 对应计划：`plan/sim-system-plan.md`
>
> `data-collect` 当前权威入口：`data-collect/docs/CURRENT.md`
>
> 执行授权：`none`
>
> 部署、远端操作和真实资金授权：`none`
>
> 状态：`只记录需求演进、冲突和决定；没有代码实施、部署或远端执行`

本记录按发生时间保存需求演进。较早条目只说明当时发生了什么；如果与后续用户决定冲突，
以后续明确取代条目和当前需求正文为准。草案需求、计划和本记录都不产生实施、部署、远端
操作、钱包/凭据、真实下单或真实资金权限。

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
