# rebuild-core 审核中实施计划

> 事项编号：`rebuild-core-demand`
>
> 生命周期：`DRAFT_PLAN_AWAITING_DEMAND_APPROVAL`
>
> 当前执行 authority：`none`
>
> 对应需求：`demand/rebuild-core-demand.md`
>
> 对应 changelog：`changelog/rebuild-core-changelog.md`
>
> `data-pipeline(training)` 决定来源：`demand/data-pipeline-training-demand.md`、
> `plan/data-pipeline-training-plan.md`、`changelog/data-pipeline-training-changelog.md`
>
> `data-collect` 当前权威入口：`data-collect/docs/CURRENT.md`
>
> 执行授权：`none`
>
> 状态：`仅供审核；需求与本计划均获批前，所有实施阶段均为 BLOCKED`

本计划是 `rebuild-core-demand` 当前业务语义和技术实施的唯一规范来源，负责把跨模块顺序、
交付边界、总集成责任、成熟工具复用、阶段完成门和最终验收连成一条可审计链。需求只保留
产品经理层面的目标、范围和验收结果；changelog 只记录当时事实与后续取代关系。本计划不
替代已经单独建立的 `data-pipeline(training)` 或 `sim-system` 归属计划，也不把未来
`data-collect` paper/live 真实适配器、`offline-evaluation`、`sim-calibration` 或其他独立事项
的审批门并入本文。

需求批准只冻结需求范围；需求与本计划均获批后，本文明确覆盖的本地实现、测试、本地
不可变测试产物、成熟依赖的本地选择/锁定/构建安装，以及阶段独立审查通过后的本地 Git
提交均取得实施游标，不再追加第三个通用“实施授权”。该游标不授权未受信外部脚本、特权
或远端安装、联网/凭据例外、Git 推送或外部发布、远端状态变更、部署、敏感凭据读取、真实
下单或真实资金。需要独立三件套、外部状态变更或单次资金授权的阶段，仍须在对应批准存在
后才可进入。

## 1. 成功标准、权威来源与停止条件

### 1.1 总成功标准

1. `demand/rebuild-core-demand.md` 的业务目标、范围和验收结果全部具有真实、可重放证据，
   不能以“代码已写”、固定 Markdown 文本或控制台输出代替通过。
2. 每个技术门都有版本化输入、判定、证据和范围特定影响；没有通用“一项异常全部停机”或
   把证据不足冒充模型失败的路径。
3. 目标仓库只有一套数据语义规范化核心、一套执行归一化实现、一套 Ledger 应用语义、
   一套 SDK 对齐执行方法面；不存在影子流水线、影子订单解释器、影子账本或影子 SDK。
4. 当前主线顺序严格为：已完成 `data-collect(training)` ->
   `data-pipeline(training)` 门 A -> `sim-system` 黄金事件与真实管道事件 ->
   外部归一化与独立交易侧 Ledger 能力就绪 -> `data-pipeline(training)` 门 B ->
   `model-train` -> 独立 offline-evaluation -> paper -> live shadow -> 逐次用户授权的真实资金阶段。
5. 正式数据处理只读取谱系源自 `/home/chillchill/new-datasets`、UTC 日不早于
   `2026-07-30`、经显式关闭清单准入并被版本化切分分配的数据；更早数据在读取入口即失败。
6. training、offline-evaluation、paper、live 使用同一处理核心、合同和可回放语义，但
   进程、队列、检查点、缓存、运行根、模拟器和 Ledger 状态实例彼此隔离。
7. 所有主动订单保持 `BUY-only`；模拟路径只到 `sim-system`，实盘路径只委托官方
   Polymarket Rust SDK；实盘执行能力的 SDK 缺口按决策门停止受影响路径。模拟规则冷启动
   使用需求已限定的官方 HTTPS 只读加载器，不以 SDK 字段覆盖情况作为阻断条件。
8. 只有全部前序阶段、当前技术门和最终验收通过，且三件套与实现事实同步，才可提议将本
   事项标记完成。

### 1.2 当前事实与权威顺序

- 正式本地实现、构建和本地验收根为 `/home/chillchill/btc-updown-marketmaker`；当前
  `/home/chillchill/sim-system` 只是文档与分支工作树，不是部署根。
- `data-collect` 现状只能由 `data-collect/docs/CURRENT.md`、当前代码和当前测试确定；本文
  不重开已完成事项，不恢复旧游标。
- `data-pipeline(training)` 当前市场事件合同为 `ExecutionMarketEventV3`。当前游标、门 A/门 B
  实施和回执签发只以
  `plan/data-pipeline-training-plan.md` 为准。本文只消费公开类型、产物和回执，并规定它们在
  总链路中的前后关系；公共类型存在不得写成门 A 已经签发。
- `sim-system` 的领域技术细节只以 `plan/sim-system-plan.md` 及其消费的公共 Rust 市场事件类型为
  准；`demand/sim-system-demand.md` 只提供宏观范围与验收。若归属计划、本文、公共类型或
  真实代码不能同时成立，必须停止并交由用户决定，不能自行调和。
- 官方文档、官方 HTTPS 规则响应、锁定官方 Rust SDK、真实产物和用户冻结的项目规则假设
  必须分别标注来源；项目假设不得写成 Global 官方事实。

### 1.3 必须停止并交由用户决定的情况

- 用户尚未批准 `demand/rebuild-core-demand.md` 或本计划；
- 需求、计划、当前代码、当前 schema、真实数据或已批准子事项出现不能同时成立的冲突；
- `sim-system` 进程启动时参考市场的官方 HTTPS 响应缺失、非法、重复、查询失败或跨端点
  冲突，参考身份、CLOB `mos`、`itode`、费用启用状态或适用费用规则无法闭合，原始响应哈希
  无法留存，或无法在快照密封后释放网络能力；此时停止整个模拟器进程初始化，不得把责任转回
  `data-collect` 或 DPT。官方 Rust SDK 未类型化覆盖所需字段本身不再构成本加载器的阻断；
- 官方 SDK 对提交、撤单、查询、认证、签名、响应解析或其他受影响能力覆盖不足，继续工作
  需要原始 REST、手工签名、自建 SDK 或其他旁路；
- PyO3/maturin、解释器、SOABI、glibc、Rust 最低版本或训练进程隔离的实测结果要求改变
  已冻结环境、版本或进程拓扑；
- sim-system 归属计划的首版项目代理时延配置档尚未达到
  `PROJECT_PROXY_PROFILE_READY`、身份不一致，而任务需要正式模拟代理时延；
- 需要创建未来未批准模块、返回 `data-collect` 实现 paper/live 真实适配器、运行未受信
  外部脚本、执行特权/远端安装、需求已限定的模拟冷启动只读加载器之外的联网或任何凭据
  例外、Git 推送/外部发布、远端状态变更、部署、读取钱包、真实下单或使用真实资金；
- 任一阶段无法证明确定性、因果边界、数量/现金守恒、依赖闭包或资源硬上限。

### 1.4 通用阶段关闭规则

每项任务完成后必须自审差异、合同、测试、来源追溯和剩余风险。每个阶段的任务与验证全部
完成后，必须由独立审查代理检查需求映射、影子路径、过度设计、边界遗漏和证据真实性；
发现可执行问题时修复并重复审查。未运行、失败、过滤或依赖未具备的验证不能写成通过。
测试以真实样本、已发生事故和每类可达不可信边界的一个代表输入为主，不建立测试登记表、
理论异常矩阵、文档守卫或读取 Markdown 固定文本的 Rust 测试。
需求与本计划获批后，阶段独立审查通过的本地 Git 提交属于本地实施游标；Git 推送、外部
发布、远端状态变更、部署以及特权/远端安装仍须对应的明确授权。

### 1.5 当前业务链路与模块所有权

1. 项目只支持 Polymarket Global CLOB `btc-updown-5m`，主动动作面固定为 `BUY-only`。
   training、offline-evaluation、paper、live 共享版本化合同和实现代码，但各自拥有隔离的进程、
   输入权限、状态、检查点、运行根、模拟器会话和 Ledger 实例。
2. 固定交付顺序为：已完成 `data-collect(training)` -> DPT 门 A -> `sim-system` 黄金/真实事件
   联合验收 -> 外部归一化与独立交易侧 Ledger 就绪 -> DPT 门 B -> `model-train` -> 独立
   offline-evaluation -> paper -> live shadow -> 逐次用户授权的真实资金阶段。
3. `data-pipeline(training)` 是类型化原始证据到规范市场事件的唯一语义归属方。DPT 门 A、
   门 B、版本化公共类型、产物和回执都由 `plan/data-pipeline-training-plan.md` 实现与签发；本文
   不解码原始 schema、不排序或去重原始证据、不构建 DPT 产物，也不签发 DPT 回执。
4. `sim-system` 只消费公共市场事件和已冻结规则快照并输出模拟平台原始证据；具体方法、规则、
   状态机、资源和三层验收只由 `plan/sim-system-plan.md` 拥有。本文不复制其字段清单、阶段状态
   或测试矩阵，只验证总集成身份和顺序。
5. 模拟与真实平台原始证据共享一套外部归一化合同和 Ledger 应用语义，但严格分流；live 不得
   调用模拟器，模拟证据也不得进入 live 状态。模型和 DPT 只读本实例 Ledger 投影，不拥有
   归一化或 Ledger 写权限。

### 1.6 DPT 版本化公共消费合同

1. rebuild-core 直接消费仓库公共 Rust 类型 `CanonicalEventRecordV1`、
   `ExecutionMarketEventV3`、`CanonicalReferenceEventV1`、`EventTimeIndexEntryV2`、
   `VisibilityIndexEntryV2`、`MarketObservationCausalShellV3` 和 `GateAReceiptV3`，不另建
   总集成副本。公共类型与
   `plan/data-pipeline-training-plan.md` 不一致时必须停止，不能在本文兼容猜值。
2. `CanonicalEventRecordV1` 封闭市场事件和全局参考事件，并作为唯一事件存储记录；不建立
   平行事件带，也不按市场复制参考事件。所有模型可见事件进入接收时间可见索引，只有存在
   真实数据源事件时间的事件进入事件时间索引；每个索引 ID 都必须解析到唯一存储记录。
   训练与离线评估的隐藏平台状态及 `sim-system` 只消费 `ExecutionMarketEventV3` 市场事件的
   数据源事件时间投影；交易侧公开视图和 `MarketObservationCausalShellV3` 可以引用市场与
   参考事件的接收时间可见投影。隐藏平台状态不得进入模型公开视图，模拟盘不得取得完整历史
   隐藏回放，实盘不得调用模拟器。只有具有真实数据源事件时间的合格市场判定才能驱动隐藏
   平台退休、资金释放和结算；只有接收时间的公开结果证据不能用市场结束时间、接收时间或
   本地时间补成平台判定时间。受影响范围只能得到 DPT 失败诊断且不存在门 A 回执；若另行
   显式缩小范围并重新验收，总集成只能消费该新范围成功回执实际覆盖的结果。采集等待耗尽
   摘要只属于 DPT 隔离审计，不是公共市场终态；总集成不得为它增加第三控制索引或模拟器外部
   生命周期分支。
3. 当前 `ExecutionMarketEventV3` 的总集成兼容边界是：首次 `InitialL2SnapshotV2` 必须携带
   逐资产正数 `initial_tick_size_pusd_per_share`，且只表示首次装载、没有 `purpose` 用途分支；动态
   `TickSizeChangeV2.old_tick_size_pusd_per_share` 可以未知，`new_tick_size_pusd_per_share`
   必须存在；`MarketTradingStateV2` 分别表达可空的 `active`、`closed`、
   `accepting_orders`，不得压成组合状态机；`PublicTradeV2.trade_id_source` 必须区分平台交易
   哈希与派生源证据。所有原含 `source_event_ids` 的嵌套载荷已经随市场事件 V3 同步升版并
   删除该字段；`EventLineageV2.source_evidence` 是唯一来源追溯，rebuild-core 不解析它来
   重建排序、去重或归约。
4. 后续完整 `book` 不得再次产生初始快照。状态相同时只确认，存在唯一净差额时只通过
   `LevelExecutionBasis::BookResyncDeltaInference` 及其既有归因/队列质量边界表达和应用一次；
   质量事件负责缺口与重同步，不建立第二条完整盘口应用路径。
5. tick 的唯一权威链为 `data-collect -> data-pipeline -> ExecutionMarketEventV3 ->
   sim-system`。CLOB `min_order_size`、完整 `feeSchedule`、`itode`、`oas` 和秒级延迟规则不进入
   DPT 市场事件或 Observation；模拟器规则快照不得查询、保存或交叉校验 tick。
6. 门 A 只接受 DPT 原子签发、能够按公共合同解码的 `GateAReceiptV3`：其绑定的
   `GateACapabilityMatrixV2` 必须全部为 `CapabilityStatus::Pass`，
   `training_dataset_ready` 必须为 false，统一事件记录、市场事件、参考事件、Observation、
   双索引合同版本、输入与实现身份以及唯一事件存储、
   事件时间索引、模型可见索引和 Observation 四份清单摘要必须一致；不读取回执中不存在的
   `status`、`verdict` 或 `artifact_kind`。上述条件共同关闭
   `REAL_EXECUTION_TAPE_READY`。能力失败、准入失败、构建失败或运行中断只构成诊断。
   门 B 同理由 DPT 在全部前置和联合证据通过后唯一签发 `TRAINING_DATASET_READY`；本文只能
   机械验证并消费，不能代签或改变状态。

### 1.7 执行、模拟、经济与账本合同

1. 共用 Intent Bridge 的公开方法精确为 `post_order`、`post_orders`、`cancel_order`、
   `cancel_orders`、`cancel_all_orders`、`order`、`orders`、`trades`。模拟后端使用无签名、
   无凭据、无交易网络的模拟领域类型；实盘后端只委托官方 Rust SDK 的对应能力，并只在提交
   时使用 `SignedOrder`。后端在实例创建时固定，不能互相回退。
2. `sim-system` 进程规则初始化只使用其归属计划限定的两个官方公开 HTTPS `GET` 端点，
   对一个参考市场同步一次并形成不可变 `PlatformRuleSnapshot`；进程内全部会话共享，历史
   进程只消费适用既有快照。该例外不进入 `data-collect`、DPT 或 live，也不扩展为
   通用 REST、任意 JSON 探测、认证、签名或交易客户端。
3. FOK/FAK 与 GTC/GTD 使用各自的业务数量口径和官方 SDK 编码等价投影；实际 maker/taker
   角色由成交分段决定。平台 dust、未知提交、批量混合结果、撤单竞态、迟到/重复反馈、结算
   和预留释放必须保持份额与 pUSD 两套守恒，且同一经济变化只应用一次。
4. 同价 FIFO 是用户冻结的项目规则假设，不写成 Global 官方保证。公开 L2 不能恢复真实逐单
   队列；首版后续 L2 净缩只使用 `L2_BOOK_DELTA_FIFO_ESTIMATE_V1` 的确定性推断边界，不能
   在同一版本增加另一套队列情景。
5. maker 与 taker rebate 均按首版项目/账户假设执行。maker 按进程快照中固定 20% 的规则，
   taker 按实际费用与固定 8% 的账户规则计算；不实现动态档位、奖金或 Liquidity Rewards。两类
   应计、日桶、付款和锁定余数彼此分离。应计只增加一次经济价值，
   付款只迁移应收款与现金分桶，不再次改变 NAV、PnL 或训练奖励。精确日结、门槛、截取和
   异常最终性语义由 `plan/sim-system-plan.md` 实现，本计划只验证外部归一化与 Ledger 不重算。
6. 每个运行实例的本地仅追加 Ledger 是现金、预留、持仓、订单、返佣应收款、NAV 与 PnL 的
   唯一热路径权威；钱包余额和 allowance 只用于启动/权限/对账，不能成为第二账本。

### 1.8 模型、评估、调度与晋级合同

1. 模型或模型包状态机唯一产生交易、风险和调度意图。`trading_risk_intent` 保持 BUY-only；
   `decision_schedule_intent` 只由 Model Runtime 调度器消费，不能进入执行桥或被改写为交易。
2. 模型调用只由可回放的业务、账本、生命周期、经 Observation 合同准入的质量事件、单次
   bootstrap 或上一轮安排的稀疏 wake 触发。固定周期、`tick_stream` 和仅采集器可见诊断不能
   触发策略。确定性重新设定唤醒基线与离散 `next_wake_bucket` 候选使用同一 Observation、
   模拟器、Ledger、奖励、场景、随机种子和资源合同，以 SMDP 真实经过时间公平比较。
3. 当前两种交易盈利机制是互补结果侧确认成交后的全成本折价配对，以及基于因果可见信息
   买入判断正确侧并持有至结算；手续费与返佣只是经济组成，不是第三种策略。训练、评估与
   报告必须分别归因并与账户 Ledger、NAV 和已结算 PnL 闭合。
4. 训练内指标只负责诊断和选优，不能代替独立 offline-evaluation。候选冻结后才可解析未见
   时间外留出集；offline-evaluation 只读运行并签发本阶段凭据，随后依次经过 paper、live
   shadow 和三档逐运行授权的真实资金阶段。ModelRegistry、晋级阶段、资金授权与执行权限
   始终分轴。
5. 训练任务、工作进程、账户轨迹和市场历史是四种不同身份。首个场景中的 `2000 pUSD` 属于
   每条独立反事实账户轨迹，不属于整个训练任务的共享池；同一轨迹跨日期、市场和数据物理边界
   严格因果串行，隔离轨迹可以并行。多个轨迹的本金或 PnL 不得合并成一个账户，同一市场时间带
   的多次反事实重放也不得冒充更多独立市场历史。
6. `model-train/native` 以版本化回合合同唯一产生 Gymnasium 回合状态。首版只有在同一模型可见
   截点上同时满足以下条件时才允许 `terminated=true`：`LedgerProjectionV1` 中精确微单位的
   可用现金为零；`plan/trading-ledger-plan.md` 规定的全部可恢复经济分桶已经为零或取得不可
   恢复终态；平台确认
   前缀、本地未决前缀及可能改变资金的反馈全部闭合；数据、合同、守恒和因果证据有效。一次资金
   不足、当前没有合法 BUY、正余额低于当前市场最小订单或只有 Ledger 单侧显示零现金均不得提前
   终止；此类非终态只允许继续推进下一项因果事件或到达预登记外部截止，不得创建新资金。负
   余额、摘要冲突或缺失必需终态属于失败，不属于资金耗尽。
7. 预登记的模型决策范围截止属于外部边界，使用 `truncated=true` 并保留最终 Observation、
   自举和删失语义；原始输入意外结束、缺少合格平台终态或经济成熟证据不完整时必须失败或隔离，
   不能伪装成截断、资金耗尽或零收益。若未来把固定时域改成任务自身的有限时域，必须新版本化
   回合与 Observation 合同并让模型可见剩余时域，不能静默重解释首版。
8. training 中合法资金耗尽只结束当前回合，不停止训练任务。训练运行清单必须在结果产生前冻结
   回合队列及每条轨迹的数据范围、集合、种子、sim 会话、账户实例和开户锚点；`reset` 只能取
   队列中的下一条独立轨迹并创建新身份、新 sim 会话、新 Ledger 和新开户锚点，不能向原轨迹
   补回 `2000 pUSD`，也不能按提前耗尽时刻自适应选择新起点。offline-evaluation 只运行预冻结
   清单，不因候选提前耗尽追加替代轨迹；paper 与 live 不使用 training 自动重置，live 也没有
   模拟初始资金。
9. 每条终止或截断转换必须先完整保留最终 Observation、奖励、Ledger 投影和边界原因，再允许
   新回合；循环模型隐藏状态不得跨开户锚点。候选共享同一预登记回合队列或等价固定配额，避免
   早耗尽工作实例通过频繁重置获得更多本金并过采样轨迹早段。报告分别给出逐轨迹最终 NAV、
   收益率、资金耗尽率与耗尽时间、最大回撤和版本化尾部风险统计，并分开报告独立市场历史数量
   与反事实轨迹数量；不得以跨账户求和冒充单账户表现。

### 1.9 成熟工具复用与自研边界

1. 实盘 Polymarket 能力复用仓库锁定的官方 Rust SDK 与唯一受审补丁；SDK 不足时停止受影响
   路径并向用户报告，不自研签名、认证或交易旁路。模拟规则冷启动复用现有 `reqwest`、
   `serde`、精确十进制与 SHA-256 依赖，只实现需求限定的私有窄读取边界。
2. DPT 的 Arrow/Parquet、外部排序和公共合同工具选择完全引用其归属计划及根锁文件，本文不
   复制依赖版本或另做数据基础设施选型。
3. `model-train` 目标组合继续采用 PyO3 进程内粗粒度绑定与 maturin 构建打包。现有候选快照
   为 PyO3 `0.29.1`、maturin `1.14.1`；实施前必须重新核实官方发布、安全公告、解释器、
   SOABI、体系结构、glibc、Rust 最低版本和目标训练服务器安装矩阵。若结果要求改变环境、
   版本或进程拓扑，必须停下由用户决定，不得临时建立纯 Python 影子实现。
4. 模型训练优先复用成熟训练框架、ONNX/ONNX Runtime、Optuna、MLflow 及适用的公开模型库，
   但任何候选只有通过同一数据、动作、模拟器、Ledger、奖励、复现、资源、导出和 Rust 运行
   时一致性门才能进入比较。当前计划不预定胜出框架，也不自研通用训练、调度、注册或插件
   基础设施。
5. 截至 2026-08-11，Gymnasium 官方时间边界教程声明兼容 `1.3.0`，明确区分环境内终止与环境外
   截断及其不同自举语义；官方向量环境从 `1.1` 起提供 next-step、same-step 和 disabled 三种
   自动重置模式，发布页将标签 `v1.3.0`、提交 `53bf3e9` 标为最新版本，官方仓库采用 MIT
   许可证。本项目复用其 `terminated/truncated`、
   环境检查与显式向量重置能力，不自研另一套环境协议；为保留最终状态并执行预登记回合队列，
   首版必须显式选择并验证自动重置模式，不能依赖库默认值。依据：
   [Gymnasium 时间边界教程](https://gymnasium.farama.org/tutorials/gymnasium_basics/handling_time_limits/)、
   [Farama 向量环境自动重置模式说明](https://farama.org/Vector-Autoreset-Mode)、
   [Gymnasium 发布记录](https://github.com/Farama-Foundation/Gymnasium/releases)、
   [Gymnasium 官方仓库](https://github.com/Farama-Foundation/Gymnasium)。
   首版向量训练明确冻结 `AutoresetMode.DISABLED`，由收集器在终态证据持久化且预登记下一轨迹
   就绪后，使用 `reset(options={"reset_mask": mask})` 只重置对应的终止或截断子环境；不得使用
   默认 `NEXT_STEP` 或 `SAME_STEP`。候选训练框架及全部向量包装器必须实测支持该模式；不支持时
   阻断该候选接入，不能回退到会绕过回合队列的自动重置。
6. Fabio Pardo 等人在 ICML 2018 的同行评审论文指出，任务自身的有限时域与仅为采样便利的外部时限
   必须区别处理，否则可能造成状态混叠、经验回放失效和训练不稳定；这支持本计划把账户经济终态
   与外部数据范围截止分离，但不证明任何模型盈利。依据：
   [Time Limits in Reinforcement Learning](https://proceedings.mlr.press/v80/pardo18a.html)。

### 1.10 外部能力归属与项目代理时延前置

1. 交易侧 Ledger、只读投影及其跨运行模式合同的当前归属、状态和公共合同只从
   `plan/trading-ledger-plan.md` 读取；rebuild-core 在阶段 4 只核对公共 `LedgerProjectionV1`
   和成功回执，不复制其阶段状态或内部应用语义。外部归一化合同及实现的事项归属仍未决定，候选为由
   `rebuild-core` 覆盖，或由另一个独立事项覆盖；用户决定前，阶段 4 不得创建归一化实现，
   也不得把 Ledger 文档、接口草图或测试替身写成能力就绪。
2. 用户已决定首版项目代理时延配置档由 sim-system 归属计划在任务 1.2 生成、验证并密封，
   由 `SimulationRunManifest` 在 DPT 门 B 前绑定其 `profile_id/version/SHA-256` 和采样算法版本。
   rebuild-core 只读消费该身份，不生成、校准、覆盖或签发配置档；未来真实反馈校准仍可形成
   独立新版本，但不改变首版前置位置。

## 2. 阶段 0：治理基线、冲突复核与实施准入

进入条件：用户已经批准需求及本计划。阶段 0 只复核获批范围和真实现状，不把取得批准
伪装成实施任务。

- 任务 0.1：冻结权威路由和实施边界
  - 步骤 0.1.1：记录事项编号、需求/计划/changelog 摘要、正式实施根、当前工作区角色和
    禁止的外部动作。
    -> 验证：三件套首屏引用一致，`authority=none` 草案未被误报为已批准；正式根与文档
    工作树没有互换。
  - 步骤 0.1.2：读取 `data-collect/docs/CURRENT.md`、当前 Cargo 工作区和相关当前测试，
    建立“已实现、审核中目标、未来独立事项”三列事实表。
    -> 验证：paper/live 采集程序当前只有心跳/状态、training 采集已完成等事实不被目标
    结构覆盖；历史归档不提供执行游标。
  - 步骤 0.1.3：把 DPT 三件套的门 A、门 B、回执和实现单元加入总依赖图。
    -> 验证：不存在“先完整实现 sim 再做门 A”或“门 B 完成前进入 model-train”的倒序，
    也没有在本文复制第二套 DPT 实施步骤。
- 任务 0.2：核对历史问题覆盖
  - 步骤 0.2.1：逐类读取权威 mistake-log 基线，确认每类问题都由本计划中的责任阶段、
    可执行合同和真实验证证据覆盖。
    -> 验证：每个问题类别都能追溯到实际责任与证据；不创建独立登记系统，单条历史实例也
    没有扩张为无界重写。
  - 步骤 0.2.2：对实盘交易 SDK-first、模拟冷启动官方 HTTPS 只读加载器、BUY-only、市场
    范围、数据下限、返佣、离散 `next_wake`、部署和资金授权建立不可变边界清单。
    -> 验证：每条边界同时有正例、负例和失败影响，不以普通英文状态词或裸字符串跨域比较。
- 任务 0.3：核实上游缺口和外部依赖
  - 步骤 0.3.1：逐字段复核 metadata v5、当前 Parquet、CLOB
    `GET /clob-markets/{condition_id}`、Gamma `GET /markets/keyset` 的官方响应和真实产物中的
    `feeSchedule`、`rebateRate`、`itode` 与 `min_order_size` 能力，并分别记录 SDK 类型覆盖。
    -> 验证：形成来源、端点、查询形状、字段、版本、是否足够和受阻范围矩阵；旧字段、近似
    命名或硬编码值不得冒充目标合同，SDK 类型覆盖缺口只作事实记录，不阻断已限定加载器。
  - 步骤 0.3.2：重新核实官方 HTTPS 端点合同、官方 SDK 基线、仓库补丁、PyO3/maturin
    官方发布与安全公告，但不在本阶段安装或升级。
    -> 验证：记录访问日、来源、许可证、已知安全问题和待实测矩阵；任何版本变化只形成
    决策证据，不擅自改变需求。
  - 步骤 0.3.3：只读核对 sim-system 归属计划的首版项目代理时延配置档状态与内容身份。
    -> 验证：只有 `PROJECT_PROXY_PROFILE_READY` 且 `profile_id/version/SHA-256` 与
    `SimulationRunManifest` 一致时准入；2026-07-10 旧分布、合成配置档和临时固定值均不能
    被正式运行发现或加载。
- 任务 0.4：冻结验证原则和阶段证据边界
  - 步骤 0.4.1：为本计划使用的技术门冻结版本化输入、判定、证据、`gate_scope` 和范围影响。
    -> 验证：training、offline_evaluation、sim_calibration、promotion、deployment 不会被错误
    映射成 paper/live 的 `execution_permission`。
  - 步骤 0.4.2：逐阶段确认必要的真实测试、产物摘要、责任归属和失败停止点。
    -> 验证：每项验证直接服务需求验收或已发生风险；没有测试登记表、空占位、理论异常排列
    或“后续再测”。

阶段 0 完成门：权威路由、顺序、上游缺口、外部依赖、技术门和验证边界均无冲突；所有
未决项都有精确阻断范围。若缺口需要用户决策，只阻断受影响阶段，不把其余范围伪报失败。

对应验收：需求第 3、4、5 节中的权威、边界、覆盖与可追溯要求。

## 3. 阶段 1：目标工作区、叶子合同与依赖所有权

本阶段中落在 `contracts`、`data-pipeline/core` 或 `data-pipeline/training` 的真实文件改动，
只通过已获批的 DPT 计划实施一次；本阶段负责总工作区归属、公共合同和跨发布目标依赖验收，
不得另建第二个合同 crate 或重复实现 DPT 核心。

- 任务 1.1：建立最小 Cargo 工作区增量
  - 步骤 1.1.1：以正式根真实 `Cargo.toml` 为基线，只在当前获批切片创建需要的 crate；
    未获批的 `data-pipeline/online`、`offline-evaluation`、`sim-calibration` 不创建空占位。
    -> 验证：`cargo metadata` 显示一个根工作区、一个根 `Cargo.lock`，`third_party` 补丁不被
    误纳为第一方成员。
  - 步骤 1.1.2：建立 `contracts` 无行为叶子 crate 和序列化/规范哈希测试。
    -> 验证：依赖图不含业务 crate、Python 或官方 SDK；合同中没有排序、撮合、归一化、
    Ledger 或生产行为。
- 任务 1.2：接入归属方公共合同
  - 步骤 1.2.1：直接消费 DPT 归属计划和第 1.6 节列明的仓库当前公共 Rust 顶层类型，并验证
    总集成兼容边界；rebuild-core 不冻结、复制或扩展 DPT 字段。
    -> 验证：唯一事件与双索引共享 `event_id`，模型公开视图不纳入接收时间晚于可见截止点的
    事件；首次逐资产正数 tick、动态旧 tick 可未知、首次快照无 `purpose`、三个独立交易状态、
    来源可辨的 `trade_id_source`，以及后续完整盘口只确认或形成
    `BookResyncDeltaInference` 均与公共类型一致；低频平台规则不进入 DPT 事件。
  - 步骤 1.2.2：冻结 Intent Bridge 八个公开方法及模拟/实盘不同具体类型边界。
    -> 验证：公开词汇精确为 `post_order`、`post_orders`、`cancel_order`、`cancel_orders`、
    `cancel_all_orders`、`order`、`orders`、`trades`；模拟类型不含签名/网络，实盘提交才使用
    `SignedOrder`，内部派发类型不可跨模块引用。
  - 步骤 1.2.3：冻结市场、模型注册、策略、门禁、执行权限、资金授权、账户全局和市场本地
    命名空间。
    -> 验证：跨域裸字符串比较编译失败或测试失败；`requires_hardware_upgrade` 不进入
    ModelRegistry 枚举，`next_wake` 不进入交易动作面。
- 任务 1.3：落实依赖所有权规则
  - 步骤 1.3.1：为 `data-pipeline/core`、`sim-system`、`execution/*`、`ledger`、
    `model-runtime`、paper/live 和 `model-train/native` 定义允许依赖清单。
    -> 验证：用每个发布目标的 `cargo tree` 负向断言禁止边；归一化层不依赖具体后端，
    sim 与 pipeline 不互相依赖。
  - 步骤 1.3.2：冻结进程内嵌入拓扑，不实现旁车或远程共享服务。
    -> 验证：paper/live 为各自纯 Rust 组合根，training 只通过 native 扩展嵌入 Rust 闭环；
    没有 IPC、文件轮询或服务发现的第二部署路径。
- 任务 1.4：建立供应链和构建证据
  - 步骤 1.4.1：锁定依赖来源、许可证、安全公告、Git/锁文件和构建摘要。
    -> 验证：`SupplyChainGate` 可从产物追溯到源提交与锁文件；未批准外部代码默认只读，
    不访问敏感材料或联网出站。
  - 步骤 1.4.2：为每个当前构建单元生成依赖闭包、二进制或库摘要和 SHA-256 清单。
    -> 验证：清单与 `cargo metadata`、`cargo tree`、实际文件一致，不含未来空 crate。

阶段 1 完成门：目标结构的当前切片、公共合同、依赖所有权和供应链证据全部通过，且没有
第二工作区、第二锁文件或未来占位实现。

对应验收：需求第 2、3、5 节中的共享合同、唯一职责、依赖闭包和可追溯要求。

## 4. 阶段 2：DPT 门 A 与真实上游投影门

本阶段是只读消费门。DPT 的数据准入、schema 解码、排序、去重、事件构建、测试、产物生成和
回执签发只由 `plan/data-pipeline-training-plan.md` 实施；rebuild-core 不重做这些职责。
`data-collect` 若需改变，必须另有获批三件套。

- 任务 2.1：验证 DPT 门 A 回执
  - 步骤 2.1.1：按公共解码入口读取 `GateAReceiptV3` 及其绑定的
    `GateACapabilityMatrixV2`，验证矩阵全部为 `CapabilityStatus::Pass`、
    `training_dataset_ready=false`，当前合同版本、输入与实现身份以及四份产物清单摘要一致。
    -> 验证：回执缺失、解码失败、矩阵非全 PASS、布尔值不符、合同版本不符或任一身份/摘要
    不符均阻断本阶段；失败诊断不能代替回执，rebuild-core 不读取不存在的 `status`、
    `verdict` 或 `artifact_kind`。
- 任务 2.2：消费不可变门 A 产物
  - 步骤 2.2.1：只读加载回执绑定的 `CanonicalEventRecordV1` 唯一规范事件存储、事件时间
    索引、模型可见索引、市场侧因果外壳，以及市场与参考事件视图；传给模拟器的输入只允许
    真实 `ExecutionMarketEventV3` 市场事件，核对其与第 1.6 节公共消费合同一致。
    -> 验证：两个索引以同一 `event_id` 引用唯一事件并保持各自时间用途；输入谱系根、日期
    下限、最终化准入和四份产物清单摘要都由 DPT 回执可追溯；rebuild-core 不直接读取
    typed_raw、不建立第二排序器，也不从测试替身推导真实纵向就绪。
- 任务 2.3：把门 A 身份交给模拟器联合验收
  - 步骤 2.3.1：将同一回执和产物身份传给阶段 3 的真实事件联合验收，不重新封装或签发。
    -> 验证：黄金事件可独立验证组件，但只有门 A 绑定的真实事件能关闭真实纵向部分；模拟器
    规则冷启动或联合验收失败不反向改变 DPT 回执。

阶段 2 完成门：DPT 已由其归属计划原子签发有效 `GateAReceiptV3`，且 rebuild-core 对可
解码性、全 PASS 能力矩阵、`training_dataset_ready=false` 及全部绑定身份/摘要的只读检查
通过，由此关闭 `REAL_EXECUTION_TAPE_READY`。

对应验收：需求第 3、5 节中的真实数据纵向链路、归属唯一和可追溯要求。

## 5. 阶段 3：Intent Bridge 模拟后端与 sim-system

本阶段只按 `plan/sim-system-plan.md` 和仓库公共 Rust 市场事件类型消费模拟器实现与三层验收证据，
不在总集成计划内重建其字段清单、阶段状态或测试矩阵。黄金事件组件验收可按归属计划推进；
真实纵向结论必须等待阶段 2 的 `REAL_EXECUTION_TAPE_READY`。冷启动或联合验收失败只阻断
模拟器及其下游，不反向改变 DPT 门 A。

- 任务 3.0：消费平台进程规则初始化证据
  - 步骤 3.0.1：按 `plan/sim-system-plan.md` 第 2.3 节和任务 1.1 验证当前、历史与恢复三种
    进程启动模式的唯一不可变规则快照及其停止点。
    -> 验证：当前进程在创建任何会话前只对一个参考市场完成限定官方 HTTPS 只读加载、原始
    响应哈希、窄型解析、身份闭合和密封后断网；历史与恢复进程只读适用既有快照。全部会话
    共享同一快照，两种来源都不从规则端点读取 tick，初始与动态 tick 只消费 DPT 公共事件。
  - 步骤 3.0.2：核对该窄加载器与 live SDK-first 的静态隔离边界。
    -> 验证：加载器无钱包、凭据、签名、订单或账户能力，不进入 `data-collect`、DPT 或 live；
    live 的认证、签名、下单、撤单和查询仍只经官方 Rust SDK。

- 任务 3.1：验收唯一模拟入口、双时间角色边界与确定性会话
  - 步骤 3.1.1：消费归属计划的公共入口、会话隔离和确定性证据。
    -> 验证：training 与 offline-evaluation 只按事件时间索引推进隐藏平台状态，并按模型可见
    索引形成公开视图；paper 只应用本机已经接收的事件，live 不可达模拟器；相同输入、规则、
    配置和种子得到相同逐事件摘要，不同会话不共享状态。
- 任务 3.2：验收订单接受与撮合语义
  - 步骤 3.2.1：按归属计划核对四类订单、实际 maker/taker 分段、项目 FIFO 假设、平台暂缓和
    订单簿重同步证据。
    -> 验证：数量口径、价格与订单约束、份额与 pUSD 守恒通过；后续完整盘口只消费公共
    `BookResyncDeltaInference`，不形成总集成侧撮合或盘口解释器。
- 任务 3.3：验收响应封装与多通道可见性
  - 步骤 3.3.1：按归属计划核对单笔/批量封装、项级混合结果、未知提交和多通道顺序证据。
    -> 验证：固定场景和种子可重放响应先后、迟到、缺失与同刻竞态；客户端未知不改写平台
    真值，总集成层不重做关联或仲裁。
- 任务 3.4：验收撤单、dust 与终态
  - 步骤 3.4.1：按归属计划核对模型撤单、到期、`end_at` 后操作门、合格
    `MarketResolved` 订单终态、紧急撤单、迟到成交和 dust 证据。
    -> 验证：`end_at` 不自动关单或释放资金预留，盘后撤单不制造成交，合格判定只关闭仍开放
    的未成交余量并释放其预留；缺少数据源事件时间的结果证据不触发隐藏平台判定或结算；实际
    成交、dust 与后续结算边界可供外部归一化层一次性消费。
- 任务 3.5：验收手续费、返佣与结算语义
  - 步骤 3.5.1：消费归属计划对进程共享规则、实际成交角色、应计、日桶、付款和结算的证据。
    -> 验证：maker/taker 经济分开追溯，应计只增加一次价值，付款只迁移资产分桶；模拟器不
    写外部 Ledger，也不建立通用规则引擎。
- 任务 3.6：关闭组件、真实纵向与资源验收
  - 步骤 3.6.1：先用黄金事件完成组件验收，再用阶段 2 回执绑定的真实
    `ExecutionMarketEventV3` 市场事件通过同一公共入口完成联合验收；参考事件不进入模拟器。
    -> 验证：两类输入均通过归属计划规定的组件、确定性、边界和资源层；黄金事件不能代替
    真实纵向，模拟器也不读取 typed_raw 或形成第二套 DPT 路径。
  - 步骤 3.6.2：只读消费 sim-system 归属计划已密封的首版项目代理时延配置档。
    -> 验证：`SimulationRunManifest` 绑定的 `profile_id/version/SHA-256` 和采样算法版本一致；
    旧配置档、合成配置和临时固定值被拒绝，本阶段不计算、发布、覆盖或签发配置档。

阶段 3 完成门：`SimSystemFidelityGate=PASS`，黄金与真实纵向链路均通过，所有平台输出
都是可归一化的原始证据，且首版项目代理时延配置档已由同一 `SimulationRunManifest` 绑定。
该配置档只取得项目代理身份，不得宣称为爱尔兰私有链路实测；本阶段不得现场校准或实现未来
`sim-calibration` 发布链。

对应验收：需求第 2、3、5 节中的模拟平台确定性、真实纵向、经济证据和 live 隔离要求。

## 6. 阶段 4：外部归一化归属门与独立交易侧 Ledger 消费

交易侧 Ledger 的当前归属、公共合同和成功回执只按 `plan/trading-ledger-plan.md` 核对，
rebuild-core 不得实现第二套 Ledger。外部归一化实现的事项归属仍须用户
决定：若选择 rebuild-core，任务 4.1 才成为本文实施任务；若选择另一个独立事项，本文只消费
其合同和成功回执。两项能力真实就绪前，均不能用文档、接口草图或测试替身代替。

- 任务 4.0：记录归一化决定并验证两项能力的唯一归属
  - 步骤 4.0.1：只按 `plan/trading-ledger-plan.md` 核对 Ledger 当前归属、公共合同和成功回执；把外部归一化的用户决定
    同步到需求、本文和 changelog，并明确其合同、实现、测试、回执及维护责任。
    -> 验证：Ledger 始终只有独立事项一个写入归属方；归一化决定前没有代码游标，决定后
    只有一个实现归属方，不存在双实现或无归属能力。
- 任务 4.1：建立唯一外部归一化入口
  - 步骤 4.1.1：若归属 rebuild-core，只从模拟或真实平台原始证据生成版本化订单、经济和
    控制归一化事件；若归属独立事项，只验证并消费等价合同与成功回执。
    -> 验证：原始封装、摘要、接收时间、来源和关联键可追溯；未知提交、撤单、dust、迟到或
    重复终态、返佣和结算由唯一解释器处理，模拟与真实证据严格分流。
- 任务 4.2：消费每实例本地仅追加 Ledger
  - 步骤 4.2.1：只读消费归属计划定义的公共 `LedgerProjectionV1` 和成功回执，不复制内部账目
    字段或应用语义。
    -> 验证：投影版本、来源、实例、序号、摘要和时间均有效；training、offline-evaluation、
    paper 和 live 的实例互不可达；allowance 不成为第二资金权威。
- 任务 4.3：交付只读投影与总集成回执
  - 步骤 4.3.1：把公共 `LedgerProjectionV1` 身份与归一化、Ledger 成功回执交给 DPT 门 B 和
    后续运行时。
    -> 验证：DPT 与模型只有读权限；公共投影身份和成功回执闭合，不产生第二写入器。
- 任务 4.4：保持离线反馈边界
  - 步骤 4.4.1：只定义 `feedback-ingestion` 与本阶段的接口和停止点；写入器仍须对应独立事项
    获批后实施。
    -> 验证：simulation/paper 反馈不能校准模拟器，真实反馈不直接改变在线模型，独立离线
    评估不读取历史行为执行标签作为当前候选结果。

阶段 4 完成门：用户已经明确外部归一化归属；唯一归一化能力与公共
`LedgerProjectionV1` 均由各自归属计划真实实现并签发可验证回执，且能被 DPT 只读消费。
外部归一化决定或任一成功回执缺失时，本阶段不得写成完成。

对应验收：需求第 2、3、5、6 节中的唯一经济解释、账本重建、实例隔离和外部归一化归属要求。

## 7. 阶段 5：DPT 门 B 与总集成消费门

本阶段必须等待 DPT 与 `sim-system` 归属流程完成门 B 候选构建和联合验收，并由 DPT 原子签发
最终 `TRAINING_DATASET_READY` 后才可进入。候选、外部前置、联合验收、完整 Observation、
标签、切分、训练归一化、回放上下文、DPT 测试与签发都不属于 rebuild-core 阶段 5；本文不
参与交付或核验这些门 B 前置，也不复制字段、阶段状态或测试矩阵。

- 任务 5.1：只读消费最终 `TRAINING_DATASET_READY`
  - 步骤 5.1.1：按 `plan/data-pipeline-training-plan.md` 的最终门 B 合同，只读验证并消费已签发
    的 `TRAINING_DATASET_READY` 及其绑定产物。
    -> 验证：最终回执及绑定产物缺失、不可解码、身份或摘要不一致均阻断本阶段；门 B 候选、
    联合验收中间证据、失败诊断或本地替身都不是 rebuild-core 输入，也不能代替最终回执。
- 任务 5.2：关闭稀疏调度总集成边界
  - 步骤 5.2.1：在最终门 B 回执通过只读消费后，让 Model Runtime 只消费其绑定的 DPT 回放
    上下文和模型的 `decision_schedule_intent`，完成确定性重新设定基线与离散
    `next_wake_bucket` 的公平比较。
    -> 验证：模型只由可回放业务、账本、生命周期、经 Observation 准入的质量事件、单次
    bootstrap 或有效 wake 触发；固定周期、`tick_stream` 和采集器诊断不能触发策略；相同
    Observation、模拟器、Ledger、场景、种子和资源合同下可重放。
- 任务 5.3：核对首版项目代理时延配置档绑定
  - 步骤 5.3.1：只读确认最终门 B 回执绑定的同一候选 `SimulationRunManifest` 已绑定 sim-system
    归属计划密封的首版项目代理时延配置档身份。
    -> 验证：`profile_id/version/SHA-256` 和采样算法版本机械一致；rebuild-core 不接触门 B
    候选或联合验收，不实现、校准、发布或签发配置档，也不让旧配置、合成配置或临时固定值
    取得正式身份。

阶段 5 完成门：`TRAINING_DATASET_READY` 已由 DPT 唯一签发并通过只读消费检查，稀疏调度
总集成证据闭合，且同一候选已经绑定 `PROJECT_PROXY_PROFILE_READY` 的首版项目代理时延
配置档。任一身份或回执缺失时不得进入正式 model-train。

对应验收：需求第 2、3、5、6 节中的训练数据、模型输入、调度、公平比较，以及外部归一化
归属与独立交易侧 Ledger 成功回执要求。

## 8. 阶段 6：model-train、Python/Rust 边界与候选冻结

- 任务 6.1：验证并锁定 PyO3/maturin 工具链
  - 步骤 6.1.1：在本地和每台获准训练服务器核实解释器、SOABI、体系结构、glibc、Rust
    最低版本、PyO3/maturin 候选及安全公告。
    -> 验证：形成兼容矩阵；受影响或不兼容版本不锁入依赖，任何需要改环境/拓扑的结果
    停止并交由用户决定。
  - 步骤 6.1.2：建立根工作区成员 `model-train/native`、`pyproject.toml` 与唯一 `uv.lock`。
    -> 验证：PyO3 只存在于 native，maturin 只参与构建/打包；paper/live 和共享 Rust 库
    依赖闭包不含 Python/PyO3。
- 任务 6.2：实现薄 Gymnasium 环境和粗粒度 native 闭环
  - 步骤 6.2.1：实现 `reset`、`step`、spaces、`terminated`/`truncated` 和结构化错误映射。
    -> 验证：通过 Gymnasium 官方环境检查；账户经济耗尽只在第 1.8 节全部条件闭合后产生
    `terminated=true`，预登记模型决策范围截止产生 `truncated=true`，两者使用不同自举掩码；
    原始输入缺失必需终态、负余额和合同无效返回结构化失败，不以 panic 或普通回合结束代替。
    现金为零但仍有可恢复经济事实、正余额暂时买不起以及平台领先而 Ledger 未闭合各有一个能
    推翻提前终止的代表性反例。
  - 步骤 6.2.2：让单次 `step(action)` 在 Rust 内推进到下一决策点并返回紧凑 Observation。
    -> 验证：没有逐市场事件跨语言调用、文件轮询、预生成未来动作、Python 模拟器或 Python
    数据管道；长 Rust 计算允许解释器处理其他工作。
  - 步骤 6.2.3：为每条并行账户轨迹创建独立 Rust 会话，一个训练工作进程可以承载多条隔离
    轨迹，但不得共享可变账户状态。
    -> 验证：单进程/多进程下的账户身份、开户锚点、资金、种子、模拟器、Ledger 和错误均隔离；
    每条轨迹只有一份配置化初始资金，结果只作分布统计，不合并余额或 PnL；不假定 PyO3 对象可
    跨进程序列化。
  - 步骤 6.2.4：实现预登记回合队列与显式向量环境重置。
    -> 验证：终止转换先返回并持久化最终 Observation、奖励、Ledger 投影和边界原因，下一次
    `reset` 才创建队列中预先冻结的新会话、账户身份和开户锚点；原账户没有补资事件，循环模型
    隐藏状态已清空。不同候选取得相同轨迹身份与数量，早耗尽工作实例不能自适应增加回合、资本
    机会或早期日期样本。向量环境实际 `metadata["autoreset_mode"]` 必须为
    `AutoresetMode.DISABLED`；收集器只在终态持久化成功且下一轨迹预登记后调用
    `reset(options={"reset_mask": mask})`，掩码只覆盖本次已终止或截断的子环境，其他并行轨迹的
    Observation、状态、身份和序号保持不变。默认 `NEXT_STEP`、`SAME_STEP` 以及不支持禁用模式的
    包装器或训练框架各有一个失败即关闭验证。
- 任务 6.3：实现 RewardContract 和训练有效性门
  - 步骤 6.3.1：按动作、父决策、Ledger 前后序号、`delta_t`、估值/终态和成熟时间计算
    唯一版本化奖励。
    -> 验证：奖励可重算、不写 Ledger，返佣应计不叠加额外奖励，付款不产生第二次奖励；
    next-wake 完整到期与抢占/删失使用冻结经过时间语义；`delta NAV` 已包含的账户损失不默认
    再叠加破产罚分，任何额外尾部风险惩罚都必须进入新版本 `RewardContract`。
  - 步骤 6.3.2：只将数据/因果/合同/数值/Ledger/Reward/产物确定性无效作为硬失败。
    -> 验证：合法资金耗尽是对应回合的业务结果和统计项，不是 Ledger 无效或训练任务硬失败；
    弱表现、零成交或低频只形成 WARNING/排名。首轮表现型提前停止禁用，资源预算只停止对应
    试验并保留模型权重/优化器检查点。
- 任务 6.4：执行模型容量与硬件公平探索
  - 步骤 6.4.1：预注册层级、深宽、状态/隐藏维、参数量、训练参数、种子和单/双/四卡预算。
    -> 验证：候选共享数据、动作、sim、Ledger、Reward 和评估合同；不因当前 live 主机先验
    淘汰大模型；候选还共享同一预登记回合队列、轨迹数量、初始资本和终止/截断合同，早耗尽
    不产生额外回合。
  - 步骤 6.4.2：在 5090 训练服务器实测 GPU/CPU/RAM/I/O、吞吐、质量和长运行。
    -> 验证：`nvidia-smi`、`lscpu`、`free`、cgroup 和运行清单固化实值；不以单一 GPU 利用率
    或批量大小判成功。
  - 步骤 6.4.3：对超过目标交易硬件能力的胜出候选标记独立部署状态。
    -> 验证：`requires_hardware_upgrade` 只阻断相应部署，不改变模型排名或 ModelRegistry。
- 任务 6.5：冻结唯一训练候选
  - 步骤 6.5.1：生成训练批次排名表、两种盈利机制归因/残差、资源、数据和全部 SHA-256。
    -> 验证：逐轨迹报告最终 NAV、收益率、资金耗尽率/时间、最大回撤和版本化尾部风险统计，
    独立市场历史数量与反事实轨迹数量分列；不把多份 `2000 pUSD` 或 PnL 求和为单账户，不读取
    晋级留出集，不自动删除合同有效候选，也不替代独立评估。
  - 步骤 6.5.2：冻结不可变 `ModelBundle` 并签发 `TrainingCandidateReceipt`。
    -> 验证：模型包绑定处理核心、Observation、归一化、sim、动作、调度、资源合同和摘要；
    只含 BUY-only，不包含 SELL 或固定周期路径，且尚未解析/读取留出集。
  - 步骤 6.5.3：构建并验证 native wheel。
    -> 验证：`maturin build --release --locked` 后在干净目标环境安装、导入和最小轨迹通过；
    wheel 绑定解释器/SOABI/体系结构/glibc/锁文件/提交/SHA，不能用本地导入冒充服务器通过。

阶段 6 完成门：训练闭环、Reward、硬件、公平探索、排名和 wheel 全部验收，唯一候选已冻结，
只签发 `TrainingCandidateReceipt`；未进入晋级留出集或 paper/live。

对应验收：需求第 2、3、5 节中的候选公平性、模型意图、资源、产物和依赖可追溯要求。

## 9. 阶段 7：独立 offline-evaluation 与 Promotion Controller

进入条件：对应独立需求、计划和 changelog 已批准；未批准前不创建空组合根或写入器。

- 任务 7.1：实现唯一 Promotion Controller 写入边界
  - 步骤 7.1.1：实现 ModelRegistry、`promotion_stage`、凭据状态和
    `FundsAuthorizationStateLog` 的唯一仅追加写入器。
    -> 验证：阶段运行时只能签发本阶段结果凭据，不能改注册表、自签下一阶段权限或改不可变
    凭据；决策幂等并校验 expected stage。
  - 步骤 7.1.2：实现跨阶段不变量、阶段本地身份和失效/回退规则。
    -> 验证：模型包/合同变化从最早受影响阶段重验；账户身份变化回到 shadow 后，环境/
    运行时/硬件变化回到 paper 后；不自动 retire。
- 任务 7.2：冻结不可见时间外留出集
  - 步骤 7.2.1：在候选、评估合同、阈值、基线、初始 Ledger、场景/种子冻结后计算
    `evaluation_snapshot_cutoff`，再生成 `OfflineEvaluationDatasetManifest`。
    -> 验证：从截止点内全部合格区间选择 `end_time` 最新者；model-train 无读取权限，
    留出集从未进入任何自适应决策。rebuild-core 只消费并核对 DPT 归属计划实际签发的内容寻址
    切分清单及其样本依赖闭区间、purge 删除和单向 post-heldout embargo 结果，不重新计算、整体
    平移切分点或复制字段语义；清单缺失、摘要不符、依赖区间与受保护决策区间或后置隔离范围
    相交而未删除、最终留出曾进入拟合/自适应选择时失败。只与留出决策前已成熟的只读历史前缀
    重叠不构成 rebuild-core 自行加严理由。评估账户轨迹、初始资金、开户锚点和数量在结果产生
    前固定。
  - 步骤 7.2.2：登记自然/注入的单源、多源、全源缺失和外生评分成熟度。
    -> 验证：自然静默不使结构完整区间失格，未知覆盖不能冒充可评分缺失；注入标记不进入
    Observation，数据或统计不足得到 `QUARANTINE/EVIDENCE_INSUFFICIENT`。
- 任务 7.3：运行只读独立评估
  - 步骤 7.3.1：用冻结候选和基线在隔离
    `pipeline -> model -> bridge(sim) -> sim -> normalization -> Ledger` 闭环运行。
    -> 验证：不训练、调参、重拟合、校准模拟器、改数据或写 Registry；当前候选执行结果
    来自本次运行，不读取历史行为执行标签或训练奖励；提前耗尽保留为该轨迹最终结果，不能
    自适应补发本金、替代轨迹或重复回放以凑足样本。
  - 步骤 7.3.2：按预冻结标准报告经济、风险、执行现实、基线、样本量和不确定性。
    -> 验证：首轮利润/Sharpe/胜率/成交率等只作建议；只有合同、因果、守恒、权限和预先
    明示安全上限可作硬门，证据不足不写成模型 FAIL；逐轨迹最终 NAV、收益率、资金耗尽率/
    时间、最大回撤、尾部风险以及独立市场历史数和反事实轨迹数分开报告，不合并成单账户。
  - 步骤 7.3.3：仅在完整合格数据上 `PASS` 时签发 `OfflineEvaluationReceipt`。
    -> 验证：结果暴露后任何影响输出/判定的身份变化要求新未见区间；完全相同身份的审计
    重放只能验证原结果，不能签发第二份晋级凭据。
阶段 7 完成门：独立评估、留出清单、报告和凭据满足只读及不可见边界；Promotion Controller
消费前序凭据后才可推进到 `OFFLINE_EVALUATION_PASSED`。本阶段只读使用阶段 5 已冻结的
`sim_system_version + latency_profile_sha256`，不得再次校准、改写或发布配置档。

对应验收：需求第 2、3、5 节中的独立评估、不可见数据、凭据链和不可跳级要求。

## 10. 阶段 8：在线处理核心复用与 paper 资格

进入条件：model-train 已完成，用户已决定返回 `data-collect`，且 paper 真实适配器及
`data-pipeline/online` 的独立三件套均已批准并完成；本文不自动创建或部署它们。

- 任务 8.1：建立 paper 在线输入和纯 Rust 组合根
  - 步骤 8.1.1：由独立 `data-collect-paper-hot` 进程经版本化本机交接提供外部市场事件，
    paper 内嵌在线 pipeline/Market Context Builder。
    -> 验证：采集程序与 paper 主程序是不同进程/SHA/日志；热路径不扫描最终化 Parquet，
    驱动层不实现第二套语义。
  - 步骤 8.1.2：组合 `model-runtime`、execution/sim、sim-system、normalization 和 ledger，
    排除 execution/live、钱包、签名和真实网络。
    -> 验证：`cargo tree` 与实际二进制依赖闭包通过；运行角色和产物根隔离。
- 任务 8.2：验证完整在线业务流程
  - 步骤 8.2.1：执行 paper 自检、热身、T-30 预加载、active 持续决策和结束前 T-30 next
    预加载。
    -> 验证：目标市场完整 5 分钟 + 30 秒持续状态，current/prefetched/旧市场结算隔离，
    热身产物不只在控制台。
  - 步骤 8.2.2：执行在线调度、重启恢复、模拟订单闭环、Ledger/NAV/PnL 和资源长运行。
    -> 验证：与离线相同规范事件/账本输入时 Observation 和调度一致；无固定节拍、影子特征、
    私有 sim 直连或奖励/标签泄漏。
- 任务 8.3：签发 paper 凭据
  - 步骤 8.3.1：在 `NO_REAL_FUNDS` 下完成预注册覆盖和审计对账。
    -> 验证：只加载同一 ModelBundle 与有效 OfflineEvaluationReceipt，资金授权字段为
    `NO_REAL_FUNDS`，不存在钱包/出资方依赖。
  - 步骤 8.3.2：仅在 `PaperPromotionGate=PASS` 后签发 `PaperQualificationReceipt`。
    -> 验证：凭据绑定运行清单、sim/校准、处理/模型/账本身份和前序凭据；不自动进入 live。

阶段 8 完成门：paper 在真实在线外部事件下完成无资金模拟闭环并签发 PASS 凭据；未获
独立 paper/data-collect-online 授权时本阶段保持阻断而不是使用心跳程序冒充完成。

对应验收：需求第 2、3、5 节中的 paper 在线闭环、实例隔离、无资金与晋级要求。

## 11. 阶段 9：live 纯 Rust 运行时与 live shadow

进入条件：用户已经批准 live 数据适配器、在线处理和 live 运行时对应三件套；正式本地构建
完成，最终 live 硬件和路径可只读验收。本阶段 shadow 不授权钱包或提交。

- 任务 9.1：建立 live 数据和执行依赖闭包
  - 步骤 9.1.1：由独立 `data-collect-live-hot` 进程通过版本化本机交接供给外部市场事件，
    live 主程序内嵌同一在线 pipeline 与 model-runtime。
    -> 验证：固定安装路径解析在 `/home/ubuntu/live-trade` 内，采集器不接管模型、钱包、
    订单或 Ledger；无第二外部数据源。
  - 步骤 9.1.2：实现 execution/live 对官方 `Client` 八个同名方法的唯一委托。
    -> 验证：依赖图和二进制不含 sim-system、模拟后端、Python/PyO3、离线 Arrow/Parquet、
    自有 SELL、平行 SDK 或 REST 旁路。
- 任务 9.2：执行 live shadow
  - 步骤 9.2.1：在最终 live 硬件以 `NO_REAL_FUNDS + NO_SUBMIT_PERMISSION` 执行自检、热身、
    T-30、在线推理、用户频道只读、查询/对账、重启和长运行。
    -> 验证：不加载可提交钱包/出资方，不调用任何平台变更方法；查询证据先归一化再进入
    Ledger，不成为轮询决策触发器或第二账本。
  - 步骤 9.2.2：验证硬件、资源、路径、写入器和失败关闭。
    -> 验证：目标硬件不足只产生部署阻断/升级状态；live 主机不运行 Rust 编译、训练重处理、
    归档或 Git 拉取。
  - 步骤 9.2.3：仅在 `LiveStageGate.admission_verdict` 与 shadow 完成判定均通过后签发
    `LiveShadowReceipt`。
    -> 验证：凭据绑定最终 live 运行时、主机/硬件、官方 SDK、合同、模型和前序 paper
    凭据；仍无真实提交权。

阶段 9 完成门：live shadow 只读闭环及最终硬件证据完整，Promotion Controller 可以推进到
`LIVE_SHADOW_PASSED`；不能因模型为 active 或 shadow PASS 自动产生真实资金权限。

对应验收：需求第 2、3、5 节中的 live shadow、官方 SDK、依赖隔离与无提交权限要求。

## 12. 阶段 10：逐次真实资金准入与不可跳级晋级

本阶段每一次运行都必须有用户针对该运行的明确授权、独立运行准入决策和获批部署/运行
材料。本计划即使将来获批，也不授权任何一次真实资金操作。

- 任务 10.1：生成并绑定单次资金授权
  - 步骤 10.1.1：Promotion Controller 根据用户明确批准生成不可变、一次性
    `UserFundsAuthorizationReceipt`，不写入敏感凭据。
    -> 验证：绑定模型、阶段、资金档、运行、市场/数量/时长、环境、公开账户身份、稳定
    `account_global_ledger_id`、签发/到期和前序凭据；最新状态为 `UNCONSUMED`。
  - 步骤 10.1.2：原子校验账户和晋级链唯一性并绑定 `active_admission_run_id`。
    -> 验证：同一环境/钱包/出资方/账户跨链及同一晋级链均最多一个未关闭准入；状态只追加
    转为 `BOUND_TO_THIS_RUN`，不能原地改凭据。
- 任务 10.2：依次执行 small-live 1、small-live 3、formal-live 1h
  - 步骤 10.2.1：每档启动前分别验证 active 模型、正确阶段、有效授权状态、运行准入决策、
    LiveStageGate、硬件/SDK/schema/写入器/钱包和账户身份全部相等。
    -> 验证：需求列出的无授权、错模型、错阶段、错档、错环境/账户、过期/撤销、并发准入和
    gate 未通过负例均不能得到 `ALLOW_SUBMIT`。
  - 步骤 10.2.2：只在授权范围内执行合法 BUY 意图，并持续记录真实平台原始证据、NOE、
    Ledger、审计、资源和准入状态。
    -> 验证：live 不改写模型意图、不调用 sim、不绕过 SDK；模型取消与运维紧急撤单分源，
    任何权限失败立即阻止新提交。
  - 步骤 10.2.3：运行结束时先追加 `COMPLETED|FAILED|INTERRUPTED|EXPIRED|REVOKED|INVALIDATED`
    终态并关闭准入，再按证据签发或拒绝本阶段结果凭据。
    -> 验证：PASS 凭据绑定授权、准入、环境/账户、Ledger 起止摘要和终态事件；失败、证据
    不完整或撤销不签 PASS、不自动推进或退役模型。
- 任务 10.3：执行分离的完成决策和下一档申请
  - 步骤 10.3.1：Promotion Controller 在准入已关闭且 PASS 结果存在后签发完成决策。
    -> 验证：small-live 1 -> small-live 3 -> formal-live 1h 严格顺序；前一档完成与下一档
    准入不是同一次状态转换。
  - 步骤 10.3.2：只有完成阶段推进后，才能向用户申请下一次独立授权。
    -> 验证：上一授权不可复用、不自动续权；formal 完成后只可审查，不自动继续运行。
- 任务 10.4：验证部署和敏感材料边界
  - 步骤 10.4.1：在本地构建发布二进制，记录提交、角色、路径和 SHA-256，再按获批部署
    事项复制到目标服务器。
    -> 验证：live 服务器无源码构建、Git pull 或部署密钥；所有运行材料解析到
    `/home/ubuntu/live-trade` 内。
  - 步骤 10.4.2：模型从已验证本地产物直接复制，钱包只来自被 Git 忽略的 `wallets` 材料。
    -> 验证：模型不经 GitHub、不进入 wallets；敏感材料不进入 Git、日志、凭据或摘要。
- 任务 10.5：离线接纳真实反馈供后续轮次使用
  - 步骤 10.5.1：在已获批的离线反馈范围内，把 live 原始证据、NOE、Ledger 应用结果和
    成熟标签通过 `feedback-ingestion` 复制到只追加离线产物。
    -> 验证：每条反馈保留来源、成熟度、决策上下文、规则/订单簿身份和摘要；不直接读取
    真实原始响应生成标签，不修改 live Ledger 或当前模型包。
  - 步骤 10.5.2：按来源把训练反馈与独立校准证据分流。
    -> 验证：真实执行反馈只能进入后续训练切分，独立真实校准证据只能进入 sim-calibration；
    simulation/paper 反馈不校准模拟器，offline-evaluation 和 paper 热路径不可达。
  - 步骤 10.5.3：更新 `OnlineFeedbackGate` 的规则、覆盖和证据状态。
    -> 验证：尚未成为本轮冻结必需输入的覆盖不足只形成精确 WARNING，不追溯改变本轮模型、
    模拟器、评估判定、晋级阶段或线上行为。

阶段 10 完成门：只有三档各自获得单次授权、完整运行、关闭准入、PASS 凭据和完成决策后，
晋级链才能到 `FORMAL_LIVE_1H_PASSED`；任何一档失败均停留在最后已通过阶段。

对应验收：需求第 2、4、5 节中的真实资金授权、不可跳级、部署和敏感材料边界。

## 13. 阶段 11：全链验收、文档闭合与事项完成

- 任务 11.1：执行跨模块最终验收
  - 步骤 11.1.1：重跑所有适用于本事项的技术门，汇总每个范围的判定、原因、证据和影响。
    -> 验证：无模糊 PASS、无跨范围权限替代；所有 WARNING、QUARANTINE、FAIL 和硬件升级
    状态均保留，未适用的门有明确理由。
  - 步骤 11.1.2：逐条核对需求第 5 节的十项业务验收结果和不可变回执链。
    -> 验证：每项都有真实测试或产物、摘要、版本和责任阶段；不创建额外验收登记系统，任何
    未通过项都阻止事项完成。
  - 步骤 11.1.3：用根 `cargo metadata`、各发布目标 `cargo tree`、wheel/二进制和运行清单
    验证物理结构与依赖闭包。
    -> 验证：目标结构与实际一致，不存在未来占位、影子实现或不应进入 live 的依赖。
- 任务 11.2：闭合 mistake-log 与文档三件套
  - 步骤 11.2.1：回到阶段 0 覆盖台账，逐类核对问题、修复、测试和残余风险。
    -> 验证：没有遗漏或用无关测试冒充覆盖；未解决项有准确 gate 和后续事项。
  - 步骤 11.2.2：同步需求、计划和 changelog 的事实、决策、验证结果和偏差。
    -> 验证：三件套使用同一事项编号并相互引用；开发中任何与需求/计划不一致的事实已记录，
    没有把目标状态写成已实施。
  - 步骤 11.2.3：执行最终独立审查和语言/链接/格式检查。
    -> 验证：Critical/Important 问题清零或由用户明确决策；中文叙述、路径、代码标识和链接
    可核验，`git diff --check` 通过。

阶段 11 完成门：需求第 5 节全部验收、适用技术门、历史问题覆盖、依赖闭包和三件套均通过，
且没有未关闭的授权准入、未说明的证据缺口或擅自扩张范围。届时才可提议将事项标记完成。

## 14. 需求范围与阶段关系

- 阶段 0 至 1 关闭权威、合同、依赖与供应链基线，为需求第 1 至 4 节提供实施入口。
- 阶段 2 至 5 按归属关系消费 DPT、`sim-system`、外部归一化、Ledger 和项目代理时延配置档
  证据，形成进入训练前的真实纵向闭环；阶段 4 只对外部归一化实现保留用户决定门，交易侧
  Ledger 已固定为独立事项，时延配置档已经固定为 DPT 门 B 前置。
- 阶段 6 至 7 完成模型训练、公平容量探索、候选冻结和独立离线评估。
- 阶段 8 至 10 依次关闭 paper、live shadow 和逐运行真实资金授权，任何阶段均不得自动取得
  下一阶段权限。
- 阶段 11 只汇总需求第 5 节的业务验收和各归属方真实回执，不建立新的文档治理系统。

## 15. 技术门收口

1. DPT 门 A、DPT 门 B、`SimSystemFidelityGate` 与项目代理时延配置档门分别由其归属计划实现、
   审查和签发；rebuild-core 只按各自公共合同验证可解码性、身份、摘要和适用范围，不推测
   合同中不存在的 `status`、`verdict` 或 `artifact_kind`。
2. `SupplyChainGate`、`ModelIntentBridgeGate`、模型候选、独立评估、paper、live shadow、晋级与
   单次资金授权等总链路门，按对应阶段真实实现和签发，不允许一个范围的 PASS 替代另一个
   范围的执行权限。
3. 每个适用技术门都必须包含版本化输入、判定、原因、证据、`gate_scope` 和影响范围；门的
   当前集合以本文对应阶段及归属计划为准，不维护固定数量的重复登记表。
4. WARNING、QUARANTINE、FAIL、证据不足和硬件升级状态必须保留原义；没有成功回执就保持
   阻断，不能用诊断、测试替身或控制台输出代替。

## 16. 最终验收收口

最终验收直接核对需求第 5 节十项业务结果：阶段 2 至 5 提供真实数据、模拟、归一化与 Ledger
闭环证据；阶段 5 至 7 提供模型输入、调度、训练、公平比较和独立评估证据；阶段 8 至 10
提供在线一致性、依赖隔离、部署和逐次授权证据；阶段 0、1、11 提供错误台账覆盖、供应链、
产物身份、资源上限和三件套一致性证据。

任何业务结果只能由其责任实现、真实测试、不可变产物或成功回执证明。未执行、失败、过滤、
身份不一致或仍待用户决定的内容均不得写成通过，也不得为验收另建字段级登记表、理论异常
矩阵、Markdown 文档守卫或 Rust 文本断言。

## 17. 当前草案非授权声明

截至 2026-08-11 本次同步，DPT 当前游标只从 `plan/data-pipeline-training-plan.md` 读取；
rebuild-core 阶段 2 在实际取得有效 `GateAReceiptV3` 前保持条件性阻断。rebuild-core 本身仍
没有任何阶段取得执行游标，也没有发生代码创建、数据读取/构建、模拟器实现、训练、校准、
远端同步、依赖安装、部署、提交、推送、钱包访问、真实下单或资金操作。下一步仅为用户审核
需求与本计划；交易侧 Ledger 的当前状态只从 `plan/trading-ledger-plan.md` 读取，缺少符合该
归属计划的有效成功回执时仍阻断阶段 4；外部归一化实现归属仍待用户决定。审核意见若改变语义，必须先同步需求、计划和
changelog，再重新判断实施游标。
