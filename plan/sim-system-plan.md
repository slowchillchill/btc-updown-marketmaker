# sim-system 已批准实施计划

> 事项编号：`SIM-SYSTEM-20260731`
>
> 生命周期：`APPROVED_ACTIVE_PLAN`
>
> 当前执行 authority：`plan/sim-system-plan.md`
>
> 当前业务与技术规范来源：`plan/sim-system-plan.md`
>
> 对应需求：`demand/sim-system-demand.md`
>
> 对应 changelog：`changelog/sim-system-changelog.md`
>
> `data-collect` 当前权威入口：`data-collect/docs/CURRENT.md`
>
> DPT 当前权威计划：`plan/data-pipeline-training-plan.md`
>
> 执行授权：`LOCAL_IMPLEMENTATION_ONLY`
>
> 部署、远端操作和真实资金授权：`none`
>
> 状态：`阶段 4 -> 任务 4.3 -> 步骤 4.3.4 执行中`

本计划依据简洁业务需求制定，是当前业务语义与技术实施的唯一规范来源。它直接规定
模块边界、业务合同、成熟工具复用、执行顺序和验收方法；需求只描述宏观目标、范围与结果级
验收，changelog 只记录当时事实和取代关系。计划中的当前语义被新决定改变时，应直接覆盖
或删除旧内容，并把实际决定追加到 changelog。

用户已于 2026-08-17 批准需求、本计划及计划范围内的本地代码、测试、数据模式和依赖实施。本授权不包含提交、
推送、发布、部署、远端操作、钱包或凭据访问、真实下单和真实资金操作。

## 1. 成功标准、测试边界和停止条件

### 1.1 总成功标准

1. 组件符合性门以固定规则、符合 DPT 当前双时间合同的黄金 `ExecutionMarketEventV3` 市场事件和
   合成请求，证明
   八方法、动态影子 CLOB、订单、成交、账户、返佣、自动赎回、查询、恢复、确定性和资源
   语义；只有规则冷启动合同测试可以启用本地 HTTP 测试替身，确定性核心必须断网运行。
2. 真实管道纵向门只接受 DPT 成功 `GateAReceiptV3` / `REAL_EXECUTION_TAPE_READY` 覆盖的
   真实事件集合及双时间投影，至少完成一个代表市场的合同兼容、状态转换、模型可见性隔离和
   真实性能复核。
3. 训练闭环门由外部归一化层和同级目录中交易侧 Ledger 的 training 实例消费模拟器原始证据，
   再与同一 DPT 候选
   形成完整 Observation 和训练产物；它不是模拟器组件开发前置，但未通过时不得进入正式
   `model-train`。
4. 真实模拟盘恢复适用门只接受真实模拟盘适配器、消费方运行时自有仅追加日志，以及经与组件
   相同窄受信读取端口完成的跨进程恢复联合证据；黄金持久源、DPT 离线 Gate A 产物和单模块
   测试均不能满足本门。
5. 四个门互不冒充，也不得为了通过外部门而在 `sim-system` crate 内实现 DPT、模拟盘适配器、
   消费方日志写入器、归一化层、交易侧 Ledger、RewardContract、模型框架或校准写入器。
6. 所有完成结论必须绑定实际命令、输入、实现、规则、场景和产物身份；未运行、失败、被
   过滤或被外部条件阻断的验证不得写成通过。

### 1.2 测试与审查边界

- 正例优先使用真实平台样本或合同有效的黄金载荷；不可信网络响应、JSON、清单、日志段和
  跨模块持久产物，每类可达损坏只保留一个代表性边界输入，另保留已发生事故的聚焦回归。
- 输入通过真实构造、解码和边界校验后，内部测试不得直接篡改类型制造平台不可能状态。
- 新增或改变可执行行为时，先用原因正确且互不遮蔽的聚焦测试证明旧行为不满足当前合同，
  再做最小实现和受影响回归；已有行为特征化、文档迁移和资源测量不强造红灯。
- 阶段 4 不获得任何新增运行时校验授权。每个拒绝或停止条件必须直接对应既有业务规则、一次性
  不可信边界解码、日志物理组完整性或已要求的资源硬上限；强类型值进入单线程归约器后不得再做
  字段、参数、归属、摘要、载荷或“为了保险”的重复核对。实施中若发现缺少新校验会让已批准需求
  无法成立，必须单独列明原因、方法、范围和性能成本并停在专项授权门，不能随代码一并加入。
- 不创建测试登记表、逐字段或理论异常矩阵、Markdown 文档守卫，也不让 Rust 测试读取、
  搜索或断言需求、计划或 changelog 文本。
- 每个任务完成后自审范围、合同、差异和测试；每个阶段关闭前由独立审查代理检查真实需求
  覆盖、正确性、影子实现、过度设计和剩余风险。问题修复后重复受影响验证和审查。

### 1.3 必须停止并交由用户决定的情况

- 需求或本计划尚未获批而准备开始实施；
- 本计划与 DPT 当前计划、公共合同类型、当前代码、真实产物、官方平台规则或用户最新决定
  不能同时成立；
- 技术选型要求改变模块边界、公开方法、业务语义、部署拓扑，新增付费/远端资源，或绕开
  官方 SDK 的实盘边界；
- 规则快照、公共事件、数量/金额守恒、经济幂等、会话隔离或资源硬上限无法形成唯一可靠
  语义；
- 阶段 6 或阶段 7 缺少其外部成功回执，却准备以替身或人工说明冒充通过。

### 1.4 实施根和阶段关闭

正式本地实现、文档落盘、构建、测试和验收根固定为当前工作树
`/home/chillchill/sim-system`，目标实施单元为该根中的 `sim-system` Rust crate；不得切换到
`/home/chillchill/btc-updown-marketmaker` 实施本事项。阶段 0 必须先以正式根真实 `Cargo.toml`、
`Cargo.lock`、`cargo metadata` 和 `cargo tree` 复核工作区事实，不能预设尚不存在的
crate、依赖或路径。同一工作树下未来同级目录中的 `ledger` crate 由独立事项
`TRADING-LEDGER-20260811` 及其归属计划 `plan/trading-ledger-plan.md` 管理，不因共享工作树而
进入本计划实施范围。阶段 4 获批后可以手术式修改现有 `contracts` crate，只加入业务消费者
必须共享的 `AccountInstanceKeyV1`、`InitialAccountStateV2`、`AccountOpeningFactV2`、
`PlatformAccountDeltaV2`、
`MarketEnrollmentV2`、直接响应 V2 和三类生命周期反馈 V2，以及这些根合同规范编码所需的最小
无行为值类型。内部 `SimEventLogV2` 记录、恢复读取器和文件写入器留在 `sim-system`，不得
进入跨模块业务合同。调用方角色和平台来源各自只有一个共享表示；模拟来源由运行根固定注入，
调用方不存在平台来源字段或构造参数。其他模块只消费已发布的窄投影，发现字段缺口时触发共享
合同新版本决策，不得重定义同名类型、直接解析内部日志或引入通用响应枚举。

阶段提交只允许包含该阶段范围，且仍须用户另行授权；推送、发布、部署和远端状态变化始终
需要对应的具体授权。

## 2. 当前业务与技术合同

本节直接拥有当前业务和技术语义。外部模块只通过其归属计划、公共类型和版本与本计划对接；
本计划不复制 DPT 的字段清单、阶段状态或执行叙事。

### 2.1 模块范围与运行边界

1. 首版只支持 Polymarket Global CLOB `btc-updown-5m` 系列、pUSD 与每个市场的两个结果代币，
   本账户主动操作仅为 BUY；同一账户轨迹必须覆盖其预登记范围内全部连续或重叠市场，但不建设
   跨市场品类插件、SELL、体育/赛事延迟或规则插件能力。
2. `model-train`、`offline-evaluation` 和 `paper-trade` 必须通过共用
   `Intent Bridge / Execution Adapter` 的模拟后端调用隔离模拟器实例。桥接器实例创建时
   固定后端，不得逐请求切换或在失败时跨模拟/实盘后端回退。
3. `live-trade` 只通过桥接器实盘后端委托官方 Polymarket Rust SDK；模拟器不接受
   `SignedOrder`、真实签名、认证、钱包、交易凭据或订单/账户网络调用，也不生成或覆盖
   真实平台事实。
4. `data-pipeline` 是规范市场事件及双时间投影的唯一生产方。`sim-system` 不读取类型化原始
   数据、Parquet、实时交接或原始帧，不重做排序、去重、规范化、缺口判定、时间投影或兑付
   派生。
5. 模拟器只拥有模拟平台账户；平台开户和每次经济变化各形成一次规范业务事实。账户、订单、
   成交、结算、查询和反馈都从该事实或同一次业务转换按消费者需要投影，不复制同一正文，也不
   向业务消费者暴露内部日志帧、恢复信息、资源证明或不变量布尔值。该账户状态与跨市场全局
   `PlatformAccountDeltaV2` 变化序列就是用户所称的“平台侧
   training ledger”，不是另一个 crate。订单 NOE、非订单经济归一化、同级目录中的交易侧 Ledger、Observation、标签、奖励、
   模型训练、筛选、晋级和 `sim-calibration` 均在本模块之外。外部归一化与 Ledger 只能单向
   消费所需投影，不得回写或参与模拟平台最终资金门。跨模块消费者需要解码的无行为身份与
   原始事实叶子归现有 `contracts`；业务转换和唯一事实生成仍归 `sim-system`，消费者只执行
   自身投影，不对模拟器内部状态重复验算。
6. 历史训练、离线评估和模拟盘复用同一核心与合同，但输入权限不同：完整的数据源事件时间
   隐藏平台回放只提供给训练和离线评估；模拟盘只能应用本机已经按模型可见时间接收的公开
   事件，不得访问隐藏未来状态。各实例可以共享符合角色权限的不可变、内容寻址事件页和规则/
   场景产物，但不同运行根的账户、订单簿叠加层、队列、订单、RNG、游标、反馈和日志必须隔离。
   首版每个 `SimProcessRoot` 只拥有一个会话和一条独立账户轨迹；同一会话的全部市场共享唯一
   模拟平台账户、全局资金池和账户变化序列。训练与离线评估通过多个运行根并行，同一经济账户
   不得拆成多个各自初始化资金的运行根或会话。
7. 阶段 2 形成的 `MarketSetV1` 与进程共享 `PlatformScenarioV1` 保持不可变，并作为每个
   运行根唯一会话的初始市场前缀。阶段 3 必须在不重建会话或账户的前提下，为该会话增加独立、版本化、
   仅追加的 `MarketEnrollmentV2` 序列；不同运行根的市场目录和登记互不改变。training 与
   offline-evaluation 必须在首个业务事件或请求被会话成功接纳前完成并冻结全部市场登记；首个
   成功业务输入形成后，任何追加登记都失败。paper 可以在一个或多个既有市场已经运行后逐项
   追加新市场，但待登记市场自身不得已经成功进入业务。受信组合根和上游负责确认候选市场属于
   本进程固定的 `btc-updown-5m` 系列；paper 的受信组合根还负责确认候选来自本机已经接收的合格
   规范市场输入。sim 不接收调用方自报的系列、接收时点、证据身份或证据摘要，也不重复证明这些
   上游资格。市场目录归属只在会话创建、市场登记和恢复边界建立一次，后续内部业务路径持有受信
   类型引用，不逐事务重复归属验证。
   每项登记只在会话内分配一个单调登记序号；会话实例和开户事实已经由控制句柄绑定，不在登记
   对象中重复携带，输入日志顺序也不另造前驱字段。控制请求只包含
   非空稳定 `enrollment_request_id` 与已经规范化的 `PlatformMarketV1`。控制错误或登记业务失败
   不属于已受理输入，不写日志，也不永久占用该身份；再次尝试可以复用该身份，改变业务意图由调用方
   使用新身份。首次成功受理后，同一 `enrollment_request_id` 始终返回该成功结果，不再次比较或
   处理后来携带的市场正文。调用方不提交载荷摘要、期望前一序号、期望序列长度或摘要；sim 按当前
   会话状态串行分配登记序号并仅追加登记序列。
   sim 必须在初始与追加市场的全目录中拒绝重复 `condition_id`、CLOB 市场 ID 或 token ID。
   paper 另按候选市场身份维护最小使用围栏：成功登记的逻辑请求若显式引用尚未登记的
   `condition_id`，后续不得再登记该 `condition_id`；请求没有 `condition_id` 而仅以 token ID
   过滤时，只围栏该 token ID。`cancel_all_orders`、不含 `condition_id`/token ID 的查询，以及
   只按平台订单 ID 操作的请求不建立候选市场围栏，并继续按其有效处理时点读取当时的会话目录。
   未登记市场的事件在入口失败时全部状态不变，不留下永久使用墓碑；市场成功登记后可以重试该
   事件。该设计不为每个业务请求绑定市场目录版本：已登记请求对未知候选身份形成的围栏已经防止
   后续登记反向改变其市场成员结论，无市场身份请求则应观察有效处理时点的当前状态。
   登记成功后原子扩展会话市场目录、适用角色的隐藏/公开市场状态、撮合队列、账户盘口叠加层及
   市场生命周期使用的目录；paper 不得因此创建隐藏状态。任一失败使这些状态全部不变；会话创建
   完成后，市场成员判断、事件授权、请求处理、`end_at`/生命周期枚举和撮合队列推进的唯一市场
   来源必须是该会话局部目录；不可变 `PlatformScenarioV1` 只用于构造初始前缀和冷启动边界，
   干净重放，后续业务不得继续直接枚举其 `market_set` 形成遗漏新增市场的影子路径。成功只增加
   新市场空状态，不预写零结果代币余额，也不复制或重置资金、占用、订单、成交、GTD、私有反馈
   序号或下一 `account_transition_seq`。
   该登记是受信组合根驱动的内部场景控制输入，不新增第九个公开交易操作。成功只在原子扩展完成
   后同步返回规范 `MarketEnrollmentV2`，直接以 `enrollment_request_id` 作为稳定登记身份，只另含
   登记序号和市场业务内容；仅追加顺序由输入日志及登记序号表达，不生成派生身份、前驱或内容摘要链。失败返回明确错误且
   全部状态不变。该同步控制结果不分配平台事件/接收时点、私有反馈序号或确认队列容量，也不经过
   生命周期反馈。阶段 4 的唯一归约器先处理该登记控制输入一次；失败直接返回且不写日志，成功只形成
   未生效状态和同步结果。`RecoverablePaper` 把同一成功输入耐久追加并完成本组一次 `sync_data` 后，
   才让状态生效并返回结果；日志不另存这份关联状态。阶段 4 的运行清单、
   日志和恢复再绑定初始场景与完整登记序列；本合同不改变阶段 2 已完成事实，阶段 3 实施只按
   当前游标推进。

### 2.2 八方法与模拟领域请求

唯一公开操作词汇为：

```text
post_order | post_orders | cancel_order | cancel_orders |
cancel_all_orders | order | orders | trades
```

1. 公开名字与官方 Rust SDK 的当前方法词汇对齐，但模拟参数和响应使用框架中立的领域类型，
   不伪装官方 `Client`，不预建通用后端 trait、插件注册表或公开总派发类型。新路径的八个方法
   特定请求、共享 `RequestContextV2` 和方法特定直接响应统一绑定
   `SIM_EXECUTION_CONTRACT_V2 = "sim_execution_contract_v2"`。八个方法在会话绑定句柄上接收
   强类型请求；边界只完成解码、输入/日志硬上限准入和同一 `global_request_id` 的结构化幂等
   查找，随后由唯一归约器对待受理请求处理一次，形成未生效的接纳与调度结果；同一请求日志成功后
   才让该结果生效并形成接收回执。不再建立“登记正文 + 执行变更正文”两份对象，也不计算请求或
   变更载荷摘要。方法特定接收回执只确认输入已进入模拟器及其
   稳定序号，不表示平台接受、拒绝、订单、撤单或查询结果。业务归约器到达请求的逻辑处理时点后
   才生成方法特定直接响应；不新增轮询、取回或第九个公开操作。
2. 会话句柄已经绑定运行、调用方、账户、会话和模拟后端，请求正文不重复携带或校验这些归属字段，
   也不携带运行时合同版本。调用方可见的 `RequestContextV2` 只含 `global_request_id` 和
   `caller_correlation_id`，批量项另有 ordinal；不再让调用方重复提交序号、发送时点、模型可见
   游标、派生时间、第二套用户幂等键或效果身份。共用桥接器第一次把策略决策交给受信会话边界时，
   该边界从自身已经闭合的公开/私有水位和模拟逻辑时点附加一份内部 `TrustedDecisionCutoffV2`，
   与请求一起只记录一次；稍后排队、投影接收暂停或物理到达顺序都不得改用演员届时的更新状态。
   日志序号只破同一决策截点的并列，再按获批代理时延配置档派生后续时间。同一
   `global_request_id` 的物理重投用结构化键直接返回原接收回执或结果，不重新比较后来正文，也不
   重新抽样、调度或执行业务；新的业务意图必须使用新身份。未知角色、live 后端或跨账户调用在
   取得会话句柄前已经不可达，不在每个请求重复验证。真实网络、重连或采集背压如何决定首次受信
   决策截点不在本轮定义；阶段 4 只使用进程内桥接器和受信测试驱动器证明组件语义。
3. 请求在 `platform_arrived_at` 到达模拟平台。未进入平台内部暂缓的订单以该时点作为有效
   处理时点；只有 FOK/FAK 市价 BUY 在冻结规则 `itode=true` 时进入 250ms 平台内部暂缓，并以
   暂缓结束后的时点处理。训练与离线评估只读取该时点已经生效的隐藏平台状态，并据此撮合、业务
   拒绝或生成响应；有效处理时点大于或等于 `end_at` 时产生正常拒绝结果，不创建订单、资金预留
   或成交。响应从 `response_received_at` 起才对交易侧可见；即使它先于对应公开市场消息到达，
   也只暴露响应自身及其明确携带的订单/账户事实，不得携带、触发或摘要化尚未接收的公开消息或
   隐藏状态。
4. GTC/GTD 限价 BUY 使用 `limit_price_pusd_per_share + size_shares`；FOK/FAK 市价 BUY
   使用 `spend_pusd + worst_price_pusd_per_share`。订单精度、计量编码和舍入复用固定官方
   SDK 的类型或工具，不以手写浮点或含义不明的裸 `size` 混用 base 与 quote。
5. `post_orders` 接受 1 至 15 项并逐项、非原子处理；一项失败不回滚已接受项，幂等重试
   保持原项到结果映射。`post_order` 只包含一项。
6. `cancel_order`、`cancel_orders` 和 `cancel_all_orders` 只取消平台处理截点仍未成交且
   可撤的本账户剩余量；新订单准入、盘口质量、tick、`accepting_orders` 或仅撤模式不得阻断
   对既有活动订单的撤销。批撤上限以项目合同 `SIM_CANCEL_BATCH_LIMIT_V1` 保守冻结为 1,000；
   官方当前 API Reference 与操作指南分别记载 1,000 和 3,000，该数值不得冒充无争议平台
   常量。重复平台订单 ID 按官方可观察语义忽略并只处理一次，直接响应按唯一目标返回一次结果，
   同时保留全部原输入项映射。批撤和全撤的逐目标业务结果不是全成全败；一个目标未撤或证据
   不足不回滚其他成功目标。单线程业务归约器在完整处理本次输入后才一次公开状态和输出；若出现
   非业务代码故障，则关闭本次运行并从已提交输入日志重新归约，不在热路径复制完整工作副本或
   追加一轮内部不变量自证。`end_at` 至合格 `MarketResolved` 到达前继续允许撤销已有活动订单，
   模拟器不得自行制造调用方撤单。
7. `order` 返回请求处理截点的单笔平台投影；`orders` 只列活动订单；`trades` 从本账户模拟
   成交形成时起列出成交，而不是等待阶段 3 最终性。已有开放订单在 `end_at` 后、合格
   `MarketResolved` 到达前仍属于活动订单并可查询。列表页大小以版本化项目兼容性假设
   `SIM_QUERY_PAGE_SIZE_V1 = 100` 固定，调用方不得自定义；官方当前仅在响应示例中给出 100，
   不构成平台永久保证。查询使用项目规范升序唯一键和冻结的不可变根投影；会话绑定接口返回
   `QueryCursorV2 { root_sequence, next_offset }`，后续页只在同一会话句柄内读取该不可变根，
   不重读当前可变状态。查询根序号只在 `order/orders/trades` 可观察内容改变时推进，纯市场事件、
   查询、响应交付和无副作用结果不推进；保留水位使用独立单调字段。物理重投按首次请求身份返回
   原查询根和结果，不重新比较过滤正文或生成查询指纹。游标不重复携带会话、账户、合同、过滤、
   页大小版本、保留水位或内容摘要；跨运行根使用由各自绑定句柄不可达，根已越过保留水位时返回明确
   `OutOfRetention` 结果。
   `caller_correlation_id` 只作追踪，不能充当重试或截点身份。合法空页、完整覆盖下未找到与
   跨保留期结果必须分层，残缺页不得伪装成完整结果。
8. 首版不存在 `amend_order`、`modify_order`、`replace_order` 或隐藏等价入口。改变订单
   必须先撤单，再以新的 `global_request_id`、本地引用和新平台订单 ID 独立提交；二者不是原子替换。
9. 边界解码错误、控制错误、正常平台业务结果和证据不足必须分层。外部请求无法按公开强类型合同
   解码时，由接口直接返回 `Result::Err`，不进入归约器或日志；会话归属已经由独占句柄确定，不在每个
   请求重复核对运行、调用方、账户、后端或合同版本。八方法经归约形成的合法平台拒绝、证据不足、
   合法空结果和未找到仍是已受理业务结果，必须随同一输入写入日志。同一 `global_request_id` 在首次
   已受理后采用首次结果为准：物理重投直接返回首次接收序号或既有结果，不比较后来正文；新的业务
   意图必须使用新身份。游标失效、
   合法平台拒绝、证据不足、合法空页、未找到和右删失都由同一业务归约器形成方法特定结果，不得
   伪装成边界解码错误、程序损坏或第二套登记状态。
10. 新订单在同一个有效处理时点按以下固定优先级消费该角色获准的平台状态：
    - 有效处理时点大于或等于 `end_at` 时形成确定性平台拒绝；
    - `active == Some(false)`、`closed == Some(true)` 或
      `accepting_orders == Some(false)` 任一成立时形成确定性平台拒绝；任何 `None` 仍表示
      未观察到，不转换为肯定值，也不单独否决；
    - 市场仍为 `PRELOADING` 时形成零业务副作用的 `EvidenceUnknownNoEffect`；
    - 目标资产盘口质量不是 `Fresh` 时形成零业务副作用的 `EvidenceUnknownNoEffect`，其中
      包括目标资产为 `UnknownNotObserved`；另一资产的盘口质量不得跨资产阻断本目标资产；
    - 通过上述门后，才继续执行 tick、最小 shares、post-only、适用本金门和订单类型规则。
    三个布尔对训练、离线评估和模拟盘使用同一谓词，但只能读取各自角色获准的平台投影：训练
    与离线评估不得把仅有接收时间的公开布尔混入隐藏平台状态，模拟盘不得读取隐藏状态。
    `QualityScopeV1::Market` 当前表达市场判定/结算质量，不是普通盘口质量；其正常盘前初值
    `UnknownNotObserved` 不构成额外订单否决。FOK/FAK 市价 BUY 在适用 `itode` 暂缓结束后，
    必须读取该平台处理时点的市场状态并执行本项业务顺序。平台拒绝和证据不足都只作用于本次新订单，不关闭、
    撤销或释放既有 maker 订单。本项中的平台拒绝与 `EvidenceUnknownNoEffect` 只是任务 2.2
    的内部准入终局判定，不预先规定任务 2.3 的公开响应变体。
11. 任务 2.2 的规则直接属于唯一业务归约器，不建立 `PreparedSubmissionV1`、通用
    `Prepared*/Committable*` 或跨任务准备状态。归约器在同一次输入处理中按有效平台时点读取
    状态，使用完成订单编码、资金计算和撮合所需的局部值，随后形成逐项终局业务结果；正常拒绝或
    证据未知不改变订单、资金和成交。接受项的订单、资金占用、撮合结果、状态与消费者投影在归约
    完成后先保持为未生效结果；同一已受理输入追加日志成功后，状态和消费者投影才一次对观察者生效，
    不另写状态增量或提交证明。归约期间不处理下一输入，也不向外暴露部分结果；非业务代码故障关闭
    运行并由已提交输入日志重放，不在当前进程做完整状态回滚。
12. 任务 2.3 的新运行初始账户、直接响应与消费者投影合同如下；原业务语义保留，持久和交付方式
    受本计划替换：
    - 每个会话必须显式取得不可变且无默认值的 `InitialAccountStateV2`。首版只由
      调用方给出非负、最多 6 位小数的 `available_pusd`；初始结果代币、资金预留和返佣应收款
      均为零。该值类型只保存实际初始值，不保存正文摘要或由正文生成的身份。该输入必须绑定会话
      规范和后续运行清单；不得采用默认零资金、无限资金或事后补余额；旧
      `InitialAccountStateV1` 只留历史只读数据模式，不进入新运行；
    - 新路径的会话创建规范只接受 `SessionSpecV3`：每个 `SimProcessRoot` 运行根
      在创建唯一会话前显式绑定一个非空、稳定且无默认值的 `run_id` 及一份在进程边界解码一次的
      `TradeFinalityPolicyV2`；该运行根只创建这一个会话。创建方在 `SessionSpecV3` 中显式
      提供同一 `run_id`，不得从 `session_id`、时间或随机数隐式推导，也不得与运行根不一致；
      另一条账户轨迹必须使用另一 `run_id` 和另一个运行根。模拟 `platform_source` 由 `sim-system` 固定产生，调用
      方不得填写或冒充 live 来源。旧 `SessionSpecV1/V2` 不满足新开户事实合同，不建立兼容回退或
      双写路径；
    - 一个运行根对应一条独立模拟账户轨迹并只拥有一个会话，轨迹身份绑定运行、会话、调用方角色与实例、账户、
      模拟来源和初始场景；场景内全部 `condition_id` 共享同一个
      `ExecutionStateV2` 和可用 pUSD。阶段 3 必须先形成
      `AccountInstanceKeyV1(run_id, caller_role, caller_instance_id, account_id, platform_source)`，
      并仅在会话创建业务条件全部满足时生成恰好一个 `AccountOpeningFactV2`。该事实直接包含
      `creation_request_id` 作为事实身份、初始 `PlatformScenarioV1` 引用和
      `InitialAccountStateV2` 值，不再生成开户锚、开户内容摘要、会话规范正文摘要或另一派生身份；
      随后仅追加市场登记不得改写它；
      `SessionSpecV3` 还必须绑定非空稳定的 `creation_request_id`、`response_bridge_binding_id` 和
      显式唯一会话 `session_seed`；该种子只随首次成功的 `CreateSession` 已受理输入记录一次，继续供
      既有无状态代理时延采样使用。新路径不增加 `run_seed`，不从运行、会话、时间或正文散列派生
      会话种子；
      桥绑定身份只标识受信组合根已经构造的进程内输出消费端，不把回调、
      闭包或 I/O 对象写入公共账户合同。`create_session` 必须把 `PREPARED_LEDGER_INIT` 唯一会话、
      账户实例键保留和开户事实作为一个同步开户结果原子形成；此状态下八方法业务入口不可用。同一
      `creation_request_id` 的物理重试直接返回首次开户事实和准备结果，不比较后来规格或桥正文，
      不返回第二个可变会话句柄、重新消费或替换桥对象；首次成功恰好交付一个独占会话句柄，重复
      结果只明确表示 `EXISTING_PREPARED`。唯一会话形成后，活动运行根不再提供第二个
      `CreateSession` 业务入口；另一条轨迹必须新建运行根，不生成第二份本金。开户事实不是平台生命周期事件，不携带
      `platform_event_at`、`feedback_received_at`、私有反馈序号或传输代理时延，也不进入确认式
      反馈队列。受信组合根必须用同一开户事实幂等初始化同级目录中交易侧 Ledger，确定成功后才以
      稳定激活身份调用 sim 本地控制面，把同一会话原子迁移为 `ACTIVE`；激活响应丢失时以同一
      身份重试并返回原结果。Ledger 结果未知时只能查询或重试同一开户事实，不得激活、失败关闭或
      创建新开户事实；Ledger 明确拒绝后才把准备会话迁移为 `INITIALIZATION_FAILED_CLOSED`，永久
      禁止业务入口，并在运行保留期内保留原键、开户事实和稳定失败决定身份，不能让同一键取得第二份
      本金。实际 Ledger 拒绝回执由受信组合根拥有；`sim-system` 不接收其正文或摘要。
      该状态是运行初始化失败而不是模型资金结果；training/offline-evaluation 不得自适应替换该
      预登记轨迹，paper 不得自动新建账户继续运行。
      阶段 3 若丢失唯一会话句柄，只能把当前运行判为本地组合失败，不能靠创建重试取得第二个
      句柄；是否按外部预登记运行合同重新开始不由 sim 推定，跨进程恢复与同一桥身份重接唯一归
      阶段 4。
      上述准备、激活和失败关闭都是受信组合根使用的同步控制面结果，不是第九个交易操作；
      `sim-system` 不调用、读取或解释 Ledger 回执，组合根独自验证该前置。
      调用方不得另填第二份初始余额、持仓、占用或返佣应收。跨模块公共根合同放入 `contracts`，
      唯一生成和状态行为仍在 `sim-system`；
      `InitialAccountStateV2` 不提供 `2000 pUSD` 默认，
      模拟器也不执行外部每市场 `1000 pUSD` 风险上限或运行中自动补资；
    - 当前宿主不得用多个运行根或会话表示同一活动经济账户。唯一会话创建必须在生成开户事实前按完整
      `AccountInstanceKeyV1` 建立准备/活动保留；首次成功创建后，同一创建身份直接返回该结果。不同 `run_id` 可以
      复用同一业务 `account_id`，但必须由不同运行根、唯一会话和开户事实形成隔离账户序列。开户事实
      只绑定初始场景，后续运行清单和日志分别绑定该开户事实与当时的市场登记序列。每个
      `SimProcessRoot` 把唯一“账户实例键 -> 原 `creation_request_id`”资本签发事实保留到该运行根
      结束；会话热态、查询投影或保留期证据删除不得删除它，也不得让失败关闭的运行重新取得本金。
      该事实不跨 `run_id`，不是永久可查询墓碑，结构数量固定为一；
    - 任务 2.3 只前移决定订单提交能否接受所需的最小资金检查与预留。任务 2.2 准入通过后，
      先以纯预演形成本次逐档撮合与即时 taker 手续费，再在分配平台订单 ID 和提交任何业务
      状态前，按同一次归约中得到的最终 SDK 编码 `maker_amount_pusd` 税费前本金与
      本次即时 taker 手续费之和检查可用 pUSD；任一不足均形成
      `InsufficientAvailablePusd` 的 `RejectedNoEffect` 且零业务副作用。不得以资金上限静默
      缩小税费前本金，也不计算 builder fee。接受时本金占用、手续费占用、订单创建和本次撮合
      结果原子提交；手续费在任务 2.3 只形成独立占用，最终应用、失败释放和补偿仍归阶段 3；
      批量按已冻结的有效处理时点、日志序号和 ordinal 逻辑顺序逐项非原子处理，后处理项
      读取此前已处理项已经占用或释放后的资金；最终响应仍按原 ordinal 排列。成交最终性、补偿、统一
      账户变化序列、返佣、赎回和后续释放仍归阶段 3；
    - 公共面只增加方法特定的 `PostOrderDirectResponseV2` 与
      `PostOrdersDirectResponseV2`，不得产生公共通用响应派发类型。模拟会话类型和已绑定输出通道
      已确定运行、账户、调用方及 `SIMULATED` 来源，因此响应不重复携带或验证这些字段。两者只保留
      消费方关联结果所需的请求、关联和方法身份、`platform_responded_at`、
      `response_received_at`、私有序号及方法特定业务结果；`post_order`
      含一项，`post_orders` 含与登记请求等长且按原
      `batch_item_ordinal` 升序的完整项数组，不提供整体 `success` 布尔值，也不部分交付批量；
    - 每项显式绑定 `item_id`、ordinal、市场、资产和有效处理时点，结果只允许
      `Accepted`、`RejectedNoEffect`、`EvidenceUnknownNoEffect` 三个封闭变体。
      `Accepted` 才能携带非空稳定模拟订单 ID、平台原始状态、稳定模拟成交 ID、可空订单类型
      终止原因以及显式单位的计量分桶。GTC/GTD 按 base 计量并表达接受、成交、开放和订单类型
      终止数量；FOK/FAK 按 quote 预算计量并表达接受预算、成交消耗、释放或未花 quote、实际
      成交 base，以及单列的固定 SDK `takerAmount` 最差价格 base 审计投影。另两个变体不得
      创建订单、成交、预留或账户变化，也不得以空订单 ID 占位。FOK 全不成和 FAK 的未成交
      余量服从官方对齐的零成交边界：FOK 无法消耗全部接受 quote 时形成
      `RejectedNoEffect(FokNotFullyFilled)`；FAK 完全没有正成交时形成
      `RejectedNoEffect(FakNoMatch)`。两者都不创建订单、成交、资金占用或订单 ID。FAK 已经有
      正成交时仍为 `Accepted + Matched`，未花 quote 以专属 `FakCompleted` 终止原因和 quote
      释放分桶表达，不生成 `canceled_base_quantity`；FOK 全成时为 `Accepted + Matched`；
    - DPT L2 不含真实逐订单边界，任务 2.3 因而明确采用
      `L2_AGGREGATE_SYNTHETIC_MAKER_V1` 项目假设：每个价格优先的 L2 聚合价位是一个合成
      maker 段，base 与 quote 均先以向零方向量化为 6 位微单位的非负整数表示；合成段完整
      quote 使用量化后 base 与价位的累计整数结果，部分消费使用官方 CLOB V2
      `floor(making_amount * taker_amount / maker_amount)` 关系，并用同一段累计结果之差形成
      逐次 quote，禁止独立逐 fill 截断和 quote 为零的成交。无法形成正 quote 的 base 余量
      留在外部簿。FOK 只有同一算法预演能够消耗全部接受 quote 预算时才全成，否则全不成；
      FAK 以同一算法尽量消费并释放剩余 quote。预演与提交不得使用不同量化路径；
      对规范价位数量 `x` 定义 `micro_base(x)=floor(x*1_000_000)`。初始价位以
      `micro_base(S0)` 创建外部段；后续 `E -> S` 只使用
      `delta_micro_base=micro_base(S)-micro_base(E)`，禁止改为
      `micro_base(S-E)`。正差作为当次转换产生的新外部段进入当时队尾，负差从当时队头消费，
      亚微余数只留在精确 canonical 外部盘口；跨越微单位阈值产生或消失的整微单位归当次
      转换。每个新外部段的完整 quote 独立按该段 base 与源 L2 价位向下形成，不使用价位总
      quote 的端点差，也不把量化后的段内有效价用于放宽限价或最差价格门；
    - 用户批准 maker 零 quote 穿越的保守合同。DPT 队头减量穿过本账户 maker 后，若从该订单
      已成交 base 起算、将既有待累计量与本次穿越量合并所得的订单累计 quote 增量仍为零，
      六位队列和账户叠加层必须立即扣减本次穿越 base，但不得形成 fill、成交经济变化、结果
      代币或返佣；该 base 从 `open_base_quantity` 转入每订单有界的
      `pending_maker_depletion_base_quantity`。后续同一订单再次被队头穿越时继续合并；首次使
      `floor((matched_base + pending_base + new_depletion) * accepted_quote / accepted_base)
      - floor(matched_base * accepted_quote / accepted_base)` 为正时，才把全部待累计量与本次
      穿越量原子形成一笔正 quote 的 maker 推定成交。待累计期间不得减少或释放该订单剩余的
      `open_encumbered_quote_amount`。对 base 订单固定
      `A(x)=floor(x*accepted_quote/accepted_base)`；稳定待累计状态必须满足
      `pending_quote=A(matched_base+pending_base)-A(matched_base)=0`，剩余未成交本金固定为
      `accepted_quote-A(matched_base+pending_base)`，禁止分别对 pending 和 open base 独立
      乘价、截断或释放。调用方撤单、GTD 到期或合格 `MarketResolved` 关闭时，
      待累计量与仍活动量一起转入对应既有终态 base 分桶并只释放一次剩余开放本金占用，仍不
      生成 fill。`end_at` 只移除撮合资格，不关闭或释放待累计量。热状态只保留完成后续数量计算
      必需的待累计 base，不保存事件数组、滚动摘要、事件计数或首末身份；来源追溯由输入日志冷端
      重放获得。该中间分桶不是 `VENUE_DUST_TERMINAL`，也不作为独立字段进入模型可见订单或成交
      查询投影；订单查询必须以 `active_unfilled_base_quantity =
      pending_maker_depletion_base_quantity + open_base_quantity` 显式表达仍可撤、可到期和可由判定
      关闭的活动总量，但不得借该总量反推出或暴露内部待累计分桶；
    - 即时 taker 手续费按每个已预演成交价位的实际成交 base 与该价位分别代入密封规则快照的
      `POLYMARKET_FEE_CURVE_V1`，先保留精确十进制并求和，再对正数总额采用五位小数的
      `MidpointAwayFromZero` 舍入；结果小于 `0.00001` pUSD 时固定为零。该汇总和中点规则是
      首版项目假设，必须由规则版本和最终清单引用；maker 不收平台费，首版请求没有
      builder code，因而 builder fee 恒为零。阶段 3 把该整单费用分配到各 taker 成交时，仍只计算一次
      上述整单总额：先求每个成交未舍入的精确费用贡献，对每份以 `0.00001 pUSD` 为最小单位向下
      取整，再把“整单已舍入单位数 - 各份向下取整单位数之和”按精确小数余数从大到小逐单位分配，
      余数相同时按同一次归约已分配的 `fill_sequence` 升序破平。每笔成交份额与整单总额均不再二次舍入，
      份额之和必须严格等于原整单费用，输入物理顺序和并行方式不得改变分配结果；
    - 每个逻辑请求恰好形成一个不可变响应。单笔 `platform_responded_at` 等于本次请求项终局
      响应原子形成时点，也就是本次响应项的 `effective_processing_at_utc_ms`。批量只有在本次
      请求全部项终局后密封，
      `platform_responded_at` 等于本次请求各响应项有效处理时点的最大值；
      `response_received_at` 继续使用登记时冻结的入站代理时延。物理请求重投只返回原登记回执，
      不重新撮合、生成响应、占槽或抽样；
    - 运行级业务演员一次只处理一个已经提交的输入并形成一个有界投影批次，不在订单、资金或撮合
      前预留和验证逐输出发件箱槽位。预绑定的唯一运行级投影接收端口暂时不能接纳时，演员保留
      当前批次并暂停后续输入；不重新执行业务、不忙循环，也不把背压改写成业务拒绝。该端口按
      `(run_id, projection_batch_sequence)` 确认连续前缀后释放当前批次；日志恢复从同一输入重建
      未越过水位的输出。该端口直接绑定唯一会话创建时的 `response_bridge_binding_id`；批次不携带
      会话路由键，也不重复保存桥绑定身份。不得持有会话锁做阻塞发送，不得暴露 `poll/take/ack` 等
      第九公开操作。单批最大值和总资源硬上限归任务 4.5 实测冻结；日志和崩溃恢复归任务 4.3。
    任务 2.3 保留以下零 quote 外部余量与幂等双时点业务合同：
    - 外部合成段有正 base、但完整六位 quote 为零时，该余量仍保留在规范外部簿，供后续 DPT
      重校准或队头减量使用；账户 taker 不消费它，它对 taker 可执行价位扫描透明，也不阻塞
      后方较差但可形成正 quote 的价位。post-only 与正式撮合必须共用该“可执行流动性”定义，
      不得一条路径跳过而另一条路径停止；
    - `global_request_id` 是唯一请求幂等身份。物理重试必须复用同一 ID 并直接返回首次接收序号或
      结果；新的 ID 始终表示新的逻辑请求，按届时业务状态处理。不存在跨 ID 效果复用、
      `NEW_EFFECT` / `EXISTING_EFFECT`、提交效果身份或首次效果时点第二套状态机。
13. 任务 2.2 保留以下订单精度与时点合同：
    - DPT 任意正数动态 tick 保持有效，包含真实已覆盖的 `0.00001`。允许窄扩展仓库现有固定
      SDK 补丁，把订单构造器已有的纯 `Decimal` 价格小数位处理、上下界与 tick 对齐校验及
      BUY maker/taker 数量编码提取为公共无状态原语，并由原订单构造器和模拟器共同复用；各
      路径保留既有截断规则，最终数量以原 `to_fixed_u128` 路径完成 6 位固定精度量化后才作为
      编码结果。该扩展不得接受或暴露 `Client`、
      网络、签名、钱包、凭据、订单提交或任意 REST，也不得在模拟器内复制第二套精度算法；
    - GTD 以有效处理时点为唯一基准。令
      `encoded_expires_sec=floor(expires_at_utc_ms/1000)`、
      `processing_sec=ceil(effective_processing_at_utc_ms/1000)`，只有
      `encoded_expires_sec >= processing_sec + minimum_lead_seconds + safety_offset_seconds`
      才通过；首版右侧总和为 240 秒，等号通过。模拟订单的实际到期时点固定为
      `encoded_expires_sec * 1000`，不得保留被 SDK 秒级编码丢弃的毫秒；
    - FOK/FAK BUY 在撮合前以 `worst_price_pusd_per_share` 进入同一 SDK 纯编码原语，把最终
      SDK 编码后的 base `takerAmount` shares 与同步 `mos` 比较；小于 `mos` 时拒绝，等号通过。
      不按实际成交 shares 决定本门，也不得因 FAK 最终部分成交少于 `mos` 而追溯改写准入。
14. 任务 2.4 保留以下撤单、查询响应、分页业务语义，并使用新消费者投影和保留读取合同：
    - 新路径公共面使用六个方法特定直接响应：`CancelOrderDirectResponseV2`、
      `CancelOrdersDirectResponseV2`、`CancelAllOrdersDirectResponseV2`、
      `OrderDirectResponseV2`、`OrdersDirectResponseV2` 与 `TradesDirectResponseV2`。内部把现有
      单一动作队头、连续私有事实顺序和预绑定桥接器泛化到八方法；不得增加公共总响应枚举、按方法
      旁路驱动器或 `poll/take/ack` 等第九操作。每个已受理请求由同一归约形成一个有界响应批次，
      不在业务变化前预留或验证逐输出槽位；同一 `global_request_id` 的物理重投遵守任务 2.3
      的首次写入和原结果复用合同，新 ID 不跨请求复用效果；
    - `cancel_order` 返回一个目标；`cancel_orders` 先按平台订单 ID 去重并按首次出现顺序返回
      唯一目标，每个目标携带按原输入顺序排列的 `input_item_refs`；`cancel_all_orders` 在有效
      处理截点冻结本会话、本账户全部市场中 `pending_maker_depletion_base + open_base > 0`
      的订单，并按 `platform_order_id` 升序形成稳定目标集，空集合合法。同一幂等效果必须复用
      首次冻结的目标集，不得顺带撤销后来订单。三方法都按目标表达 `Canceled`、
      `NotCanceledNoEffect` 或 `EvidenceUnknownNoEffect`；当前运行轨迹内不存在或不归属本账户的目标
      统一为不泄露归属的 `NotFoundOrNotOwned`，已有终态或无可撤余量为
      `NoCancellableRemainder`。未知只保留给明确证据覆盖缺口，首版不为满足类型而制造可达未知；
      过保留期会话走请求级范围错误，内部不变量损坏失败即关闭。新路径模拟订单 ID 在运行根范围内使用
      `SIM_PLATFORM_ORDER_ID_V2 = "sim-" + base10(order_sequence)`；跨运行聚合时由
      `(AccountInstanceKeyV1, platform_order_id)` 定位，
      不散列为 64 位十六进制；格式非法、空批次和超过 1,000 项在改变状态前走
      `Result::Err`。批量结果用 ordinal 唯一区分，调用方重复 `item_id` 只作为原样关联值返回，
      不触发另一套唯一性验证；
    - 每个成功撤单目标原子执行：`canceled_base += pending_maker_base + open_base`，只把
      `open_quote` 转入调用方撤单释放分桶并令 `available_pusd += open_quote`、
      `principal_encumbered -= open_quote`，随后把 pending、open 和 open quote 清零。撮合队列
      只移除仍开放段，账户叠加层只扣仍开放 base；已成交 base/quote、成交记录、结果代币与
      即时 taker 手续费占用保持不变，pending 数量进入既有终态分桶，来源由输入日志冷端重放。
      本任务先推进已经
      实现的 GTD 到期，并且不得用撤单覆盖任何已经形成的订单终态；最终系统中已生效合格判定
      先于同刻撤单的订单终态迁移及端到端验证仍唯一归任务 3.5.1，不在任务 2.4 读取市场判定后
      建立临时拦截或影子终态路径。`end_at` 只停止撮合，不关闭或阻断撤单；
    - `order` 的结果为 `Found`、`NotFound` 或 `OutOfRetention`；`orders` 与 `trades` 的结果为
      `Page`、`InvalidCursor` 或 `OutOfRetention`，合法过滤无匹配返回成功空页。`orders` 的活动定义仍为
      `pending_maker_depletion_base + open_base > 0`，包括 `end_at` 后至合格判定前的订单；
      `trades` 从成交记录与零偏移最终性首态原子形成时起可查，首态可以是
      `MATCHED_NOT_BROADCASTED` 或 `MATCHED`，不得等待推进到 `MATCHED`；后续最终性状态只影响
      新查询根。投影同时保留模拟平台原始
      状态与强类型已知语义，任何新拼写进入 `Unknown(raw)`，不得把某一官方页面的枚举当成
      穷尽集合。`orders` 首版只保留市场与资产过滤，单 ID 使用 `order`；`trades` 保留成交 ID、
      市场、资产及时间过滤，maker/taker 地址因单会话单模拟账户边界而有意不暴露；
    - `orders` 固定按 `(created_at_utc_ms ASC, platform_order_id ASC)`，`trades` 固定按
      `(matched_at_utc_ms ASC, trade_id ASC)`；两者属于
      `PROJECT_CANONICAL_QUERY_ORDER_V1`，不声称官方默认排序。成交时间过滤使用现有毫秒领域
      合同和严格开区间 `after_utc_ms < matched_at_utc_ms < before_utc_ms`：负时间或
      `after > before` 为合同错误，`after == before` 为合法空区间。官方 SDK 秒级输入只能在
      外部对照层做受检映射，不得静默改变本合同单位；若精确 `trade_id` 在当前保留投影中
      已存在，则该正向事实足以形成完整结果，不因省略时间边界而误报跨保留期；精确 ID 不存在
      且其规范时间范围跨过成交保留水位时仍返回 `OutOfRetention`，不得伪装成完整空页；
    - 根查询物化完整、已过滤且已排序的不可变投影；新路径游标只用强类型
      `QueryCursorV2 { root_sequence, next_offset }` 引用当前会话自己的查询根，不生成载荷摘要、
      签名或跨字段归属证明。终页在项目响应中统一为无后续游标，不复制官方文档中互相冲突的
      空字符串或 `LTE=` 哨兵。续页先比较 `root_sequence` 与单调根保留水位：低于水位时返回方法
      特定 `OutOfRetention`，该结果只表示判断所需证据已退出保留范围，不证明游标曾经合法；仍在
      保留范围但对应方法和强类型过滤根不存在，或 `next_offset` 越过冻结根边界时返回
      `InvalidCursor`。根按修订序号整代裁剪，低于水位后不得以同一旧序号重新物化；不增加偏移页
      边界、游标签名、签发墓碑或过滤摘要校验。会话句柄本身完成隔离，不再重复编码会话、方法和过滤
      条件供二次验证。
      任务 2.4 只实现进程内紧凑根投影、
      显式保留水位和读取结果；实际保留时长、投影释放、运行关闭后的整体删除、日志和恢复仍归阶段 4，
      不得提前宣称完成。100 项代表页的投影字节必须在任务验证中记录；最终硬上限仍由
      任务 4.5 实测冻结。

### 2.3 进程级低频规则初始化与成熟工具

1. `sim-system` 进程的组合根必须在创建任何训练、离线评估或模拟盘会话、处理任何业务请求
   前取得唯一不可变 `PlatformRuleSnapshot`。当前进程使用 `CURRENT_QUERY`，只对调用方
   显式给出的一个 `btc-updown-5m` 参考市场完成一次同步；随后全部运行、会话和后续同系列
   市场共享同一个强类型快照句柄到进程退出，不得逐运行、逐会话或逐目标市场查询、扩展或
   热刷新。新写路径从步骤 4.3.1 起只使用
   `PLATFORM_RULE_SNAPSHOT_SCHEMA_V3 = "platform_rule_snapshot_v3"`；已实施 V2 中的原始响应摘要、
   内存快照摘要和固定为真的来源核对字段不得进入 V3，V2 只由历史只读入口解码，不再直接用于新运行。
   此前审核中的逐运行 V1 从未形成正式产物，不得被读取或伪装成 V3。
2. 参考市场由进程组合根作为唯一 `ReferenceMarketSpecV1` 提供，必须包含精确
   `condition_id`、CLOB/Gamma 市场身份及两个 token/outcome。加载器不发现市场，也不接受
   市场集合。私有 `PlatformRuleHttpLoader` 只允许：
   - 对该参考 `condition_id` 调用一次 CLOB
     `GET https://clob.polymarket.com/clob-markets/{condition_id}`；
   - 对同一 `condition_id` 调用 Gamma
     `GET https://gamma-api.polymarket.com/markets/keyset`，分别查询 `closed=false` 与
     `closed=true`，两份响应合并后必须恰好得到一个参考市场。
   不使用批量目标、`condition_ids[]`、逗号拼接、旧 `/markets`、全市场扫描、重定向、订单簿、
   订单、账户、用户频道或认证端点。
3. 每个完整响应只由专用窄 DTO 强类型解析一次，不为原始响应另算内容摘要。DTO 忽略未知附加
   字段，不使用 `deny_unknown_fields`；数值先取得 `serde_json::value::RawValue` 原始词法，
   再由 `rust_decimal` 精确解析，禁止先进入 `f64` 或用 `serde_json::Value` 任意探测。
   Gamma 的 JSON 编码 `outcomes` 与 `clobTokenIds` 必须二次强类型解析，并与参考身份和 CLOB
   token/outcome 精确一致。未完整接收的响应直接失败，不产生候选规则事实。
4. 同步出的进程规则只包含：CLOB `mos`、CLOB `itode`、Gamma `feesEnabled` 和完整
   `feeSchedule`；费用必须启用，`feeSchedule` 必须合法，且其
   `rate/exponent/takerOnly` 与 CLOB `fd.r/e/to` 精确一致。maker `rebateRate` 必须为用户冻结
   的 20%，否则进程启动失败。`negRisk`、CLOB `nr/oas`、Gamma
   `orderMinSize/orderPriceMinTickSize` 没有首版消费者，不进入快照、诊断或一致性门。tick 只
   来自规范事件链，规则初始化不得查询、保存、覆盖或交叉校验 tick。CLOB 官方线协议在
   `itode=false` 时省略该键；加载器必须把这种合法省略强类型规范化为 `false`。该省略不属于
   第 7 项的“规则缺失”；`itode` 存在但不是布尔值等无法确定语义的响应仍须在密封前失败。
5. 官方响应只证明参考市场在 `rules_observed_at_utc_ms` 的真实取值；把该组值应用到本进程
   后续全部 `btc-updown-5m` 市场，是用户确认的
   `BTC_UPDOWN_5M_PROCESS_RULE_TEMPLATE_V1` 项目范围假设，不得写成对后续每个市场逐项查询
   或平台永久保证。规则实际变化只在新进程启动时重新同步；恢复既有进程或运行必须读取原
   快照，不能以新查询改写旧结果。
6. 历史训练和离线评估进程禁止联网回填，只能通过隔离的历史只读入口取得适用规则值；历史格式
   不进入新写路径，也不在运行时自动兼容。快照观测时点必须不晚于回放因果起点。`RECOVERY`
   同样只读既有规则和实现身份；历史与恢复入口不接受 HTTP 客户端、端点或参考市场。
7. 只读 GET 的瞬时传输错误或官方列明的 `500/503` 在同一初始化总时限内最多额外重试一次；
   请求最终失败、解析失败、必需身份或规则缺失、非法或冲突不重试，并在任何会话创建前使
   整个进程规则能力失败关闭。单个完整 HTTP 响应和最终快照落盘字节分别硬限 16 MiB；不够
   时只能经后续明确计划调整到用户允许的 52 MiB，不能在运行时自动放宽。
8. 仓库版本化 `sim-system/config/platform-rules-v1.toml` 保存官方文档规则和用户冻结的
   项目/账户规则。首版至少固定：
   - 只有 `itode=true` 的 FOK/FAK 市价 BUY 应用 250ms 平台内部 taker 暂缓；暂缓前只完成方法分类
     和排程，结束后的有效处理时点才首次且仅一次按已到达状态执行准入与撮合。GTC/GTD 限价 BUY
     不因到达时盘口可立即成交而进入该路径，地域网络
     时延配置档不能覆盖或重复叠加；
   - 批量提交 15、批量撤单 1,000、GTD 声明提前 180 秒并含 60 秒安全偏移；
   - 手续费公式版本、5 位手续费精度、最小非零手续费、返佣日界与 1 pUSD 付款门槛；
   - 可立即成交 GTC/GTD 限价 BUY 的提交本金至少 1 pUSD；非可立即成交限价单及
     FOK/FAK 不推测套用该门；
   - `end_at` 起拒绝新订单，已有开放订单保持可查询、可撤但不再参与撮合；合格
     `MarketResolved` 到达时仍开放的余量才进入 `ORDER_STATUS_CANCELED_MARKET_RESOLVED`；
   - 平台小额余量、固定 8% taker 返佣、固定 20% maker 返佣、两类正常现金可用时刻和付款
     截取规则；不查询或累计 wV，不动态定级，不实现一次性奖金或 Liquidity Rewards。
   - `2000 pUSD` 初始本金、每个 `condition_id` 当前含即时手续费占用上限 `1000 pUSD` 和约
     三分钟市场结束至现金回账均不进入 `PlatformRuleSnapshot`：前两者属于外部账户运行配置，
     后者只可属于版本化场景/不确定性输入，三者都不是模拟平台固定规则或计时器。
9. 快照按 `synchronized_platform_rules`、`documented_platform_rules`、
   `project_account_rules` 与 `provenance` 分区；来源追溯绑定参考市场、系列适用策略、观测时点、
   两个端点、尝试数、实际规则值、静态配置版本、依赖和实现身份，不保存完整原始响应摘要或内存
   对象摘要，也不伪造后续市场查询证据且不含机密。序列化尾换行后的精确落盘字节必须在创建候选前通过
   16 MiB 门；同父目录候选、文件同步、`RenameFlags::NOREPLACE` 和父目录同步共同完成无覆盖
   密封，并只对这一个规范快照文件计算一次文件 SHA-256。读取时使用同一上限和同一已打开文件
   解码；运行和会话只复用已解码句柄，不重复散列或核对字段。
10. 每条可执行规则必须且只能归入一种证据类别：`OFFICIAL_PLATFORM_RULE`、
   `OFFICIAL_SDK_CONTRACT`、`LOCAL_SDK_PATCH_CONTRACT`、`OBSERVED_PLATFORM_BEHAVIOR`、
   `USER_CONFIRMED_PLATFORM_BEHAVIOR`、`RULE_DERIVATION`、`PROJECT_ASSUMPTION` 或
   `UNCERTAINTY_PROFILE`。参考市场响应属于官方平台观测；跨市场/进程期复用、固定 8% taker
   返佣、固定 20% maker 返佣、L2 队列模型与返佣付款时刻/截取属于项目假设。当前 250ms
   `itode` 行为来源属于官方规则，平台小额余量、合格 `MarketResolved` 到达且结算屏障闭合后的
   即时原子赎回，以及 `end_at` 至合格 `MarketResolved` 的订单
   行为属于用户确认的平台行为，可立即成交 GTC 的 1 pUSD 门属于已观察平台行为，GTD 复用
   该门属于机械推导；`ORDER_STATUS_CANCELED_MARKET_RESOLVED` 的原始状态拼写属于官方订单
   状态合同。规则产物记录来源、日期、范围、限制、版本、文件摘要和失败语义；不得把观察、推导
   或假设升级成官方永久保证。规则产物只引用上一项唯一规范文件及其文件摘要，不另建原始响应或
   内存对象摘要。约三分钟只可描述代表性场景中 `end_at` 至合格判定/现金回账的
   间隔，不能被改写为 `end_at + 3 分钟` 自动触发赎回或授予可用资金。
11. 优先复用正式工作区现有已锁定的成熟 Rust 依赖：`reqwest 0.13.4` 负责固定 HTTPS 与查询
    编码，目标 crate 直接声明 `default-features=false` 及实际需要的 `query/rustls`，并显式
    配置 `redirect::Policy::none()`；`serde 1.0.228`、带 `raw_value` 的
    `serde_json 1.0.150` 负责窄 DTO；`sha2 0.10.9` 只负责既有代理时延无状态采样和低频文件摘要；
    `rust_decimal 1.42.1` 负责精确数值；`rustix 1.1.4` 的 `fs/std` 负责无覆盖原子密封。
    固定 SDK 身份为官方 `0.7.0` 基线加仓库唯一 `0.7.0-p1` 补丁，模拟器只启用所需
    `clob` 能力，不启用 `gamma/rtds/ws`；模拟器自己的最小 `reqwest` 声明不代表能从全工作区
    解析图移除 SDK 已激活的默认特性。`toml` 只负责静态配置，目标 crate 已直接精确声明
    `1.1.4+spec-1.1.0` 及 `parse/serde/std`，当前锁文件绑定该包、`toml_parser 1.1.3`、
    `toml_writer 1.1.2` 和 `serde_spanned 1.1.1`。阶段 0 已复核目标依赖的实际解析版本、许可证、
    安全公告、Rust 版本和锁文件，任务 1.1 实际引入时再次机械核对锁图；
    不自研 HTTP、十进制、哈希、TOML、数据库、重试框架、调度器或通用规则引擎。既有
    `data-collect` 通过 SDK `rtds/ws` 激活的 `backoff 0.4.0` 停止维护公告只作为现存工作区
    风险记录，不授权本事项改写采集依赖。任务 2.2 只允许按第 2.2 节第 13 项窄扩展现有 SDK
    补丁中的无状态精度原语，不改变既有 `data-collect` 调用行为或启用特性。进程组合根只初始化一次；后续会话
    只接收共享不可变快照引用，不接收加载器、端点或刷新能力，也不增加全局可变单例。

### 2.4 公共事件消费合同（`SIM-S-004`）

1. `CanonicalEventRecordV1`、`ExecutionMarketEventV3`、`CanonicalReferenceEventV1`、
   `EventTimeIndexEntryV2`、`VisibilityIndexEntryV2` 和 `MarketObservationCausalShellV3` 的
   生产语义、字段和版本由
   `plan/data-pipeline-training-plan.md` 及公共 Rust 类型拥有；本计划只规定模拟器消费
   不变量，不复制 DPT 字段清单或建立兼容猜值层。
2. 初始 tick 与动态 tick 的唯一输入都是 `ExecutionMarketEventV3`。首次逐资产完整 L2
   快照携带必填正数初始 tick；动态 `tick_size_change` 的旧 tick 可以未知，新 tick 推进
   当前状态。冷启动规则快照不得提供、覆盖或交叉校验 tick。
3. 首次完整 L2 快照只表达初始装载，不携带 `purpose`。后续完整 `book` 状态相同时只
   确认，有唯一净差额时只沿公共 `BookResyncDeltaInference` 与质量边界路径应用；不得
   再生成第二个初始快照或恢复初始/重同步用途分支。
4. `active`、`closed`、`accepting_orders` 三个可空布尔彼此独立，不压成生命周期枚举
   或组合状态机，也不得根据 `end_at` 或结算阶段改写。模拟器的盘后操作门消费公共
   `ExecutionMarketEventV3.market.window.end_at_utc_ms`；只有在事件时间索引中具有真实数据源
   事件时间的合格市场判定才能推进隐藏平台判定与结算。仅有接收时间的公开结果证据不能用
   `end_at`、接收时间或其他本地时间补成平台判定时间，受影响市场必须按 DPT 能力范围失败或
   隔离。三个布尔中的任一个都不能作为盘前、盘中、盘后或结算后的单一判据。公开成交必须
   保留上游 `trade_id`，且 `trade_id_source` 能区分平台交易哈希与派生源证据；模拟器不得猜测
   平台 ID。
5. 训练和离线评估的隐藏平台状态只在初始装载完整且当时市场状态允许时接受相应订单。缺口、
   陈旧、epoch 切换或 `source_attribution_quality=UNATTRIBUTED` 本身不关闭既有 maker 订单；
   下一份合同有效完整 `book` 在其数据源事件时间形成唯一新平台基准。`end_at` 后任何后续
   市场事件都不得再使本账户开放订单成交。初始快照始终缺失、完整 `book` 自身非法/冲突或
   规范数量不能去重守恒时，相关市场进入明确未知或失败。模拟盘只有在该事件已按模型可见
   时间到达本机后才能应用它，不得回填更早的模拟盘决策。
6. 同一 `event_id` 只引用唯一事件存储中的一份 `CanonicalEventRecordV1`。模拟器输入只允许
   其中的 `ExecutionMarketEventV3` 市场事件；`CanonicalReferenceEventV1` 只供 DPT 构建
   Observation，不得进入模拟器撮合、平台状态或私有响应。市场事件的
   `EventTimeIndexEntryV2` 只推进训练与离线评估的隐藏平台状态，市场事件的
   `VisibilityIndexEntryV2` 只推进交易侧公开视图。订单准入与撮合读取第 2.2 和 2.5 节定义的
   有效处理时点对应的隐藏平台状态；模型决策只能读取自身截止点前已经接收的公开事件和已经
   收到的私有响应。物理预取、隐藏状态和尚未接收的公开消息都不得产生未来可见性或事后回写。
   模拟器把 `EventLineageV2.source_evidence` 作为不透明审计证据，不以其内容二次排序、去重、
   归约或重建来源语义。训练与离线评估在有效处理时点 `t` 只应用 DPT 事件时间索引中数据源
   事件时间小于或等于 `t` 的完整毫秒桶前缀，并保持 DPT 原有桶内顺序；“包含 `t`”是毫秒
   离散边界的 `PROJECT_ASSUMPTION`，不得按 `event_id` 或事件类别另排。平台前缀不得跨过更早
   未密封请求或待处理动作；同一有效处理时点的动作按 `log_sequence`、批量项 ordinal 逐项
   释放，前一项完成前不得处理后一项。批量项只根据已解析的方法类型和冻结 `itode` 规则，在
   读取盘口前静态绑定 `platform_arrived_at` 或 `platform_arrived_at + 250ms`：只有 FOK/FAK
   市价 BUY 使用后者，GTC/GTD 限价 BUY 不按盘口动态归类。模型可见截点分别绑定公开可见游标
   和私有反馈序号；私有事实只有在其接收时间前缀闭合后，才按接收时点与稳定私有发射序号分配
   私有反馈序号。公开、私有事实之间不建立全局同毫秒优先级，也不要求模型等待两条流共同闭桶。
   前缀闭合入口只供 crate 内公共桥接器背后的受信输入驱动器使用：驱动器必须已经按全局 DPT
   原索引消费并闭合所有 `event_at <= t` 的事件，训练与离线评估运行根只应用其中属于自身冻结
   市场范围的事件。闭合证据必须来自全局游标的下一条 `event_at > t` 或 EOF，不能从稀疏运行根
   的下一条相关事件反推；驱动器再把原索引位置与 `t` 一次提交给水位原语。模型、调用方和八方法
   入口均不能自行声明游标。
   任务 1.2 只实现并验证该受信水位原语，不把尚未接入的 DPT 读取器或阶段 6 适配器冒充完成。
7. `observed_transport_delay_ms` 只作可空诊断，不是平台顺序、模型可见性、数据质量或正式网络
   时延配置档。数据源服务器和采集服务器时钟没有共同权威保证时，模拟器不得把该差值解释为
   精确网络延迟或跨时钟不变量。尽管如此，用户已批准把同日合格消息的
   `timestamp_received - timestamp` 经验分布作为首版请求出站、直接响应入站和生命周期私有
   反馈入站的双向项目代理；这只升级其项目用途，不升级证据等级。历史公开事件继续使用自身
   已记录的数据源时间和接收时间，绝不再次抽样或改写延迟。
8. 首版代理配置档固定使用 2026-08-03 已关闭且无缺口的单日清单中全部 24 个
   `polymarket_orderbook_v3` 小时文件，并以每条解码 WebSocket 消息为一个样本。样本总数为
   `39,618,963`，源时间缺失数和负差值数均为 `0`，最小值为 `6ms`、最大值为 `29,697ms`；
   nearest-rank `p50/p85/p90/p95/p99` 分别为 `10/19/30/63/187ms`。正式配置档必须保存全部
   `9,666` 个升序整数毫秒频数，而不是按五个分位点或固定百分比桶近似；运行时以配置档
   SHA-256、会话种子、稳定逻辑请求或反馈身份和出站/直接响应入站/生命周期反馈入站采样域做
   无状态 SHA-256 秩映射，从经验累计频数确定样本。算法版本作为固定域前缀，不增加运行时可变
   输入；物理重投不重采样。`itode` 不属于该配置档，只对 FOK/FAK 市价 BUY 按第 2.5 节独立
   追加 250ms 平台内部暂缓。
9. 阶段 0 至 5 的黄金组件入口和模拟器确定性核心只接收有界、强类型、仅含市场事件的
   `ExecutionMarketEventV3` 流，不依赖真实 Gate A。阶段 6 的 DPT/联合验收适配层位于
   sim-system 公共输入边界之前，先验证 Gate A 回执、唯一事件存储及双索引 manifest，再只
   组合 DPT 已公开的强类型读取器：事件时间路径由 `PlatformReplayCursor` 保持既有索引顺序
   并过滤参考事件；模型可见
   路径由 `VisibilityIndexCursor` 保持既有索引顺序，并通过有界页解析把相应 `event_id` 解析为
   同一唯一事件存储中的市场事件。当前没有现成、对称的模型可见索引解析游标；首版联合适配
   必须使用 `ExecutionMarketEventCursor` 等现有公开原语完成有界解析，且不得直接读 Parquet、
   重排、重建任一索引或复制完整事件带。适配层实现身份、页上限、完整性验证和实际扫描资源
   必须绑定阶段 6 联合清单；若现有公开原语不能在真实输入下同时满足正确性和资源硬上限，
   阶段 6 停止并回到 DPT 公共读取合同决策，不得在模拟器核心内建立旁路。该停止点不阻断
   阶段 0 至 5 的黄金组件开发和组件符合性门。
10. 进程组合根在创建任何会话前还必须从获准的内容寻址来源读取并解码一次
    `PlatformScenarioV1`。首版 schema 固定为
    `PLATFORM_SCENARIO_SCHEMA_V1 = "platform_scenario_v1"`，市场集 schema 固定为
    `MARKET_SET_SCHEMA_V1 = "market_set_v1"`，市场系列固定为 `BTC_UPDOWN_5M`。市场集按
    `condition_id` 规范排序且不得重复；每个成员包含精确 `MarketIdentityV2` 和按 token ID
    规范排序的恰好两个唯一 `AssetIdentityV1`，两项 outcome index 也必须唯一。组合根在该外部
    边界完成这些结构条件，形成共享不可变句柄；`SimExecutionProcessV1` 构造时只注入该句柄，
    进程内各运行根的唯一会话复用，不在 `SessionSpecV3` 重传市场集或场景摘要，也不在会话创建和普通业务
    中重复验证。最终清单在冷路径引用原来源文件及其已有 SHA-256。参考规则市场只提供系列规则
    事实，不能代替该目标市场集。
11. 任务 2.1 的事件原子消费单位固定为一个已经由 crate 内受信输入驱动器证明闭合的索引
    毫秒桶。事件时间路径的桶内事件必须全部具有与桶时点相同的真实数据源事件时间，模型可见
    路径的桶内事件必须全部具有与桶时点相同的接收时间；两条路径都保持 DPT 原始索引顺序。
    每个唯一桶在进入当前进程时只通过 DPT 的 `decode_execution_market_event_v3` 解码一次，边界
    同时建立路径、桶时点、起止位置和不可变事件切片。同一不可变事件切片可以交给多个相互隔离的
    运行根；每个被调用运行根只处理自己的唯一会话，不扫描其他运行根、不生成目标列表，也不在根内
    执行会话路由。该单根所有权边界已经批准。

    训练与离线评估的受信驱动器只把属于各根冻结市场范围的事件交给该根，保留 DPT 原始索引位置
    和桶内顺序；根游标允许原位置稀疏前进，只要求新前缀不与已经受理的原位置重叠或倒退，不做根内
    重编号。普通目录外间隔不生成日志记录。只有全局闭合证据已经跨过该根最早 GTD、既排定最终性
    或返佣时点，而该根没有市场正文可处理时，驱动器才提交一次
    `PlatformPrefixAdvanceV2 { path, closed_through_utc_ms, next_index_position }`。该控制输入只推进
    对应路径的原位置与逻辑时点，并按第 2.6 节既有顺序释放到期业务义务；不携带会话、市场列表、
    定时类别、原因或摘要，不生成盘口变化、市场判定、成交或现金事实。等待最终性闭合的赎回继续
    只是原最终性因果子项，不另建时钟。

    `NextEventAt(n)` 只能证明当前全局前沿到 `n` 之间没有更早市场事件：最早义务早于 `n` 时可在其
    自身时点形成上述必要空前缀；义务恰好等于 `n` 时，必须先取得 `n` 桶，命中本根时由真实闭桶按
    GTD、最终性、返佣、DPT 的顺序处理，未命中时才用空前缀推进。`EndOfInput` 仍由每个运行根显式
    接收一次；若 EOF 位于本根最后相关正文之后，先用必要空前缀闭合遗漏尾段，再执行既有 EOF 原子
    语义。成功 EOF 后只排空已经有限排定的业务义务，失败 EOF 不再推进。

    `PlatformPrefixAdvanceV2` 作为一个已受理控制输入只在同一 `SimEventLogV2` 编码一次，恢复直接
    重放，不读取或伪造不存在的空市场正文；事件时间与模型可见时间两条路径分别持有自己的前沿，
    不因空前缀合并。驱动器只需从根状态读取 GTD、最终性和返佣三个已有首项的最小值；多个并行根
    由组合层按该最早时点调度，无相关事件且无到期义务的根不接收输入。Paper 分配给某运行根的公开
    桶不得在根外按当前市场目录预筛；其中含未登记市场事件时仍按第 2.1 节入口零副作用失败，登记后
    同一桶可重试。

    市场更新、日志引用和消费者投影不得再次解码、散列或逐字段自证。后项业务处理失败时不得留下
    前项状态或前移水位。分页边界本身不证明毫秒桶闭合；位置跨度不得从过滤后的市场事件数量反推。
    `LevelExecutionTransitionV3.causal_group_id` 只作为 DPT 已提供的来源字段保留，不据此重排或
    另造组边界。新进程恢复允许从日志引用重新读取并解码一次；同一次恢复过程对同一桶建立有界
    缓存并最多解码一次，不承诺跨进程零解码。
12. 场景是任务 2.1 判断首次装载完整的唯一预期集合：事件的市场与逐资产载荷必须匹配场景，
    每个预期资产只允许一份初始 L2 快照，两个预期资产均完成后才结束 `PRELOADING`。训练和
    离线评估的隐藏平台初始化不以前置的市场生命周期事件或三个交易状态布尔为条件，因为没有
    真实数据源事件时间的元数据不进入事件时间索引。`active`、`closed`、
    `accepting_orders` 在模型可见路径仍逐字段原样独立保存；任务 2.1 不据此推导内部生命周期
    或订单准入真值表。订单准入、空值和未知值处理现由第 2.2 节第 10 项拥有。

### 2.5 动态影子 CLOB、队列和撮合

1. 每个运行根的唯一会话分别维护外部影子订单簿、本账户订单叠加层和只读派生聚合盘口。外部数量、账户
   数量和所有权不能互写；账户订单进入盘口后仍保持账户身份。
2. 唯一 maker 队列模型为 `L2_BOOK_DELTA_FIFO_ESTIMATE_V1`，证据类别为
   `PROJECT_ASSUMPTION`。账户订单排在其有效处理时点前已经存在于隐藏平台状态的同价数量
   之后；未暂缓订单的该时点是 `platform_arrived_at`，FOK/FAK 市价 BUY 进入 `itode` 暂缓后
   则使用暂缓结束后的实际平台处理
   时点。后续外部新增按 DPT 的数据源事件时间投影推进并进入当时队尾。缺少平台回放所需源
   时间时明确失败或隔离；不得提供乐观、保守、比例或随机配置档选择。
3. 对去除同一底层变化已应用数量后的外部旧数量 `E` 与新规范数量 `S`，精确 canonical
   残差仍为 `R=S-E`；六位可执行队列则按第 2.2 节冻结的端点累计规则计算
   `delta_micro_base=micro_base(S)-micro_base(E)`。零只确认，正值作为当次转换的新外部段排到
   当时队尾，负值作为一次性队头消耗；不得逐次量化 `R`，也不得把亚微余数写入可执行队列。
   消耗穿过本账户订单后，只有按第 2.2 节累计关系得到正 quote 的部分才形成 maker 推定成交；
   零 quote 穿越立即从活动队列和账户叠加层转入该订单的有界待累计分桶，既不回退队列也不
   形成成交。完成后精确外部状态锚定 `S`，六位队列总量锚定 `micro_base(S)`。
4. `price_change`、公开成交和后续 `book` 对同一底层变化只应用一次；
   `best_bid_ask` 只更新或校验最优价格，不推进数量。公开成交与盘口变化没有权威共同
   因果 ID 时保留 `UNATTRIBUTED`，L2 推定结果标记 `DETERMINISTIC_INFERRED` 与
   `not_official_fifo=true`，不能冒充真实订单排位。
5. FOK/FAK 市价 BUY 只有在规则快照 `itode=true` 时才进入 250ms 内部暂缓并在结束时按届时
   市场状态执行订单业务规则；GTC/GTD 限价 BUY 即使到达时可立即成交也不进入该暂缓。未暂缓订单的
   有效处理时点为 `platform_arrived_at`，暂缓订单为
   暂缓结束后的实际平台处理时点；该时点大于或等于 `end_at` 时拒绝且不产生订单、资金预留或成交。通过业务门
   后按价格优先从最优卖价逐档消费至限价/最差价格。GTC/GTD 剩余量挂单，FOK 全成或全不成，
   FAK 成交后终止，post-only 会立即成交时拒绝。
6. 同一外部流动性、账户流动性或规范队列消耗证据在会话内只能消费一次。外部档位、队列
   归因、账户 taker、订单 base、成交最终性分桶与 quote 资金必须分别守恒，任何单位不得
   跨 base、quote、pUSD 或手续费币种相加。
7. GTD 使用会话内稳定 `(encoded_expires_at, platform_order_id)` 逻辑截止时间队列，不依赖
   下一条市场业务事件才到期。角色授权平台前缀或统一逻辑驱动器证明时间已封口到截止时点后，
   必须在 `t >= encoded_expires_at` 且任何同毫秒 DPT 数量变化或订单动作之前先终结该订单；
   驱动器不得越过更早的未处理平台事件、订单动作或截止时间，也不得接受时间倒退。即使没有
   市场事件，具备封口证据的空前缀仍须触发；缺少该证据时不得从墙钟猜测到期。`end_at` 只先
   移除撮合资格，不替代 GTD 到期，也不阻止之后的截止时间迁移与本金释放。

核心数量关系固定为：

```text
external_pre + external_addition
= external_decrease + external_post

canonical_queue_head_depletion_delta
= external_queue_progress_delta
+ sum(account_maker_fill_base_delta
      + pending_maker_depletion_base_after
      - pending_maker_depletion_base_before)

account_taker_matched_quantity
= external_eligible_liquidity_consumed_by_account_taker
```

第二式只约束同一次规范队列减量；待累计量转入成交时，以有符号的 pending 前后差抵消先前
已计入的 base，不能再次算作新的队列消耗。撤单、到期和合格判定关闭属于订单分桶迁移，不是
新的规范队列减量。第一、二式是同一外部证据在外部状态与账户队列叠加层的分层投影，不能相加为两次市场变化；
第三式的成交两侧也不能相加为双倍执行。

### 2.6 订单、成交、账户和平台小额余量

首版每个 `SimProcessRoot` 运行根必须在创建唯一会话前显式绑定一份已封存
`TradeFinalityPolicyV2`；该规范策略文件的 SHA-256 直接作为唯一 `policy_id`，不再另算规范
正文摘要。运行根 `run_id`、策略文件或业务内容缺失、不一致或非法时失败即
关闭，不提供隐藏默认值。唯一会话只读使用该运行根绑定的策略；另一 `run_id` 使用另一个运行根，
不得在既有运行根内热换策略。同一不可变策略文件可以由多个运行根共享；账户专属覆盖项继续只按
完整 `AccountInstanceKeyV1` 自然匹配，不跨根改写选择器，未命中仍按普通模板处理。
该策略是 `sim-system` 自有、无网络的确定性场景输入，不进入
`contracts`：它为普通成交给出从 `matched_at` 起机械实例化的封闭状态模板；首态逻辑偏移必须
为零，其后每个状态的偏移必须严格大于前一状态，并明确唯一经济应用截点。组件故障测试可以另带
按预登记请求、批量项或成交稳定身份命中的有限覆盖项，显式形成未应用失败或应用后补偿。每份
策略必须为全部覆盖项选择且只选择一个 `REQUEST`、`BATCH_ITEM` 或 `FILL` 选择域；该域从首个
带类型标签的选择器推导，不要求顶层重复声明。每个选择键都必须由完整
`AccountInstanceKeyV1` 与该轨迹内的规范请求、批量项或成交身份共同构成，不得
脱离完整账户实例键直接使用会话局部原始字符串匹配。不同选择域不得混用，同一域内规范身份不得
重复。密封时拒绝混用选择域和同域重复身份，因此合法密封策略在运行时不可构造多重命中；一个
成交只能命中零项并走普通模板，或命中一项并走该覆盖，不能按优先级猜测。选择键编码非法或
缺少账户实例限定时在运行根密封前拒绝；编码合法但本次运行没有任何成交命中不构成错误，也不
要求预先知道动态请求。覆盖项不得按
模型结果或运行表现自适应选择，未命中的成交只走普通模板。状态推进只使用模拟逻辑事件
时间，不等待现实墙钟；不得从公共市场成交、`MarketResolved`、请求/反馈传输代理时延或未经
支持的随机概率生成私有最终性。相同策略、脚本身份、输入和种子必须产生相同路径、时点、经济
应用与补偿。官方
[订单生命周期](https://docs.polymarket.com/concepts/order-lifecycle) 和
[用户频道](https://docs.polymarket.com/market-data/websocket/user-channel) 只作为状态词汇与可观察
含义来源；本项目的路径模板、时点和经济应用截点明确记为 `PROJECT_ASSUMPTION`，不得冒充实际
私有链路证据。阶段 3 组件测试可以使用显式测试策略；首个正式 `COMPONENT_GOLDEN` 运行必须在
查看结果前密封具体策略文件；阶段 6/7 只消费清单绑定的同一文件 SHA-256 / `policy_id`。

`TradeFinalityPolicyV2` 的首版路径文法只允许以下有限模板，其中方括号表示可选的单个前缀：

```text
普通成功：       [MATCHED_NOT_BROADCASTED ->] MATCHED -> MINED -> CONFIRMED
重试后成功：     [MATCHED_NOT_BROADCASTED ->] MATCHED -> RETRYING -> MINED -> CONFIRMED
未广播即失败：   MATCHED_NOT_BROADCASTED -> FAILED
普通失败：       [MATCHED_NOT_BROADCASTED ->] MATCHED -> FAILED
挖矿后失败：     [MATCHED_NOT_BROADCASTED ->] MATCHED -> MINED -> FAILED
重试后失败：     [MATCHED_NOT_BROADCASTED ->] MATCHED -> RETRYING -> FAILED
```

普通模板只能选择两条成功模板之一；预登记故障覆盖只能选择四条失败模板之一。每条路径必须有限、
从首态完整列出并恰好终止于 `CONFIRMED` 或 `FAILED`，两个终态之后不得再有状态。成功路径必须在
`MATCHED`、`MINED` 或 `CONFIRMED` 的一个且仅一个状态出现处应用经济变化；失败路径要么从未
应用经济变化，要么只在严格早于 `FAILED` 的 `MATCHED` 或 `MINED` 出现处应用一次，并在
`FAILED` 原子形成唯一反向补偿。`MATCHED_NOT_BROADCASTED`、`RETRYING` 和 `FAILED` 不得作为
经济应用截点。路径缺少终态、越过终态、跳过模板必需状态、重复应用、在失败后应用或声明补偿
却没有先前应用均为非法策略并在会话创建前拒绝。

统一逻辑驱动器在同一平台逻辑时点 `t` 必须按以下优先级完整排空一类后再进入下一类，并据此
分配全局 `account_transition_seq`：

1. 到期 GTD，沿用 `(encoded_expires_at, platform_order_id)` 顺序；
2. 先前已经排定的成交最终性状态，按
   `(platform_event_at, fill_sequence, path_ordinal)` 顺序；
3. 返佣日结与付款时钟事件，同刻固定为 maker 日结、taker 日结、maker 付款、taker 付款，再按
   结算身份排序；
4. 已封口 DPT 毫秒桶，保持桶内规范原序；合格 `MarketResolved` 的接纳和开放订单余量关闭在
   该规范事件的原子处理内完成，经济前驱已经闭合时同项完成赎回，否则只保留第 2.6 节规定的
   内部屏障义务，待最后前驱闭合后再作为其因果子项完成；
5. 订单/撤单/查询动作，沿用既有逻辑动作键和批量项 ordinal。

不得为了处理后一类而越过同刻尚未完成的前一类。一次撮合产生的零偏移首态属于该撮合的原子
因果转换，不重新排入同刻队列：DPT 桶内按规范事件与稳定成交身份处理，订单动作内在进入下一
批量项前完成，同一原因产生多笔成交时按 `fill_sequence` 升序；后续状态因偏移
严格递增，只能进入未来逻辑时点。因而同刻补偿或资金释放必须先于
同刻新订单资金门，多个成交则按上述稳定键形成唯一账户变化序列；串行、并行或重放不得改变顺序。
已经到达但尚有合法已排定前驱的合格市场判定或已成熟返佣时钟，必须作为有界、幂等的内部屏障
义务保留，而不是当场失败或丢弃。任一驱动转换闭合某项义务的最后前驱时，必须先把由它新解锁
的义务作为该转换的因果子项排空，再进入同类下一稳定身份或下一优先级；多个子项依次按市场结算、
maker 日结、taker 日结、maker 付款、taker 付款及各自市场/日桶稳定身份排序。该子项使用当前
平台逻辑时点，不回写过去时点，也不形成第二条触发路径。
该优先级只约束隐藏模拟平台状态与经济转换，不合并公开市场可见流和私有反馈交付流，也不改变
第 2.4 节各自按接收时点可见且不存在跨流同毫秒优先级的合同。

1. 订单撮合轴与成交最终性轴分离。订单投影区分已接受、已撮合、活动未成交、平台小额余量
   关闭、调用方撤单、订单类型终止、到期和合格市场判定关闭；成交投影区分待处理且经济已
   应用、待处理且未应用、最终成功、终态失败已补偿和终态失败未应用；不存在可观察的
   “失败补偿待处理”中间态。
2. 每笔本账户撮合成交由会话内单调 `fill_sequence` 唯一标识；公开 `trade_id` 在运行根范围内只按
   `sim-{fill_sequence}` 编码一次，不另算成交内容身份。跨运行聚合时由既有
   `AccountInstanceKeyV1` 与该 ID 共同定位；策略为该成交选定唯一经济
   应用截点。后续
   `MATCHED_NOT_BROADCASTED`、`MATCHED`、`MINED`、`CONFIRMED`、`RETRYING`、
   `FAILED` 推进生命周期；进入 `FAILED` 时，若原变化量已经应用，则失败终态与引用原身份的
   唯一反向补偿在同一个不可分割的平台转换中完成并由一项成交反馈承载，不暴露二者之间状态；
   原变化量未应用时，同一失败转换只释放相应待处理资金占用，不生成反向经济变化。
3. 订单接受产生的本金/即时手续费占用，以及所有改变模拟账户的成交、补偿、资金释放、返佣、
   付款和赎回，共用一个账户轨迹内跨全部 `condition_id` 全局单调的
   `account_transition_seq`，禁止逐市场分别从 1 编号。开户事实是序列起点；每项变化只记录一次
   原因身份、受影响市场、平台生效时间和显式单位增量，不记录前驱摘要或账户之前/之后摘要。
   反馈可以迟到或乱序，但调用方可以按账户实例与序号识别缺口、缓冲、按序应用和幂等去重。
   该序列只表达模拟平台原始账户事实，不携带同级目录中交易侧 Ledger 的本地待提交
   预留、NAV、PnL、奖励或模型动作。
4. `VENUE_DUST_TERMINAL` 只适用于本账户已接受、按 base 计量且已经有正撮合数量的订单，
   并且使用两个互补而不重复的触发点：
   - GTC/GTD 在订单动作中产生正的 taker 撮合并把剩余量插入同价 FIFO 后，若该价格队列
     除该账户段外已经没有外部段、其他账户段或未清 taker depletion debt，且精确活动余量
     严格位于 `(0, 0.01 shares)`，立即进入小额余量终态；不等待一个未来市场事件；
   - 若订单动作结束时同价队列仍有其他段，则只在后续单个合格
     `LevelExecutionTransition` 抵扣既有 taker depletion debt 后仍有正的 segment 队头消耗，
     并在该事件的 maker depletion 与零偏移成交最终性完成后使同价 FIFO 只剩该账户段时，
     再按同一严格余量区间进入终态。
   两个触发点都必须使用本账户订单的精确 `open_base_quantity`。外部小量本身不得被当作本账户
   小额余量；若合格 `LevelExecutionTransition` 真实消费的 FIFO 前段恰为外部小量，且消费后
   本账户精确余量成为唯一候选，仍按前款触发。纯 debt 回声、零数量转换、消费后队列仍有其他
   段或待累计 maker depletion 均不触发。合格时原子把精确余量转入小额余量终态并把活动余量
   归零。
5. 平台小额余量不得读取外部盘口余量、使用概率或随机数量、夹紧余量、倒扣已撮合数量或
   生成额外成交、份额、费用、名义金额和现金流；只幂等释放该活动余量对应的实际资金占用。
   FAK 未花 quote、撤单、到期、合格市场判定关闭、手续费份额差异和外部小量均使用各自语义。
6. 查询和反馈中的 base、quote、pUSD、手续费、原始计量基准、标度和币种必须显式。调用方
   不得被迫从裸份额反推资金变化，也不得把外部盘口数量写入本账户分桶。

按 base 计量的已接受订单必须满足：

```text
accepted_base_quantity
= order_matched_base_quantity
+ pending_maker_depletion_base_quantity
+ venue_dust_closed_base_quantity
+ canceled_base_quantity
+ expired_base_quantity
+ market_resolved_canceled_base_quantity
+ open_base_quantity

active_unfilled_base_quantity
= pending_maker_depletion_base_quantity
+ open_base_quantity
```

`pending_maker_depletion_base_quantity` 已退出活动队列但尚未形成成交；它不得进入成交最终性、
结果代币、返佣或 `VENUE_DUST_TERMINAL`，也不得提前释放本金。撤单、GTD 到期或合格
`MarketResolved` 关闭时，它与当时 `open_base_quantity` 一起转入对应既有终态分桶；`end_at`
本身不移动该分桶。待累计量虽然不再位于可撮合队列，仍属于可查询、可撤销且受 GTD 到期与
合格判定关闭约束的活动未终结数量；后续撤单与查询不得只读取 `open_base_quantity`。
按 base 计量订单的 quote 同时满足：

```text
accepted_quote_amount
= matched_quote_amount
+ open_encumbered_quote_amount
+ released_price_improvement_quote_amount
+ canceled_released_quote_amount
+ expired_released_quote_amount
+ market_resolved_released_quote_amount
+ venue_dust_released_quote_amount
```

GTD 到期只把到期前的 `open_encumbered_quote_amount` 原子迁移到
`expired_released_quote_amount`，可用 pUSD 同量增加、在途本金占用同量减少，即时 taker
手续费占用不变；待累计 base 已经退出账户叠加层，不得再次扣减。到期稳定身份必须绑定订单、
编码到期时点和该账户的全局变化序号；到期前 pending/open base 与释放的 open quote 是本次
变化的显式业务量，不另造证据摘要，重复时点推进不得再次释放。
`order_matched_base_quantity` 另在最终成功、待处理已应用、待处理未应用、终态失败已补偿和终态
失败未应用五个互斥结算分桶中守恒。按 quote 预算计量的订单必须满足：

```text
accepted_quote_budget
= final_success_budget_consumed_quote_amount
+ pending_applied_budget_consumed_quote_amount
+ pending_unapplied_encumbered_quote_amount
+ released_or_unspent_quote_amount
+ open_encumbered_quote_amount
```

FOK/FAK 的固定 SDK `takerAmount` shares 等价值只作 base 审计投影，不进入 base 守恒式；
订单类型终止只保留专属 `terminal_reason`，未花预算只进入 quote 释放分桶，不生成无法由
quote 预算唯一推出的 `canceled_base_quantity`。
`end_at` 本身不增加 `market_resolved_canceled_base_quantity`，也不释放开放订单的资金预留；
只有合格 `MarketResolved` 才把当时仍开放的未成交余量移入该分桶，并把订单原始状态转为
`ORDER_STATUS_CANCELED_MARKET_RESOLVED`。每次余额或资金占用变化均由版本化、结构化
`PlatformAccountDeltaV2` 表达，并只记录一次全局序号、原因身份、受影响市场、平台生效时间和
显式单位增量；当前余额、占用、持仓和应收款由开户事实与有序增量投影，不在每项变化中生成
前驱摘要、前后账户摘要或变化正文摘要。调用方撤单、订单类型终止、到期、合格市场判定关闭、平台小额
余量和未应用失败各使用独立幂等 `encumbrance_release`，不得重复释放。合格市场判定关闭只
释放该订单未成交余量的 `open_encumbered_quote_amount`；已经撮合并经济应用的 quote 已转换
为结果代币持仓，既不属于该资金预留，也不得由同一变化量当作自动赎回兑付资金再次释放。

### 2.7 maker/taker 返佣与自动赎回

1. maker 返佣只来自最终成功、实际以 maker 身份成交的本账户数量；每笔使用进程启动时共享
   `PlatformRuleSnapshot` 中冻结的手续费表，其 `rebate_rate` 固定为 20%：

   ```text
   maker_fee_equivalent_pusd
   = executed_maker_shares × rate × [price × (1 - price)]^exponent

   maker_rebate_accrual_contribution_pusd
   = maker_fee_equivalent_pusd × rebate_rate
   ```

   逐笔贡献只供审计和逐市场汇总；只有唯一
   `MakerRebateAccrualFinalized` 可以增加 maker 应收款。规则有效但异常输入终态使潜在
   合格成交永远无法闭合时才使用 `UNKNOWN_INCOMPLETE`；无效规则快照不得进入核心。
   首版结果只区分 `CALCULATED`、`ZERO_NO_ELIGIBLE_MAKER_FILL` 与
   `UNKNOWN_INCOMPLETE`；费用关闭、零返佣、`taker_only=false` 或其他不受支持费表均在进程
   规则初始化时拒绝，不能进入返佣核心制造不可达终态。
2. taker 返佣只来自最终成功、实际收取正 taker 手续费的成交：

   ```text
   taker_rebate_accrual_pusd = actual_taker_fee_pusd × 0.08
   ```

   每笔唯一 `TakerRebateAccrualApplied` 只增加 taker 应收款。首版固定为 8%，不累计或校验
   wV，不查询等级、不实现动态等级或一次性奖金；Silver 只说明当前比例来源，不形成运行时
   等级状态。
3. maker 与 taker 以“返佣种类 + 成交归属 UTC 日”分别维护未结日桶。D 日 taker 的正常
   现金可用时刻为 D+1 00:10 UTC，maker 为 D+1 00:45 UTC；只有该日全部潜在贡献在模拟
   平台内部闭合后才日结。现金可用时刻已经到达但仍有合法已排定贡献前驱时，保留同一成熟
   时钟义务；最后前驱闭合后按第 2.6 节因果子项顺序在当时完成日结及适用付款，不追溯改写过去
   可用时间，也不重复创建时钟。普通训练截尾只保留未结应收款，不提前付款或制造未知终态。
4. 同类当日应计小于 1 pUSD 不付款且永久不并入后续日期；达到门槛时
   `paid=floor(accrual×10000)/10000`，截取余数继续锁定且不参与后续门槛。付款只把对应
   应收款一比一转为现金和可用现金，不改变 NAV、PnL 或训练奖励；两类返佣永不互相凑门槛。
   日结分别生成 `MakerRebateDayFinalized` / `TakerRebateDayFinalized`，付款分别生成
   `MakerRebatePaid` / `TakerRebatePaid`；日结与付款各自只发生一次。
5. 每个市场在市场结束且潜在 maker 经济最终性闭合后形成一次 maker 应计终态，全部合格
   taker 应计也必须形成。市场判定处理器只引用这些结果，不重新汇总或应用。
6. 合格 `MarketResolved` 对训练和离线评估在隐藏事件的数据源事件时间进入平台状态；paper
   只在本机已接收的公开桶到达后，以该桶的 `received_at_utc_ms` 进入本地平台状态，同时保留
   原始数据源事件时间用于判定合格性和结算证据，不回写此前的 paper 决策。进入平台状态后，
   先把仍开放订单的未成交余量原子转为 `ORDER_STATUS_CANCELED_MARKET_RESOLVED`，并用独立幂等变化只释放该余量的资金
   预留；这一步不释放已成交资金，也不产生兑付现金。模拟器同时幂等保留该合格判定及兑付向量。
   成交、补偿和返佣终态已经闭合时，立即以模拟账户实际结果代币余额执行一次原子赎回并生成
   唯一 `AutoRedeemApplied`；仍有策略中合法已排定的前驱时，不失败、不赎回，也不丢弃判定，
   而是在最后前驱闭合的当前平台逻辑时点按第 2.6 节因果子项顺序完成同一次赎回。订单余量释放
   与结果代币兑付使用不同账户变化身份并保持上述前后顺序；零余额也生成零变化终态反馈，重复、
   恢复或迟到不得重复关闭、释放或赎回。`end_at + 3 分钟` 不是另一条触发路径；约三分钟只能由
   版本化场景中 `end_at`、合格判定证据和现金回账的实际时点差表达。判定更晚时不得提前贷记现金。
7. 只有到达声明的经济成熟边界后仍永久缺少合格判定、合法最终性路径没有可到达终态、必需
   前驱证据缺失或兑付冲突，才明确失败/隔离；已接纳判定仅因策略中已排定前驱尚未到达不属于
   失败。不得猜测、先按较小余额赎回，或新增模型可见的赎回待处理、重试和失败反馈状态；内部
   屏障义务只保留原判定与前驱身份，不形成现金、生命周期反馈或第二条结算路径。

### 2.8 最小单一日志、一次归约与恢复

1. 每个新运行根只对应一条账户轨迹、一个会话和一个有硬上限的 `SimEventLogV2` JSON Lines 文件，不使用 SQLite、
   `rusqlite`、SQL、WAL、应用检查点、可切换存储抽象或第二份恢复正文。第一条 `RunOpened`
   记录一次 `CleanRun` 或 `RecoverablePaper` 耐久角色、规则、场景和实现身份；唯一会话的调用方
   角色和 `session_seed` 只随唯一 `CreateSession(SessionSpecV3)` 记录一次。后续控制、请求和闭桶
   输入由运行根句柄直接作用于该会话，不重复携带 `session_id`；日志只保存确定性重放所需的已受理
   强类型输入，不逐条重复运行身份，也不保存处理后状态、状态增量、输出、资源证明或对象摘要。
   `run_id` 标识这条账户轨迹的正式模拟运行，不标识包含多条轨迹的训练任务；外部训练编排器必须为
   并行轨迹提供不同 `run_id`，DPT 训练数据集清单绑定这些逐轨迹 `SimulationRunManifestV2`。
   `sim-system` 不为此增加跨进程全局注册表或普通热路径身份核对。

2. 普通热路径固定为：

   ```text
   有正文的边界输入 -> 一次强类型解码 -> 同一个业务归约器处理一次
                      -> 未生效状态与至多一个投影批次 -> 同一输入编码与有界组追加一次
                      -> 日志成功后状态生效并交付投影
   必要空前缀       -> 零正文解码     -> 同一归约器处理一次 -> 同一日志编码一次
                      -> 日志成功后状态生效并交付投影
   ```

   同一个强类型值供归约器和随后一次日志编码借用；闭桶正文只解码一次，日志保存既有桶引用，归约器直接
   消费同一次解码得到的事件切片。必要空前缀不伪造市场事件或第二份正文。强类型值进入归约器后，
   不再重复检查字段、参数、归属、正文摘要
   或调用方已经建立的边界。订单金额、价格、数量、资金、批量、订单状态、市场状态和因果顺序仍是
   必须执行的业务规则，不得以“删除校验”为由删除。

3. 归约器是状态改变的唯一入口，按值独占当前运行状态，处理期间不接纳下一输入。控制错误或控制
   业务失败直接返回且不写日志；八方法的正常平台拒绝、证据不足、合法空结果和未找到属于正常业务
   结果。归约成功时一次返回未生效状态和至多一个投影批次；同一输入日志成功后才安装状态并交付
   结果。编码或写入失败时丢弃未生效结果并关闭运行根；`RecoverablePaper` 的 `sync_data` 错误属于
   耐久结果未知，当前进程同样丢弃未生效结果、不交付输出并关闭，恢复再按实际完整组裁决。实现不得
   使用完整运行克隆、通用
   `Prepared*/Committable*` 两阶段框架、共享可变 `Arc::get_mut` 所有权门、提交后重复核对或
   第二套业务/审计归约器。物理重投按稳定业务身份直接返回首次结果，不重新编码日志或执行业务。

4. `CleanRun` 与 `RecoverablePaper` 共用同一记录格式和归约器，只在耐久时点不同：

   - `CleanRun` 的归约结果只在同一输入顺序追加成功后生效，普通输入不逐项同步；输入结束、业务义务闭合且没有待消费投影后，
     `RunSeal` 随最后一组一次同步。封口前崩溃整次作废，封口后只允许恢复产物发布。
   - `RecoverablePaper` 先按序把已经就绪的输入归约为未生效组结果，再把同组输入写入并执行一次
     `sync_data`；只有同步成功，组结果才在当前进程生效并对外可见。编码或写入失败时丢弃整组结果并
     关闭运行根；`sync_data` 返回错误时，本次耐久结果未知，当前进程同样零状态生效、零结果交付并
     关闭。冷恢复从日志起点重放到最后一个实际存在的完整组，只舍弃或截去文件末尾的不完整组；因此
     同步错误前已经完整留存的组在恢复后成为已受理输入，不完整尾组作废。调用方只使用原稳定身份
     恢复或重试：完整组已恢复时返回既有结果，否则按新输入处理。首版不增加提交标记、第二次同步、
     周期检查点或数据库；若从起点恢复不能满足实测门，必须停止并交由用户决定。
   - 每个运行根按构造所有权只交给一个写入器，不为普通追加反复取锁、扫描目录、回读文件或核对
     写后正文。

5. 每个已受理输入至多生成一个 `BusinessProjectionBatchV2`。唯一运行级投影接收端口完整接纳批次后，
   再把其中的窄业务对象交给桥接器、查询、外部归一化或 Ledger；确认不追加模拟器日志，不重新生成
   投影，也不让各下游分别确认同一输入。该端口直接绑定唯一会话创建时的消费端，批次不携带
   会话路由键或重复的 `response_bridge_binding_id`；恢复只读取该端口的一条运行级连续耐久水位，水位以前只
   重建状态，水位以后重发原投影。

6. 新写合同删除逐对象 SHA-256、账户前后摘要、前驱摘要、自证布尔值、重复账户变化和由业务正文
   派生的第二身份。代理时延既有业务算法所需的一次无状态采样不属于校验摘要。历史格式只保留在
   物理隔离的只读模块中，不续写、不恢复业务，也不作为新路径兼容或性能对照。

7. 最终审计和产物只在封口冷路径从完整输入日志重放一次；完整日志文件和最终产物各按实际字节
   计算一次 SHA-256。普通业务不调用冷审计、不计算文件摘要、不扫描最终成员。

8. 内存对象、队列、索引、日志和产物使用随真实增减常数时间更新的数量/字节计数与硬上限，不通过
   完整状态序列化、未来失败预演或重复业务输出估算资源。迁移时新计数必须先接管，随后才能删除旧
   资源实现；可重建投影越过既有保留水位后释放，已关闭运行在保留期结束后整体删除日志和未发布候选，
   活动单文件不滚动。正式数值只在唯一新路径上实测并由用户批准。

9. 本节只冻结上述最小结果，不预先设计绝对到期状态机、数据库替代层、分段协议、校验矩阵或通用
   存储框架。实施中若发现新增运行时校验是满足既有需求的必要条件，必须按第 1.2 节单独申请专项
   授权；不得把它夹带进日志实现。

### 2.9 外部完成门与回执身份

1. 组件符合性可以在共享公共合同冻结后与 DPT Gate A 实现并行；正式真实纵向必须等 DPT
   `GateAReceiptV3` / `REAL_EXECUTION_TAPE_READY` 成功签发。DPT 失败诊断、部分能力或缺少成功
   回执均不满足入口。
2. 阶段 6 既定的不可变联合清单是 DPT/sim 双方真实纵向门的唯一联合回执载体。该清单必须
   绑定 Gate A、模拟器组件证据、代表市场、公共合同、实现和资源基线；清单的内容身份与
   SHA-256 直接作为联合回执 ID/SHA。不得另建回执类型、登记系统、重复清单载荷或人工批准门。
3. 阶段 7 只消费实际归一化层、同级目录中交易侧 Ledger 的 training 实例、DPT Gate B 候选及其
   回执身份，不在本事项
   实现这些外部能力。联合训练闭环证据交还 DPT，供其完成全量验证和
   `TRAINING_DATASET_READY` 签发；本计划不能签发 Gate B。
4. 真实模拟盘恢复适用门当前为 `NOT_READY`。它只接受真实模拟盘适配器、由模拟盘运行时拥有并
   实际使用第 2.8 节 `SimEventLogV2` 合同的单一日志，以及 `sim-system` 经同一日志读取端口
   完成的跨进程恢复联合证据；`data-collect` 只负责交接，不写交易运行时日志。
   `COMPONENT_GOLDEN`、DPT 离线 Gate A 产物、心跳或状态帧、阶段 5 或阶段 6 单模块证据均不能
   满足本门。该门未满足不阻断阶段 4 至 5 的组件关闭，也不授权当前事项实现外部适配器、宿主
   或部署；阶段 4 的本地文件参考写入器只是同一日志合同的组件实现，不拥有真实运行路径。该门
   只阻断真实模拟盘恢复准入和适用声明，不另建回执类型或登记系统。
5. 用户已决定在任务 1.2 实现前生成、验证并内容寻址密封首版项目代理时延配置档；不存在
   Gate B 前后两种门位分支。配置档只决定请求出站、直接响应入站和生命周期私有反馈入站的
   代理传输时间；`platform_responded_at` 仍是模拟平台实际完成该请求处理的逻辑时点，不由
   传输配置档凭空生成。配置档不得替代或改写公开市场数据已记录的数据源事件时间、
   `received_at_utc_ms` 或反馈的 `platform_event_at`，也不得宣称为爱尔兰订单私有链路实测、
   纯网络延迟或平台处理延迟。真实配置字节、`profile_id/version/SHA-256` 和全部统计不变量
   完成核验前状态为 `NOT_READY`；完成后状态为 `PROJECT_PROXY_PROFILE_READY`，且正式
   `SimulationRunManifestV2` 与 DPT Gate B 候选必须绑定同一身份。未来 `sim-calibration` 只能
   产生新的显式版本，不得热改或覆盖首版配置档，也不属于本事项实现范围。首版配置档现已
   生成并独立复核，状态为 `PROJECT_PROXY_PROFILE_READY`：
   `profile_id=btc_updown_5m_collector_observed_delay_20260803_v1`、`version=1`、文件
   SHA-256 为 `98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596`、
   精确大小为 `280853` 字节；版本化路径以该文件 SHA-256 为目录名，不建立可变 `current` 别名。

### 2.10 非目标与禁止扩展

- 不新增 SELL、改单、八方法之外的查询、通用后端 trait、市场插件或第二套操作词汇。
- `sim-system` crate 不实现运行时原始数据解析、第二套 DPT、NOE、同级目录中的交易侧 Ledger、
  RewardContract、模型框架、HPO、
  晋级、`sim-calibration`、paper/live 宿主或部署。只有任务 1.2.1 的独立离线、固定输入、
  一次性配置档构建工具可只读已核验清单及其 24 个成员；Rust 运行时和模拟器核心永不读取
  `typed_raw`，该工具不得扩成通用校准服务或运行时路径。
- `sim-system` 不实现真实模拟盘适配器或宿主。阶段 4 只实现第 2.8 节单一日志合同及本地文件
  参考写入器，不扩展为通用存储后端、可切换数据库层、路径发现器、网络读取器、原始
  JSON/Parquet 解析器或第二套 DPT；也不增加目录外单调锚、WORM 或主动防回滚基础设施。
- 不实现 Liquidity Rewards、wV、动态等级、奖金、返佣插件或规则专用语言。
- 除固定公开规则冷启动外，不增加任何真实网络、REST、签名、钱包、凭据或实盘回退。
- 不复制完整事件带，不宣称真实 FIFO，不模拟账户订单对历史外部市场的反事实影响。
- 不为“证明不存在”建设静态分析器、运行时治理系统、文档守卫或重复跨模块测试体系。

## 3. 阶段 0：批准范围、公共合同和实施基线

进入本阶段的前置条件是需求与本计划均获批准。阶段 0 只复核批准事实，不把“取得批准”写成
实施任务。

- 任务 0.1：复核实施根、事项权限和边界
  - 步骤 0.1.1：记录需求/计划批准版本、Git 身份、正式工作树和当前授权。
    -> 验证：实施只落在正式根的目标 crate；没有隐含提交、推送、部署、远端或真实资金权限。
  - 步骤 0.1.2：以 `cargo metadata`、`cargo tree` 和源码检索建立开始状态。
    -> 验证：不存在第二个模拟器、影子规范化、实盘回退或未获准宿主接线。

- 任务 0.2：冻结当前公共合同
  - 步骤 0.2.1：核对八方法、请求身份、四个请求/响应时间坐标、规则快照、双时间事件消费、
    反馈和运行清单的版本边界。
    -> 验证：本计划第 2 节与公共类型可同时成立，跨模块只引用归属计划和公共类型/版本。
  - 步骤 0.2.2：用真实公共载荷确认 `SIM-S-004` 的 tick、首次快照、后续 `book`、三个
    独立交易状态和成交身份来源。
    -> 验证：只保留真实合法样例、一个实际解码损坏代表和已发生兼容事故回归；不重复 DPT
    的逐字段测试。

- 任务 0.3：复核成熟工具和最小技术试验
  - 步骤 0.3.1：核对 `reqwest`、`serde/serde_json`、`sha2`、`rust_decimal`、`toml` 和固定
    官方 SDK 的实际锁定版本、特性、许可、安全公告与 Rust 兼容性，并确认当前 Rust `1.95.0`
    标准库提供顺序文件写入和 `sync_data`。
    -> 验证：依赖边界满足 2.3 节，不增加第二套 Arrow、通用客户端或框架；阶段 0 不预选或引入
    持久存储依赖，阶段 4 固定复用标准库与既有 `serde_json`，并删除 SQLite 依赖。
  - 步骤 0.3.2：用固定 HTTP 响应和临时目录验证查询编码、窄 DTO、精确十进制，以及只对规范
    文件计算一次 SHA-256 的同文件系统原子密封路线。
    -> 验证：技术试验不形成生产实现或第二套规则核心；任何缺口影响和选项在实施前可见。

- 任务 0.4：预登记资源测量
  - 步骤 0.4.1：按 2.8 节冻结组件黄金负载和真实纵向的事件、请求、成交、查询、保留期
    单位及计数边界。
    -> 验证：所有计数可由已受理输入日志和冷端运行清单重算；来源各层行数与模拟器唯一输入、
    消费者投影和容量分栏记录，参考事件不能进入核心。
  - 步骤 0.4.2：冻结构建、主机、单调墙钟、延迟分位数、系统/cgroup、内部高水位和原始证据
    口径。
    -> 验证：阶段 4 首次正式计量前密封确切黄金负载并由实测冻结数值；阶段 6 对真实范围按
    同一口径重新定标，不循环真实事件，也不把 DPT 资源或原始数据行数移植成模拟器预算。

阶段 0 完成门：当前合同、成熟依赖路线、实施边界和测量方法无未决冲突，任务自审与独立
阶段审查通过，才进入阶段 1。

## 4. 阶段 1：规则冷启动、会话、时间和八方法入口

- 任务 1.1：实现进程级规则初始化与共享不可变快照
  - 步骤 1.1.1：按 2.3 节以一个参考市场实现有界 CLOB/Gamma 读取、窄 DTO、精确规则合并、
    16 MiB 双边界和原子密封。
    -> 验证：完整真实形态正例恰好请求参考市场三次；省略 `itode` 的官方 `false` 编码成功
    密封为 `false`；身份、费用关闭或 maker 比例冲突的代表输入均在发布前失败，最终失败不形成
    成功快照。
  - 步骤 1.1.2：实现进程 `CURRENT_QUERY`、`HISTORICAL_AS_OF` 和 `RECOVERY` 三种启动入口。
    -> 验证：历史与恢复入口不创建 HTTP 客户端；共享不可变句柄保持同一对象、观测时点和规则值，
    规范快照文件只在低频文件边界具有一份 SHA-256，后续市场不会触发请求、追加、重复散列或热刷新。任务 1.2 再用真实会话工厂证明
    每个运行根的唯一会话只能接收该共享句柄。

- 任务 1.2：实现隔离运行根、唯一会话、逻辑时间和身份
  - 步骤 1.2.1：从固定 2026-08-03 清单机械生成全部消息的整数毫秒经验直方图，核验输入清单
    与 24 个成员身份、样本计数、缺失/负值、最小值、最大值和五个 nearest-rank 分位数，随后
    以 `profile_id/version/SHA-256` 内容寻址密封首版代理配置档。
    -> 验证：配置档逐项重算得到 `39,618,963` 个样本、`9,666` 个严格升序非零频数及
    `10/19/30/63/187ms` 五个分位数；运行时只读并校验密封配置档，不读取 `typed_raw`，也不
    新建通用 `sim-calibration` 写入器或运行时生成路径。
  - 步骤 1.2.2：使进程组合根只初始化一份共享规则快照和一份共享代理时延配置档，把相同
    不可变句柄绑定到每个隔离运行根的唯一会话；实现 DPT 平台前缀、公开可见游标、私有反馈序号和单调逻辑
    输入水位，并让批量项最终处理时点集合在平台前缀推进前按方法类型与冻结 `itode` 规则密封。
    -> 验证：公开 API 编译边界证明跨运行根、跨账户和后端切换不能通过普通请求字段表达；调用方不
    提交未来游标、合同版本或重复会话身份。平台前缀不跨过未密封请求或更早动作，同一有效处理毫秒保持 DPT 原顺序并
    每次只释放一个稳定排序动作；私有事实按闭合接收时间前缀和稳定发射序号释放，公开与私有
    可见水位独立，墙上时钟和物理到达顺序不决定业务结果。前缀闭合接口保持 crate 内不可由
    模型调用；本任务用受信测试驱动器提供“下一条晚于 `t` 或 EOF”的闭桶前提，不预建 DPT
    读取适配器。
  - 步骤 1.2.3（业务时点语义已完成；请求合同受任务 4.3 重构）：实现请求/直接响应四个时间坐标、生命周期反馈的 `platform_event_at` 与
    `feedback_received_at`、无状态双向经验采样、请求身份、首次写入幂等和查询平台状态修订号。
    -> 验证：固定种子与稳定身份得到固定样本；物理重投复用原日志序号、样本、派生时间、查询截点
    和首次结果，不再比较后来正文；新逻辑请求才取得新日志序号，发送截点来自共用桥接器首次交给
    受信会话边界时附加的一份 `TrustedDecisionCutoffV2`，不是调用方参数，也不因演员稍后处理、
    投影接收暂停或物理到达顺序改写；同一 `global_request_id` 重试不重复创建对象，新 ID 按新请求处理；
    直接响应和生命周期反馈都只在各自交易侧接收时点后可见，且 `itode` 不进入传输采样。

- 任务 1.3：实现八方法模拟领域入口（业务方法已完成；请求合同受任务 4.3 至 4.4 重构）
  - 步骤 1.3.1：实现精确公开名字、八个方法特定领域请求、`RequestContextV2`、内部
    `TrustedDecisionCutoffV2`、方法特定接收回执
    和合同错误层；每个入口只完成边界解码、硬上限准入和结构化幂等查找，随后把同一强类型输入交给
    唯一归约器一次形成未生效业务结果，并在同一输入日志成功后使其生效；不得另建日志前业务预演、
    第二次归约或阶段 2 影子实现。
    -> 验证：最小桥接器可调用八方法并取得对应接收回执；合同错误通过 `Result::Err` 在改变
    状态前拒绝，物理重投复用原输入序号与结果且不重复追加；旧方法名、改单、额外查询、
    `SignedOrder`、模型和 Ledger 对象不可达；接收回执不能表达平台接受、拒绝、未知结果、
    订单、撤单或查询投影，也不增加第九个公开操作。

阶段 1 的规则、时间和八方法业务词汇保持完成；规则快照新写 schema/文件身份、`SessionSpecV3`、
`RequestContextV2`、内部 `TrustedDecisionCutoffV2`、结构化身份和
输入日志接线必须在任务 4.3 至 4.4 重验后才恢复完整完成状态。规则响应最终失败或冲突时能力保持
阻断，不得伪造完成。八方法接收入口不证明任何阶段 2 业务结果或方法特定直接响应已经形成。

## 5. 阶段 2：动态影子 CLOB、撮合、撤单和查询

本阶段在任务 1.3 已受理并调度的逻辑请求基础上实现平台业务处理，生成方法特定直接响应，并
经 crate 内私有响应可见性链路在 `response_received_at` 后交付；不得增加公开响应轮询或取回
操作。

- 任务 2.1：实现公共事件消费与市场前半生命周期（已完成）
  - 步骤 2.1.1：注入并核验进程级不可变 `PlatformScenarioV1` 与 `MarketSetV1`，通过 DPT
    当前冻结的 `ExecutionMarketEventV3` 唯一解码入口，按已封口索引毫秒桶实现首次装载、
    因果审计、tick、
    独立交易状态、后续 `book` 重校准、`end_at` 盘后门和合格市场判定终态。
    -> 验证：一个合法代表序列覆盖 2.4 节关键不变量；训练和离线评估的隐藏订单簿按
    Polymarket orderbook 数据源事件时间推进，模拟盘只应用本机已接收事件；非法初始状态或
    不守恒失败可见，后项非法时整桶状态与水位零提交，缺口后的有效完整 `book` 不回写历史
    决策，三个平台布尔不被改写或冒充盘前、盘中、盘后及结算后的单一判据。
  - 步骤 2.1.2：实现外部簿、本账户叠加层和派生盘口的所有权隔离。
    -> 验证：外部和账户数量不能互写，完整事件正文不进入运行状态或日志；多个运行根只共享
    不可变事件切片，各自日志只记录桶引用。

- 任务 2.2：实现订单业务规则和平台内部暂缓（业务语义已完成；归约接线受任务 4.3 重构）
  - 步骤 2.2.1：在唯一业务归约器中实现 BUY、四种订单类型、post-only、tick、SDK 成熟编码工具、
    最小 shares、可立即成交限价 BUY 本金门，以及第 2.2 节固定优先级的业务判定。
    -> 验证：每个业务分支用最小正例和一个真实可达拒绝边界；`end_at`、显式布尔否决、
    `PRELOADING` 与目标资产非 `Fresh` 的优先级稳定，FOK/FAK 不被推测套用限价单本金门；
    动态 tick `0.00001` 通过公共原语，并证明该原语与原订单构造器共用同一价格校验、截断和
    最终量化路径；GTD 240 秒等号通过、少 1 秒拒绝且实际到期毫秒来自编码秒；FOK/FAK 最终
    6 位量化 `takerAmount` 对 `mos` 等号通过、最小下界拒绝。
  - 步骤 2.2.2：在同一次归约中处理单笔和逐项非原子批量，并复用任务 1.3 冻结的 `itode`
    条件化 250ms 有效处理时点；不形成跨任务准备对象、持久状态增量或第二次业务提交。
    -> 验证：批量边界、混合业务结果和物理重投映射；`itode` true/false 各一条真实路径，maker
    与 post-only 拒绝不进入暂缓；未暂缓订单在 `end_at` 到达、暂缓订单从边界前跨至实际处理
    时点时均拒绝，且不创建订单、资金预留或成交。一个受理输入只调用一次归约器并一次公开完整结果。

- 任务 2.3：实现初始资金、L2 队列撮合与直接响应（业务语义已完成；投影接线受任务 4.3 重构）
  - 步骤 2.3.1：实现唯一开户事实、可用 pUSD、订单接受前资金检查与资金预留；响应作为同一次
    归约产生的有界消费者投影，不在业务变化前预留另一套持久响应槽位。
    -> 验证：非法精度或负数在会话登记前失败且不占用会话身份；本金或即时 taker 手续费任一
    不足均零副作用，税费前本金与费用之和等额通过且不缩量；批量后处理项读取此前已处理项占用
    或释放后的资金，响应仍按原 ordinal 排列；消费者暂不能接纳时暂停下一输入，不把背压改写为
    平台拒绝，也不撤销已经形成的订单、成交或资金变化。
  - 步骤 2.3.2：实现净差额、队头消耗、队尾新增、交错账户订单的稳定相对顺序，以及 taker
    逐档消费、maker 推定、FOK/FAK、价格改善、手续费引用和同一证据只应用一次。
    -> 验证：代表黄金序列分别证明未穿过账户订单不成交、穿过后部分/全部成交、连续重校准
    不把后方外部量越过账户订单；用真实邻接样本覆盖成交/`price_change`/`book` 防双算，五类
    独立守恒均成立。价格改善下 FOK/FAK 实际成交 base 可大于 SDK `takerAmount` 审计投影，
    quote 守恒仍成立；多价位、小量余数、零 quote 禁止、FOK 预演/提交一致、五位手续费汇总
    舍入和金额溢出各用一个可区分错误实现的代表边界；maker 穿越先以零 quote 进入有界待累计
    分桶、后续累计为正时只形成一笔成交，且待累计期间本金不释放、事件证据不线性增长。
    FAK 最终部分成交少于 `mos` 不追溯改写
    任务 2.2 的准入结果；`end_at` 后到达的后续成交或盘口事件不得再使本账户开放订单成交；
    正 base、零 quote 的低价外部余量保留但不阻塞后方可执行价位，post-only 预判与正式撮合
    结果一致；不穷举数值排列。
  - 步骤 2.3.3：形成两类方法特定三分支响应，并通过唯一有界投影批次和预绑定运行时投影接收端口完成
    进程内连续交付。
    -> 验证：单笔与混合批量的稳定映射、GTC/GTD base 守恒、FOK/FAK quote 守恒、实际成交
    base 与 SDK 审计投影分离、响应时点和一个请求
    一个响应成立；响应先于公开消息不泄漏隐藏状态；消费者暂不可用时暂停下一输入，连续水位推进
    后释放批次，物理重投和恢复均不重复业务变化。同一 `global_request_id` 直接复用首次结果而不
    比较后来正文；新的 ID 形成新的请求与自身响应时点，不存在跨 ID 效果复用；
    外部编译测试继续证明八方法之外没有公开
    响应轮询、取回或确认入口；进程重启不被误报为已恢复。

- 任务 2.4（已完成）：实现撤单、查询、分页和保留期读取
  - 步骤 2.4.1：实现单撤、批撤、全撤与逐目标结果。
    -> 验证：活动、部分成交、pending maker、终态、重复 ID、全撤冻结目标、平台时间边界、
    同一请求 ID 重投和两个隔离运行根目标不可串用各用一个最小代表；重复平台订单 ID 只有一个效果且保留全部输入映射，混合目标不是
    业务全成全败，内部错误没有半提交。模拟器不生成策略性撤单；GTD 到期与同刻撤单按已实现
    顺序处理，已有终态不被撤单覆盖；`end_at` 后、合格 `MarketResolved` 前的撤单仍成功且只
    释放未成交余量预留。由判定生成订单终态及其同刻端到端顺序已由任务 3.5.1 实现并验证，本任务
    不伪造或重复该生产路径。
  - 步骤 2.4.2：实现 `order`、`orders`、`trades` 的截点投影和不透明游标。
    -> 验证：101 行固定页、同毫秒稳定键、严格时间开区间、活动/终态、原始未知状态、空页、
    未找到、精确成交 ID 的已保留正向事实、游标滥用和跨保留期各有代表；第一页后发生撤单、
    成交或状态更新时，旧游标仍读取
    原根且不重不漏，新根才看见变化。盘后未判定的开放订单仍可由 `order` 和 `orders` 查询；
    查询自身不推进业务修订号，查询可见业务提交只原子推进一次，保留水位独立。训练与离线
    会话在隐藏事件已处理、对应公开 `received_at` 尚未到达时，查询仍可返回订单和成交自身事实，
    但公开投影及响应不得包含 DPT 事件 ID、事件计数、来源摘要或其他隐藏事件证据；pending
    maker、maker fill 与 taker fill 各用一个可区分反例验证。订单和成交自身稳定身份继续属于
    可返回事实，但不得附带用于生成或审计该身份的隐藏事件来源字段。

阶段 2 完成门：市场状态、准入、队列、撮合、撤单和查询满足业务合同与守恒，任务自审和
独立阶段审查通过。

## 6. 阶段 3：市场接入、账户经济、平台小额余量、返佣和自动赎回

- 任务 3.0：先建立运行根、公共账户身份、唯一开户事实和账户变化序列骨架
  - 步骤 3.0.1（业务语义已完成；文件身份合同受步骤 4.3.1 重构）：先按第 1.4 节技术前置发布
    `AccountInstanceKeyV1` 及其唯一共享角色/来源值类型；在任何会话创建前实现最终性策略的规范解码、
    完整路径与账户限定选择器业务检查、运行根密封，并把一个显式 `run_id` 与一份策略绑定到
    `SimProcessRoot`。新运行改用 `TradeFinalityPolicyV2`，直接以实际策略文件 SHA-256 作为
    `policy_id`；现有 `TradeFinalityPolicyV1.content_sha256` 与规范正文二次编码只留历史只读解码。
    -> 验证：缺失/空 `run_id`、缺失策略、第 2.6 节之外的路径、倒退时点、非法经济
    截点和既有运行根热换策略均在创建会话前失败；运行根的唯一会话只读使用相同文件 SHA-256 / `policy_id`，
    另一 `run_id` 使用另一个运行根且状态完全隔离；一个运行根不能形成第二个会话或开户事实。
  - 步骤 3.0.2（业务语义已完成；合同受任务 4.3 重构）：在 `contracts` 补齐第 1.4 节获准的共享值类型；在调用方角色
    和平台来源这两个共享表示中，`SessionSpecV3` 只直接接收调用方角色，由运行根 `run_id` 与
    内部固定注入的共享 `SIMULATED` 来源形成 `AccountInstanceKeyV1`；会话创建业务条件满足后才
    同步返回唯一
    `AccountOpeningFactV2` 并登记 `PREPARED_LEDGER_INIT` 账户实例保留；实现第 2.2 节幂等
    创建、激活和初始化失败关闭控制面，只有 `ACTIVE` 会话开放八方法。
    -> 验证：缺失/空或不匹配运行根的 `run_id`、旧 `SessionSpecV1/V2` 和调用方覆盖模拟来源在
    任何状态形成前零副作用失败且不绑定创建身份；首次成功创建并提交日志后，同一创建身份后来携带
    不同规格或桥正文仍直接返回该成功结果，不再比较；唯一会话形成后该运行根不再接纳第二个创建
    意图。不同运行根复用同一业务
    `account_id` 时得到不同开户事实和完全隔离的账户序列。开户事实与准备会话原子形成，创建重试
    返回同一结果且不进入私有反馈队列；首次成功只交付一个绑定首次桥身份的独占会话句柄，重试不
    返回第二句柄、不替换桥；句柄丢失在
    阶段 3 只导致当前运行失败。准备态八方法拒绝，Ledger 成功后的同身份激活
    可幂等重试，结果未知只查询/重试同一开户事实，明确失败关闭后同一键不能重开户。开户事实恰好一次
    绑定显式初始资金及初始场景，调用方不能另填或在后续市场、日期、文件或批次边界重建本金。
  - 步骤 3.0.3（业务语义已完成；合同受任务 4.3 重构）：把阶段 2 已有的订单接受本金/即时手续费占用、FAK 订单类型终止的未花预算释放、
    调用方撤单释放和 GTD 到期释放接入统一 `PlatformAccountDeltaV2` 与全局
    `account_transition_seq`。先建立第 2.6 节唯一内部逻辑驱动入口，把阶段 2 的市场事件
    桶和动作处理两处 GTD 直接推进改为只向该入口提交推进请求，禁止保留能够直接改变 GTD 或
    账户状态的旁路；后续最终性、返佣时钟、市场判定和动作只向同一入口注册或提交各自事件。
    同时实现共享有界确认基础设施和承载这些变化的最小 `OrderLifecycleFeedbackV2` 驱动路径。
    -> 验证：账户变化从开户事实后的序号 1 连续增加；状态与必需订单反馈不会部分提交，
    直接响应只能引用而不能复制规范变化，生命周期反馈位于队头时可以正常交付、确认和继续推进。
    两个重叠市场共享余额和序列；两条隔离轨迹即使使用相同配置化初始金额也不共享状态或合并
    资金。某轨迹可用现金归零后，后续市场仍保持同一账户和余额，不关闭会话、不补资；只有新的
    账户实例与开户事实才可取得新的显式初始资金。源码检索和碰撞测试证明旧 GTD 两处调用不再
    直接推进状态，同一 `t` 的 GTD 只执行一次。

- 任务 3.1：建立同一账户轨迹的仅追加市场接入
  - 步骤 3.1.1（业务语义已完成；合同受任务 4.3 重构）：把阶段 2 固定 `MarketSetV1` 作为运行根唯一会话的初始前缀，并按第 2.1 节实现会话
    局部、版本化 `MarketEnrollmentV2` 序列、同步幂等控制结果及原子状态扩展；只有 `ACTIVE`
    会话可登记。
    -> 验证：paper 在一个或多个既有市场已运行后登记待追加新市场，新市场看到既有市场形成的
    资金状态，另一运行根的目录与账户状态完全不变；既有市场已经成功进入业务不阻止 paper 追加尚未使用的
    新市场。training 与 offline-evaluation 在首个成功业务事件或请求后追加登记零副作用失败。
    首次成功登记并提交日志后，同一 `enrollment_request_id` 的重试直接返回原稳定登记身份和序号，
    不比较后来市场正文；用新身份重复 `condition_id`、CLOB 市场 ID 或 token ID，以及准备态、失败
    关闭、冻结或围栏拒绝均零副作用失败，不写日志且不绑定失败结果。paper 在登记前成功登记显式引用候选 `condition_id`
    的请求，或登记没有 `condition_id` 但以候选 token ID 过滤的请求，都会使该候选市场后续登记
    失败；`cancel_all_orders`、无市场/资产过滤的查询和只按平台订单 ID 操作的请求不建立围栏，
    不阻止后续登记。未登记市场事件失败后不留永久墓碑，成功登记后重试可以进入；不为任何业务
    请求保存目录版本。登记结果不占私有反馈队列，同一账户没有第二次初始化，资金、订单、GTD、
    私有序号和下一账户序号均不漂移。登记成功后的新市场事件授权、BUY 请求、`end_at` 停止撮合、
    合格判定关闭和查询均只从会话局部目录发现该市场；源码检索与反例证明业务路径不再直接枚举
    初始 `PlatformScenarioV1.market_set`，既有市场行为保持不变。受信组合根和上游对固定系列及
    paper 本机接收资格负责，sim 的控制请求不存在系列、接收时点、证据身份、证据摘要、载荷摘要
    或调用方期望链字段；`MarketEnrollmentV2` 直接复用请求身份，只另含序号与市场业务内容。
  - 步骤 3.1.2（业务语义已完成；合同受任务 4.3 重构）：把初始场景、开户事实和登记序列纳入内存确定性重放上下文。
    -> 验证：training 的预冻结链与 paper 由受信组合根按本机接收顺序发起的追加链，分别在全新
    干净会话中重放相同登记、市场事件和请求序列，得到相同登记序列、市场目录、账户状态和下一
    全局序号。阶段 3 不写持久日志、不执行进程重启恢复，也不把测试内重新构造会话表述为恢复通过。

- 任务 3.2：实现显式成交最终性策略和账户经济应用
  - 步骤 3.2.1（已完成）：消费任务 3.0 已密封的策略，实现普通模板实例化、有限预登记故障覆盖和
    逻辑时间调度，不建立第二个加载或校验入口。
    -> 验证：非法或非规范稳定身份、缺少账户实例限定、混用请求/批量项/成交选择域、同域重复
    身份和运行结果自适应覆盖均在改变对应成交状态前失败；密封期唯一性使运行时多重命中不可
    构造。请求级覆盖作用于该请求全部成交，但不能再叠加批量项级或成交级覆盖。两个隔离会话使用
    相同原始请求/批量项字符串
    时，只能命中策略中账户实例键完全
    一致的那条轨迹；合法但全程未命中的选择器不报错，相关成交走普通模板。相同策略、脚本、
    输入和种子重复运行确定，不等待现实时间，也不读取公共成交、市场判定或代理传输时延生成最终性。
  - 步骤 3.2.2（业务语义已完成；合同受任务 4.3 重构）：实现订单撮合/成交最终性分轴、策略指定的唯一经济应用截点、补偿和资金
    占用释放，并把变化接入既有全局账户序列与 `TradeLifecycleFeedbackV2` 驱动路径。
    -> 验证：普通成功、待处理且已应用、待处理且未应用、未应用失败与已应用后补偿各用一个
    代表；后继反馈先可见时能识别缺口，前驱到达后按序应用并去重。开放订单未成交余量预留和
    已成交结果代币分别守恒，重复状态不重复计值；结果代币只作为任务 3.5.2 后续兑付的唯一输入，
    本任务不提前实现或宣称兑付守恒已经闭合。`FAILED` 与必要补偿同一转换完成，不出现补偿待处理
    中间态。另以同刻“已应用失败补偿 + 新 BUY + 另一成交经济截点”反例证明
    统一驱动器先按稳定最终性键完成补偿/应用，再执行新 BUY 资金门；串行、扰动和干净重放的
    准入结果、全局账户序号和显式经济增量相同。

- 任务 3.3：实现确定性 `VENUE_DUST_TERMINAL`
  - 步骤 3.3.1（已完成）：按本账户所有权、正撮合、严格余量区间、同价 FIFO 唯一候选和终态优先级
    判定；订单动作部分成交后若已经只剩候选段则立即判定，否则只在抵债后仍真实推进 segment
    的逐事件 DPT 边界再次判定。
    -> 验证：订单动作立即进入与后续 DPT 进入各一个精确账户余量正例，一个外部小量反例，
    一个严格边界/其他所有权失败代表，以及一个纯 debt 回声反例；不使用随机数或伪造成交。
  - 步骤 3.3.2（已完成）：原子关闭精确余量、只释放对应活动资金占用，并复用既有
    `OrderLifecycleFeedbackV2` 驱动路径交付该终态和账户变化。
    -> 验证：成交、份额、费用和现金变化均不被制造，重复驱动与迟到撤单不重复释放。

- 任务 3.4：实现 maker/taker 返佣、日结和付款
  - 步骤 3.4.1（已完成）：实现 maker 逐笔贡献、逐市场最终化和应收款，taker 实际手续费乘固定 8%
    应收款，并通过 `SettlementLifecycleFeedbackV2` 交付应计事实。
    -> 验证：实际 maker/taker 分段、零合格、失败/补偿，以及两个独立进程中均通过 V2
    合法门的不同历史 `feeSchedule` 各用代表；无效或不受支持规则快照在核心前拒绝。
  - 步骤 3.4.2（已完成）：实现隔离日桶、正常现金可用时刻、1 pUSD 门槛、四位向下截取、锁定余数和
    付款，并通过 `SettlementLifecycleFeedbackV2` 交付返佣、日结和付款事实。
    -> 验证：低于、等于和高于门槛各一条代表，最终性延迟和普通截尾不提前付款，重复驱动与
    干净重放不重复付款，两类返佣不互相凑门槛。可用现金为零但仍有合格应收款时继续日结和
    付款，不关闭会话、不创建新开户事实或补资。正常现金可用时刻先到、最后一笔合法已排定最终性
    后到时，成熟义务只保留一次，并在最后前驱闭合的当前时点完成日结/付款，不追溯提前可用。

- 任务 3.5：实现市场结束、结算屏障和自动赎回
  - 步骤 3.5.1（已完成）：在合格 `MarketResolved` 到达时，把仍开放订单的未成交余量转为
    `ORDER_STATUS_CANCELED_MARKET_RESOLVED`，并用独立幂等变化只释放其资金预留。
    -> 验证：`end_at` 本身不关闭订单或释放预留；调用方盘后撤单与判定关闭分别只发生一次，
    外部盘口释放不生成账户事实，已成交结果代币不被当作未成交余量释放；订单终态反馈只在
    经过适用返回延迟后对交易侧可见。
  - 步骤 3.5.2（已完成）：在合格 `MarketResolved` 按角色授权的平台时点生效且结算屏障满足后原子赎回并
    通过 `SettlementLifecycleFeedbackV2` 驱动路径生成唯一赎回反馈。
    -> 验证：获胜、未获胜、零持仓、重复驱动、干净重放和前驱反馈迟到各有代表；订单余量释放与
    结果代币兑付的账户变化身份和顺序可辨，不生成模型可见的中间赎回状态，永久缺失或冲突明确
    失败。以 `fill@1000 -> MarketResolved@4000 -> CONFIRMED/FAILED@6000` 反例证明 4000 时只
    保留判定并关闭订单余量，不赎回、不失败；6000 时先完成最终性及适用补偿，再以同一判定只
    赎回一次。另以配置化约三分钟的正常场景和一个更晚判定反例证明两者都只由真实合格事件
    触发，固定时长本身不增加现金。可用现金为零但仍持有获胜结果代币或待赎回资产时继续处理
    判定和赎回，不关闭
    会话、不创建新开户事实或补资。
  - 步骤 3.5.3（业务语义已完成；交付接线受任务 4.3 重构）：对任务 3.0、3.2、3.4 和 3.5 已分别实现的订单、成交和结算反馈执行稳定关联、
    原始状态保留及第 2.8 节有界连续交付的端到端验收，不在此首次实现共享交付机制。
    -> 验证：三类生命周期不混合，交错反馈的队头不会阻塞后续可交付事实，状态与必需输出不发生
    部分提交，模拟证据不冒充 NOE、Ledger 或真实平台产物。

阶段 3 完成门：运行根与最终性策略先决、公共账户身份、唯一开户事实、准备/激活/失败关闭、
仅追加市场同步接入、全局同刻顺序、全局账户经济、平台小额余量、返佣、市场结束、自动赎回和
有界反馈合同通过代表性业务与故障测试；同一 `t` 同时包含 GTD、先前排定最终性、返佣时钟、
已封口 DPT 桶和新 BUY 的五类碰撞测试必须证明只有唯一驱动入口、无旧路径旁路且稳定顺序不变；
内存确定性重放通过但不冒充阶段 4 恢复，任务自审和独立阶段审查通过。

## 7. 阶段 4：最小单一日志与废弃面清除

本阶段只完成五项工作，顺序不可倒置：

1. 冻结业务结果基线；
2. 先删除不承担公开业务职责的废弃实现；
3. 建立唯一的“归约—日志—状态生效与投影交付”路径；
4. 切换全部公开路由并删除剩余旧活动路径；
5. 只在唯一新路径上测量并验收。

当前已核实：公开构造、恢复和业务路由仍使用 V1 整态文件路径；没有正式产物兼容责任的私有
SQLite V2、专属恢复标签和独立 B1/B2 编排已经删除。原私有路径暴露的闭桶重复解码和双市场
`EndOfInput` 部分公共终态随该路径删除而不可达，保留路径的两项绿色回归已经固化；热路径重复摘要
和完整状态序列化红灯仍待任务 4.3 消除。当前代码游标进入任务 4.3。

在没有新增外部阻塞的前提下，本阶段仍按 `8–12` 个完整开发日估算；到第 12 个开发日仍未通过
完成门时必须停止并报告偏差，不得靠新增抽象、校验或兼容路径延长。

### 任务 4.1：冻结业务结果基线（已完成）

- 步骤 4.1.1：固定当前 Git 基线和公开行为。
  -> 验证：用现有公开入口记录八方法、闭桶、订单、成交、账户、返佣、赎回、查询、会话控制、
  恢复和封口的业务结果；只记录外部结果和必要结构计数，不把 V1 内部字段当作未来合同。
- 步骤 4.1.2：建立能够区分错误实现的最小红灯。
  -> 验证：当前实现必须暴露闭桶重复解码、热路径重复摘要/全状态序列化，以及双市场 EOF 部分公共
  终态；测试计数只存在于测试构建，不进入运行产物或新增运行时拒绝条件。

完成标准：业务基线、已知缺陷和待删除调用图清楚可复核；本任务不修改生产行为。

### 任务 4.2：废弃面先行清除（已完成）

- 步骤 4.2.1：删除没有公开业务职责、没有正式产物兼容责任的私有 SQLite V2 完整实现，包括
  `rusqlite`、SQL/WAL/应用检查点、私有协调器、专属恢复/摘要链及其测试；同时删除可独立移除的
  B1/B2 编排、自举预算、失败探针、固定宽度身份和对应巨型测试。
  -> 验证：Cargo 清单、锁文件和依赖树不再包含 `rusqlite` 或 `libsqlite3-sys`；私有 SQLite
  构造和测试调用归零。
- 步骤 4.2.2：只保留读取真实历史产物所需的最小 wire DTO，并移入隔离的 `legacy_reader`；
  删除把 SQLite V2、B1/B2 或失败预留描述为当前方案的活动说明。
  -> 验证：`legacy_reader` 只读既有文件，不创建、续写或恢复业务；公开 V1 基线仍可编译并通过
  任务 4.1 的业务测试。本任务不实现替代日志。

完成标准：第一道清除门通过后才可进入任务 4.3；不得为了对照而复制或改名已删除实现。

### 任务 4.3：建立唯一最小路径

任务 4.3 复用的基础包括：单文件 `SimEventLogV2` 物理读写、单根单会话的创建/激活/初始化失败控制
演员、规则 V3 与最终性 V2 冷句柄、无摘要账户经济叶、无摘要资产队列，以及四类 BUY、单笔 GTD
到期、成交最终性和 taker 返佣成熟前驱的 crate-private 业务叶。步骤 4.3.1 已把控制骨架与
`ExecutionStateV2` 收束为唯一会话业务状态，账户只由 `ExecutionStateV2` 持有；步骤 4.3.2 已在
同一状态上接通预解码市场事件与 maker 会话内子转换。后续步骤必须在同一组现有类型和业务叶上
顺序收束，不得重写已完成业务叶、另建第二个演员、第二份账户状态或第二个归约器。阶段 1 至 3 和任务 4.1 已验证的
金额、撮合、撤单、判定、返佣、赎回与查询纯业务计算必须直接复用或抽取为 V1/V2 共用叶；迁移只
替换旧状态所有权、摘要和状态级两阶段事务外壳，不得并排重写业务公式，也不得把 V1 状态当作桥。

- 步骤 4.3.1（已完成）：把现有控制骨架和 V2 业务叶收束为唯一会话业务状态。唯一会话同时拥有初始化状态、
  会话局部市场目录、角色适用的公开/隐藏市场状态、`ExecutionStateV2`、市场登记序列和 Paper 最小
  使用围栏；`BusinessReducerStateV2` 同时拥有这些业务容器的资源计数，`PlatformAccountStateV2`
  只由 `ExecutionStateV2` 持有。首次创建原子形成完整初态，市场登记沿现有同一个控制归约与提交
  路径，在一个转换中同步扩展目录、市场状态、队列和账户盘口叠加层；不得为登记建立临时演员或专用
  归约器。首次成功候选只在日志提交后生效并绑定登记身份，未受理错误不绑定，成功后的物理重投返回
  原结果。业务资源只保存一个聚合 `ResourceQuantityV1`：`records` 按实际插入或移除的逻辑增长条目
  计数，`bytes` 只累计这些条目实际拥有的变长正文与索引字节；固定叶和分配器开销不使用
  `size_of` 猜测，由 `records` 约束结构规模并在任务 4.5 以 RSS 实测定标。此后每一步创建或移除
  业务容器时同步维护同一聚合计数；演员在步骤 4.3.5 接管待交付投影计数，`SimEventLogV2` 继续独占
  既有日志字节计数和输入序号，冷封口在步骤 4.3.6 接管产物计量。不得复制计数，也不得留到步骤
  4.3.7 再回头扫描或重接。
  -> 验证：一个运行根始终只有一个会话状态和一个账户权威；初始市场与追加市场只有一个目录来源，
  所有市场共享连续账户变化序号；训练/离线初态同时建立角色适用的公开与隐藏市场状态，Paper 只
  建立公开市场状态；登记转换任一子状态失败时全部不变，首次成功重投不重复登记。测试不得通过第二
  状态、旧 `SessionSpecV2`、开户锚、登记摘要链、完整会话克隆，或旧状态级、完整候选状态/提交
  协议的通用两阶段框架过渡。业务输入触发的冻结与使用围栏分别留到步骤 4.3.2 和 4.3.3 用真实入口
  验证，本步骤不造测试旁路。

- 步骤 4.3.2（已完成）：接通市场事件与 maker 业务。角色适用的市场状态直接消费同一次边界解码得到的事件
  切片形成会话内市场/maker 子转换；初始 L2 与档位转换同时维护平台市场状态和六位执行队列。合格
  档位减量在一个候选转换内完成 taker debt 抵扣、maker 零 quote 待累计、首次正 quote 成交、订单
  分桶、账户盘口叠加、唯一 `fill_sequence`、零偏移最终性、返佣前驱和平台小额余量；`end_at` 只
  结束撮合资格，不提前释放订单或资金。本步骤不建立 `ClosedMarketBucket` 分派或临时闭桶处理器，
  唯一闭桶入口、一次解码和整桶原子性统一在步骤 4.3.4 接入。
  -> 验证：用真实边界解码得到的事件切片和既有 V2 BUY 叶形成规范初始快照、有效挂单、零 quote
  穿越和正 quote maker 成交的会话内候选，证明队列、订单、账户叠加、成交和资金守恒，同一档位
  证据只消费一次；未登记市场事件在该子转换入口失败时不留下状态或墓碑，完成登记后同一事件可以
  重试；丢弃返回候选时原状态不变。测试不得手工篡改状态、直接提交 maker，或把本步骤的子转换
  冒充完整闭桶入口。

- 步骤 4.3.3（已完成）：按以下内部顺序闭合其余确定性业务叶和领域效果，
  不在本步骤创建或交付投影批次：
  1. 请求与订单（已完成）：把五个变更方法接入市场准入、tick、数量、资金、批量、逻辑时点和首次业务结果
     索引，完成三种撤单及 pending/open 订单终态。
  2. 经济闭合（已完成）：完成合格市场判定、maker/taker 应计、各自日结和付款、判定屏障及一次自动赎回。
  3. 查询与输出领域效果（已完成）：在同一订单、成交和账户状态上形成三种查询的根、排序、分页和保留期
     结果，以及八个方法特定响应和三类生命周期反馈所需的业务效果；步骤 4.3.5 才把这些效果组合成
     唯一 `BusinessProjectionBatchV2`。
  用户已批准方案 A：`orders`、`trades` 的方法特定结果冻结为
  `Page | InvalidCursor | OutOfRetention`；低于根保留水位的旧根返回 `OutOfRetention`，仍在保留
  范围但精确方法/过滤根不存在或偏移越界时返回 `InvalidCursor`。两者都是已受理业务结果并服从
  `global_request_id` 首次结果幂等，不得改成日志前 `Result::Err`。
  正常平台拒绝、证据不足、合法空结果和未找到仍是已受理业务结果，不得改成控制错误。
  -> 验证：一条请求/订单轨迹证明订单金额、价格、数量、资金、批量、订单状态和五个变更方法结果
  与任务 4.1 基线一致，pending maker 进入撤单或 GTD 时不伪造成交且只释放一次开放本金；一条经济
  轨迹证明 pending maker 进入市场判定、maker/taker 应计、日结、付款和自动赎回各只发生一次，
  “判定先到”和“最终性先到”得到相同最终结果；一条查询轨迹证明三种查询及其方法特定结果观察同一
  状态。同组代表还必须证明 training/offline 在首个成功业务输入后冻结登记，而 Paper 已受理的未知
  市场请求会建立使用围栏并阻止该市场后来登记。不得新增返佣、市场结算或查询专用归约器，不得
  复用 V1 查询持久正文，或复活旧状态级、完整候选状态/提交协议的通用两阶段框架；局部纯计算结果
  不得仅因类型名称机械删除或重写。

- 步骤 4.3.4：建立唯一逻辑驱动器并闭合整桶原子归约。同一 `t` 只由一个循环依次排空 GTD、已排定
  最终性、maker/taker 日结与付款、一个已封口 DPT 毫秒桶，以及订单、撤单和查询动作；前项解锁的
  返佣、市场结算或赎回作为当前转换的因果子项先于下一稳定身份排空。`PlatformPrefixAdvanceV2`
  只在没有正文但确有到期义务时推进；普通原位置缺口不写日志。有正文桶直接借用第一次解码形成的
  事件切片，双市场 EOF 和整个桶在状态生效前一次算清。
  -> 验证：一条同刻代表轨迹证明五类义务的固定顺序；必要空前缀满足零正文解码和一次归约，普通
  稀疏缺口没有空日志；双市场 EOF 要么整体成功，要么整体零生效。不得建立 GTD、最终性、返佣或
  市场各自的驱动器，也不得在强类型边界后重新解码、散列或核对正文。

- 步骤 4.3.5：形成唯一投影批次并扩展现有演员与日志提交路径。单一业务归约器对有界组内每个待
  受理输入各调用一次，按序把同一份未生效候选状态推进到组末，并为每个输入形成至多一个
  `BusinessProjectionBatchV2`；演员把一个或多个已经就绪的输入各编码一次，顺序汇入同一个有界组，
  整组追加一次，`RecoverablePaper` 对整组同步一次，成功后只安装组末状态并按序交付各批次。投影
  端口暂时不能接纳时保留当前批次及同组有界未交付后缀，在该后缀全部被接纳前不处理新输入，并同步
  维护待交付投影计数；消费者确认只推进一条连续水位，不写日志、不重新归约或重投影。组形成遇到
  首个按既有合同返回且不关闭运行根的未受理输入错误时停止，包括边界/控制业务失败和未登记市场
  闭桶入口失败；该错误输入不进入日志，此前非空成功前缀可以作为一个组提交。内部致命故障仍丢弃
  当前组并关闭运行根，不得按成功前缀处理。产生正常业务结果的已受理输入只写日志一次，业务结果
  和投影本身不写日志。
  -> 验证：一个至少含两个输入的 Paper 组在测试构建内证明两次归约、两次输入编码、一次组追加和
  一次同步、一次组末状态安装，每个输入至多形成一次投影；必要空前缀满足“零正文解码、一次归约、
  一次日志编码、至多一次投影”。写入失败或同步结果未知时，整组候选状态和输出在当前进程全部不
  生效，运行根关闭且不回退 V1；本步骤只证明物理日志完整组的实际留存裁决，步骤 4.3.6 再证明同一
  归约器的业务状态和投影恢复。投影背压代表至少包含同组两个待交付批次，证明当前批次及未交付
  后缀保持原序且各交付一次，全部被接纳前不会处理下一输入或重复归约。上述观测计数只存在于测试
  构建。

- 步骤 4.3.6：用同一个业务归约器完成恢复、封口和内部完成门。从 `RunOpened` 后的实际完整日志组
  顺序重放；闭桶引用在恢复边界读取并在本次恢复中解码一次，水位以前只重建状态，水位以后重建并
  重发原投影。Paper 只舍弃末尾不完整组并从最后完整组续写；CleanRun 未见完整 `RunSeal` 时整次
  作废，业务义务闭合且投影已消费后只同步一次封口；完整封口后按第 2.8 节从同一日志冷重放形成
  未发布产物候选，只允许恢复该产物发布，不得重新开放业务输入。
  -> 验证：一条训练双根轨迹同时证明共享一次全局解码、根内隔离、原位置稀疏、普通缺口无日志、
  必要空前缀、EOF 和恢复一致；一条 Paper 轨迹证明未登记市场桶没有在根外预筛，首次零副作用失败
  且登记后同桶可重试。末尾不完整组、日志写入失败和同步结果未知各保留一个代表；八方法、控制、
  闭桶、恢复、封口和产物候选全部通过同一内部路径，不存在 V1、SQLite 或第二归约器回退。

- 步骤 4.3.7：完成资源与保留综合门。汇总核对前六步已经随容器维护的业务、投影、日志和产物
  常数时间数量/字节计数，不在本步骤首次补实现或重新扫描状态；按既有保留水位释放可重建投影，
  并在已关闭运行的保留期结束后整体删除运行文件。
  -> 验证：各增长容器只由其所有者随真实增减维护一次计数，不序列化或扫描完整状态、不预演未来失败；
  一条跨保留水位和运行关闭的代表轨迹证明投影释放与整体删除；代表超限返回既有类型化错误且不形成
  部分业务结果。数值暂由测试显式给出，正式值留到任务 4.5。

完成标准：八方法、闭桶、控制、恢复、封口和产物候选都能通过同一内部新路径，不存在 SQLite
回退或第二套归约器。

### 任务 4.4：公开切换并删除剩余旧路径

- 步骤 4.4.1：一次切换公开新建、八方法、闭桶、控制、恢复、会话重新附着和封口，使它们只调用
  任务 4.3 的日志、归约器、投影和资源计数。
  -> 验证：一条综合黄金轨迹覆盖上述入口；向唯一日志分别注入一次写入失败和同步结果未知，归约均
  恰好执行一次，当前进程状态和输出零生效、运行根关闭且不回退旧写入器；恢复对实际完整组的裁决
  与第 2.8 节一致。
- 步骤 4.4.2：在同一修改中删除公开 V1 活动写入/恢复、V1/V2 转换桥、失败证据预留、逐事务
  全状态资源证明、完整失败检查点、目录预览、剩余 B1/B2 测量耦合、旧产物写入和过渡测试/说明。
  历史格式只允许留在 `legacy_reader`。
  -> 验证：生产调用点清点证明只有一个新建/恢复/追加/归约入口；普通构建不包含旧写入器、影子
  归约器、SQLite、`into_v1` 或旧资源证明。运行
  `cargo check --workspace --all-targets --locked --offline` 和仓库规定的固定全工作区普通回归。

完成标准：第二道清除门通过后，旧实现已经退出活动构建；在此之前不得执行任何正式性能或资源
测量。

### 任务 4.5：唯一新路径测量与阶段验收

- 步骤 4.5.1：使用 release 构建和最终公开入口，分别测量纯业务、投影、日志组追加、
  `CleanRun`、`RecoverablePaper`、从起点恢复和冷封口。
  -> 验证：固定黄金负载串行和多个隔离运行根并发各三次，不删除慢样本；并发不得解释为单根内
  多会话并行。记录吞吐、p50/p95/p99/最大延迟、CPU、
  RSS、读写字节、日志大小、恢复时间、保留释放前后占用和资源高水位。正式计时不装配结构计数或故障注入组件。
- 步骤 4.5.2：基于探索结果向用户提交吞吐、p99、日志组、日志大小、恢复时间、保留时长和资源硬上限候选，
  取得批准后再运行正式门。
  -> 验证：不得从失败结果反推放宽门槛；若单日志从起点恢复或业务吞吐不能满足用户批准的目标，
  停止阶段 4 并报告选择，不得自行增加 SQLite、检查点、摘要链或其他校验。
- 步骤 4.5.3：运行聚焦测试、`sim-system --lib`、仓库规定的固定全工作区普通回归、全目标
  `cargo check`、`cargo clippy`、格式和差异检查，再做一次独立只读阶段审查。
  -> 验证：业务等价、唯一日志、一次归约、恢复不重复、资源有界和性能结论均能由独立证据区分；
  没有旧活动路径或新增未授权校验。

阶段 4 完成门：五项任务依次通过，用户批准正式数值，独立审查为
`Critical 0 / Important 0`。阶段完成只代表组件技术验收；真实 DPT 纵向、真实模拟盘适用和训练
闭环仍分别归阶段 6、独立模拟盘适用门和阶段 7。

## 8. 阶段 5：组件符合性门

- 任务 5.1：运行完整组件验收
  - 步骤 5.1.1：先运行冷启动 HTTP 合同测试，再在断网、无钱包、无凭据、无模型框架环境
    运行业务、故障、确定性、恢复和资源测试。
    -> 验证：`SIM-AC-001` 至 `SIM-AC-008` 的组件范围均有真实命令、输入与产物；双时间
    黄金序列和内容寻址黄金恢复源证明隐藏平台撮合、模型公开数据可见性、响应先到隔离与本地
    组件恢复；两个重叠市场共享全局账户变化序列且没有重复账户；外部真实纵向、真实模拟盘恢复
    适用和训练闭环明确未被替身冒充。
  - 步骤 5.1.2：干净重跑并进行一个不改变逻辑语义的物理调度扰动。
    -> 验证：已受理输入、响应、反馈、查询和最终状态一致；冷端最终文件摘要可重算，分歧报告
    能定位首个输入。

- 任务 5.2：执行边界和过度设计审查
  - 步骤 5.2.1：检查依赖图、公开 API、代码路径和产物。
    -> 验证：2.10 节禁止路径均不存在，且每个抽象和依赖都能追溯到当前首版能力。

- 任务 5.3：封存组件证据
  - 步骤 5.3.1：完成任务自审和独立阶段审查，修复后重复全部受影响验证。
    -> 验证：组件范围没有失败、过滤或未解释缺口。
  - 步骤 5.3.2：原子封存 `SimulationRunManifestV2` 与最终文件 SHA-256。
    -> 验证：组件运行清单的内容身份和文件 SHA-256 直接作为 DPT 所消费的组件回执 ID/SHA，
    不另建回执类型或重复清单载荷；证据只声明组件范围，不宣称真实管道，不把项目代理时延
    配置档升级为私有链路实测，也不宣称训练、真实模拟盘恢复或 live 生产适用。

阶段 5 完成门：组件符合性证据完整且独立审查通过。它可以先于 DPT Gate A 完成，但不能
替代阶段 6。

## 9. 阶段 6：DPT Gate A 后的联合真实管道纵向门

阶段入口必须机械验证 DPT 成功 `GateAReceiptV3` / `REAL_EXECUTION_TAPE_READY` 的 ID/SHA、
公共合同版本、唯一事件存储 manifest、事件时间索引 manifest、模型可见索引 manifest、
Observation manifest、实现身份、输入集合和能力范围；
部分能力、失败诊断或缺少成功回执不满足入口。

- 任务 6.1：只通过公共事件带准入真实输入
  - 步骤 6.1.1：按 2.4 节的 DPT/联合验收适配边界，从当前 Gate A 只生成两条有界、强类型、仅含
    `ExecutionMarketEventV3` 的流。隐藏平台状态路径保持 `EventTimeIndexEntryV2` 的既有顺序；
    交易侧公开视图路径保持 `VisibilityIndexEntryV2` 的既有顺序；两条路径都把 `event_id`
    解析到同一唯一事件存储并在 sim-system 公共输入边界之前排除参考事件，模拟器确定性核心
    不接收 `CanonicalReferenceEventV1`。用于语义对照的模拟盘回放测试生产器只允许消费测试
    主机已经到达的可见路径；本阶段不读取或验证真实模拟盘消费方日志，也不满足
    `SIM-AC-011`。
    -> 验证：唯一事件存储及上述四份 manifest 被 Gate A 覆盖；适配层使用 DPT 已验证公开读取器，
    保持索引原顺序和有界页解析，绑定实现身份与资源证据；模拟器不扫描原始目录、不判断 DPT
    日期、不直接读 Parquet，也不从一个索引重建另一个索引。现有公开原语不能形成合格流时，
    本阶段失败关闭而不是在模拟器核心中补建读取旁路。
  - 步骤 6.1.2：核对公共合同、事件带身份、模拟器规则快照和组件证据。
    -> 验证：不兼容项产生带归属的联合失败证据，不在模拟器中补字段、兼容猜值或重做 DPT。

- 任务 6.2：执行代表市场真实纵向
  - 步骤 6.2.1：至少一个代表市场由真实事件驱动影子簿、队列、订单和生命周期。
    -> 验证：初始 tick、动态 tick、首次快照、后续 `BookResyncDeltaInference`、独立交易
    状态、成交身份来源、缺口/重同步和市场判定均沿同一公共入口可追溯；同语义黄金事件按
    数据源事件时间产生相同隐藏平台状态转换，并按 `received_at_utc_ms` 产生相同模型可见轨迹。
  - 步骤 6.2.2：比较训练回放生产器和只用于语义对照的模拟盘回放测试生产器。
    -> 验证：训练回放按完整数据源事件时间投影生成隐藏影子簿；模拟盘只应用本机已经按模型
    可见时间接收的事件。一个私有响应早于对应公开消息的代表序列只暴露响应自身，不泄漏
    隐藏盘口或公开消息。

- 任务 6.3：真实性能重新定标
  - 步骤 6.3.1：测量真实事件数量、速率、突发、市场状态、请求负载、处理吞吐和延迟。
    -> 验证：不把原始行数冒充模拟器事件数；按阶段 4 同一硬件、构建和统计口径更新预算。
  - 步骤 6.3.2：重复受影响压力、故障和确定性测试。
    -> 验证：真实输入下仍有硬上限，预算变化不改变业务语义。

- 任务 6.4：封存双方联合真实纵向证据
  - 步骤 6.4.1：DPT 与 `sim-system` 分别自审归属和结果，再完成独立阶段审查。
    -> 验证：联合失败包含输入、首个分歧、归属和影响，没有影子规范化或临时补丁。
  - 步骤 6.4.2：原子封存阶段 6 既定不可变联合清单、代表市场报告和资源基线。
    -> 验证：联合清单绑定 Gate A 与组件证据；其内容身份和 SHA-256 就是 DPT/sim 联合真实
    纵向回执 ID/SHA，不新增登记系统、重复回执载荷、第二份清单或人工批准门。

阶段 6 完成门：Gate A、组件符合性和双方联合真实纵向均通过并完成独立阶段审查，才满足
`SIM-AC-009`。本阶段不反向改变 Gate A，也不宣称训练闭环或把项目代理时延配置档升级为
爱尔兰私有链路实测。

## 10. 阶段 7：外部训练闭环联合验收和正式就绪边界

本阶段不实现归一化层、同级目录中的交易侧 Ledger、DPT Gate B、RewardContract 或
`sim-calibration`。它只在
实际外部能力按获批归属形成实现和通过证据，且 DPT Gate B 形成绑定身份的候选产物后运行
联合验收；不要求 Gate B 预先签发成功回执。

- 任务 7.1：核对外部前置
  - 步骤 7.1.1：验证实际订单/非订单经济归一化、独立事项
    `TRADING-LEDGER-20260811` 交付的交易侧 Ledger training 实例及只读投影、阶段 6
    联合回执、`SimulationRunManifestV2` 和 DPT Gate B 候选的 ID/SHA。
    -> 验证：没有测试替身或本模块占位实现冒充外部能力；模拟原始证据仍由唯一归属方消费。
    对每条轨迹机械核对 `AccountInstanceKeyV1`、`AccountOpeningFactV2` 稳定身份、市场登记序列、精确输入
    范围、模型决策截止和经济成熟/删失边界在 sim 清单、归一化回执、Ledger 回执及 DPT 候选间
    完全一致；缺失、跨运行根拼接或把两个开户事实合成一条轨迹时停止。
  - 步骤 7.1.2：机械核对 `SimulationRunManifestV2` 与 DPT Gate B 候选绑定同一已验证首版项目
    代理时延配置档的 `profile_id/version/SHA-256`、采样算法版本及同一
    `TradeFinalityPolicyV2` 文件 SHA-256 / `policy_id`。
    -> 验证：配置档不是 `PROJECT_PROXY_PROFILE_READY`、身份失配或缺少绑定时停止；不得换用
    旧配置档、临时固定值、另一最终性策略或未来 `sim-calibration` 产物补门。

- 任务 7.2：执行同一候选的训练闭环轨迹
  - 步骤 7.2.1：以一个干净开户事实、固定随机种子、至少两个连续或重叠市场且跨 UTC 日但不
    强制完整日的轨迹，驱动 `sim-system -> 外部归一化层 -> TradingLedger(training) -> DPT-B`。
    -> 验证：同一开户事实和 `AccountInstanceKeyV1` 跨市场、日期和读取批次连续，只有一份显式初始
    资金及全局账户序号，不因现金归零、日期切换或批次切换补资或重开户；订单、成交、手续费、
    返佣、付款和赎回只应用一次，应计先为不可支出应收款，只有符合规则时才转现金，付款前截尾
    不提前付款。预登记轨迹数量与身份不因提前耗尽结果自适应增加或替换。
  - 步骤 7.2.2：验证完整 Observation 与 RewardContract 输入合同兼容性。
    -> 验证：未来反馈、尚未接收的公开消息和隐藏平台状态不可达；先到私有响应只贡献响应
    自身及其明确携带的事实，返佣不重复进入经济结果；不执行 RewardContract 实际计算。

- 任务 7.3：封存联合训练闭环证据并交付 DPT
  - 步骤 7.3.1：完成任务自审和独立阶段审查，修复后重复受影响联合测试。
    -> 验证：候选、四类外部回执、阶段 6 回执、逐轨迹账户/范围身份、同一最终性策略及代理
    时延配置档身份一致。
  - 步骤 7.3.2：原子封存同一候选的阶段 7 联合证据并交付 DPT。
    -> 验证：该证据只供 DPT 完成后续全量验证与 Gate B 签发，不扩大为
    `TRAINING_DATASET_READY` 已签发、正式 `model-train`、paper/live、部署或真实资金授权。

阶段 7 完成门：联合训练闭环证据通过并交付 DPT，满足 `SIM-AC-010` 的适用部分。首版项目
代理时延配置档必须已按本计划关闭，DPT Gate B 仍按其归属计划关闭；本计划不签发 Gate B 回执。

## 11. 需求验收映射

| 需求验收 | 主要阶段 | 核心证据 |
| --- | --- | --- |
| `SIM-AC-001` | 0、1、5 | 依赖/API 边界、三角色、无原始数据/模型/同级目录 Ledger/实盘回退 |
| `SIM-AC-002` | 1、2、3、5 | 八方法、请求/响应时间、逐项批量、撤单、查询及三类反馈 |
| `SIM-AC-003` | 2、3、5 | 数据源时间平台状态、动态簿、队列、撮合、小额余量与守恒 |
| `SIM-AC-004` | 1、5 | 当前/历史规则快照、固定端点、来源、失败门和恢复 |
| `SIM-AC-005` | 3、5、7 | 跨市场全局账户序列、成交最终性、资金、返佣、付款、赎回与外部唯一消费 |
| `SIM-AC-006` | 1、3、4、5、6 | 双时间确定性、仅追加市场接入、会话/隐藏状态隔离、恢复、资源硬上限及真实重新定标 |
| `SIM-AC-007` | 4、5、6、7 | 两道废弃面清除门、单一已受理输入日志、消费者窄投影、冷端清单、完整日志/最终产物摘要和保留期 |
| `SIM-AC-008` | 0 至 5 | 双时间黄金正例、响应先到隔离、单一日志恢复、代表性边界、事故回归和独立审查 |
| `SIM-AC-009` | 6 | Gate A、双时间真实代表市场和既定不可变联合清单内容身份/SHA |
| `SIM-AC-010` | 7 | 同一候选外部归一化/Ledger/DPT-B 闭环及配置档门 |
| `SIM-AC-011` | 独立真实模拟盘适用门 | 真实模拟盘适配器、运行时唯一 `SimEventLogV2` 及同一读取端口的跨进程联合证据 |

## 12. 当前游标与审批门

- 用户已批准压缩后的三份文档及计划范围内本地实施，并于 2026-08-18 批准“一条账户轨迹、一个
  会话、一个运行根”的并行边界，并批准“原位置稀疏前进 + 必要空前缀”、Paper 不在根外预筛，
  以及“归约一次形成未生效结果 -> 日志追加及必要同步 -> 日志成功后状态生效与结果交付”的方案 B；
  同步失败按选项 A 处理：当前进程关闭且零状态/输出生效，恢复以实际完整日志组裁决，不增加提交
  标记或第二次同步。任务 4.3 依次执行步骤 4.3.1 至 4.3.7；步骤 4.3.1 已把控制骨架、既有目录、
  角色市场初态、`ExecutionStateV2`、登记语义和业务容器聚合资源计数收束进唯一会话状态，没有重建
  第二份会话状态、账户、演员或归约器。步骤 4.3.2 已在该唯一会话候选内接通同一次边界解码所得
  事件的角色市场状态与执行队列，闭合 taker 债务回声、maker 零 quote 累计与首次正 quote 成交、
  平台小额余量、`end_at` 撮合关闭及对应资源增减；本步没有提前建立闭桶分派、演员日志接线或投影。
  步骤 4.3.3 已闭合五个变更方法，合格判定、maker/taker 应计、各自日结与付款、判定屏障、
  一次自动赎回、三种查询、八个方法特定响应草稿和三类生命周期反馈草稿。查询结果合同已经按
  用户批准的方案 A 消除冲突，成交从用户批准的零偏移最终性首态形成时起进入查询；当前游标为
  `阶段 4 -> 任务 4.3 -> 步骤 4.3.4 执行中`，下一步只建立唯一逻辑驱动器并闭合整桶原子归约，
  提交、推送、发布、部署和外部操作仍未授权。
- 公开路径仍是 V1 整态文件实现；私有 SQLite V2、`rusqlite`、专属恢复标签和独立 B1/B2 编排
  已经退出活动构建。真实历史测量报告由隔离的 `legacy_reader` 只读；仍被公开 V1 直接依赖的失败
  预留和资源证明留到任务 4.4 与旧写路径一并删除。
- 当前阶段 4 的唯一执行顺序是：
  `4.1 冻结业务基线 -> 4.2 删除私有 SQLite/B1-B2 等废弃面 -> 4.3 建立单一日志/归约/投影 ->
  4.4 切公开路由并删除剩余 V1 活动路径 -> 4.5 只测量和验收唯一新路径`。
- 普通正文热路径目标只有一次边界解码、一次归约形成未生效结果、一次日志编码与有界组追加，以及
  日志成功后的至多一次投影；必要空前缀为零正文解码、同一归约器处理一次、同一日志编码一次；不使用
  SQLite、逐对象摘要、账户前后摘要、自证报告、完整状态克隆、热审计或第二份恢复正文。
- 本轮删除了 A/B/C/D 授权矩阵、绝对到期状态机、历史规则迁移框架、分段/头部协议和穷举式符号/
  资源清单。最小日志只保留单文件、完整组、`CleanRun` 封口同步和 `RecoverablePaper` 每组一次
  同步与从起点恢复。若实施需要新增运行时校验，必须重新单独申请授权。
- 任务 4.5 的任何正式性能或资源测量都必须等两道清除门完成；吞吐、p99、组大小、日志上限、
  恢复时间和资源硬上限仍须实测后由用户批准。
- 此前已经撤回的“网络、重连、背压异常是否直接进入模拟器和模型决策”选择仍保持未决定，不进入
  本次文档或实施范围。
- 任务 4.1 已固定业务基线和三个区分性红灯；任务 4.2 删除私有废弃路径后，闭桶一次解码和双市场
  EOF 两项已经转为普通绿色回归，热路径摘要/完整状态序列化红灯仍按原计数保持失败。第一道清除门
  已通过。当前不提前切换公开路由、实现影子演员或开始正式性能测量。
