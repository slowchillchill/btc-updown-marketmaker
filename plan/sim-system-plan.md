# sim-system 已批准实施计划

> 事项编号：`SIM-SYSTEM-20260731`
>
> 生命周期：`APPROVED_ACTIVE_PLAN`
>
> 当前执行 authority：`plan/sim-system-plan.md`
>
> 当前业务与技术规范来源：本计划
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
> 状态：`阶段 2 本地完成门重新通过；提交与推送已获批准，阶段 3 尚未获批`

本计划依据简洁业务需求制定，是本事项当前业务语义与技术实施的唯一规范来源。它直接规定
模块边界、业务合同、成熟工具复用、执行顺序和验收方法；需求只描述宏观目标、范围与结果级
验收，changelog 只记录当时事实和取代关系。计划中的当前语义被新决定改变时，应直接覆盖
或删除旧内容，并把实际决定追加到 changelog。

用户已于 2026-08-09 批准需求与本计划，计划范围内的本地代码、测试和不可变本地测试产物
已取得实施游标；本计划不授权提交、推送、发布、部署、远端操作、钱包或凭据访问、真实下单
和真实资金操作。

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
4. 三个门互不冒充，也不得为了通过外部门而在 `sim-system` crate 内实现 DPT、归一化层、
   交易侧 Ledger、RewardContract、模型框架或校准写入器。
5. 所有完成结论必须绑定实际命令、输入、实现、规则、场景和产物身份；未运行、失败、被
   过滤或被外部条件阻断的验证不得写成通过。

### 1.2 测试与审查边界

- 正例优先使用真实平台样本或合同有效的黄金载荷；不可信网络响应、JSON、清单、检查点和
  跨模块持久产物，每类可达损坏只保留一个代表性边界输入，另保留已发生事故的聚焦回归。
- 输入通过真实构造、解码和边界校验后，内部测试不得直接篡改类型制造平台不可能状态。
- 新增或改变可执行行为时，先用原因正确且互不遮蔽的聚焦测试证明旧行为不满足当前合同，
  再做最小实现和受影响回归；已有行为特征化、文档迁移和资源测量不强造红灯。
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
进入本计划实施范围。阶段 3 为关闭既有跨模块输出合同，可以手术式修改现有 `contracts` crate，
但只允许加入 `AccountInstanceKeyV1`、`AccountOpeningAnchorV1`、`PlatformAccountDeltaV1` 和
`MarketEnrollmentV1` 四个跨模块根合同，以及只由这些根合同引用、完成规范编码所必需的最小
无行为值类型；其中调用方角色和平台来源必须各自只有一个共享表示。在这两个共享表示中，
`SessionSpecV2` 只直接接收共享调用方角色，`SimProcessRoot` 在内部向账户键与锚点注入共享来源的
`SIMULATED` 变体；调用方不存在平台来源字段或构造参数，也不得保留可独立演化并参与同一锚点
哈希的 sim 专用重复枚举。
不得借内部 Rust 类型拆分引入第五类业务合同；模拟账户、最终性策略、市场登记状态机、Ledger
行为、SDK 类型和任何 I/O 均不得进入 `contracts`。上述四个根合同的首版唯一发布者是
`sim-system` 阶段 3；其他模块只消费已发布版本，发现缺口时必须触发共享合同新版本决策，不得
重定义同名类型。

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
5. 模拟器只拥有模拟平台账户并输出 `AccountOpeningAnchorV1`、版本化原始账户变化及其他平台
   原始证据；该账户状态与跨市场全局 `PlatformAccountDeltaV1` 变化链就是用户所称的“平台侧
   training ledger”，不是另一个 crate。订单 NOE、非订单经济归一化、同级目录中的交易侧 Ledger、Observation、标签、奖励、
   模型训练、筛选、晋级和 `sim-calibration` 均在本模块之外。外部归一化与 Ledger 只能单向
   消费这些证据，不得回写或参与模拟平台最终资金门。跨模块消费者需要解码的无行为身份与
   原始证据叶子归现有 `contracts`；其唯一生成、校验和状态转换行为仍归 `sim-system`。
6. 历史训练、离线评估和模拟盘复用同一核心与合同，但输入权限不同：完整的数据源事件时间
   隐藏平台回放只提供给训练和离线评估；模拟盘只能应用本机已经按模型可见时间接收的公开
   事件，不得访问隐藏未来状态。各实例可以共享符合角色权限的不可变、内容寻址事件页和规则/
   场景产物，但不同实例的账户、订单簿叠加层、队列、订单、RNG、游标、反馈和检查点必须隔离。
   同一会话的全部市场则共享唯一模拟平台账户、全局资金池和账户变化序列；同一经济账户不得
   拆成多个各自初始化资金的并行会话。
7. 当前阶段 2 代码中的 `MarketSetV1` 与进程共享 `PlatformScenarioV1` 保持不可变，并作为每个
   会话的初始市场前缀。阶段 3 必须在不重建会话或账户的前提下，为每个会话增加独立、版本化、
   仅追加的 `MarketEnrollmentV1` 链；一个会话的登记不得改变其他既有或未来会话。training 与
   offline-evaluation 在首个业务事件或请求前冻结完整登记链；paper 只按本机已接收的合格证据
   逐项追加。每项登记绑定本会话 `AccountInstanceKeyV1`、开户锚点 ID/SHA 和前一登记身份。
   同系列新市场必须在首个业务使用前完成系列、市场/资产身份、前驱链、重复身份和使用围栏
   校验，然后原子扩展会话市场目录、适用角色的隐藏/公开市场状态、撮合队列、账户盘口
   叠加层及市场生命周期使用的目录；paper 不得因此创建隐藏状态。任一失败使这些状态全部不变；
   会话创建完成后，市场成员判断、事件授权、请求校验、`end_at`/生命周期枚举和撮合队列推进的
   唯一市场来源必须是该会话局部目录；不可变 `PlatformScenarioV1` 只用于构造初始前缀、校验
   初始身份和干净重放，后续业务不得继续直接枚举其 `market_set` 形成遗漏新增市场的影子路径。
   成功只增加新市场空状态，不复制或重置资金、占用、订单、成交、GTD、私有反馈序号、账户摘要
   或下一 `account_transition_seq`。该登记是受信组合根驱动的内部场景控制输入，不新增第九个
   公开交易操作。组合根提交带稳定 `enrollment_request_id` 和规范载荷摘要的候选登记参数；同一
   身份与载荷的物理重试返回原结果，同一身份绑定不同载荷时失败。成功只在原子扩展完成后同步
   返回由 sim 生成的规范 `MarketEnrollmentV1`，其中可取得登记 ID/SHA、前驱、新链头、链长度
   与链 SHA；失败返回明确错误且全部状态不变，使用新请求身份重复已有 `condition_id` 也失败。
   该同步控制结果不分配平台事件/接收时点、私有反馈序号或确认队列容量，也不经过生命周期反馈；
   首个新市场事件或请求只能在成功结果返回后进入。阶段 4 持久模式还必须先耐久提交登记记录与
   全部关联状态，再返回成功。阶段 4 的运行清单、检查点和恢复再绑定初始场景与完整登记链身份；
   本合同不改变阶段 2 已完成事实，也不授权阶段 3 代码。

### 2.2 八方法与模拟领域请求

唯一公开操作词汇为：

```text
post_order | post_orders | cancel_order | cancel_orders |
cancel_all_orders | order | orders | trades
```

1. 公开名字与官方 Rust SDK 的当前方法词汇对齐，但模拟参数和响应使用框架中立的领域类型，
   不伪装官方 `Client`，不预建通用后端 trait、插件注册表或公开 `SimRequest` 派发类型。
   八个方法特定请求、共享请求上下文和方法特定直接响应统一绑定
   `SIM_EXECUTION_CONTRACT_V1 = "sim_execution_contract_v1"`；共同版本标识不产生总派发类型。
   八个公开方法采用两步合同：任务 1.3 只在改变任何平台业务状态前完成领域请求校验、请求
   登记和逻辑动作调度，并返回方法特定登记回执；回执只确认请求身份、物理重投归类和稳定
   批量项调度映射，不是平台直接响应，不表示接受、拒绝、订单、撤单或查询结果，也不进入私有
   反馈序号。阶段 2 处理已登记动作后才生成方法特定直接响应，并经 crate 内私有响应可见性
   链路在 `response_received_at` 交付；该后续结果不新增轮询、取回或第九个公开操作。
2. 每个请求绑定合同版本、原始调用方角色和实例、会话、账户、全局请求 ID、调用方关联 ID、
   单调调用方序列、不得倒退且不得早于所引用可见事实的 `request_sent_at`、决策模型可见游标，
   以及由获批首版项目代理时延配置档决定的
   `platform_arrived_at`、`platform_responded_at` 和 `response_received_at`；变更请求另有
   幂等键，批量项另有稳定项身份。跨会话/账户、未知角色、`live-trade`、错误版本、未来游标
   和幂等载荷冲突必须在改变状态前安全拒绝。同一 `global_request_id` 的物理重投必须复用
   原 `caller_sequence`、发送时点、模型可见截点、代理时延样本和全部派生时间；同一身份绑定
   不同规范载荷时失败，只有新的逻辑请求才分配新身份、序列和样本，物理尝试次数只作诊断。
3. 请求在 `platform_arrived_at` 到达模拟平台。未进入平台内部暂缓的订单以该时点作为有效
   处理时点；只有 FOK/FAK 市价 BUY 在冻结规则 `itode=true` 时进入 250ms 平台内部暂缓，
   并以暂缓结束后的重新
   校验时点作为有效处理时点。训练与离线评估只读取有效处理时点按 Polymarket orderbook
   数据源事件时间已经生效的隐藏平台状态，并据此撮合、拒绝或生成响应；有效处理时点大于
   或等于 `end_at` 时必须在创建订单、资金预留或成交前拒绝。响应从 `response_received_at`
   起才对交易侧可见；即使它先于对应公开市场消息到达，也只暴露响应自身及其明确携带的
   订单/账户事实，不得携带、触发或摘要化尚未接收的公开消息或隐藏状态。
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
   不足不回滚其他成功目标，但内部不变量失败必须使本次工作副本整体不提交。`end_at` 至合格
   `MarketResolved` 到达前继续允许撤销已有活动订单，模拟器不得自行制造调用方撤单。
7. `order` 返回请求处理截点的单笔平台投影；`orders` 只列活动订单；`trades` 从本账户模拟
   成交形成时起列出成交，而不是等待阶段 3 最终性。已有开放订单在 `end_at` 后、合格
   `MarketResolved` 到达前仍属于活动订单并可查询。列表页大小以版本化项目兼容性假设
   `SIM_QUERY_PAGE_SIZE_V1 = 100` 固定，调用方不得自定义；官方当前仅在响应示例中给出 100，
   不构成平台永久保证。查询使用项目规范升序唯一键和冻结的不可变根投影；不透明游标完整绑定
   查询种类、规范过滤、会话、账户、合同、根身份、查询投影修订号、保留水位、页大小版本和
   下一位置，后续页不得重读当前可变状态。查询投影修订号只在 `order/orders/trades` 可观察内容
   改变时原子推进，纯市场事件、查询、响应交付和无副作用结果不推进；保留水位使用独立单调
   字段。同一请求身份和查询指纹的物理重投复用原根；同一身份绑定不同指纹时失败。
   `caller_correlation_id` 只作追踪，不能充当重试或截点身份。合法空页、完整覆盖下未找到与
   跨保留期结果必须分层，残缺页不得伪装成完整结果。
8. 首版不存在 `amend_order`、`modify_order`、`replace_order` 或隐藏等价入口。改变订单
   必须先撤单，再以新请求、幂等键、本地引用和新平台订单 ID 独立提交；二者不是原子替换。
9. 合同错误、合法平台拒绝和证据不足的未知/未建模结果必须分层。登记阶段发现的合同版本、
   调用方身份、会话、账户、游标、请求身份、稳定项身份或幂等载荷错误，由对应方法的
   `Result::Err` 在改变状态前返回；合法平台拒绝和证据不足只进入阶段 2 生成的方法特定直接
   响应，不得伪装成任务 1.3 的登记成功或合同错误。合法空页、未找到、右删失和反馈延迟不得
   误报成合同错误或程序损坏。
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
    `UnknownNotObserved` 不构成额外订单否决。FOK/FAK 市价 BUY 在适用 `itode` 暂缓结束后
    必须按重新校验时点完整重跑本项顺序。平台拒绝和证据不足都只作用于本次新订单，不关闭、
    撤销或释放既有 maker 订单。本项中的平台拒绝与 `EvidenceUnknownNoEffect` 只是任务 2.2
    的内部准入终局判定，不预先规定任务 2.3 的公开响应变体。
11. 任务 2.2 只实现无业务副作用的准入判定和内部 `PreparedSubmissionV1`，供任务 2.3 在同一
    有效处理时点立即消费。任务 2.2 不把已准入项标记为逻辑完成，不创建订单、资金预留或
    成交，不生成提交成功直接响应，也不引入运行时 `PENDING_MATCHING`、额外模拟等待或跨任务
    持久暂存。只有成功通过准入的项才形成 `PreparedSubmissionV1`；拒绝或证据未知项不形成。
    它只绑定任务 1.3 已有的请求、动作和批量项身份、规范化 BUY 意图、有效处理时点及角色授权
    平台修订，不预分配平台订单 ID、资金预留、成交或响应序号，也不复制持久市场状态。任务 2.3
    在同一有效处理时点消费该准备结果并形成逐项终局业务结果；提交响应的载荷、聚合方式和私有
    可见性序号由下项已批准的任务 2.3 合同统一规定，任务 2.2 不建立第二套语义。
12. 用户已批准任务 2.3 的初始账户、提交响应与确认式交付合同：
    - 每个会话必须显式取得不可变、可规范哈希且无默认值的 `InitialAccountStateV1`。首版只由
      调用方给出非负、最多 6 位小数的 `available_pusd`；初始结果代币、资金预留和返佣应收款
      均为零。该输入必须绑定会话规范和后续运行清单；不得采用默认零资金、无限资金或事后补
      余额；
    - 阶段 3 的当前会话创建规范必须发布并只接受 `SessionSpecV2`：每个 `SimProcessRoot` 运行根
      在创建任何会话前显式绑定一个非空、稳定且无默认值的 `run_id` 及一份已经校验并密封的
      `TradeFinalityPolicyV1`；该运行根内全部会话共享二者。创建方在 `SessionSpecV2` 中显式
      提供同一 `run_id`，不得从 `session_id`、时间或随机数隐式推导，也不得与运行根不一致；
      另一 `run_id` 必须使用另一个运行根。模拟 `platform_source` 由 `sim-system` 固定产生，调用
      方不得填写或冒充 live 来源。旧 `SessionSpecV1` 不满足开户身份合同，不建立兼容回退或
      双写路径；
    - 一个会话对应一条独立模拟账户轨迹，轨迹身份绑定运行、会话、调用方角色与实例、账户、
      模拟来源、初始场景和初始账户摘要；场景内全部 `condition_id` 共享同一个
      `ExecutionStateV1` 和可用 pUSD。阶段 3 必须先形成
      `AccountInstanceKeyV1(run_id, caller_role, caller_instance_id, account_id, platform_source)`，
      并仅在会话全部校验成功时生成恰好一个 `AccountOpeningAnchorV1`。该锚点绑定初始
      `PlatformScenarioV1` 和 `InitialAccountStateV1` 内容身份，随后仅追加市场登记不得改写它；
      `SessionSpecV2` 还必须绑定非空稳定的 `creation_request_id`、`response_bridge_binding_id`
      和完整规范规格摘要；桥绑定身份只标识受信组合根已经构造的进程内输出消费端，不把回调、
      闭包或 I/O 对象写入公共账户合同。`create_session` 必须把 `PREPARED_LEDGER_INIT` 会话、
      账户实例键保留和锚点作为一个同步开户结果原子形成；此状态下八方法业务入口不可用。相同
      `session_id`、完整账户实例键、创建请求、桥绑定身份和规范规格摘要的物理重试返回同一锚点
      和准备结果，但不得返回第二个可变会话句柄、重新消费或替换桥对象；首次成功恰好交付一个
      独占会话句柄，重复结果只明确表示 `EXISTING_PREPARED`。任一相同身份绑定不同载荷或桥身份
      时失败；另一
      会话尝试占用同一账户实例键也失败。开户锚点不是平台生命周期事件，不携带
      `platform_event_at`、`feedback_received_at`、私有反馈序号或传输代理时延，也不进入确认式
      反馈队列。受信组合根必须用同一锚点幂等初始化同级目录中交易侧 Ledger，确定成功后才以
      稳定激活身份调用 sim 本地控制面，把同一会话原子迁移为 `ACTIVE`；激活响应丢失时以同一
      身份重试并返回原结果。Ledger 结果未知时只能查询或重试同一锚点，不得激活、失败关闭或
      创建新锚点；Ledger 明确拒绝后才把准备会话迁移为 `INITIALIZATION_FAILED_CLOSED`，永久
      禁止业务入口，并在运行保留期内保留原键、锚点和失败证据，不能让同一键取得第二份本金。
      该状态是运行初始化失败而不是模型资金结果；training/offline-evaluation 不得自适应替换该
      预登记轨迹，paper 不得自动新建账户继续运行。
      阶段 3 若丢失唯一会话句柄，只能把当前运行判为本地组合失败，不能靠创建重试取得第二个
      句柄；是否按外部预登记运行合同重新开始不由 sim 推定，跨进程恢复与同一桥身份重接唯一归
      阶段 4。
      上述准备、激活和失败关闭都是受信组合根使用的同步控制面结果，不是第九个交易操作；
      `sim-system` 不调用、读取或解释 Ledger 回执，组合根独自验证该前置。
      调用方不得另填第二份初始余额、持仓、占用或返佣应收。跨模块公共根合同放入 `contracts`，
      唯一生成、校验和状态行为仍在 `sim-system`；
      `InitialAccountStateV1` 不提供 `2000 pUSD` 默认，
      模拟器也不执行外部每市场 `1000 pUSD` 风险上限或运行中自动补资；
    - 当前宿主不得用多个独立会话表示同一活动经济账户。阶段 3 必须在生成锚点前按完整
      `AccountInstanceKeyV1` 建立唯一准备/活动保留；同一创建身份和载荷只作幂等重试，另一创建
      身份对已准备、活动或初始化失败关闭的键均零副作用拒绝。不同 `run_id` 可以复用同一业务
      `account_id`，但必须由不同运行根、会话和开户锚点形成隔离账户链。开户锚点只绑定初始
      场景，后续运行清单和检查点分别绑定该锚点与当时的市场登记链身份。每个 `SimProcessRoot`
      还必须把“账户实例键 -> 原锚点/创建摘要”的最小资本签发围栏保留到该运行根结束；会话热态、
      查询投影或保留期证据删除不得删除该围栏，也不得让同一运行内的失败关闭键重新取得本金。
      该围栏不跨 `run_id`，不是永久可查询墓碑，数量受单运行账户实例硬上限约束；
    - 任务 2.3 只前移决定订单提交能否接受所需的最小资金检查与预留。任务 2.2 准入通过后，
      先以纯预演形成本次逐档撮合与即时 taker 手续费，再在分配平台订单 ID 和提交任何业务
      状态前，按 `PreparedSubmissionV1` 的最终 SDK 编码 `maker_amount_pusd` 税费前本金与
      本次即时 taker 手续费之和检查可用 pUSD；任一不足均形成
      `InsufficientAvailablePusd` 的 `RejectedNoEffect` 且零业务副作用。不得以资金上限静默
      缩小税费前本金，也不计算 builder fee。接受时本金占用、手续费占用、订单创建和本次撮合
      结果原子提交；手续费在任务 2.3 只形成独立占用，最终应用、失败释放和补偿仍归阶段 3；
      批量按已冻结的有效处理时点、调用方序列和 ordinal 逻辑顺序逐项非原子处理，后处理项
      读取此前已处理项已经占用或释放后的资金；最终响应仍按原 ordinal 排列。成交最终性、补偿、统一
      账户摘要链、返佣、赎回和后续释放仍归阶段 3；
    - 公共面只增加方法特定的 `PostOrderDirectResponseV1` 与
      `PostOrdersDirectResponseV1`，不得产生公共通用响应派发类型。两者共同显式绑定合同版本、
      `SIMULATED` 证据来源、会话、账户、调用方角色与实例、请求、关联和方法身份、
      `platform_responded_at`、`response_received_at`、私有序号、稳定交付身份及规范载荷
      SHA-256；`post_order` 含一项，`post_orders` 含与登记请求等长且按原
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
      生成 fill。`end_at` 只移除撮合资格，不关闭或释放待累计量。待累计来源只保留顺序滚动
      SHA-256、事件计数、首项与末项身份，不保留事件数组；该中间分桶不是
      `VENUE_DUST_TERMINAL`。这些来源字段只属于内部执行、守恒与审计状态，不进入模型可见
      订单或成交查询投影及其规范摘要；
    - 即时 taker 手续费按每个已预演成交价位的实际成交 base 与该价位分别代入密封规则快照的
      `POLYMARKET_FEE_CURVE_V1`，先保留精确十进制并求和，再对正数总额采用五位小数的
      `MidpointAwayFromZero` 舍入；结果小于 `0.00001` pUSD 时固定为零。该汇总和中点规则是
      首版项目假设，必须写入订单、响应和运行证据身份；maker 不收平台费，首版请求没有
      builder code，因而 builder fee 恒为零；
    - 每个逻辑请求恰好形成一个不可变响应。单笔 `platform_responded_at` 等于本次请求项终局
      响应原子形成时点，也就是本次响应项的 `effective_processing_at_utc_ms`；对
      `NEW_EFFECT`，该时点同时是首次业务效果形成时点，对 `EXISTING_EFFECT` 则不得改写响应项
      另行携带的首次业务效果形成时点。批量只有在本次请求全部项终局后密封，
      `platform_responded_at` 等于本次请求各响应项有效处理时点的最大值；
      `response_received_at` 继续使用登记时冻结的入站代理时延。物理请求重投只返回原登记回执，
      不重新撮合、生成响应、占槽或抽样；
    - 会话核心使用正数、有限容量的逻辑发件箱，并在任何订单、资金或撮合变化前为响应预留
      槽位。只有预绑定外部桥接器把同一不可变载荷接纳进自身有界队列，并精确确认
      `(session_id, delivery_id, private_feedback_sequence, payload_sha256)` 的连续队头后，
      核心才删除载荷并释放槽位。满载时保留同一身份、摘要、序号和逻辑时间非阻塞重试，不重新
      执行业务、不忙循环；断开或身份冲突只关闭对应会话的新接入。不得持有会话锁做阻塞发送，
      不得暴露 `poll/take/ack` 等第九公开操作。最终容量数值仍归任务 4.4 实测冻结；任务 2.3
      只承诺进程内接纳确认，持久化、检查点和崩溃恢复仍归任务 4.2，在其完成前不得宣称可恢复
      发件箱或恢复交付已经实现。
    用户已批准任务 2.3 的零 quote 外部余量与幂等双时点合同：
    - 外部合成段有正 base、但完整六位 quote 为零时，该余量仍保留在规范外部簿，供后续 DPT
      重校准或队头减量使用；账户 taker 不消费它，它对 taker 可执行价位扫描透明，也不阻塞
      后方较差但可形成正 quote 的价位。post-only 与正式撮合必须共用该“可执行流动性”定义，
      不得一条路径跳过而另一条路径停止；
    - 同一用户幂等键与同一业务载荷在新的 `global_request_id` 下复用首次终局效果时，响应项的
      `effective_processing_at_utc_ms` 保留本次逻辑请求的有效处理时点；本次响应的
      `platform_responded_at` 与 `response_received_at` 也按本次请求形成。响应项同时显式携带
      并纳入规范载荷摘要的稳定提交效果身份、首次效果形成时点，以及 `NEW_EFFECT` 或
      `EXISTING_EFFECT` 标记。首次形成时两时点相同；后续复用不得把首次业务效果伪装成本次
      重新判定，也不得重新创建订单、成交、资金变化或效果身份。
13. 用户已批准任务 2.2 的订单精度与时点合同：
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
14. 用户已批准任务 2.4 的撤单、查询响应、分页和保留读取合同：
    - 公共面新增且只新增六个方法特定直接响应：`CancelOrderDirectResponseV1`、
      `CancelOrdersDirectResponseV1`、`CancelAllOrdersDirectResponseV1`、
      `OrderDirectResponseV1`、`OrdersDirectResponseV1` 与 `TradesDirectResponseV1`。内部把现有
      单一动作队头、有限响应发件箱、连续私有事实顺序和预绑定桥接器泛化到八方法；不得增加
      公共总响应枚举、按方法旁路驱动器或 `poll/take/ack` 等第九操作。每个新逻辑请求在登记和
      业务变化前预留一个响应槽，物理重投与跨 `global_request_id` 幂等复用继续遵守任务 2.3
      已冻结的身份、首次效果时点和本次响应时点合同；
    - `cancel_order` 返回一个目标；`cancel_orders` 先按平台订单 ID 去重并按首次出现顺序返回
      唯一目标，每个目标携带按原输入顺序排列的 `input_item_refs`；`cancel_all_orders` 在有效
      处理截点冻结本会话、本账户全部市场中 `pending_maker_depletion_base + open_base > 0`
      的订单，并按 `platform_order_id` 升序形成稳定目标集，空集合合法。同一幂等效果必须复用
      首次冻结的目标集，不得顺带撤销后来订单。三方法都按目标表达 `Canceled`、
      `NotCanceledNoEffect` 或 `EvidenceUnknownNoEffect`；当前完整活动会话内不存在或跨会话目标
      统一为不泄露归属的 `NotFoundOrNotOwned`，已有终态或无可撤余量为
      `NoCancellableRemainder`。未知只保留给明确证据覆盖缺口，首版不为满足类型而制造可达未知；
      过保留期会话走请求级范围错误，内部不变量损坏失败即关闭。模拟订单 ID 使用
      `SIM_PLATFORM_ORDER_ID_V1` 的 64 位小写十六进制格式；格式非法、空批次、超过 1,000 项、
      重复 `item_id` 和幂等载荷冲突均在改变状态前走 `Result::Err`；
    - 每个成功撤单目标原子执行：`canceled_base += pending_maker_base + open_base`，只把
      `open_quote` 转入调用方撤单释放分桶并令 `available_pusd += open_quote`、
      `principal_encumbered -= open_quote`，随后把 pending、open 和 open quote 清零。撮合队列
      只移除仍开放段，账户叠加层只扣仍开放 base；已成交 base/quote、成交记录、结果代币与
      即时 taker 手续费占用保持不变，pending 来源摘要进入固定大小终态证据。本任务先推进已经
      实现的 GTD 到期，并且不得用撤单覆盖任何已经形成的订单终态；最终系统中已生效合格判定
      先于同刻撤单的订单终态迁移及端到端验证仍唯一归任务 3.5.1，不在任务 2.4 读取市场判定后
      建立临时拦截或影子终态路径。`end_at` 只停止撮合，不关闭或阻断撤单；
    - `order` 的结果为 `Found`、`NotFound` 或 `OutOfRetention`；`orders` 与 `trades` 的结果为
      `Page` 或 `OutOfRetention`，合法过滤无匹配返回成功空页。`orders` 的活动定义仍为
      `pending_maker_depletion_base + open_base > 0`，包括 `end_at` 后至合格判定前的订单；
      `trades` 从 `MATCHED` 起可查，阶段 3 后续状态只影响新查询根。投影同时保留模拟平台原始
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
    - 根查询物化完整、已过滤且已排序的不可变投影，游标由本系统签发并绑定前述完整身份；终页
      在项目响应中统一为无后续游标，不复制官方文档中互相冲突的空字符串或 `LTE=` 哨兵。
      未签发、损坏、跨方法、跨过滤、跨会话或跨合同游标在登记阶段零副作用拒绝；合法游标在
      登记后因根投影被裁剪时返回方法特定 `OutOfRetention`。任务 2.4 只实现进程内紧凑根投影、
      显式保留水位和读取结果；实际保留时长、滚动、最终删除、持久化和恢复仍归任务 4.1、4.2
      与 4.4，不得提前宣称完成。100 项代表页的投影字节必须在任务验证中记录；最终硬上限仍由
      任务 4.4 实测冻结。

### 2.3 进程级低频规则初始化与成熟工具

1. `sim-system` 进程的组合根必须在创建任何训练、离线评估或模拟盘会话、处理任何业务请求
   前取得唯一不可变 `PlatformRuleSnapshot`。当前进程使用 `CURRENT_QUERY`，只对调用方
   显式给出的一个 `btc-updown-5m` 参考市场完成一次同步；随后全部运行、会话和后续同系列
   市场共享同一快照身份与 SHA-256 到进程退出，不得逐运行、逐会话或逐目标市场查询、扩展或
   热刷新。首个已实施进程快照 schema 固定为
   `PLATFORM_RULE_SNAPSHOT_SCHEMA_V2 = "platform_rule_snapshot_v2"`；此前审核中的逐运行 V1
   从未形成正式产物，不得被读取或伪装成 V2。
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
3. 每个完整响应先对原始字节计算 SHA-256，再由专用窄 DTO 强类型解析。DTO 忽略未知附加
   字段，不使用 `deny_unknown_fields`；数值先取得 `serde_json::value::RawValue` 原始词法，
   再由 `rust_decimal` 精确解析，禁止先进入 `f64` 或用 `serde_json::Value` 任意探测。
   Gamma 的 JSON 编码 `outcomes` 与 `clobTokenIds` 必须二次强类型解析，并与参考身份和 CLOB
   token/outcome 精确一致。未完整接收的响应不得把前缀摘要冒充完整原始响应 SHA。
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
6. 历史训练和离线评估进程禁止联网回填。`HISTORICAL_AS_OF` 是读取既有同 schema 密封快照
   的访问模式，不重写其创建来源；快照观测时点必须不晚于回放因果起点。`RECOVERY` 同样只
   验证并读取既有快照及实现身份。三种入口进入同一确定性核心，历史与恢复入口的签名不接受
   HTTP 客户端、端点或参考市场；同一进程只能选择一种启动模式并绑定一份快照。
7. 只读 GET 的瞬时传输错误或官方列明的 `500/503` 在同一初始化总时限内最多额外重试一次；
   请求最终失败、解析失败、必需身份或规则缺失、非法或冲突不重试，并在任何会话创建前使
   整个进程规则能力失败关闭。单个完整 HTTP 响应和最终快照落盘字节分别硬限 16 MiB；不够
   时只能经后续明确计划调整到用户允许的 52 MiB，不能在运行时自动放宽。
8. 仓库版本化 `sim-system/config/platform-rules-v1.toml` 保存官方文档规则和用户冻结的
   项目/账户规则。首版至少固定：
   - 只有 `itode=true` 的 FOK/FAK 市价 BUY 应用 250ms 平台内部 taker 暂缓，并在结束时按
     已到达状态重新校验；GTC/GTD 限价 BUY 不因到达时盘口可立即成交而进入该路径，地域网络
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
   两个端点、尝试数、完整原始响应 SHA、静态配置、依赖、实现身份、内容摘要和文件 SHA，
   不伪造后续市场查询证据且不含机密。序列化尾换行后的精确落盘字节必须在创建候选前通过
   16 MiB 门；同父目录候选、文件同步、`RenameFlags::NOREPLACE` 和父目录同步共同完成无覆盖
   密封，读取时使用同一上限和同一已打开文件验证。
10. 每条可执行规则必须且只能归入一种证据类别：`OFFICIAL_PLATFORM_RULE`、
   `OFFICIAL_SDK_CONTRACT`、`LOCAL_SDK_PATCH_CONTRACT`、`OBSERVED_PLATFORM_BEHAVIOR`、
   `USER_CONFIRMED_PLATFORM_BEHAVIOR`、`RULE_DERIVATION`、`PROJECT_ASSUMPTION` 或
   `UNCERTAINTY_PROFILE`。参考市场响应属于官方平台观测；跨市场/进程期复用、固定 8% taker
   返佣、固定 20% maker 返佣、L2 队列模型与返佣付款时刻/截取属于项目假设。当前 250ms
   `itode` 行为来源属于官方规则，平台小额余量、合格 `MarketResolved` 到达且结算屏障闭合后的
   即时原子赎回，以及 `end_at` 至合格 `MarketResolved` 的订单
   行为属于用户确认的平台行为，可立即成交 GTC 的 1 pUSD 门属于已观察平台行为，GTD 复用
   该门属于机械推导；`ORDER_STATUS_CANCELED_MARKET_RESOLVED` 的原始状态拼写属于官方订单
   状态合同。规则产物记录来源、日期、范围、限制、版本、摘要和失败语义；不得把观察、推导
   或假设升级成官方永久保证。约三分钟只可描述代表性场景中 `end_at` 至合格判定/现金回账的
   间隔，不能被改写为 `end_at + 3 分钟` 自动触发赎回或授予可用资金。
11. 优先复用正式工作区现有已锁定的成熟 Rust 依赖：`reqwest 0.13.4` 负责固定 HTTPS 与查询
    编码，目标 crate 直接声明 `default-features=false` 及实际需要的 `query/rustls`，并显式
    配置 `redirect::Policy::none()`；`serde 1.0.228`、带 `raw_value` 的
    `serde_json 1.0.150` 负责窄 DTO；`sha2 0.10.9` 负责内容身份；
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
   保留 `trade_id`，且 `trade_id_source` 能区分平台交易哈希与派生源证据；模拟器不得猜测
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
   未密封请求或待处理动作；同一有效处理时点的动作按 `caller_sequence`、批量项稳定次序逐项
   释放，前一项完成前不得处理后一项。批量项只根据已解析的方法类型和冻结 `itode` 规则，在
   读取盘口前静态绑定 `platform_arrived_at` 或 `platform_arrived_at + 250ms`：只有 FOK/FAK
   市价 BUY 使用后者，GTC/GTD 限价 BUY 不按盘口动态归类。模型可见截点分别绑定公开可见游标
   和私有反馈序号；私有事实只有在其接收时间前缀闭合后，才按接收时点与稳定私有发射序号分配
   私有反馈序号。公开、私有事实之间不建立全局同毫秒优先级，也不要求模型等待两条流共同闭桶。
   前缀闭合入口只供 crate 内公共桥接器背后的受信输入驱动器使用：驱动器必须已经按原序应用
   所有 `event_at <= t` 事件，并通过窥视下一条 `event_at > t` 或到达 EOF 证明完整桶闭合，
   再把对应位置与 `t` 一次提交给水位原语；模型、调用方和八方法入口均不能自行声明游标。
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
10. 进程组合根在创建任何会话前还必须取得一份共享、不可变、内容寻址的
    `PlatformScenarioV1`。首版 schema 固定为
    `PLATFORM_SCENARIO_SCHEMA_V1 = "platform_scenario_v1"`，市场集 schema 固定为
    `MARKET_SET_SCHEMA_V1 = "market_set_v1"`，市场系列固定为 `BTC_UPDOWN_5M`。市场集按
    `condition_id` 规范排序且不得重复；每个成员包含精确 `MarketIdentityV2` 和按 token ID
    规范排序的恰好两个唯一 `AssetIdentityV1`，两项 outcome index 也必须唯一。市场集摘要是
    带固定域前缀的规范 JSON SHA-256；场景摘要以另一固定域绑定场景 schema、固定市场系列及
    市场集摘要。`SimExecutionProcessV1` 构造时注入该场景，进程内全部会话共享其不可变引用；
    `SessionSpecV1.market_set_sha256` 与 `scenario_sha256` 必须分别精确匹配实物摘要，否则在
    创建会话时失败。参考规则市场只提供系列规则证据，不能代替该目标市场集。
11. 任务 2.1 的事件原子消费单位固定为一个已经由 crate 内受信输入驱动器证明闭合的索引
    毫秒桶。事件时间路径的桶内事件必须全部具有与桶时点相同的真实数据源事件时间，模型可见
    路径的桶内事件必须全部具有与桶时点相同的接收时间；两条路径都保持 DPT 原始索引顺序。
    模拟器必须先通过 DPT 唯一 `decode_execution_market_event_v3` 入口解码并预检整桶，再原子
    提交紧凑市场状态及对应水位；后项非法时不得留下前项状态或前移水位。分页边界本身不证明
    毫秒桶闭合。每个桶显式绑定当前起始索引位置与提交后的下一索引位置，起始位置必须等于当前
    游标；位置跨度不得从过滤后的市场事件数量反推。`LevelExecutionTransitionV3.causal_group_id`
    只作为桶内市场身份、数据源事件时间与接收时间的一致性审计身份，不推断 DPT 没有提供的
    独立组长度或结束标志，也不得以其重排桶内事件。
12. 场景是任务 2.1 判断首次装载完整的唯一预期集合：事件的市场与逐资产载荷必须匹配场景，
    每个预期资产只允许一份初始 L2 快照，两个预期资产均完成后才结束 `PRELOADING`。训练和
    离线评估的隐藏平台初始化不以前置的市场生命周期事件或三个交易状态布尔为条件，因为没有
    真实数据源事件时间的元数据不进入事件时间索引。`active`、`closed`、
    `accepting_orders` 在模型可见路径仍逐字段原样独立保存；任务 2.1 不据此推导内部生命周期
    或订单准入真值表。订单准入、空值和未知值处理现由第 2.2 节第 10 项拥有。

### 2.5 动态影子 CLOB、队列和撮合

1. 每个会话分别维护外部影子订单簿、本账户订单叠加层和只读派生聚合盘口。外部数量、账户
   数量和所有权不能互写；账户订单进入盘口后仍保持账户身份。
2. 唯一 maker 队列模型为 `L2_BOOK_DELTA_FIFO_ESTIMATE_V1`，证据类别为
   `PROJECT_ASSUMPTION`。账户订单排在其有效处理时点前已经存在于隐藏平台状态的同价数量
   之后；未暂缓订单的该时点是 `platform_arrived_at`，FOK/FAK 市价 BUY 进入 `itode` 暂缓后
   则使用重新校验
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
5. FOK/FAK 市价 BUY 只有在规则快照 `itode=true` 时才进入 250ms 内部暂缓并在结束时完成
   适用准入与重校验；GTC/GTD 限价 BUY 即使到达时可立即成交也不进入该暂缓。未暂缓订单的
   有效处理时点为 `platform_arrived_at`，暂缓订单为
   重新校验时点；该时点大于或等于 `end_at` 时拒绝且不产生订单、资金预留或成交。通过准入
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

首版每个 `SimProcessRoot` 运行根必须在创建任何会话前显式绑定内容寻址的
`TradeFinalityPolicyV1`；运行根 `run_id`、策略 ID/SHA 或规范内容缺失、不一致或非法时失败即
关闭，不提供隐藏默认值。同一运行根全部会话只读共享该策略；另一 `run_id` 使用另一个运行根，
不得在既有运行根内热换策略。该策略是 `sim-system` 自有、无网络的确定性场景输入，不进入
`contracts`：它为普通成交给出从 `matched_at` 起机械实例化的封闭状态模板；首态逻辑偏移必须
为零，其后每个状态的偏移必须严格大于前一状态，并明确唯一经济应用截点。组件故障测试可以另带
按预登记请求、批量项或成交稳定身份命中的有限覆盖项，显式形成未应用失败或应用后补偿。每份
策略必须为全部覆盖项选择且只选择一个 `REQUEST`、`BATCH_ITEM` 或 `FILL` 选择域；每个选择键
都必须由完整 `AccountInstanceKeyV1` 与该轨迹内的规范请求、批量项或成交身份共同构成，不得
直接用会话局部原始字符串在运行根内跨会话匹配。不同选择域不得混用，同一域内规范身份不得
重复。一个成交因而至多命中一项覆盖：命中零项走普通模板，
命中一项走该覆盖；策略混用选择域、重复身份或运行时出现多重命中时，必须在该成交及其经济
变化形成前失败即关闭，不能按优先级猜测。选择键编码非法或缺少账户实例限定时在运行根密封前
拒绝；编码合法但本次运行没有任何成交命中不构成错误，也不要求预先知道动态请求。覆盖项不得按
模型结果或运行表现自适应选择，未命中的成交只走普通模板。状态推进只使用模拟逻辑事件
时间，不等待现实墙钟；不得从公共市场成交、`MarketResolved`、请求/反馈传输代理时延或未经
支持的随机概率生成私有最终性。相同策略、脚本身份、输入和种子必须产生相同路径、时点、经济
应用与补偿。官方
[订单生命周期](https://docs.polymarket.com/concepts/order-lifecycle) 和
[用户频道](https://docs.polymarket.com/market-data/websocket/user-channel) 只作为状态词汇与可观察
含义来源；本项目的路径模板、时点和经济应用截点明确记为 `PROJECT_ASSUMPTION`，不得冒充实际
私有链路证据。阶段 3 组件测试可以使用显式测试策略；首个正式 `COMPONENT_GOLDEN` 运行必须在
查看结果前密封具体策略字节、ID 和 SHA-256，阶段 6/7 只能消费清单绑定的同一策略身份。

`TradeFinalityPolicyV1` 的首版路径文法只允许以下有限模板，其中方括号表示可选的单个前缀：

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
   `(platform_event_at, fill_economic_delta_identity, path_ordinal)` 顺序；
3. 返佣日结与付款时钟事件，同刻固定为 maker 日结、taker 日结、maker 付款、taker 付款，再按
   结算身份排序；
4. 已封口 DPT 毫秒桶，保持桶内规范原序；合格 `MarketResolved` 的接纳和开放订单余量关闭在
   该规范事件的原子处理内完成，经济前驱已经闭合时同项完成赎回，否则只保留第 2.6 节规定的
   内部屏障义务，待最后前驱闭合后再作为其因果子项完成；
5. 订单/撤单/查询动作，沿用既有逻辑动作键和批量项 ordinal。

不得为了处理后一类而越过同刻尚未完成的前一类。一次撮合产生的零偏移首态属于该撮合的原子
因果转换，不重新排入同刻队列：DPT 桶内按规范事件与稳定成交身份处理，订单动作内在进入下一
批量项前完成，同一原因产生多笔成交时按 `fill_economic_delta_identity` 升序；后续状态因偏移
严格递增，只能进入未来逻辑时点。因而同刻补偿或资金释放必须先于
同刻新订单资金门，多个成交则按上述稳定键形成唯一账户摘要链；串行、并行或重放不得改变顺序。
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
2. 每笔撮合成交拥有唯一 `fill_economic_delta_identity` 和策略选定的经济应用截点。后续
   `MATCHED_NOT_BROADCASTED`、`MATCHED`、`MINED`、`CONFIRMED`、`RETRYING`、
   `FAILED` 推进生命周期；进入 `FAILED` 时，若原变化量已经应用，则失败终态与引用原身份的
   唯一反向补偿在同一个不可分割的平台转换中完成并由一项成交反馈承载，不暴露二者之间状态；
   原变化量未应用时，同一失败转换只释放相应待处理资金占用，不生成反向经济变化。
3. 订单接受产生的本金/即时手续费占用，以及所有改变模拟账户的成交、补偿、资金释放、返佣、
   付款和赎回，共用一个账户轨迹内跨全部 `condition_id` 全局单调的
   `account_transition_seq`、前驱身份和账户之前/之后摘要，禁止逐市场分别从 1 编号。首项前驱
   绑定 `AccountOpeningAnchorV1`，后项绑定前一全局变化；每项同时绑定原因身份、受影响市场、
   平台生效时间和显式单位变化。反馈可以迟到或乱序，但调用方必须能识别缺口、缓冲、按序应用
   和幂等去重。该链只表达模拟平台原始账户事实，不携带同级目录中交易侧 Ledger 的本地待提交
   预留、NAV、PnL、奖励或模型动作。
4. `VENUE_DUST_TERMINAL` 只适用于本账户已接受、按 base 计量且已经有正撮合数量的订单：
   撮合/队列转换冻结后，其精确活动余量严格位于 `(0, 0.01 shares)`，且同价外部前后数量
   与其他账户订单均已由同一合格转换耗尽，才原子把该精确余量转入小额余量终态并把活动
   余量归零。
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
编码到期时点、到期前 pending/open base、释放的 open quote 与固定大小待累计证据摘要，重复
时点推进不得再次释放。
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
`PlatformAccountDeltaV1` 表达，并至少绑定全局序号、前驱、原因身份、受影响市场、平台生效
时间、显式单位变化和前后账户摘要；调用方撤单、订单类型终止、到期、合格市场判定关闭、平台小额
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
6. 合格 `MarketResolved` 在其数据源事件时间进入平台状态后，先把仍开放订单的未成交余量
   原子转为 `ORDER_STATUS_CANCELED_MARKET_RESOLVED`，并用独立幂等变化只释放该余量的资金
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

### 2.8 输出、生命周期、恢复与资源

1. 直接请求响应、`OrderLifecycleFeedback`、`TradeLifecycleFeedback` 和
   `SettlementLifecycleFeedback` 是彼此独立的封闭合同，通过稳定请求、订单、成交、市场、
   账户和结算身份关联。直接响应绑定 `platform_responded_at` 与 `response_received_at`，只在
   后者对交易侧可见；三类生命周期反馈分别绑定 `platform_event_at` 及经过适用返回延迟后的
   `feedback_received_at`，也只能从后者起对交易侧可见。响应或反馈可以暴露自身明确携带的
   请求、订单、成交、账户或结算事实，即使对应公开消息尚未按模型可见时间到达；但不得附带、
   触发或摘要化该公开消息、盘口或其他隐藏平台状态。结算反馈首版包含自动赎回、两类返佣
   应计、两类日结和两类付款。三类反馈 schema 分别固定为
   `ORDER_LIFECYCLE_FEEDBACK_SCHEMA_V1 = "order_lifecycle_feedback_v1"`、
   `TRADE_LIFECYCLE_FEEDBACK_SCHEMA_V1 = "trade_lifecycle_feedback_v1"` 和
   `SETTLEMENT_LIFECYCLE_FEEDBACK_SCHEMA_V1 = "settlement_lifecycle_feedback_v1"`。
2. 输出保留模拟平台原始状态拼写和 SDK `Unknown(raw)`，明确标记为模拟；其中
   `AccountOpeningAnchorV1` 和 `PlatformAccountDeltaV1` 仍是模拟平台原始证据。下游唯一外部
   归一化入口才负责生成 Ledger 输入；模拟器不输出 NOE、`NormalizedLedgerTransactionV1`、
   同级目录中的交易侧 Ledger 事件、奖励、标签或真实平台格式。开户锚点遵守第 2.2 节同步
   开户合同，不属于直接响应或生命周期反馈，不分配私有反馈序号、传输时点或确认式队列
   容量。三类生命周期反馈及其承载的账户变化必须复用阶段 2 已建立的会话级有界、稳定身份、
   匹配确认后释放容量的交付语义；复用的是交付协议与内部基础设施，不把不同公共载荷合并为
   通用响应枚举，也不新增 `poll/take/ack` 或第九个交易操作。每项反馈绑定稳定 `delivery_id`、
   私有发射序号、平台事实时点、交易侧接收时点和载荷摘要，物理重投只返回同一逻辑对象。任何
   账户或业务变化必须先取得对应证据的有界保留能力，再把状态与必需输出作为一个原子结果提交；
   容量不足、桥断开或确认身份冲突时不得只提交状态变化。每个 `PlatformAccountDeltaV1` 只由
   一个原因匹配的类型化生命周期反馈规范承载：订单接受、撤单、到期和判定关闭归订单反馈，
   成交应用、最终性失败和补偿归成交反馈，返佣、日结、付款和自动赎回归结算反馈；直接响应
   可以引用同一变化身份，但不得生成第二份规范账户变化。`account_transition_seq` 表达平台经济
   顺序，私有反馈序号只表达交易侧交付顺序，两者不得合并。阶段 3 的匹配确认只表示进程内调用
   方已接纳并用于释放内存容量，不得冒充下游已经耐久消费。持久发件箱、检查点、下游耐久消费
   水位和崩溃恢复仍归阶段 4；耐久水位只能由受信组合根取得并核对下游已持久保存的账户实例、
   交付身份、私有序号和载荷摘要前缀证据后推进，进程内桥回调本身无此权限。阶段 4 必须保留
   稳定载荷证据，并从最后一个双方可证明的耐久消费水位之后重放，缺少该水位时从开户锚点之后
   重放，由下游按稳定身份幂等去重，不宣称跨崩溃物理“恰好一次”。
   阶段 4 的持久模式必须把输入/控制身份与游标、业务状态、账户变化、必需不可变输出或同步控制
   结果证据组成一个本地原子耐久单元；只有该单元跨过本地耐久屏障后，才可调用桥接器、返回
   `create_session`/激活/市场登记成功或推进可对外观察的完成水位。耐久提交失败时不得对外成功
   或交付；提交前崩溃从原输入重做且外部未见成功，提交后任一点崩溃都以原稳定身份重放已耐久
   结果。桥的进程内确认只能释放内存副本，完整耐久证据必须保留到下游耐久消费水位跨过。
3. 市场生命周期单向为：

   ```text
   PRELOADING
     -> ACTIVE
     -> POST_END_PENDING_RESOLUTION
     -> RESOLUTION_TERMINAL
     -> EVICTED
   ```

   到达 `end_at` 时进入 `POST_END_PENDING_RESOLUTION`：有效处理时点不早于该边界的新订单
   一律拒绝，已有开放订单保持可查询、可撤并继续保留未成交余量的资金预留，但不再参与撮合。
   边界前已进入的普通处理完成、`itode` 暂缓也完成重新校验后，可以释放完整外部盘口与撮合
   大缓冲，但必须保留支持开放订单、资金预留、撤单和查询的紧凑状态。合格
   `MarketResolved` 到达时，仍开放的未成交余量才按第 2.6 至 2.7 节转为
   `ORDER_STATUS_CANCELED_MARKET_RESOLVED` 并释放对应资金预留；结算、返佣、反馈、查询投影和
   幂等证据持久化并满足引用屏障后，才移除紧凑热状态。合格判定已经接纳但仍有合法已排定经济
   前驱时，市场继续留在 `POST_END_PENDING_RESOLUTION`，直至最后前驱闭合、唯一赎回完成后才
   进入 `RESOLUTION_TERMINAL`；不得因判定证据已到达而提前宣称经济终态。输入结束仍缺少合格
   终态时，只形成
   模拟器内部的运行失败封存和隔离，不生成公共市场事件或外部生命周期分支，也不能伪造订单
   终态、资金释放或自动赎回。
4. 热移除不等于查询记录删除。`order` 和 `trades` 在保留期内从紧凑索引返回相同终态；
   `orders` 不含终态订单；历史被裁剪时返回明确水位线/超出保留期，保留期到期后旧会话
   整体不可查询，不永久积累逐订单墓碑。第 2.2 节最小资本签发围栏仍保留到所属
   `SimProcessRoot` 结束，只阻止同一运行内重复开户，不提供旧会话查询，也不跨运行永久保留。
5. 训练首版从干净账户轨迹起点确定性重放，不从中途模拟器、账户或 Ledger 检查点续跑。
   模拟盘必须能从原规则快照、原子检查点、紧凑仅追加日志和 DPT 仅追加事件日志恢复，且不
   重复撮合、撤单、资金释放、返佣、付款、赎回或反馈。恢复点位于 `end_at` 与合格
   `MarketResolved` 之间时，开放订单仍须保持可查询、可撤、不撮合且资金预留不变；恢复点
   位于判定处理期间时，不得重复写入订单终态、释放未成交余量预留或应用自动赎回。
6. 内存、热市场、输入/请求/反馈队列、活动订单、内部事件、查询缓存、单会话索引、单运行
   资本签发围栏、
   产物，以及整个运行时/产物根的记录数量和字节分别具有实测硬上限。超限时停止新接入并
   明确失败，不静默丢弃、删除未到期证据或无界扩容。
7. 每个正式运行至少绑定进程共享的 `PlatformRuleSnapshot` 及其系列适用策略、
   `PlatformScenario`、公共事件合同/生产器、代码/构建、配置、随机种子和输入谱系，并生成
   `SimulationRunManifest`、仅追加请求/反馈证据、紧凑查询投影、生命周期/保留期报告、资源
   高水位和不变量报告。`SimulationRunManifest` 必须明确绑定规则快照、场景、随机种子、
   `AccountInstanceKeyV1`、`AccountOpeningAnchorV1` 的 ID/SHA、最终会话初始化状态与稳定激活
   身份、创建请求与桥绑定身份、单运行资本签发围栏计数与摘要、初始场景、完整市场登记链的
   链头、长度与 SHA、
   `TradeFinalityPolicyV1` 的 ID/SHA、请求/反馈合同、代理时延配置档的
   `profile_id/version/SHA-256`、采样算法版本与采样域、确定性脚本意图身份、最终
   `account_transition_seq`、账户摘要、各类反馈下一序号、进程内确认水位、下游耐久消费水位
   和原始反馈摘要；其内容身份与 SHA-256
   供 DPT 候选
   清单直接引用。首版 schema 固定为
   `SIMULATION_RUN_MANIFEST_SCHEMA_V1 = "simulation_run_manifest_v1"`；它是 sim-system 自有
   清单的首版，不继承 DPT 运行清单的版本号。阶段 6 使用真实输入时，它必须原样绑定实际
   Gate A 回执、DPT 运行清单、公共事件和双索引的各自版本，不把这些外部版本改写成 V1。
   全部持久产物均绑定 SHA-256 并原子封存。
8. 产物不得包含钱包、凭据、模型张量、训练检查点、类型化原始数据、同级目录中交易侧 Ledger
   状态或完整
   规范事件带副本；确定性核心除已释放的冷启动加载器外必须能在断网、无钱包、无凭据、
   无模型框架和无 GPU 环境运行。
9. 资源测量只使用两类命名负载。`COMPONENT_GOLDEN` 由一个内容寻址
   `PlatformScenario`、`TradeFinalityPolicyV1`、有界黄金 `ExecutionMarketEventV3` 双时间流和
   确定性八方法脚本组成，
   用于阶段 4 组件计量；`REAL_VERTICAL` 只在阶段 6 使用一个成功 Gate A 覆盖的真实范围，
   不循环、复制或按结果缩放真实事件。两类负载都在首次正式计量前冻结输入、脚本、调度、
   随机种子和负载刻度的身份，并由对应 `SimulationRunManifest` 绑定；不另建登记系统。
10. 负载计数单位固定如下：事件同时记录唯一 `event_id` 数、事件时间路径投递数、模型可见
    路径投递数、事件种类和同逻辑时点突发量；同一事件经过两条路径只形成一个唯一事件和两次
    路径投递，模拟器核心接收的 `CanonicalReferenceEventV1` 必须为零。请求分别记录顶层调用、
    八方法调用、批量项和批大小；成交按稳定成交身份记录唯一成交、受影响订单和三类反馈数；
    查询分别记录 `order/orders/trades` 的调用、分页、返回行及热状态或保留投影来源；保留期
    以确定性时钟跨至少两个完整滚动周期，记录市场、会话和记录从热状态到紧凑投影再到过期
    删除的数量和字节高水位。
11. 性能只用单调墙钟计量，不把数据源时间、`received_at_utc_ms`、模拟请求时间或反馈时间
    冒充执行延迟。分别报告事件路径、请求调用/批量项、唯一成交和查询调用/分页/返回行吞吐，
    以及事件应用、请求响应、查询响应、检查点/密封和保留滚动的 p50、p95、p99、最大值与
    整次墙钟时间；同时报告用户/系统 CPU、最大 RSS、cgroup `memory.peak`、swap、
    `memory.events`、文件描述符、读写字节、临时/持久产物字节和第 6 项所列每层内部高水位。
12. 每次正式计量固定并记录发布构建、Git/构建身份、目标平台、主机、操作系统、CPU、内存、
    文件系统、线程数、cgroup 配额、缓存前置、配置、随机种子和输入/脚本 SHA-256。原始逐次
    结果、系统/cgroup 证据、内部高水位和逻辑摘要由运行清单绑定；相同负载重复运行的逻辑
    摘要必须一致，任何丢弃、重复应用、未解释错误或资源事件都使该次结果失败而不是从统计中
    删除。
13. 阶段 6 分栏记录联合适配层的页读取、解析、过滤和扫描资源，以及模拟器核心资源；核心
    容量只按实际接收的市场事件和上述负载单位计算。`typed_raw` 文件行、接收/市场过滤后的
    解码行、唯一事件存储行、参考事件行和双索引行只作分栏来源证据，任何一项都不得代替
    模拟器事件量或容量。阶段 0 不冻结吞吐、延迟、并发、配额、保留期、负载倍率或重复次数
    的数值；阶段 4 在查看正式结果前先密封确切黄金负载，再由实测冻结组件数值，阶段 6 只按
    同一口径真实重新定标且不得改变业务语义。

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
4. 用户已决定在任务 1.2 实现前生成、验证并内容寻址密封首版项目代理时延配置档；不存在
   Gate B 前后两种门位分支。配置档只决定请求出站、直接响应入站和生命周期私有反馈入站的
   代理传输时间；`platform_responded_at` 仍是模拟平台实际完成该请求处理的逻辑时点，不由
   传输配置档凭空生成。配置档不得替代或改写公开市场数据已记录的数据源事件时间、
   `received_at_utc_ms` 或反馈的 `platform_event_at`，也不得宣称为爱尔兰订单私有链路实测、
   纯网络延迟或平台处理延迟。真实配置字节、`profile_id/version/SHA-256` 和全部统计不变量
   完成核验前状态为 `NOT_READY`；完成后状态为 `PROJECT_PROXY_PROFILE_READY`，且正式
   `SimulationRunManifest` 与 DPT Gate B 候选必须绑定同一身份。未来 `sim-calibration` 只能
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
  - 步骤 0.3.1：核对 `reqwest`、`serde/serde_json`、`sha2`、`rust_decimal` 和固定官方 SDK
    的实际锁定版本、特性、许可、安全公告与 Rust 兼容性，并核对尚未引入的 `toml` 候选，
    供目标 crate 创建时精确声明。
    -> 验证：依赖边界满足 2.3 节，不增加第二套 Arrow、数据库、通用客户端或框架。
  - 步骤 0.3.2：用固定 HTTP 响应和临时目录验证查询编码、原始字节哈希、窄 DTO、精确十进制
    与同文件系统原子密封路线。
    -> 验证：技术试验不形成生产实现或第二套规则核心；任何缺口影响和选项在实施前可见。

- 任务 0.4：预登记资源测量
  - 步骤 0.4.1：按 2.8 节冻结组件黄金负载和真实纵向的事件、请求、成交、查询、保留期
    单位及计数边界。
    -> 验证：所有计数可由输入、请求/反馈证据和运行清单重算；来源各层行数与模拟器唯一
    事件、双路径投递和容量分栏记录，参考事件不能进入核心。
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
    -> 验证：历史与恢复入口不创建 HTTP 客户端；共享不可变句柄保持同一对象、内容 SHA、
    观测时点和规则值，后续市场不会触发请求、追加或热刷新。任务 1.2 再用真实会话工厂证明
    每个会话只能接收该共享句柄。

- 任务 1.2：实现隔离会话、逻辑时间和身份
  - 步骤 1.2.1：从固定 2026-08-03 清单机械生成全部消息的整数毫秒经验直方图，核验输入清单
    与 24 个成员身份、样本计数、缺失/负值、最小值、最大值和五个 nearest-rank 分位数，随后
    以 `profile_id/version/SHA-256` 内容寻址密封首版代理配置档。
    -> 验证：配置档逐项重算得到 `39,618,963` 个样本、`9,666` 个严格升序非零频数及
    `10/19/30/63/187ms` 五个分位数；运行时只读并校验密封配置档，不读取 `typed_raw`，也不
    新建通用 `sim-calibration` 写入器或运行时生成路径。
  - 步骤 1.2.2：使进程组合根只初始化一份共享规则快照和一份共享代理时延配置档，把相同
    不可变句柄绑定到每个隔离会话；实现 DPT 平台前缀、公开可见游标、私有反馈序号和单调逻辑
    输入水位，并让批量项最终处理时点集合在平台前缀推进前按方法类型与冻结 `itode` 规则密封。
    -> 验证：跨会话、未来游标、错误版本、重复会话身份、后端切换和晚于已关闭水位到达的输入
    在改状态前拒绝；平台前缀不跨过未密封请求或更早动作，同一有效处理毫秒保持 DPT 原顺序并
    每次只释放一个稳定排序动作；私有事实按闭合接收时间前缀和稳定发射序号释放，公开与私有
    可见水位独立，墙上时钟和物理到达顺序不决定业务结果。前缀闭合接口保持 crate 内不可由
    模型调用；本任务用受信测试驱动器提供“下一条晚于 `t` 或 EOF”的闭桶前提，不预建 DPT
    读取适配器。
  - 步骤 1.2.3：实现请求/直接响应四个时间坐标、生命周期反馈的 `platform_event_at` 与
    `feedback_received_at`、无状态双向经验采样、请求身份、变更幂等和查询平台状态修订号。
    -> 验证：固定种子与稳定身份得到固定样本；物理重投复用原序列、样本、派生时间和查询截点，
    同一身份不同规范载荷失败；新逻辑请求才取得新身份与截点，请求发送时点不倒退且不早于
    所引用的公开或私有事实，变更幂等重试不重复创建对象；
    直接响应和生命周期反馈都只在各自交易侧接收时点后可见，且 `itode` 不进入传输采样。

- 任务 1.3：实现八方法模拟领域入口（已完成）
  - 步骤 1.3.1：实现精确公开名字、八个方法特定领域请求、共享请求上下文、方法特定登记回执
    和合同错误层；每个入口只完成全量前置校验、请求登记和逻辑动作调度，不生成平台直接响应
    或阶段 2 业务结果。
    -> 验证：最小桥接器可调用八方法并取得对应登记回执；合同错误通过 `Result::Err` 在改变
    状态前拒绝，物理重投复用原登记与动作且不重复入队；旧方法名、改单、额外查询、
    `SignedOrder`、模型和 Ledger 对象不可达；登记回执不能表达平台接受、拒绝、未知结果、
    订单、撤单或查询投影，也不增加第九个公开操作。

阶段 1 完成门：规则快照、会话、时间、身份、幂等和八方法入口通过边界验证并完成独立审查；
规则响应最终失败或冲突时能力保持阻断，不得伪造完成。阶段 1 完成只证明八方法登记入口成立，
不证明任何阶段 2 业务结果或方法特定直接响应已经实现。

## 5. 阶段 2：动态影子 CLOB、撮合、撤单和查询

本阶段在任务 1.3 已登记并调度的逻辑请求基础上实现平台业务处理，生成方法特定直接响应，并
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
    -> 验证：外部和账户数量不能互写，完整事件带不被复制到每个会话。

- 任务 2.2：实现准入、提交准备和平台内部暂缓（已完成）
  - 步骤 2.2.1：实现 BUY、四种订单类型、post-only、tick、SDK 成熟编码工具、最小 shares、
    可立即成交限价 BUY 本金门，以及第 2.2 节固定优先级的纯准入判定。
    -> 验证：每个业务分支用最小正例和一个真实可达拒绝边界；`end_at`、显式布尔否决、
    `PRELOADING` 与目标资产非 `Fresh` 的优先级稳定，FOK/FAK 不被推测套用限价单本金门；
    动态 tick `0.00001` 通过公共原语，并证明该原语与原订单构造器共用同一价格校验、截断和
    最终量化路径；GTD 240 秒等号通过、少 1 秒拒绝且实际到期毫秒来自编码秒；FOK/FAK 最终
    6 位量化 `takerAmount` 对 `mos` 等号通过、最小下界拒绝。
  - 步骤 2.2.2：实现单笔和逐项非原子批量的内部 `PreparedSubmissionV1`，复用任务 1.3 已冻结
    的 `itode` 条件化 250ms 有效处理时点，不完成已准入逻辑动作或生成提交成功响应。
    -> 验证：批量边界、混合准入结果和物理重投映射；`itode` true/false 各一条真实路径，
    maker 与 post-only 拒绝不进入暂缓；未暂缓订单在 `end_at` 到达、暂缓订单从边界前跨至
    重新校验时点时均拒绝，且不创建订单、资金预留或成交。

- 任务 2.3：实现初始资金、L2 队列撮合与确认式提交响应（已完成）
  - 步骤 2.3.1：实现内容寻址初始账户、可用 pUSD、提交前资金检查与资金预留，并在业务变化前
    预留有限响应槽位。
    -> 验证：非法精度或负数在会话登记前失败且不占用会话身份；本金或即时 taker 手续费任一
    不足均零副作用，税费前本金与费用之和等额通过且不缩量；批量后处理项读取此前已处理项占用
    或释放后的资金，响应仍按原 ordinal 排列；槽位满时不创建订单、成交或资金变化。
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
  - 步骤 2.3.3：形成两类方法特定三分支响应，并通过有界会话逻辑发件箱和预绑定桥接器完成
    进程内精确接纳确认。
    -> 验证：单笔与混合批量的稳定映射、GTC/GTD base 守恒、FOK/FAK quote 守恒、实际成交
    base 与 SDK 审计投影分离、响应时点和一个请求
    一个响应成立；响应先于公开消息不泄漏隐藏状态；容量 1/2、满载、断开、错误确认、物理重投
    和成功确认均保持同一载荷身份且不重复业务变化。跨 `global_request_id` 的同幂等效果响应
    保留各自处理与响应时点，并复用同一效果身份和首次形成时点，明确标记首次形成或复用；
    外部编译测试继续证明八方法之外没有公开
    响应轮询、取回或确认入口；进程重启不被误报为已恢复。

- 任务 2.4（已完成）：实现撤单、查询、分页和保留期读取
  - 步骤 2.4.1：实现单撤、批撤、全撤与逐目标结果。
    -> 验证：活动、部分成交、pending maker、终态、重复 ID、全撤冻结目标、平台时间边界、
    幂等复用和跨会话各用最小代表；重复 ID 只有一个效果且保留全部输入映射，混合目标不是
    业务全成全败，内部错误没有半提交。模拟器不生成策略性撤单；GTD 到期与同刻撤单按已实现
    顺序处理，已有终态不被撤单覆盖；`end_at` 后、合格 `MarketResolved` 前的撤单仍成功且只
    释放未成交余量预留。由判定生成订单终态及其同刻端到端顺序留在任务 3.5.1 验证，本任务不
    伪造该尚未实现的生产路径。
  - 步骤 2.4.2：实现 `order`、`orders`、`trades` 的截点投影和不透明游标。
    -> 验证：101 行固定页、同毫秒稳定键、严格时间开区间、活动/终态、原始未知状态、空页、
    未找到、精确成交 ID 的已保留正向事实、游标滥用和跨保留期各有代表；第一页后发生撤单、
    成交或状态更新时，旧游标仍读取
    原根且不重不漏，新根才看见变化。盘后未判定的开放订单仍可由 `order` 和 `orders` 查询；
    查询自身不推进业务修订号，查询可见业务提交只原子推进一次，保留水位独立。训练与离线
    会话在隐藏事件已处理、对应公开 `received_at` 尚未到达时，查询仍可返回订单和成交自身事实，
    但公开投影及响应摘要不得包含 DPT 事件 ID、事件计数、来源摘要或其他隐藏事件证据；pending
    maker、maker fill 与 taker fill 各用一个可区分反例验证。订单和成交自身稳定身份继续属于
    可返回事实，但不得附带用于生成或审计该身份的隐藏事件来源字段。

阶段 2 完成门：市场状态、准入、队列、撮合、撤单和查询满足业务合同与守恒，任务自审和
独立阶段审查通过。

## 6. 阶段 3：市场接入、账户经济、平台小额余量、返佣和自动赎回

- 任务 3.0：先建立运行根、公共账户身份、唯一开户锚点和账户变化链骨架
  - 步骤 3.0.1：在任何会话创建前实现 `TradeFinalityPolicyV1` 的规范解码、内容身份、完整路径
    文法校验和运行根密封，并把一个显式 `run_id` 与一份策略绑定到 `SimProcessRoot`。
    -> 验证：缺失/空 `run_id`、缺失策略、摘要冲突、第 2.6 节之外的路径、倒退时点、非法经济
    截点和既有运行根热换策略均在创建会话前失败；同一运行根全部会话只读共享相同 ID/SHA，
    另一 `run_id` 使用另一个运行根且状态完全隔离。
  - 步骤 3.0.2：在 `contracts` 发布第 1.4 节获准的四个根合同及最小共享值类型；在调用方角色
    和平台来源这两个共享表示中，`SessionSpecV2` 只直接接收调用方角色，由运行根 `run_id` 与
    内部固定注入的共享 `SIMULATED` 来源形成 `AccountInstanceKeyV1`；全部会话校验通过后才同步
    返回唯一
    `AccountOpeningAnchorV1` 并登记 `PREPARED_LEDGER_INIT` 账户实例保留；实现第 2.2 节幂等
    创建、激活和初始化失败关闭控制面，只有 `ACTIVE` 会话开放八方法。
    -> 验证：缺失/空或不匹配运行根的 `run_id`、旧 `SessionSpecV1`、调用方覆盖模拟来源、同一
    完整账户实例键重复创建和锚点摘要冲突均在任何状态形成前零副作用失败；不同运行根复用同一
    业务 `account_id` 时得到不同锚点和完全隔离的账户链。锚点与准备会话原子形成，创建重试返回
    同一结果且不进入私有反馈队列或消耗反馈容量；首次成功只交付一个绑定稳定桥身份的独占会话
    句柄，重试不返回第二句柄、不替换桥，创建身份/规格/桥身份冲突零副作用失败；句柄丢失在
    阶段 3 只导致当前运行失败。准备态八方法拒绝，Ledger 成功后的同身份激活
    可幂等重试，结果未知只查询/重试同一锚点，明确失败关闭后同一键不能重开户。锚点恰好一次
    绑定显式初始资金及初始场景，调用方不能另填或在后续市场、日期、文件或批次边界重建本金。
  - 步骤 3.0.3：把阶段 2 已有的订单接受本金/即时手续费占用、FAK 订单类型终止的未花预算释放、
    调用方撤单释放和 GTD 到期释放接入统一 `PlatformAccountDeltaV1`、全局
    `account_transition_seq` 与摘要链。先建立第 2.6 节唯一内部逻辑驱动入口，把阶段 2 的市场事件
    桶和动作处理两处 GTD 直接推进改为只向该入口提交推进请求，禁止保留能够直接改变 GTD 或
    账户状态的旁路；后续最终性、返佣时钟、市场判定和动作只向同一入口注册或提交各自事件。
    同时实现共享有界确认基础设施和承载这些变化的最小 `OrderLifecycleFeedback` 驱动路径。
    -> 验证：首项前驱只绑定开户锚点，此后每项绑定前一变化；状态与必需订单反馈不会部分提交，
    直接响应只能引用而不能复制规范变化，生命周期反馈位于队头时可以正常交付、确认和继续推进。
    两个重叠市场共享余额和序列；两条隔离轨迹即使使用相同配置化初始金额也不共享状态或合并
    资金。某轨迹可用现金归零后，后续市场仍保持同一账户和余额，不关闭会话、不补资；只有新的
    账户实例与开户锚点才可取得新的显式初始资金。源码检索和碰撞测试证明旧 GTD 两处调用不再
    直接推进状态，同一 `t` 的 GTD 只执行一次。

- 任务 3.1：建立同一账户轨迹的仅追加市场接入
  - 步骤 3.1.1：把阶段 2 固定 `MarketSetV1` 作为每个会话的初始前缀，并按第 2.1 节实现会话
    局部、版本化 `MarketEnrollmentV1` 链、同步幂等控制结果及原子状态扩展；只有 `ACTIVE`
    会话可登记。
    -> 验证：paper 在 M0/M1 已运行后登记 M2，M2 看到前两市场资金状态；另一会话的目录完全
    不变。同一请求身份/载荷重试返回原登记 ID/SHA 与链头，身份载荷冲突、用新身份重复
    `condition_id`、准备/失败关闭会话、首个事件或请求后迟到登记、错误系列和摘要链冲突均零
    副作用失败；成功结果返回前 M2 事件/请求不可进入，结果不占私有反馈队列。同一账户没有第二
    次初始化，资金、订单、GTD、私有序号和下一账户序号均不漂移。登记成功后的 M2 事件授权、
    BUY 请求、`end_at` 停止撮合、合格判定关闭和查询均只从会话局部目录发现 M2；源码检索与
    反例证明业务路径不再直接枚举初始 `PlatformScenarioV1.market_set`，M0/M1 行为保持不变。
  - 步骤 3.1.2：把初始场景、开户锚点和登记链身份纳入内存确定性重放上下文。
    -> 验证：training 的预冻结链与 paper 的按接收追加链分别在全新干净会话中重放相同登记、
    市场事件和请求序列，得到相同链头、市场目录、账户摘要和下一全局序号。阶段 3 不写持久
    检查点、不执行进程重启恢复，也不把测试内重新构造会话表述为恢复通过。

- 任务 3.2：实现显式成交最终性策略和账户经济应用
  - 步骤 3.2.1：消费任务 3.0 已密封的策略，实现普通模板实例化、有限预登记故障覆盖和逻辑
    时间调度，不建立第二个加载或校验入口。
    -> 验证：非法或非规范稳定身份、缺少账户实例限定、混用请求/批量项/成交选择域、同域重复
    身份、多重命中和运行结果自适应覆盖均在改变对应成交状态前失败；请求级覆盖作用于该请求
    全部成交，但不能再叠加批量项级或成交级覆盖。两个隔离会话使用相同原始请求/批量项字符串
    时，只能命中策略中账户实例键完全
    一致的那条轨迹；合法但全程未命中的选择器不报错，相关成交走普通模板。相同策略、脚本、
    输入和种子重复运行确定，不等待现实时间，也不读取公共成交、市场判定或代理传输时延生成最终性。
  - 步骤 3.2.2：实现订单撮合/成交最终性分轴、策略指定的唯一经济应用截点、补偿和资金占用
    释放，并把变化接入既有全局账户链与 `TradeLifecycleFeedback` 驱动路径。
    -> 验证：普通成功、待处理且已应用、待处理且未应用、未应用失败与已应用后补偿各用一个
    代表；后继反馈先可见时能识别缺口，前驱到达后按序应用并去重。开放订单未成交余量预留、
    已成交结果代币及其后续兑付分别守恒，重复状态不重复计值；`FAILED` 与必要补偿同一转换完成，
    不出现补偿待处理中间态。另以同刻“已应用失败补偿 + 新 BUY + 另一成交经济截点”反例证明
    统一驱动器先按稳定最终性键完成补偿/应用，再执行新 BUY 资金门；串行、扰动和干净重放的
    准入结果、全局账户序号与摘要相同。

- 任务 3.3：实现确定性 `VENUE_DUST_TERMINAL`
  - 步骤 3.3.1：按本账户所有权、正撮合、严格余量区间、同价队列耗尽和终态优先级判定。
    -> 验证：一个精确账户余量正例、一个外部小量反例、一个边界/所有权失败代表；不使用
    随机数或伪造成交。
  - 步骤 3.3.2：原子关闭精确余量、只释放对应活动资金占用，并复用既有
    `OrderLifecycleFeedback` 驱动路径交付该终态和账户变化。
    -> 验证：成交、份额、费用和现金变化均不被制造，重复驱动与迟到撤单不重复释放。

- 任务 3.4：实现 maker/taker 返佣、日结和付款
  - 步骤 3.4.1：实现 maker 逐笔贡献、逐市场最终化和应收款，taker 实际手续费乘固定 8%
    应收款，并通过 `SettlementLifecycleFeedback` 交付应计事实。
    -> 验证：实际 maker/taker 分段、零合格、失败/补偿，以及两个独立进程中均通过 V2
    合法门的不同历史 `feeSchedule` 各用代表；无效或不受支持规则快照在核心前拒绝。
  - 步骤 3.4.2：实现隔离日桶、正常现金可用时刻、1 pUSD 门槛、四位向下截取、锁定余数和
    付款，并通过 `SettlementLifecycleFeedback` 交付返佣、日结和付款事实。
    -> 验证：低于、等于和高于门槛各一条代表，最终性延迟和普通截尾不提前付款，重复驱动与
    干净重放不重复付款，两类返佣不互相凑门槛。可用现金为零但仍有合格应收款时继续日结和
    付款，不关闭会话、不创建新锚点或补资。正常现金可用时刻先到、最后一笔合法已排定最终性
    后到时，成熟义务只保留一次，并在最后前驱闭合的当前时点完成日结/付款，不追溯提前可用。

- 任务 3.5：实现市场结束、结算屏障和自动赎回
  - 步骤 3.5.1：在合格 `MarketResolved` 到达时，把仍开放订单的未成交余量转为
    `ORDER_STATUS_CANCELED_MARKET_RESOLVED`，并用独立幂等变化只释放其资金预留。
    -> 验证：`end_at` 本身不关闭订单或释放预留；调用方盘后撤单与判定关闭分别只发生一次，
    外部盘口释放不生成账户事实，已成交结果代币不被当作未成交余量释放；订单终态反馈只在
    经过适用返回延迟后对交易侧可见。
  - 步骤 3.5.2：在合格 `MarketResolved` 按数据源事件时间生效且结算屏障满足后原子赎回并
    通过既有 `SettlementLifecycleFeedback` 驱动路径生成唯一赎回反馈。
    -> 验证：获胜、未获胜、零持仓、重复驱动、干净重放和前驱反馈迟到各有代表；订单余量释放与
    结果代币兑付的账户变化身份和顺序可辨，不生成模型可见的中间赎回状态，永久缺失或冲突明确
    失败。以 `fill@1000 -> MarketResolved@4000 -> CONFIRMED/FAILED@6000` 反例证明 4000 时只
    保留判定并关闭订单余量，不赎回、不失败；6000 时先完成最终性及适用补偿，再以同一判定只
    赎回一次。另以配置化约三分钟的正常场景和一个更晚判定反例证明两者都只由真实合格事件
    触发，固定时长本身不增加现金。可用现金为零但仍持有获胜结果代币或待赎回资产时继续处理
    判定和赎回，不关闭
    会话、不创建新锚点或补资。
  - 步骤 3.5.3：对任务 3.0、3.2、3.4 和 3.5 已分别实现的订单、成交和结算反馈执行稳定关联、
    原始状态保留及第 2.8 节有界确认式交付的端到端验收，不在此首次实现共享交付机制。
    -> 验证：三类生命周期不混合，交错反馈的队头不会阻塞后续可交付事实，状态与必需输出不发生
    部分提交，模拟证据不冒充 NOE、Ledger 或真实平台产物。

阶段 3 完成门：运行根与最终性策略先决、公共账户身份、唯一开户锚点、准备/激活/失败关闭、
仅追加市场同步接入、全局同刻顺序、全局账户经济、平台小额余量、返佣、市场结束、自动赎回和
有界反馈合同通过代表性业务与故障测试；同一 `t` 同时包含 GTD、先前排定最终性、返佣时钟、
已封口 DPT 桶和新 BUY 的五类碰撞测试必须证明只有唯一驱动入口、无旧路径旁路且稳定顺序不变；
内存确定性重放通过但不冒充阶段 4 恢复，任务自审和独立阶段审查通过。

## 7. 阶段 4：释放、恢复、产物和资源硬上限

- 任务 4.1：实现市场两阶段释放和有界保留投影准备
  - 步骤 4.1.1：实现 2.8 节单向生命周期、`end_at` 后外部撮合状态释放、合格判定后的订单与
    结算释放，以及输入终态缺失时的内部运行失败封存。
    -> 验证：正常路径在盘后保留开放订单、资金预留和查询/撤单能力，合格判定后才写入订单
    终态并释放对应预留；失败封存不得生成公共市场终态、查询空页、资金释放或赎回。
  - 步骤 4.1.2：形成紧凑索引、幂等记录和保留期水位线，但在阶段 4 持久屏障完成前不物理移除
    热状态或删除旧会话。
    -> 验证：紧凑投影与原热状态查询等价；单独完成任务 4.1 不会越过仅追加证据、反馈、查询和
    幂等记录的持久化前置。

- 任务 4.2：实现持久化、检查点和恢复
  - 步骤 4.2.1：实现仅追加证据、紧凑状态和原子检查点。
    -> 验证：检查点绑定代码、规则、`AccountInstanceKeyV1`、原 `AccountOpeningAnchorV1`、
    会话初始化状态及其稳定创建/激活/失败关闭身份与结果、唯一会话句柄状态、桥绑定身份、
    单运行资本签发围栏摘要、
    初始场景、完整市场登记链、
    `TradeFinalityPolicyV1`、待处理最终性调度、已接纳但尚未闭合的市场判定与成熟返佣义务、
    输入游标、账户摘要、
    下一 `account_transition_seq`、私有反馈下一序号、进程内确认水位、下游耐久消费水位、耐久
    水位之后的稳定载荷摘要及有界待重放输出；任一截断、前驱缺口或摘要不符均拒绝恢复。进程内
    匹配确认不得推进下游耐久消费水位，已获进程内确认的载荷证据也不得因此删除。分别在本地
    原子耐久提交前、提交后但桥交付前、桥确认后但下游耐久水位前注入崩溃：前者不得产生外部
    成功，后两者必须以原身份重放直接响应、三类反馈及适用的开户/市场登记同步结果，不丢状态、
    账户变化或载荷。
  - 步骤 4.2.2：实现模拟盘恢复，训练保持干净起点重放。
    -> 验证：分别从 `end_at` 与合格 `MarketResolved` 之间、判定处理期间恢复；开放订单权限与
    预留不漂移，市场目录、开户锚点、账户摘要和下一全局序号连续，且不重复撮合、最终性推进、
    撤单、订单终态、补偿、资金释放、返佣、付款、赎回或反馈；从最后一个双方可证明的下游耐久
    消费水位之后继续使用原交付身份、摘要和载荷重放，缺少该水位时从开户锚点之后重放，由下游
    幂等消费闭合。桥已经进程内确认但下游随进程崩溃的代表反例不得丢消息，也不宣称跨崩溃物理
    “恰好一次”。另从 Ledger 初始化结果未知的 `PREPARED_LEDGER_INIT`、Ledger 已成功但 sim
    激活响应丢失和 `INITIALIZATION_FAILED_CLOSED` 三个点恢复：分别只允许同锚点查询/重试、
    同身份激活重试和永久拒绝业务，不生成第二个锚点或第二份本金。恢复时只有受信组合根以
    检查点中的同一桥绑定身份完成重接后才能继续交付；桥身份冲突不得恢复，回调对象本身不
    持久化。
  - 步骤 4.2.3：在仅追加证据、查询/幂等记录、反馈耐久水位和原子检查点全部跨过对应屏障后，
    才执行热状态移除及保留期结束后的旧会话最终删除。
    -> 验证：热移除前后查询等价，恢复不依赖已删除热状态；保留期结束后旧会话不可查询且没有
    跨运行永久墓碑，任一持久屏障未满足时不删除；同一运行根的最小资本签发围栏仍拒绝原账户
    实例键重新开户，恢复后也不能取得第二份本金，运行根结束后不影响另一 `run_id`。

- 任务 4.3：生成并原子封存最小产物
  - 步骤 4.3.1：生成 2.8 节运行清单、证据、投影、生命周期、资源和不变量产物。
    -> 验证：逐轨迹的账户实例键、开户锚点、初始场景、市场登记链、最终性策略、精确输入范围、
    请求至最终状态、最终账户序号与反馈闭合水位可追溯，账户摘要链连续，全部产物有内容身份和
    SHA-256。
  - 步骤 4.3.2：执行机密和越界内容审查。
    -> 验证：不含钱包、凭据、模型状态、原始数据、同级目录中交易侧 Ledger 状态或完整事件带副本。

- 任务 4.4：实测并冻结资源预算
  - 步骤 4.4.1：用预登记黄金负载跨至少两个保留期/滚动周期执行性能分析。
    -> 验证：负载至少包含两条身份隔离的并发账户轨迹，每条各含两个重叠市场、跨市场余额滚动、
    最终性调度、账户变化/反馈积压及结算迟到；同一预登记负载串行与并行执行的逐轨迹逻辑摘要
    一致，不合并资金、序号或状态。记录总量及逐轨迹吞吐、延迟、内存和高水位，在查看结果前
    冻结并发刻度；不得把模拟逻辑等待实现为现实休眠。
  - 步骤 4.4.2：用一个代表性配额超限和一个持久写入失败验证失败语义。
    -> 验证：停止受影响范围的新接入并保留失败证据，不删除未到期数据、不静默丢弃或无界增长；
    一条轨迹的局部配额或写入失败不得污染另一条隔离轨迹的账户、目录、输出或完成判定；只有
    明确命中预登记的进程级总上限或共享持久层失败时才可停止全部新接入，既有轨迹状态仍不得
    交叉污染。资本签发围栏按单运行账户实例硬上限计量，不能靠删除旧会话绕过，也不能无界增长。

阶段 4 完成门：释放、保留、恢复、产物和全部资源硬上限已由实测冻结，任务自审和独立阶段
审查通过。

## 8. 阶段 5：组件符合性门

- 任务 5.1：运行完整组件验收
  - 步骤 5.1.1：先运行冷启动 HTTP 合同测试，再在断网、无钱包、无凭据、无模型框架环境
    运行业务、故障、确定性、恢复和资源测试。
    -> 验证：`SIM-AC-001` 至 `SIM-AC-008` 的组件范围均有真实命令、输入与产物；双时间
    黄金序列证明隐藏平台撮合、模型公开数据可见性及响应先到隔离；两个重叠市场共享全局账户
    变化链且没有重复账户；外部真实纵向和训练闭环明确未被替身冒充。
  - 步骤 5.1.2：干净重跑并进行一个不改变逻辑语义的物理调度扰动。
    -> 验证：响应、反馈、查询、最终状态和产物逻辑摘要一致；分歧报告能定位首个原因。

- 任务 5.2：执行边界和过度设计审查
  - 步骤 5.2.1：检查依赖图、公开 API、代码路径和产物。
    -> 验证：2.10 节禁止路径均不存在，且每个抽象和依赖都能追溯到当前首版能力。

- 任务 5.3：封存组件证据
  - 步骤 5.3.1：完成任务自审和独立阶段审查，修复后重复全部受影响验证。
    -> 验证：组件范围没有失败、过滤或未解释缺口。
  - 步骤 5.3.2：原子封存组件运行清单与 SHA-256。
    -> 验证：组件运行清单的内容身份和文件 SHA-256 直接作为 DPT 所消费的组件回执 ID/SHA，
    不另建回执类型或重复清单载荷；证据只声明组件范围，不宣称真实管道，不把项目代理时延
    配置档升级为私有链路实测，也不宣称训练、paper 或 live 生产适用。

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
    不接收 `CanonicalReferenceEventV1`。paper 只允许消费本机已经到达的可见路径。
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
  - 步骤 6.2.2：比较训练回放生产器和模拟盘回放测试生产器。
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
    联合回执、`SimulationRunManifest` 和 DPT Gate B 候选的 ID/SHA。
    -> 验证：没有测试替身或本模块占位实现冒充外部能力；模拟原始证据仍由唯一归属方消费。
    对每条轨迹机械核对 `AccountInstanceKeyV1`、开户锚点 ID/SHA、市场登记链头/SHA、精确输入
    范围、模型决策截止和经济成熟/删失边界在 sim 清单、归一化回执、Ledger 回执及 DPT 候选间
    完全一致；缺失、跨会话拼接或把两个锚点合成一条轨迹时停止。
  - 步骤 7.1.2：机械核对 `SimulationRunManifest` 与 DPT Gate B 候选绑定同一已验证首版项目
    代理时延配置档的 `profile_id/version/SHA-256`、采样算法版本及同一
    `TradeFinalityPolicyV1` ID/SHA。
    -> 验证：配置档不是 `PROJECT_PROXY_PROFILE_READY`、身份失配或缺少绑定时停止；不得换用
    旧配置档、临时固定值、另一最终性策略或未来 `sim-calibration` 产物补门。

- 任务 7.2：执行同一候选的训练闭环轨迹
  - 步骤 7.2.1：以一个干净开户锚点、固定随机种子、至少两个连续或重叠市场且跨 UTC 日但不
    强制完整日的轨迹，驱动 `sim-system -> 外部归一化层 -> TradingLedger(training) -> DPT-B`。
    -> 验证：同一锚点和 `AccountInstanceKeyV1` 跨市场、日期和读取批次连续，只有一份显式初始
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
| `SIM-AC-005` | 3、5、7 | 跨市场全局账户链、成交最终性、资金、返佣、付款、赎回与外部唯一消费 |
| `SIM-AC-006` | 1、3、4、5、6 | 双时间确定性、仅追加市场接入、会话/隐藏状态隔离、恢复、资源硬上限及真实重新定标 |
| `SIM-AC-007` | 4、5、6、7 | 运行清单、摘要链、保留期、不变量和内容身份 |
| `SIM-AC-008` | 0 至 5 | 双时间黄金正例、响应先到隔离、代表性边界、事故回归和独立审查 |
| `SIM-AC-009` | 6 | Gate A、双时间真实代表市场和既定不可变联合清单内容身份/SHA |
| `SIM-AC-010` | 7 | 同一候选外部归一化/Ledger/DPT-B 闭环及配置档门 |

## 12. 当前游标

- 用户已于 2026-08-09 批准需求与本计划；当前执行 authority 为本计划，执行授权为
  `LOCAL_IMPLEMENTATION_ONLY`；
- 用户已于 2026-08-11 批准模拟平台账户与同级目录中交易侧 Ledger 的目录、单向依赖、多市场
  共享账户及配置化 `2000/1000/约三分钟` 场景边界，并只授权本轮三件套文档同步。交易侧
  Ledger 事项归属、草案状态及其内部拟议语义只从 `plan/trading-ledger-plan.md` 读取；阶段 3
  四个共享根合同的当前规范仍由本计划拥有，并以未来 `contracts` 中实际发布版本为跨模块边界。
  外部归一化实现归属仍未决定。
  独立审查进一步确认阶段 2 固定市场集合尚不支持 paper 持续接入新市场，相关
  `MarketEnrollmentV1` 只进入阶段 3 计划。上述决定没有重开阶段 2，也不授权阶段 3 代码、创建
  `ledger` crate、提交或推送；
- 用户随后批准按阶段 3 准入审查的推荐方案和顺序修正文档：公共账户叶子进入 `contracts`，
  显式 `run_id` 与固定模拟来源先形成唯一开户锚点，市场登记后移，成交最终性只由密封的
  `TradeFinalityPolicyV1` 确定；阶段 3 只验证进程内确定性与有界交付，持久恢复仍归阶段 4，
  阶段 4/7 补齐并行与逐轨迹身份门。该批准只授权本轮文档收口和审查，不授权阶段 3 Rust 代码、
  提交或推送，计划标题与当前游标继续保持“阶段 3 尚未获批”；
- 后续两路独立准入复审发现并已在文档内修正运行根先决顺序、同步开户、三类反馈实施归属、
  最终性有限路径文法、全局同刻顺序、准备/激活/失败关闭、市场登记同步结果、持久屏障后淘汰、
  先耐久提交后交付及下游耐久消费水位。上述修正仍只关闭实施前合同矛盾，没有前移任何阶段、
  代码、测试、提交或推送权限；
- 最终代码结构回归又把最终性故障覆盖收敛为单一、账户限定选择域，把阶段 2 两处 GTD 推进和
  后续事件收敛到唯一逻辑驱动入口，并冻结唯一会话句柄/桥绑定、会话局部市场目录、判定先于
  最终性时的内部屏障义务及单运行资本签发围栏。上述仍是尚未获批阶段的实施合同，不代表代码
  已存在、测试已运行或阶段 3 已获授权；
- 当前游标为
  `阶段 2 本地完成门重新通过；提交与推送已获批准，阶段 3 尚未获批`。阶段 1/2 重新审查发现的
  双时间查询泄漏已经完成手术式修复：完整 DPT 来源证据继续只留在内部执行与审计状态，模型
  可见 `order/orders/trades` 投影及其响应摘要不再包含隐藏事件 ID、事件计数、来源摘要或 maker
  来源证据；pending maker、maker fill 与 taker fill 三个反例均明确证明查询平台到达时点早于
  对应公开 `received_at`，同时仍保留订单和成交自身事实。CLOB 官方省略 `itode` 即 `false` 的
  规则冷启动回归也已补齐。最新本地验证为 110 项库测试、2 项时延集成测试、1 项外部 API
  集成测试、8 项文档测试、固定补丁 SDK 的 102 项库测试与 112 项 `order` 回归全部通过；
  `cargo check`、`cargo clippy -D warnings`、`cargo fmt --check`、`git diff --check` 和固定全工作区
  普通回归通过，其中具名性能测试按全局规则明确跳过，不记作通过。修复后的 100 项代表订单
  投影规范编码为 42,853 字节。`gpt-5.6-sol/ultra` 独立只读阶段复审结论为 `PASS`，严重、重要、
  次要问题均为 0；公共面仍只有八个操作，也未前移阶段 3/4 职责。用户已于 2026-08-10 明确
  批准创建阶段 2 提交并推送当前 `sim-system` 分支；该授权不包含进入阶段 3、发布、部署、访问
  钱包或凭据、真实下单或产生真实资金变化。用户已于 2026-08-10 批准两步调用合同：
  任务 1.3 的八方法只校验、登记、调度并返回方法特定登记
  回执，阶段 2
  才生成方法特定直接响应并经 crate 内私有响应可见性链路交付；合同错误走 `Result::Err`，
  合法平台拒绝与证据不足保留在方法特定直接响应中，不增加第九个公开操作；
  任务 1.1 的进程级规则初始化、三种启动入口、共享不可变快照、16 MiB 双边界、原子密封、
  聚焦测试、主代理自审和独立只读审查已经通过；任务 1.2 的代理时延配置档、运行时加载与
  确定性采样、会话隔离、双水位、请求身份、重投、幂等和查询截点原语也已通过任务自审、
  聚焦测试、全工作区回归和独立只读代码审查。任务 1.1 与任务 1.2 的完整差异已由提交
  `eb7a5983d6f6280d3270c86eb03cb8a31a442292` 推送至 `origin/sim-system`；任务 1.3 的八个
  方法特定登记入口、原子请求登记与动作调度、聚焦测试、主代理自审和
  `gpt-5.6-sol/ultra` 独立只读阶段审查已经通过，审查结论为 `PASS`，严重、重要、次要问题
  均为 0。用户已于 2026-08-10 明确批准提交、推送并进入下一阶段；阶段 1 完整差异已由提交
  `80335341cb323576705153447a1cb502a260c34e` 推送至 `origin/sim-system`，独立核验本地与远端
  分歧为 `0/0`。进入任务 2.1 后核实，现有 `SessionSpecV1` 只有不可反解的市场集与场景摘要，
  当前合同也没有可独立证明完整的因果组结束标志，因而不能自行判定预期两项资产首次装载
  完整或推断因果组闭合。用户随后批准推荐合同：进程级注入共享、不可变、内容寻址的
  `PlatformScenarioV1` 与 `MarketSetV1`，每个 `btc-updown-5m` 市场绑定精确市场身份和恰好
  两项资产并核对会话摘要；事件原子消费单位固定为已封口索引毫秒桶，`causal_group_id` 只作
  桶内审计。任务 2.1 的场景注入、精确范围、双时间市场投影、整桶唯一解码与原子提交、
  初始 L2/tick/质量重校准、三个独立交易布尔、`end_at`、合格终态和盘口所有权隔离已经通过
  14 项聚焦测试、完整回归、主代理自审和附加独立只读对抗性审查。用户随后批准第一性原理与
  对抗复审后的分段合同：任务 2.2 按第 2.2 节固定优先级实现同一准入谓词与内部
  `PreparedSubmissionV1`，不完成撮合、订单业务状态或提交成功响应；任务 2.3 在同一有效处理
  时点消费该准备结果。任务 2.2 已实现角色授权平台投影上的固定准入门、四种 BUY 准备结果、
  动态 tick SDK 共享编码、GTD 秒边界、FOK/FAK 最小 shares、`itode` 重新校验、完整在途动作
  绑定和零业务副作用边界，并通过 11 项聚焦测试、SDK 既有订单回归、完整工作区回归和任务
  对抗性自审。任务 2.3 前的对抗审查进一步确认初始 pUSD 与资金预留合同缺口；用户随后批准
  第 2.2 节第 12 项的最小合同：显式内容寻址初始账户、提交前资金检查与预留、两类三分支方法
  特定响应、FOK/FAK 已接受订单类型终态、一个逻辑请求一个完整响应，以及有限会话发件箱和
  预绑定桥接器的进程内精确接纳确认。任务 2.3 开始本地实施后，对抗复核确认原合同尚不能
  唯一决定 FOK/FAK 的报价币预算与基础资产终止投影、逐档撮合量化以及 taker 手续费资金门；
  用户随后批准第 2.2 节第 12 项的最小修正：GTC/GTD 按 base 守恒，FOK/FAK 按 quote
  守恒且不虚构 `canceled_base_quantity`；L2 聚合价位使用明确标记的 6 位合成 maker 段项目
  假设，预演与提交共用累计整数向下量化并禁止零 quote 成交；税费前本金不缩量，接受前同时
  覆盖按价位精确汇总后五位半向上舍入的即时 taker 手续费，任务 2.3 只形成费用占用，最终
  应用仍归阶段 3。实施前反例进一步证明逐次量化 `S-E` 与先量化 `S/E` 会产生不同 FIFO
  数量和年龄；用户明确批准端点累计方案，以 `micro_base(S)-micro_base(E)` 推进六位队列，
  阈值跨越归当次转换，亚微余数只留精确外部盘口，新外部段 quote 仍独立按该段与源价形成。
  后续对抗审查确认，在低动态 tick 下，一个已穿越本账户 maker 的正微份额仍可能产生零微
  quote；用户已批准保守合同：队列和账户叠加层立即扣减并转入每订单有界待累计分桶，不生成
  零 quote fill；跨事件累计为正后合并为一笔 maker 推定成交，撤单、GTD 到期或合格判定关闭
  时转对应既有终态且只释放一次剩余本金，`end_at` 本身不关闭。任务 2.3 已恢复本地实施。
  后续提交响应终审又核实，官方当前文档把 FOK 无法全成和 FAK 完全无成交列为订单处理错误，
  成功下单的原始状态只列 `matched`、`live`、`delayed` 与 `unmatched`。用户已批准官方对齐
  修正：前两者分别形成 `FokNotFullyFilled` 与 `FakNoMatch` 的 `RejectedNoEffect`，不创建
  订单、成交、资金占用或订单 ID；FAK 有正成交时仍为 `Accepted + Matched` 并释放未花 quote。
  该零成交响应决策门已关闭。后续对抗复核又确认两个会改变结果或审计语义的边界：正 base、
  零 quote 的低价外部余量在正式撮合中会阻塞后方价位，但 post-only 准入会跳过它；跨
  `global_request_id` 的同幂等效果复用也只有一个响应项有效处理时点，不能同时表达本次解析
  与首次业务效果形成时点。用户已批准推荐合同：零 quote 外部余量保留在规范外部簿但对账户
  taker 可执行价位扫描透明，post-only 与正式撮合共用可执行流动性定义；响应项保留本次处理
  时点，并另行绑定首次效果形成时点、稳定效果身份和首次形成/复用标记，本次响应元数据仍使用
  本次响应时点。任务 2.3 随后完成本地实施：真实相邻 `public_trade`、
  `LEVEL_DELTA_INFERENCE` 与被抑制完整盘口确认已形成不冒充共同因果的防双算夹具；零 quote
  透明扫描与后续 DPT 减量、双时点三分支与交错批量幂等、GTD 截止时间及费用占用、资金与
  quote/base 守恒、有限发件箱和精确确认均已有可区分错误实现的代表测试。聚焦、静态、SDK
  与固定全工作区回归已通过，主代理任务自审和两轮独立只读对抗性任务审查均为 `PASS`，未
  发现剩余严重、重要或次要问题。
  用户随后批准第一性原理与对抗复审后的任务 2.4 修正版合同：撤单重复平台订单 ID 按唯一
  目标只形成一次效果并保留全部输入映射；批撤与全撤逐目标业务结果不是全成全败，内部状态
  仍以工作副本一次提交防止程序半状态；批撤 1,000 项是面对官方 1,000/3,000 冲突的保守项目
  上限。查询固定使用 100 项项目兼容页、不可变根投影、稳定升序唯一键、严格毫秒开区间、
  查询可见内容修订号和独立保留水位；上述页大小、排序、快照与幂等均明确是项目确定性合同，
  不冒充官方服务器保证。六个后续方法特定响应继续复用统一有限发件箱与私有确认链，不增加
  第九公开操作。实施前对抗审查曾把最终同刻顺序与分阶段实现归属判断为冲突；继续核对任务专属
  验收和事件处理代码后确认二者可以同时成立：任务 2.4 只消费订单活动分桶、推进已实现的 GTD
  到期并且不覆盖已有终态，不读取市场判定建立临时拦截；合格判定生成订单终态、释放开放本金
  及其同刻端到端验证仍唯一归任务 3.5.1。该复核关闭决策门后，任务 2.4 已完成统一八方法
  响应链、单撤/批撤/全撤、订单与成交投影、100 项不可变分页根、稳定游标、查询可见修订号和
  独立保留水位；任务自审、撤单与查询专项审查以及 `gpt-5.6-sol/ultra` 独立只读阶段审查均已
  通过，阶段 2 完成门成立。阶段 3 的结算、返佣与赎回以及阶段 4 的实际淘汰、持久化、恢复和
  资源硬上限均未前移；
  阶段 0 已在提交 `25a2260fae59f5135ae7010b3612522e8f031b0b` 完成提交与推送，提交基线
  与 `origin/sim-system` 一致；任务 0.1 的实施根、权限和边界基线、
  任务 0.2 的公共合同、正式样本和最小异常证据、任务 0.3 的成熟依赖与临时技术试验，以及
  任务 0.4 的资源测量预登记均已通过任务自审与只读复核，正式实施根固定为
  `/home/chillchill/sim-system`；
- 当前固定 SDK 供应商 `src/` 相对官方 0.7.0 共有六个差异文件：五个包含可执行行为补丁，
  一个只修正 `post_orders` 的批量语义注释。其中任务 2.2 新增的
  `clob/order_builder.rs` 差异只提供无状态 BUY 价格校验与数量编码原语，并由既有订单构造器
  复用；`data-collect/docs/current/contracts.md` 已按真实差异最小同步，`data-collect` 的调用
  行为、需求与计划均未改变；
- 阶段 0 全新独立审查结论为 `PASS`，严重、重要、次要问题均为 0；用户已明确授权阶段 0
  提交与推送并同意进入阶段 1。任务 1.1 只建立规则边界和不可变共享句柄，没有预建任务
  1.2、1.3 或阶段 2 行为；
- 已有成功 DPT Gate A 外部前置，当前公共合同为 `GateAReceiptV3`、
  `CanonicalEventRecordV1`、`ExecutionMarketEventV3`、双索引 V2 和
  `MarketObservationCausalShellV3`；该外部前置只供未来阶段 6 机械核对，不能跳过阶段 0 至 5；
- 当前 DPT 公共读取面已有 `PlatformReplayCursor`、`VisibilityIndexCursor` 和
  `ExecutionMarketEventCursor`，但没有现成、对称、受测的模型可见索引解析市场事件游标。
  阶段 6 按 2.4 和 6.1 节使用 sim-system 公共输入边界之前的有界 DPT/联合适配层组合现有
  公开原语；该适配层在真实输入上不满足正确性或资源门时只阻断阶段 6，不反向阻断阶段 0 至 5；
- 阶段 0 开始时尚不存在 `sim-system` crate、模拟器代码、专用 Cargo 依赖、配置或运行产物；
  阶段 0 只记录真实基线、冻结合同、复核工具和预登记资源口径，不预建后续阶段实现；
- 首版项目代理时延配置档的方法、输入范围、抽样语义和 Gate B 前置位置已经用户批准；配置档
  已在内容寻址路径生成并独立重算通过，当前状态为 `PROJECT_PROXY_PROFILE_READY`，身份为
  `btc_updown_5m_collector_observed_delay_20260803_v1 / 1 /
  98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596`。任务 1.2 的 Rust
  加载校验、确定性采样、会话与身份实现、聚焦测试、任务自审和独立只读代码审查已经完成；
  不再存在门位待决定，也不得宣称为爱尔兰私有链路实测；
- 当前授权不包含提交、推送、发布、部署、远端操作、钱包/凭据访问、真实下单或真实资金变化。
