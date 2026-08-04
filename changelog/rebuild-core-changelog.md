# rebuild-core 变更记录

> 事项编号：`rebuild-core-demand`
>
> 生命周期：`DRAFT_REQUIREMENTS_CHANGELOG`
>
> 当前执行 authority：`none`
>
> 对应需求：`demand/rebuild-core-demand.md`
>
> 对应计划：`plan/rebuild-core-plan.md`
>
> `data-pipeline(training)` 决定来源：`demand/data-pipeline-training-demand.md`、
> `plan/data-pipeline-training-plan.md`、`changelog/data-pipeline-training-changelog.md`
>
> `data-collect` 当前权威入口：`data-collect/docs/CURRENT.md`
>
> 执行授权：`none`
>
> 状态：`只记录需求事实、顺序和决策变化；没有代码、数据构建、部署、远端或资金执行`

本文件只记录 `rebuild-core-demand` 的需求演变、跨事项决定和实施状态。它不是
`data-collect` 当前权威文档，也不恢复用户已经删除的旧 rebuild-core 子事项文档。

## 当前需求草案基线

当前 `demand/rebuild-core-demand.md` 仍处于整体审核中，现有内容表达的是目标合同而不是
工作树现状。草案已经明确以下核心边界：

- 正式本地开发和 Rust 发布构建仓库为 `/home/chillchill/btc-updown-marketmaker`；当前
  `/home/chillchill/sim-system` 是文档与分支工作树，不是部署根，也不得把 `data-collect`
  搬入其 `data-pipeline` 子目录。
- 项目范围只支持 Polymarket Global CLOB 的 `btc-updown-5m`，主动动作面为 `BUY-only`。
- training、offline-evaluation、paper、live 共用同一确定性 `data-pipeline` 处理核心、
  Intent Bridge、归一化和 Ledger 合同，但运行实例、状态和产物根隔离。
- 共用 Intent Bridge 的公开方法精确为 `post_order`、`post_orders`、`cancel_order`、
  `cancel_orders`、`cancel_all_orders`、`order`、`orders`、`trades`。模拟后端使用模拟领域
  类型；实盘后端只委托官方 Rust SDK，并只在提交时使用 `SignedOrder`。
- `sim-system` 是 training、offline-evaluation 和 paper 唯一共用的模拟平台；live 不得
  调用或回退到模拟器。
- paper/live 目标程序为纯 Rust 独立二进制程序；paper 包含模拟器，live 不含模拟器、
  Python、PyO3 或离线训练专用 Arrow/Parquet 路径。
- `model-train` 是独立 Python 训练进程，使用 `GymnasiumEnv -> PyO3 -> Rust` 的粗粒度
  进程内闭环；maturin 只负责构建和打包。该选型仍须在实现前按实际解释器、SOABI、
  glibc、Rust 和安全公告重新核实。
- 模型生命周期不可跳级；ModelRegistry、`promotion_stage`、单次资金授权和执行权限是
  四条独立轴，每次真实资金运行仍须用户单独授权。

以上基线没有获得整体需求批准，不构成任何实现或外部执行授权。

## 2026-08-03：纠正跨模块开发顺序

### 用户决定

- 用户指出此前“先完成 `sim-system`，再实现 `data-pipeline(training)`”的顺序有误，
  并批准采用新的主线顺序。
- 用户同时批准修改旧活动文档中的顺序，并开始建立独立的
  `data-pipeline(training)` 需求、计划和变更记录。

### 当前有效顺序

1. 已完成 `data-collect(training)`；
2. 先冻结 `data-pipeline(training)` 需求和共享合同；
3. 先完成“最终化类型化原始数据 -> 规范全序 -> `ExecutionMarketEvent`”门 A；
4. `sim-system` 同时使用黄金/合成事件和真实管道事件开发、验收；
5. 再完成同一 `data-pipeline(training)` 的归一化反馈/Ledger 投影合并、完整 Observation、
   标签、切分、归一化和训练产物门 B；
6. 最后进入 `model-train`。

黄金事件继续负责组件符合性、边界和确定性回归，但不能替代真实纵向链路。完整归一化层
和 Ledger 不是 `sim-system` 组件开发的前置；RewardContract 的实际计算属于
`model-train` 首门，门 B 只验证其输入合同兼容性。当前阶段不在 `sim-system` 中实现原始
文件排序器、第二套管道、`feedback-ingestion` 或 `sim-calibration` 写入链。

上述列表描述正式完成顺序，不把全部 `sim-system` 组件开发串行绑定在门 A 之后。共享
`ExecutionMarketEvent` 与执行桥合同冻结后，不依赖真实事件带的组件、黄金样例和固定符合性
剖面测试可以与门 A 实现并行；只有真实纵向验证及“适用于真实训练”的结论必须等待
`REAL_EXECUTION_TAPE_READY`。缺少 CLOB 最小数量或完整 `feeSchedule` 时，也只阻塞依赖
相应字段的能力和完整正式回执。

### 反向决定来源

本顺序由以下独立 DPT 三件套记录并约束，本文不得另写一套相反顺序：

- `demand/data-pipeline-training-demand.md`
- `plan/data-pipeline-training-plan.md`
- `changelog/data-pipeline-training-changelog.md` 的 2026-08-03 顺序决定及后续修订

## 2026-08-02 至 2026-08-03：补回 maker/taker rebate 首版经济合同

- 用户纠正此前把返佣排除在 `sim-system` 首版之外的遗漏；maker rebate 与 taker rebate
  都是首版必须模拟的经济结果。
- maker 按撮合时逐市场官方 `feeSchedule` 和最终成功 maker 成交计算；taker 按实际收取的
  taker 手续费乘首版固定 Silver `8%` 计算，但不累计/校验 wV，不实现动态等级或一次性奖金。
- 两类返佣分别进入不可支出的应收款与独立 UTC 日桶，正常现金可用时刻为 D+1 taker
  `00:10 UTC`、maker `00:45 UTC`；内部最终性未闭合时不提前日结。
- 同类日桶低于 `1 pUSD` 不付款且不跨日凑门槛；达到门槛时向下截取四位小数，低于门槛
  金额和截取余数继续锁定且不参与后续日期门槛。
- 应计只产生一次 NAV/PnL 经济价值，付款只把同一价值从应收款转为现金，不得重复产生
  NAV、PnL 或 RewardContract 奖励。

当前完整 `feeSchedule` 仍存在上游数据合同缺口；上述需求修订不授权硬编码、原始 REST 或
影子 SDK，受影响真实接入继续服从下文决策门。

## 2026-08-04：冻结数据资格下限并撤销旧时延配置档权威

### 用户决定

- 当前训练、开发、校准和评估数据只允许谱系源自
  `/home/chillchill/new-datasets`，且 UTC 日不早于 `2026-07-30`。
- 更早采集数据不读取、不迁移、不回填、不统计、不拟合、不标准化、不训练、不校准、
  不评估，也不能因缺少当前字段而建立旧版兼容路径。
- 日期达到下限只表示有资格接受后续清单审查，不自动获得训练、开发、校准、验证或留出集
  分配；仍须显式清单准入和版本化切分。

### 时延配置档影响

- 先前基于 `2026-07-10` 数据计算的爱尔兰入站观测经验分布失去当前配置档权威。
- 新正式配置档只能使用合格日期、清单准入且最终化的数据重新计算，并绑定数据谱系、
  日期/范围、输入清单和文件 SHA-256、schema、过滤与逐消息加权口径、主机/区域/端点、
  样本统计、时钟同步证据、长尾政策、生成代码、`profile_id/version/hash`。
- 在重新计算、计划门禁和独立复核全部完成前，正式配置档状态为 `NOT_READY`；training、
  offline-evaluation 和 paper 不得把旧分布或测试夹具当作正式配置档。
- 明确标识的模块级黄金/符合性测试可以使用固定合成配置档，但不得冒充正式运行证据。
- `sim-calibration` 是正式配置档的唯一写入方；`sim-system` 组件或总集成计划的模拟器阶段
  不得直接重新计算、校准或发布配置档。用户仍须在进入受影响的 DPT 门 B 前决定：正式
  配置档回执位于门 B 前，还是采用推荐位置放在门 B 后、正式 `model-train` 前。该决定不
  阻断 DPT 门 A、模拟器黄金组件或真实事件纵向验证，但任一方案都在 `model-train` 前阻断。
- 当前冻结的双向使用仍只是项目代理假设，不是实测订单出站、反馈通道、RTT、平台处理或
  撮合延迟；固定 250ms taker 暂缓是独立平台业务规则，不能被配置档修改或重复叠加。

上述决定与 DPT changelog 的 2026-08-04 数据资格和时延修订互为引用；任何后续变更必须
同步两套三件套，不能只改一处。

## 2026-08-04：冻结单一市场范围和固定 taker 暂缓

- 用户明确整个项目只支持 Polymarket Global CLOB `btc-updown-5m`，不扩展体育、赛事或
  其他市场类型。
- 此决定取代把 `itode`/秒级延迟经 data-collect、data-pipeline 和
  `ExecutionMarketEvent` 动态传播给模拟器的通用化方案。
- `sim-system` 对适用的可立即成交订单只使用集中代码常量
  `BTC_UPDOWN_5M_TAKER_HOLD_MS=250`；它不是配置档或校准参数，真实 live 不插入本地
  250ms 等待。
- 若官方规则或真实产物以后与该项目假设冲突，必须停止受影响新会话并由用户决定新版本，
  不能静默修改常量或恢复通用化路径。

## 2026-08-04：修订三件套批准语义

### 当前有效语义

- 在需求整体批准前，允许为三件套对齐创建明确标为 `authority=none` 的 plan 和 changelog
  草案；草案本身不产生实施游标。
- 需求批准只冻结需求范围；需求与对应计划均获批后，计划明确范围内的本地代码、测试、
  本地不可变测试产物、成熟依赖的本地选择/锁定/构建安装，以及阶段独立审查通过后的本地
  Git 提交取得实施游标，不再要求第三次通用“实施授权”。
- 上述语义不扩大授权范围。未受信外部脚本、特权/远端安装、联网/凭据例外、Git 推送或
  外部发布、远端状态变更、部署、读取敏感材料、真实下单、真实资金、SDK 旁路、返回
  `data-collect` 或创建明确要求独立三件套的未来模块，仍须各自的批准或用户决定。
- 计划实施中若发现需求、计划、代码、数据或当前权威文档不能同时成立，必须停止并记录
  精确冲突，不能用通用实施游标自行选择采信一方。

### 本次文档处置

- 创建 `plan/rebuild-core-plan.md`，按阶段、任务、步骤、逐步验证和阶段完成门覆盖需求全部
  主要板块，并建立 19 个 P0 门禁与 34 项最终验收映射。
- 计划初稿曾把正式配置档复算放进 `sim-system` 阶段，并使该阶段完成依赖正式配置档，
  与 `sim-calibration` 唯一写入权和“门 B 后、model-train 前”推荐时序形成循环；现已修为
  模拟器阶段只验证固定符合性配置档，正式复算由独立 `sim-calibration` 按用户所选门位在
  DPT 门 B 前或后关闭，两条路径都在 `model-train` 前汇合。
- 计划初稿还曾让 DPT 门 A 在签发前依赖真实事件经 `sim-system` 公共入口消费，形成下游
  倒置前置；现已改为门 A 只用框架中立解码器、schema 校验器和黄金文件验证公共消费合同，
  真实 `sim-system` 消费只在步骤 3.6.3 验收。
- 创建本文件 `changelog/rebuild-core-changelog.md`，补齐 `rebuild-core-demand` 三件套的
  变更记录位置。
- 需求、计划和本记录均使用事项编号 `rebuild-core-demand`，相互引用对应
  demand/plan/changelog，并显式标记 `authority=none`、`执行授权=none`。

本次只补齐审核材料，不代表需求、计划或任一实现阶段已经批准。

## 2026-08-04：低频平台规则冷启动与存储责任纠偏

- 用户确认每个新 `sim-system` 运行在首个业务请求前，通过官方 Rust SDK 按有界目标
  `condition_id` 集合加载一次低频平台规则，交叉校验后密封为不可变
  `PlatformRuleSnapshot`。同一运行恢复复用原快照，运行中不轮询或热刷新；快照外新市场
  必须创建新运行。
- 目标快照分为 `queried_market_rules`、`documented_platform_rules`、
  `project_account_rules` 和 `provenance`。仓库目标结构增加
  `sim-system/config/platform-rules-v1.toml`；每个运行生成
  `platform-rule-snapshot.json`。动态 `tick_size_change` 继续由
  `ExecutionMarketEvent` 推进。
- 当前官方 Rust SDK 能读出 `mos/mts/nr/fd.r/e/to` 和 Gamma `feesEnabled`，但
  `mos/nr/fd.r/e/to` 的默认反序列化无法证明字段存在；SDK 也缺少 `itode`、`oas`、
  `feeSchedule.rebateRate`。首版必需字段的存在性与 `itode`、`feeSchedule.rebateRate` 取值
  是真实 SDK 阻断，未消费的 `oas` 只登记覆盖缺口。本决定不授权 REST、手动 JSON、影子 SDK
  或硬编码回退。
- `Client::version()` 会缓存首次成功结果，因此每个新运行必须新建不跨运行复用的 CLOB/Gamma
  客户端；每个市场只接受一个成功逻辑结果，成功后不重查，同一运行恢复不联网。
- 四日合格数据的 491 个 Parquet 合计 9,202,902,694 字节；当前 v5 没有规则专用列。删除
  Gamma 原始证据 JSON 中十个同名或相似键的反事实压缩收益约 9,317 字节，仅占总数据约
  0.000101%，还会破坏证据摘要，因此 `data-collect` 不增列、不删键、不重写数据。
- DPT Gate A 不再因低频规则缺失降级；事件带只负责精确市场身份、动态市场状态和
  `tick_size_change`。本次只修改审核中文档，没有代码、schema、数据、远端或部署变化。

## 当前决策门与已解决项

### 1. 已解决：低频平台规则归属

用户在 2026-08-04 明确决定：CLOB `min_order_size`、完整 `feeSchedule`、`itode` 等低频
规则不归 `data-collect` 或 `ExecutionMarketEvent`；每个新 `sim-system` 运行通过官方 Rust
SDK 按目标市场执行一次冷启动加载并冻结快照。首版未消费的 `oas` 也不进入事件带，只登记
覆盖缺口。当前采集 schema 不增列，Gamma 原始证据 JSON 不删键。此前“返回
`data-collect` 建三件套补齐”的决策门已撤销。

### 2. 官方 SDK 覆盖缺口

当前固定版及官方主线 Rust SDK 尚未类型化暴露 `itode`、`oas` 和
`feeSchedule.rebateRate`，并且无法审计 `mos/nr/fd.r/e/to` 的字段存在性。首版所需字段
存在性与 `itode`、`feeSchedule.rebateRate` 取值使受影响加载器保持阻断；未消费的 `oas`
只登记覆盖缺口。实现前必须列明官方 SDK 更新或受审补丁选项并取得用户批准，不得使用原始
REST、手动 JSON、影子 SDK 或网页值旁路。实盘八方法、认证、签名或其他 SDK 缺口仍适用
同一停止门。

### 3. paper/live 真实数据适配器

当前只有 `data-collect-training-collector` 连接真实外部源，paper/live 程序只有心跳/状态。
paper/live 真实适配器不属于 `model-train`；必须先完成 model-train，之后仍只有在用户决定
返回 `data-collect` 并批准对应三件套后才能实施。目标二进制、路径和进程边界不能被写成
当前已实现事实或部署授权。

### 4. 未来独立角色

`data-pipeline/online`、`offline-evaluation` 和 `sim-calibration` 是已知的未来独立交付边界。
各自三件套获批前不得创建空 crate、占位实现或写入器。`sim-calibration` 才能成为模拟器版本
和校准产物唯一写入器；当前 sim 首版不得实现其 CLI、切分或发布链。

### 5. Python/Rust 环境与进程拓扑

当前需求把 PyO3/maturin 作为目标组合，并记录了需求起草时的候选版本快照。实现前必须用
官方发布说明和安全公告重新核实，并在本地及获准训练服务器完成解释器、SOABI、体系结构、
glibc、Rust 和 wheel 安装矩阵。若矩阵要求升级环境、改变版本策略或改成进程隔离方案，
必须先报告具体缺口并由用户决定；不得临时降级为纯 Python 影子实现。

### 6. 真实资金与外部动作

small-live 1、small-live 3 和 formal-live 1h 每次都是独立的一次性真实资金运行，必须分别
取得用户对本次运行的明确授权。任何需求/计划批准、模型 `active`、前一阶段 PASS 或钱包
文件存在，都不能替代单次 `UserFundsAuthorizationReceipt`、运行准入决策和最新授权状态。
部署、远端同步、服务启停、Git 推送、外部发布和敏感材料访问也不由本草案授权；需求与
计划将来获批后，阶段独立审查通过的本地 Git 提交不需要第三次通用授权。

## 当前未实施事实

截至 2026-08-04 本记录创建时：

- 没有在正式仓库创建或修改任何 rebuild-core Rust/Python 实现；
- 没有创建目标 Cargo crate、Python native 扩展、paper/live 主程序或未来模块占位；
- 没有读取或构建正式训练数据，没有重算爱尔兰延迟配置档；
- 没有运行模拟器、归一化层、Ledger、模型训练、offline-evaluation、paper 或 live；
- 没有实现、运行或发布 `sim-calibration`、`feedback-ingestion` 或 Promotion Controller；
- 没有修改当前 `data-collect` 生产实现、schema、服务、数据或部署；为保持文档路由和草案
  状态可机械验证，本轮同步修改了文档权威说明及其 Rust 契约测试；
- 没有安装依赖、同步远端、构建/部署远端产物、提交、推送、访问钱包、真实下单或使用资金；
- 本轮动作限于补齐三套无权威审核文档、修正文档路由与一致性问题，并同步文档权威契约
  测试；没有取得或执行任何业务实现游标。

下一步只允许用户审核三件套。若用户修改需求或批准范围，必须先在本文件记录决定并同步
计划，再判断任何阶段是否取得实施游标。

## 2026-08-04：官方 HTTPS 只读规则加载器取代冷启动 SDK-only 决策

本节只取代上文“低频平台规则冷启动与存储责任纠偏”和“当前决策门与已解决项”中有关
`sim-system` 冷启动必须经官方 Rust SDK、且 SDK 字段覆盖缺口阻断实现的决定。旧段落作为
当时的审查历史保留，不再提供当前执行语义；实盘交易 SDK-first、`data-collect` SDK 边界、
DPT 离线边界以及其余未实施、未授权状态不变。

- 每个新 `sim-system` 运行必须在首个业务请求前，按已经冻结的有界目标市场集合创建一个
  不跨运行复用的窄型、只读、无凭据官方 HTTPS 规则加载器。加载器逐个访问 CLOB
  `GET /clob-markets/{condition_id}`；访问 Gamma `GET /markets/keyset` 时每批不超过 100 项，
  使用重复 `condition_ids` 查询参数，并分别查询 `closed=false` 与 `closed=true`。
- 每份官方响应必须先按原始响应字节计算 SHA-256，再以专用窄 DTO 解析；不得使用
  `serde_json::Value` 任意取值。DTO 忽略未知附加字段且禁止 `deny_unknown_fields`；规则数值
  使用精确十进制解析并禁止先进入 `f64`。除官方明确允许 `itode=false` 时省略字段、费用禁用时省略费用
  对象外，字段缺失必须与真实 `0/false` 区分；合并 Gamma 两类响应后，每个目标在每个来源
  只能有一个成功逻辑结果。condition、代币和结果必须一致；CLOB `mos/mts`、Gamma `negRisk`
  与完整 `feeSchedule` 分别作为规则来源，费用启用时核对 CLOB `fd` 的重叠字段。Gamma
  `orderMinSize`/`orderPriceMinTickSize` 与响应自然含有的 CLOB `nr` 只作诊断，不构成硬相等门。
  必需语义缺失、非法、重复、查询失败或跨端点冲突继续使受影响的新运行在
  首个业务请求前失败即关闭。
- 详细字段、条件省略和负向测试合同只由 `demand/sim-system-demand.md` 第 10.1 节及
  `plan/sim-system-plan.md` 任务 1.0 定义；rebuild-core 只复用该合同并完成总集成证据，不另立
  第二套响应 DTO、精度规则或失败门。
- 独立终审确认无效规则快照若被冷启动门拒绝，就不能再进入确定性核心并降级为返佣
  `UNKNOWN_INCOMPLETE`。当前合同只在规则完整有效、但异常输入终态使潜在合格成交的运行时
  经济最终性永远无法闭合时使用该状态；规则缺失、非法或冲突始终在会话/恢复入口前拒绝。
- 全部目标校验通过后才可原子落盘并密封 `PlatformRuleSnapshot`。快照必须记录目标市场集合、
  端点与查询形状、查询时刻、每份原始响应 SHA-256、窄 DTO schema、规范响应摘要、静态配置
  和最终 SHA-256；随后释放加载器与网络能力。运行中不得轮询或热刷新，同一运行恢复必须在
  断网条件下复用原快照，快照外市场必须创建新运行。
- 官方 Rust SDK 尚未类型化覆盖 `itode`、`feeSchedule.rebateRate` 或若干字段存在性仍是技术
  事实，但不再阻断上述只读加载器；未消费的 `oas` 只登记官方响应覆盖情况。该例外只属于
  `sim-system` 冷启动，不授权通用 REST 客户端、手工签名、影子交易 SDK、网页值回退或任何
  交易请求，也不得进入 `data-collect`、DPT 或 live 后端。
- live 的提交、撤单、查询、认证、签名和响应解析仍只经官方 Polymarket Rust SDK；
  `data-collect` 的既有 SDK/协议边界、当前 schema 和原始证据不变，DPT Gate A 继续保持离线
  消费边界。本次只同步需求、计划和变更记录，没有授予代码实施、依赖安装、联网运行、远端
  变更、部署、钱包访问、真实下单、Git 提交或推送权限。

## 2026-08-04：总集成合同同步 tick 上游权威和历史规则证据

本节取代此前把 CLOB `mts` 归入模拟器冷启动规则的决定；旧段落只保留历史事实。

- tick 唯一由 `data-collect -> data-pipeline -> ExecutionMarketEvent -> sim-system` 推进。
  初始值来自采集时点证据，动态变化来自市场事件；CLOB `mts` 与 Gamma
  `orderPriceMinTickSize` 均不得进入 `PlatformRuleSnapshot` 或一致性门。
- 当前时点新运行可以形成 `CURRENT_QUERY` 规则快照；历史 training/offline-evaluation 只能
  绑定适用时点合法、来源不可变的 `HISTORICAL_AS_OF` 同 schema 快照，不能查询当前平台
  回填历史。缺少必需历史规则只阻断受影响的正式历史运行。
- DPT 公共事件合同同步增加逐资产初始 tick、允许旧值未知的动态 tick、三个独立市场交易
  状态和来源可辨的公开成交身份；不新增第二套状态机、规则引擎、采集 schema 或网络路径。
- 初始 L2 载荷只表示首次装载，不再携带初始/重同步用途分支；后续完整 `book` 只形成确认或
  `BOOK_RESYNC_DELTA_INFERENCE` 净差额转换，避免总集成中出现第二条完整盘口应用路径。
- 固定 BTC 二元市场不再被要求使用非 1/0 兑付向量；250ms 平台暂缓的 `delayed` 可见性按
  冻结情景和真实响应证据表达，不做无证据的绝对断言。
- 本次只是审核中总集成文档同步，不授予 rebuild-core 代码、数据构建、远端、部署、提交、
  推送、钱包或真实资金权限。
