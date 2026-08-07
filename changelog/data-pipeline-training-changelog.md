# data-pipeline(training) 变更记录

> 事项编号：`DATA-PIPELINE-TRAINING-20260803`
>
> 生命周期：`IMPLEMENTATION_CHANGELOG`
>
> 当前执行 authority：`none`
>
> 对应需求：`demand/data-pipeline-training-demand.md`
>
> 对应计划：`plan/data-pipeline-training-plan.md`
>
> `data-collect` 当前权威入口：`data-collect/docs/CURRENT.md`
>
> 执行授权：`LOCAL_IMPLEMENTATION_ONLY`
>
> 状态：`仅记录当时事实与后续取代关系`

本文件不承担当前业务或技术权威，也不维护当前执行游标。当前规范和游标只在
`plan/data-pipeline-training-plan.md`；以下旧状态、方案、命令和测试结果只作为当时事实，
发生新决策时由明确的取代关系解释，不得据此恢复旧语义。

## 2026-08-03：创建独立需求草案

### 用户决定

- 用户确认此前“`data-collect(training) -> sim-system -> data-pipeline(training)
  -> model-train`”的顺序有误。
- 用户批准采用新顺序、修改旧活动文档中的顺序，并开始撰写独立
  `data-pipeline(training)` 需求草案。
- 新顺序要求在 `sim-system` 前先冻结管道合同并建立真实类型化原始数据到
  `ExecutionMarketEvent` 的最小纵向链路；模拟器以黄金事件和真实管道事件共同验证；
  完整 Observation、反馈/Ledger 投影、标签、切分和归一化在模拟器之后闭合，再进入
  `model-train`。

### 分层决策

- 将同一 `data-pipeline(training)` 定义为两个交付门：
  - 门 A：只有全部必需能力通过才签发 `REAL_EXECUTION_TAPE_READY`；部分真实投影只产生
    `REAL_EXECUTION_TAPE_PARTIAL` 诊断，不能满足门 A；
  - 门 B：`TRAINING_DATASET_READY`，负责完整训练数据闭环。
- 两门必须复用同一个唯一语义规范化核心。门 A 不能冒充整个模块或训练数据集完成，
  门 B 不能重新解释门 A 已发布的规范事件。
- 黄金事件仍用于 `sim-system` 组件符合性、边界、故障和确定性回归，但不能替代真实
  “最终化 typed_raw -> 规范全序 -> ExecutionMarketEvent -> sim-system”纵向验收。
- 完整归一化层、Ledger 和 RewardContract 不作为模拟器组件开发前置；门 B 未通过则
  阻断进入 `model-train`。

### 已核实证据

- 只读核实 2026-07-30、2026-07-31、2026-08-01 和 2026-08-02 四日样本清单：均为
  `data_collect_daily_manifest_v3`、`state=closed`、
  `closure_reason=utc_day_barrier` 和覆盖状态 `ended_known`。
- 四个清单合计声明 491 个最终化 Parquet、151,056,246 行、9,202,902,694 字节；本次
  起草未逐一重算 491 个文件的内容 SHA/行数/字节，不能把清单声明冒充门 A 文件准入已通过。
- 2026-07-31 清单有 6 条 `gaps` 且 `reactivated_within_day=true`；它只作为缺口、重启和
  隔离负例，不得因 `closed` 被冒充为业务完整训练日，也不得从自由文本推造精确缺口时段。
- 当前六个新写入 schema 为 `polymarket_orderbook_v3`、
  `polymarket_market_metadata_v5`、`polymarket_rtds_chainlink_btc_usd_v4`、
  `binance_depth_snapshot_v3`、`binance_depth_update_v4` 和 `binance_aggtrade_v7`。

### 已确认上游缺口

- metadata v5 schema 没有专用 `min_size` 或 `fee_schedule_raw` 列；其
  `source_evidence_payload_json` 是官方 SDK Gamma `Market` 的再序列化，在上游提供时
  可以包含 Gamma `orderMinSize` 和旧手续费相关键。旧 `min_size` /
  `fee_schedule_raw` 只属于旧 metadata 布局或发现阶段内存结构。
- Gamma `orderMinSize` 不是 CLOB `/book.min_order_size`，不能改名为
  `clob_min_order_size_shares`；当前样本也没有完整官方
  `feeSchedule(rate, exponent, takerOnly, rebateRate)`，旧手续费键不能替代。
- 进一步按固定版官方 Rust SDK 与当前官方 API 复核：CLOB `OrderBookSummaryResponse` /
  `ClobMarketInfoResponse` 已能提供真正的 `min_order_size`，因此该字段只需要获批的
  `data-collect` 变更复用现有 SDK 能力；官方 Markets API 已公开完整 `feeSchedule`，但
  当前 SDK Gamma `Market` 和 CLOB `FeeDetails` 仍缺完整对象/`rebateRate`，只有这一部分
  需要后续精确的 SDK 缺口决定。
- RTDS v4 没有独立采集器接纳时间/序号；必须降低该源的排序/因果证据质量。
- 公共 L2 没有逐订单身份和成交/价位变化共同因果 ID；不能恢复真实 FIFO、精确账户成交
  或凭相邻顺序声称精确归因。
- 当前样本没有账户提交/撤单/`user-channel`/Ledger 证据，不能生成执行反馈标签或校准
  全部模拟平台行为。

上述缺口不得由管道联网补查、REST 旁路、手动签名、自建 SDK、硬编码或字段改名修补。
不依赖缺失字段的真实 L2/成交/参考价/判定/排序/去重/资源验证可以先推进，但门 A 回执
必须缩小能力范围；完整字段若仍是门 A 前置，需由用户批准新的 `data-collect` 三件套。

### 文档处置

- 新建：
  - `demand/data-pipeline-training-demand.md`
  - `plan/data-pipeline-training-plan.md`
  - `changelog/data-pipeline-training-changelog.md`
- 三件套统一使用 `DATA-PIPELINE-TRAINING-20260803`，当前均为无执行权威、无执行授权的
  审核材料。计划包含完整步骤和验收映射，但全部实施任务在用户批准需求并另行授权前
  保持 `BLOCKED`。
  这是 2026-08-03 当时的治理表述；当前批准门语义已由下方 2026-08-04“批准门修订”取代，
  不再要求计划批准之后的第三次通用实施授权。
- 同步修改当前活动文档中的旧顺序；保留 2026-08-02 旧顺序作为历史证据，以本节记录的
  2026-08-03 决定明确取代，不追溯改写当时的过程记录。
- 新三件套提及 `data-collect`，因此纳入 `data-collect` 文档权威守卫的显式“无权威跨模块
  草案/记录”分类；这不改变 `data-collect` 当前合同、完成状态或运行时。

### 当前未执行事项

截至本记录创建时：

- 没有创建或修改 `data-pipeline` 实现；
- 没有运行正式数据构建、模型训练或模拟器集成；
- 没有修改 schema、采集器或任何远端状态；
- 没有部署、提交或推送授权；
- 下一步仅为用户审核独立需求草案。

## 2026-08-03：独立审查后的草案修订

- 消除门 A 与 `sim-system` 的顺序循环：门 A 只到规范事件带、
  `ExecutionMarketEvent`、市场侧 Observation 因果外壳及独立合同/能力验证；新增
  `DPT-AC-019`，把门 A 通过后的 `sim-system` 真实管道事件验收明确为独立下游门。
- 收紧回执语义：只有全部必需能力通过且 `gate_verdict=PASS` 才能签发
  `REAL_EXECUTION_TAPE_READY`；部分真实投影只能产生
  `gate_verdict=PARTIAL` / `REAL_EXECUTION_TAPE_PARTIAL`，不能满足门 A。
- 明确门 B 完成训练数据底座、动态回放接口和反馈闭环能力，而不是预生成未来模型动作；
  门 B 以冻结的确定性脚本意图验收真实
  `sim-system -> 外部归一化层 -> training Ledger -> 只读投影` 闭环。
- 修正 RewardContract 边界：训练闭环门只验证 Ledger/Observation/训练产物对
  RewardContract 输入合同的兼容性；RewardContract 实际计算由 `model-train` 首门验收，
  不再作为进入 `model-train` 前的循环条件。
- 明确 `tick_stream` 只是审计、派生特征、统计、兼容分析或回放索引，不能生成模型
  决策点；权威触发/调度证据独立保留在回放上下文。
- 同步修正 `sim-system` 数据流图为
  `Ledger -> data-pipeline(training)-B -> 完整 Observation -> model-train`，不再让
  Ledger 绕过处理核心直达训练模块。
- 按当前 Rust schema 纠正 metadata v5 字段事实：v5 只有
  `source_evidence_payload_json` 承载 SDK Gamma `Market` 再序列化证据，没有专用
  `min_size` / `fee_schedule_raw` 列；旧布局/内存字段不得冒充当前类型化列。
- 收紧文档治理守卫：三套跨模块三件套必须逐份精确声明首屏事项编号、生命周期、
  `当前执行 authority`、`执行授权`、状态和对应文档链接；不能凭正文其他位置出现正确
  词或路径而误通过，并以逐字段篡改负例证明守卫会拒绝错误首屏。
- 修正后的独立终审结果为 `Critical 0 / Important 0 / Minor 0`；文档权威测试
  `4 passed; 0 failed`、部署说明测试 `6 passed; 0 failed`，Rust 格式检查和
  `git diff --check` 均通过。
- 上述修订仍只改变审核中文档和治理守卫，不代表任何实现、数据构建或部署已发生。

## 2026-08-04：冻结数据下限并修正批准门和计划闭环

### 用户决定

- 用户批准修复不需要再次作业务决定或授予实施权的文档问题；该授权只覆盖文档修订，
  不等于 DPT 需求整体获批、计划获批、代码实施、正式数据构建、远端操作、部署、提交、
  推送或发布授权。
- 用户冻结训练数据资格：只使用谱系源自 `/home/chillchill/new-datasets` 且 UTC 日不早于
  `2026-07-30` 的数据；更早采集数据全部不使用。

### 数据资格和时延配置档修订

- 新增 `dataset_lineage_root=/home/chillchill/new-datasets` 和
  `dataset_date_floor=2026-07-30`。日期下限前输入固定为
  `INELIGIBLE_BEFORE_DATASET_FLOOR`，不得读取、迁移、回填、统计、拟合、标准化、训练、
  校准或评估，也不能降级为 PARTIAL；达到下限但未分配的日期保持 `UNASSIGNED`。
- 2026-07-30 至 2026-08-02 继续作为首批已核实验收基线，不是永久输入全集，也不是独立
  未见留出集。后续合格日期仍须显式清单准入和版本化切分，不要求逐日人工批准。
- 撤销以 2026-07-10 数据计算的爱尔兰延迟配置档的当前权威。正式配置档只能用日期合格
  数据重新计算，并绑定输入、口径、时钟证据、代码和 `profile_id/version/hash`；复算和
  独立复核前状态为 `NOT_READY`。固定合成配置档只可用于明确标识的模块符合性测试。
- 正式 `sim-system` 纵向验证只消费被合格门 A 回执覆盖的 `ExecutionMarketEvent`；模拟器
  不直接扫描类型化原始数据、判断日期或扩大管道输入。

### 合同和命名修订

- DPT 局部真实事件带评估字段改为 `tape_assessment=PASS|PARTIAL|FAIL`；`PARTIAL` 只产生
  `REAL_EXECUTION_TAPE_PARTIAL` 诊断。`rebuild-core` 的跨范围 `gate_verdict` 保持不变，
  本事项不定义两者映射。
- `confirmed-gap` 文档漂移统一改为精确字段 `confirmed_gap`。
- 增加 `itode` 负向合同：原始证据中的同名键可以无损保留，但
  `ExecutionMarketEvent` 和 Observation 不输出 `itode`/秒级延迟，也不据此选择 250ms；
  固定常量只归 `sim-system`。

### 计划和归属修订

- 正式实施根固定为 `/home/chillchill/btc-updown-marketmaker`；当前分支工作树不成为第二
  工作区。当前事项只覆盖 `contracts`、`data-pipeline/core` 和
  `data-pipeline/training` 三个实施单元，不创建未来模块空 crate。
- 计划新增成熟 Arrow/Parquet/通用外部排序能力试验和合格真实输入资源实测；不预填未经
  验证的依赖版本或资源数值，也不自研 Parquet 或通用排序数据库。
- 门 A 必须在全部门 A 测试、任务自审和独立阶段审查后签发；门 B 必须在外部
  `sim-system`、归一化层和 training Ledger 通过，且全部门 B/共同测试及独立审查完成后
  签发。阶段 5 不再首次执行可能推翻已经签发回执的关键验证。
- 消除门 A/门 B 的切分身份时序循环：两门共享 `input_set_identity`；门 A 的
  `training_split_manifest_id/hash=N/A`，门 B 才绑定实际切分身份且不得扩大门 A 输入集合。
- 门 B 最终回执必须机械绑定 `sim-system` 组件符合性、DPT/sim 联合真实纵向、外部归一化
  合同/实现和 training Ledger/只读投影四类通过回执的 ID/SHA，以及同一候选的 sim 阶段 7
  联合训练闭环证据 ID/SHA；正式时延配置档状态/身份按尚待用户决定的门时序绑定，不能
  默认放行。
- `sim-system` 组件符合性由独立模拟器事项负责；DPT 消费其回执，真实事件纵向验收由双方
  联合负责。上游 CLOB 最小订单数量/完整 `feeSchedule` 缺口只阻断完整门 A 和正式阶段 3，
  不全局阻断与缺口无关的 scoped PARTIAL 开发。
- 每个任务完成后自审，每个阶段关闭前独立审查；本地提交与推送/发布分开，后者仍需具体
  外部动作授权。

### 批准门修订

- 删除“需求批准 + 计划批准 + 再次通用实施授权”的三重重复门：需求批准冻结范围，计划
  批准即赋予计划范围内的本地代码、测试和不可变本地测试产物游标。
- 整体需求批准一次性覆盖 Gate A/B、`SIM-S-004` 能力边界、验收标准和非目标，不要求逐节、
  逐项或逐日期重复签字。成熟依赖选择和既定硬件范围内的资源实测属于获批计划内工作。
- 门 B 有三项外部能力和四类通过回执；回执由验收机械签发，不要求用户逐份人工批准，
  DPT/sim 联合真实纵向回执也不新增事项批准门。三项能力究竟由 `sim-system + rebuild-core`
  两个事项覆盖，还是分别由三个事项覆盖，当前草案间存在未获用户裁定的归属冲突；只阻断
  受影响的门 B 工作，不反向阻断门 A。
- 补齐 DPT 与 `sim-system` 的门 B 依赖连边：DPT 阶段 4 候选必须先完成 sim 计划阶段 7
  联合训练闭环并取得绑定同一候选的证据，DPT 阶段 5 才能全量验证和签发门 B。该证据与
  `DPT-AC-013` 是同一次跨模块运行，不新增重复闭环或人工批准门。
- 真实资金、真实下单、钱包/敏感材料、远端操作、训练服务器环境升级、部署拓扑变化、部署、
  推送/发布，以及官方 SDK 缺口下任何旁路，仍保留具体动作或范围的独立授权门。

### 当前状态

- 独立终审确认一个必须由用户决定的时序问题：`sim-calibration` 是时延/可见性配置产物的
  唯一写入方且当前仍是未来独立事项，但旧正式配置档已经失效。可选项为：
  1. 推荐：门 B 脚本闭环只使用明确标识的固定符合性配置档，在门 B 与正式
     `model-train` 之间增加最小 `sim-calibration` 三件套和正式配置档回执门；
  2. 把正式配置档回执提前为门 B 的外部前置。
  两种方式都要求 `sim-calibration` 只消费 DPT 已准入、日期合格且可哈希的时延证据产物，
  不直接读取原始目录或建立第二套准入/规范化路径。用户决定前不阻断 Gate A 或其他 DPT
  局部开发，但进入受影响的 Gate B 阶段或签发 Gate B 前必须完成决定；任一方案下，正式
  配置档尚未就绪都继续阻断 `model-train`。
- 本次只修改审核中文档；未编写实现、未运行正式数据构建或配置档复算、未改变远端状态，
  未创建提交或推送。
- 需求、计划和变更记录的生命周期及执行 authority 继续保持草案/`none`；下一步仍是用户
  审核 DPT 需求和计划，而不是进入开发。

## 2026-08-04：低频平台规则退出事件带

### 用户决定与合同修订

- 用户确认 CLOB `min_order_size`、完整 `feeSchedule`、`itode` 等低频平台规则由
  `sim-system` 每个新运行通过官方 Rust SDK 冷启动查询并冻结，不由 `data-collect` 持久化，
  也不进入 `ExecutionMarketEvent` 或 Observation；首版未消费的 `oas` 同样不进入事件带，
  但只登记 SDK 覆盖缺口。
- 本决定取代本文件前述“固定 250ms 只归代码常量”和“CLOB 最小数量/完整
  `feeSchedule` 缺口阻断完整门 A”的旧快照。DPT Gate A 现在只要求精确市场身份和动态
  市场事件；低频规则缺失不得把 `REAL_EXECUTION_TAPE_READY` 降级为 PARTIAL。
- 启动时 `initial_tick_size` 可用于跨模块一致性校验，但四日数据的 624 条
  `tick_size_change` 继续进入规范事件带；当前 tick 不能被启动配置整场覆盖。

### data-collect 存储结论

- 四日合格数据共 491 个 Parquet、151,056,246 行、9,202,902,694 字节；metadata 为 97
  个 v5 文件、4,620 行、4,119,473 字节。当前没有规则专用列，删除收益为 0 字节。
- 1,167 个 Gamma 原始证据载荷中十个同名或相似键全部删除的压缩反事实约 9,317 字节，
  占全部数据约 0.000101%，且会破坏原始载荷及摘要。因此 DPT 不要求上游增列、删键或重写
  数据，也不把物理值重叠误判为语义冗余。
- 当前官方 Rust SDK 缺少首版必需的 `itode`、`feeSchedule.rebateRate`，且无法审计
  `mos/nr/fd.r/e/to` 的字段存在性；这些是模拟器冷启动的 SDK 能力门，不是 DPT 或采集
  schema 缺口。缺少未消费的 `oas` 不单独阻断。本事项继续保持纯离线且不联网补字段。

本次仅修改审核中文档，没有实现、数据构建、schema、远端、部署、提交或推送授权。

## 2026-08-04：模拟器冷启动来源改为官方公开 HTTPS 窄加载器

- 用户批准 `sim-system` 在每个新运行冷启动时，通过固定的官方公开 HTTPS 端点和窄类型
  响应加载器查询低频平台规则。本决定取代本文件此前“模拟器冷启动必须通过官方 Rust SDK”
  以及“SDK 类型覆盖缺口阻断模拟器冷启动”的表述；此前带日期内容保留为历史决策记录。
- 官方 Rust SDK 的类型覆盖缺口不再阻断模拟器。平台响应缺少必需字段、字段非法、
  条件/市场/代币身份不一致、重复结果冲突，或 CLOB 与 Gamma 交叉绑定冲突时，受影响的
  模拟器冷启动能力仍必须失败即关闭；未消费的 `oas` 只登记为未消费字段，不单独阻断。
- DPT 继续完全离线，不得调用该加载器、联网查询或补齐规则；上述失败属于 `sim-system`
  能力门，不降低 Gate A，也不要求 `data-collect` 增列、删键、改 schema 或重写数据。
- 本次只修订审核中文档语义，没有实现加载器、运行正式数据构建、修改 schema、操作远端、
  部署、提交或推送；事项 authority 和执行授权继续保持 `none`。

## 2026-08-04：需求与计划获批并开始阶段 0

### 用户决定

- 用户明确批准 `demand/data-pipeline-training-demand.md` 和
  `plan/data-pipeline-training-plan.md`，要求按需求与计划开始执行。
- 用户要求实际情况与需求、计划或上下文不能同时成立时立即停止并报告，由用户决策。
- 只读准入复核发现旧 `ConsumerKind::Training` 同时直接读取 `typed_raw` 并要求 DPT 输出；
  用户裁定该历史名称表示下游 `model-train`，批准建立
  `DATA-COLLECT-MODEL-TRAIN-BOUNDARY-20260804` 窄事项，取消其直接原始读取权限并同步
  文档守卫。

### 权威和游标

- 需求改为 `APPROVED_REQUIREMENTS`，计划改为 `APPROVED_ACTIVE_PLAN`，本记录改为
  `ACTIVE_IMPLEMENTATION_CHANGELOG`；当前执行 authority 为对应计划。
- 计划范围内本地实现与本地提交已授权；推送、发布、部署、远端操作、敏感材料、真实下单
  和真实资金仍未授权。
- 当前游标为 `阶段 0 -> 任务 0.1 -> 步骤 0.1.1`；本条不宣称阶段 0 或任何验收已经通过。

### 仓库基线

- 正式根 `/home/chillchill/btc-updown-marketmaker` 已创建本地 `data-pipeline` 分支，基于
  `3145a63594333fee50260db116db1ce11bf8a07e`，使已批准文档与后续代码处于同一谱系。
- 正式根已有未跟踪二进制 `data-collect-training-collector` 保持未修改、未暂存；其
  SHA-256 为 `5359b4cfcd397f8fd83a648808950e635df54b7fff5ee7ab584238d2c5784b0e`。

### 前置窄事项完成

- `DATA-COLLECT-MODEL-TRAIN-BOUNDARY-20260804` 已完成定向测试、自审和独立审查；独立
  审查结论为 `Critical 0 / Important 0 / Minor 0`。
- 该事项已经关闭执行授权，不形成第四个 DPT 实施单元；DPT 阶段 0 继续消费其已验证边界，
  但本条不宣称阶段 0 或 `DPT-AC-018` 已完成。

## 2026-08-04：实施工作树纠正

### 用户决定

- 用户明确要求本事项回到当前工作树 `/home/chillchill/sim-system` 开发，不得继续切换到
  `/home/chillchill/btc-updown-marketmaker`。
- 本决定取代需求、计划及仓库指南中针对本事项的旧正式根表述；不改变其他事项、远端目录
  或部署拓扑。

### 已核实事实与处置

- `/home/chillchill/sim-system` 在迁移前保持干净，位于已推送基线
  `3145a63594333fee50260db116db1ce11bf8a07e`。
- 前置窄事项曾在错误工作树形成未推送提交
  `2fe4b0997dfbf5b3cc467c94ee169ab7c31ca2a2`；用户批准迁移后，已将相同改动原样
  `cherry-pick` 到当前工作树，形成提交
  `9548077ea956d20d1b913b50f26ccd91298a4345`。该提交仍只在本地，未推送。
- 错误工作树的 `data-pipeline` 分支及其未跟踪二进制保持原样；本事项不再从该路径读取、
  修改、构建或验收。
- DPT 需求、计划与根 `AGENTS.md` 已同步当前工作树窄例外；阶段 0 从当前工作树继续，尚未
  宣称完成任何阶段 0 验收。

## 2026-08-04：阶段 0 合同冻结与实施准入复核

### 任务 0.1：批准范围和数据资格

- 当前实现继续绑定 `dataset_lineage_root=/home/chillchill/new-datasets` 和
  `dataset_date_floor=2026-07-30`。纯语义核心已冻结日期下限前的终态
  `INELIGIBLE_BEFORE_DATASET_FLOOR` 与合格但未分配的 `UNASSIGNED`，没有逐日人工批准或
  回退到更早日期的路径。
- 在当前工作树重新只读读取 2026-07-30、2026-07-31、2026-08-01、2026-08-02 四个每日
  清单。四者仍分别声明 121/127/121/122 个最终化文件，43,741,059/42,259,888/
  31,336,754/33,718,545 行和 2,658,237,003/2,604,557,207/1,892,535,675/
  2,047,572,809 字节；清单 SHA-256 仍分别为
  `4b2cd62cea8740bf4cdb29971941118e70d869835496aece714458d12ea96557`、
  `4fac5a9440c14dbb8acfca18c7a944b2fc33f7f67b5866caeb89d646b87f2296`、
  `e31b122f6debad9863fac973903714baa739282e4ebba1c9093d9972618a3d30`、
  `2ea1a7460cf7fde05a498d0b3e338eb3c10a305333fd3a7ddeca46a4a9e19889`。
- 四个清单均为 `data_collect_daily_manifest_v3`、`closed`、
  `closure_reason=utc_day_barrier` 和 `collection_day_coverage.state=ended_known`；合计仍为
  491 个文件、151,056,246 行、9,202,902,694 字节。2026-07-31 仍有 6 条 `gaps` 且
  `reactivated_within_day=true`，只保持缺口/重新激活负例地位。
- 上述数值仍是清单声明值，不是逐一重算 491 个文件后的 `DPT-AC-001` 通过证据；阶段 0
  没有读取 2026-07-29 真实数据，日期下限负例只使用纯合同值。

### 任务 0.2：低频规则与动态事件所有权

- `ExecutionMarketEventV1` 和市场侧 Observation 因果外壳不定义 `min_order_size`、
  `feeSchedule`、`itode`、`oas` 或秒级延迟字段；未知字段负例逐项证明这些字段不能混入
  事件载荷。公开成交自身的 `fee_rate_bps` 可以作为成交证据保留，不等于平台
  `feeSchedule`。
- 动态 `TickSizeChangeV1` 明确保留资产身份、`old_tick_size_pusd_per_share` 与
  `new_tick_size_pusd_per_share`。本阶段没有修改采集 schema、源证据 JSON、SDK 路径、
  模拟器冷启动加载器或远端运行状态。

### 任务 0.3：跨模块合同

- 新建无行为 `contracts` crate，冻结 `execution_market_event_v1`、
  `market_observation_causal_shell_v1`、`total_order_key_v1`、Gate A 16 项静态能力集合和
  `real_execution_tape_receipt_v1`。
- 时间命名空间已分离源业务时间、接收时间、接纳 epoch/序号/时间和规范可用时间。RTDS v4
  不伪造接纳坐标，只允许 `RECEIVED_ONLY` 与 `RECEIVED_TIME_STABLE_EVIDENCE`。
- `CanonicalSourceKind` 只冻结当前六个来源；全序固定使用可用时间、来源固定序、接纳或接收
  坐标、源原生键和语义决胜 SHA。事件 ID 和语义决胜使用不同域分隔，均排除文件名、清单/
  文件 SHA、Parquet 行号、行组、分片、读取批次和线程；物理重分片证据变化不改变事件 ID，
  真实语义变化必须改变事件 ID。
- 精确价格和数量使用带单位名称的规范十进制字符串，不使用二进制浮点真相。未知业务码只能
  显式进入 `UNKNOWN_UNMODELED`；未知版本、关闭枚举和未知字段失败可见。
- 与 `SIM-S-004` 对齐的事件载荷覆盖市场生命周期、完整 L2 初始/重同步快照、最优买卖价、
  动态 tick、公开成交、质量边界、市场判定终态以及完整
  `LevelExecutionTransitionV1`。后者包含因果组、市场/资产、方向、价格、变更前后数量、已
  归因成交量、已覆盖量、净增/净缩、队头消耗量、源事件 IDs、三种依据、组可用时间和两类
  质量；核心只冻结非负、数量守恒、队头消耗上限和推断质量组合，不提前实现阶段 2 归因。
- 市场侧 Observation 只含市场/窗口身份、同源谱系、最终截止点、九个市场组件、六值质量、
  信息年龄、缺失掩码和摘要，不冻结训练张量、dtype、固定特征数、账户、Ledger、标签、
  奖励或未来信息。
- Gate A 能力集合不允许 `NOT_APPLICABLE` 绕过；只有 16 项全部 `PASS` 才允许
  `REAL_EXECUTION_TAPE_READY`。`PARTIAL` 只能发布 `REAL_EXECUTION_TAPE_PARTIAL`，
  `FAIL` 无发布状态；Gate A 始终要求 `training_dataset_ready=false` 且切分 ID/SHA 为
  `N/A`。

### 任务 0.4：三个 crate 与成熟依赖技术试验

- 根工作区新增且只新增本事项三个实际单元：`contracts`、`data-pipeline/core` 和
  `data-pipeline/training`。`cargo metadata --locked --offline --no-deps` 证明：
  `contracts` 只依赖 `serde`；`core` 只依赖 `contracts`、`rust_decimal`、`serde`、
  `serde_json`、`sha2`；`training` 显式依赖前两者并独占 Arrow/Parquet 与排序编排。
- `cargo tree` 证明 `core` 不含 I/O、网络、Arrow/Parquet、Python、SDK、`sim-system` 或
  Ledger；`training` 不依赖网络、SDK、Python、`sim-system`、Ledger 或 `data-collect`。
  工作区没有预建 online、paper/live、`model-train`、execution 或 Ledger 空 crate。
- 技术试验精确直接使用 `arrow-array`、`arrow-schema`、`arrow-ord` 和
  `parquet` 59.0.0。它只接受 `data_pipeline_core::total_order_key_bytes` 产生的完整键，
  通过 Arrow 形成有界排序 run、Parquet 临时 run 和固定 fan-in `BinaryHeap` 多轮 k 路合并；
  文件名、run 编号和读取位置不进入逻辑摘要，完整键重复失败关闭。
- 试验分别记录输入/输出行数、初始 run、合并轮次、run/写入批次行数与字节高水位、临时
  run 数、打开文件数、堆项、临时字节和逻辑 SHA-256；`max_temp_run_files` 与
  `max_open_files` 是不同硬上限，配置门要求 `merge_fan_in + 1 <= max_open_files`。
- DataFusion 54.1.0 当前依赖 Arrow 58.x，会与工作区 59.0.0 形成两套不能直接互操作的
  `RecordBatch` 类型；其 Parquet 路径还会重新引入旧 `thrift`。因此本阶段不把 DataFusion
  放入锁文件，只有出现与当前 Arrow 同系列的正式发布版本后再用同一向量复评。
- 当前解析版本和许可证已核对：Arrow/Parquet 59.0.0 为 Apache-2.0 或
  Apache-2.0 AND MIT，`rust_decimal` 1.42.1 为 MIT，`serde`/`serde_json`/`sha2`/
  `tempfile` 为 MIT OR Apache-2.0。2026-08-03T23:58:42Z 对官方
  `https://api.osv.dev/v1/querybatch` 重新查询锁文件解析出的 9 个直接依赖：
  `arrow-array/arrow-ord/arrow-schema/parquet 59.0.0`、`rust_decimal 1.42.1`、
  `serde 1.0.228`、`serde_json 1.0.150`、`sha2 0.10.9` 和 `tempfile 3.27.0`；响应为 9 个空
  结果，响应字节 SHA-256 为
  `f9a53dc4c376ae02e24c72cf1c3f67df0436c8c52c13578e62f825846991db25`。这是时点直接依赖
  证据，不替代后续完整传递依赖审计。

### 当前验证结果与治理处置

- `cargo test -p contracts --locked --offline`：crate 构建、单元测试入口和文档测试通过；该
  无行为合同叶子当前没有独立测试函数。
- `cargo test -p data-pipeline-core --locked --offline`：16 项合同测试通过。
- `cargo test -p data-pipeline-training --locked --offline`：5 项排序试验通过；强制 9 个初始
  run 和 4 轮合并，并覆盖配置无关摘要、Arrow/core 顺序一致、重复键，以及归本试验所有的
  行描述符缓冲、临时 run、打开文件和临时字节上限。该技术试验不把
  `max_run_bytes` 冒充进程常驻内存或总内存硬上限；正式资源数值仍须由阶段 1 真实输入实测。
- 三个新 crate 的定向 `cargo clippy -- -D warnings` 均通过；格式检查和
  `git diff --check` 通过。
- 初次把阶段 0 证据另写为新的 Markdown 时，`data-collect` 文档权威守卫按设计以
  `3 passed / 1 failed` 拒绝未登记文档。为避免重开已关闭的采集边界事项，本次删除该新增
  文档并把证据合并进既有、已登记的 DPT changelog；没有修改采集运行时、合同或守卫集合。
- 既有模型训练消费边界回归测试 10 项继续通过；文档权威守卫重跑为 4 项通过。
- `cargo metadata` 和 `cargo tree` 复核工作区只有既有 `data-collect` 与本事项三个单元；
  `contracts` 只依赖 `serde`，`core` 不含 I/O、网络、Arrow/Parquet 或下游模块，
  `training` 不含网络、SDK、`data-collect`、模拟器、Ledger、Python 或 DataFusion 路径。
- 阶段 0 任务自审和全部定向验证已完成；本节尚不宣称阶段 0 完成，须等待独立阶段审查。

### 下游阻断登记

- 六类真实字段映射、逐文件 `DPT-AC-001`、真实去重、盘口差额、因果组归因、分页事件带、
  原子发布和正式资源预算仍按计划留在阶段 1/2；阶段 0 没有提前实现。
- `DPT-AC-019` 的真实模拟器纵向门位于 Gate A 之后，不反向阻断阶段 0。
- Gate B 的外部归一化层和 training Ledger 批准归属仍待后续用户决定；不阻断 Gate A，
  但在受影响 Gate B 工作开始前必须解决。
- 阶段 0 外部排序试验仅在正常返回 `Err` 时尽力清理专用临时目录；进程中止、恐慌和内存
  耗尽后的恢复、陈旧目录处置及原子发布仍按计划属于阶段 2 故障注入范围，当前不宣称通过。

## 2026-08-04：删除无独立前像的派生摘要并收敛阶段 0 审查

### 用户决定与依据

- 用户质疑派生摘要强制绑定是否过度。重新核对合同后确认：规范事件 ID 已绑定 schema、
  市场身份、时间、规范规则版本、语义来源证据、质量和完整载荷；
  `LevelExecutionTransitionV1` 与市场判定终态也已显式保存组成源事件、规则、结果及适用的
  守恒字段。原 `derivation_digest_sha256` 没有定义独立前像、独立记录或重算协议，不能提供
  额外可验证性，反而会产生第二套不明确身份。
- 用户批准删除该重复要求。需求现改为：已有规范载荷和事件 ID 完整绑定时，不再增加无独立
  前像的摘要；只有独立、版本化、持久化的派生证据记录才保存其内容寻址摘要。
- 阶段 2 的合并、覆盖、删除和确认证据归版本化独立去重/守恒报告；每条记录保存源事件 ID、
  原因、规则版本、适用守恒摘要和 `result_event_id`，无输出时使用明确的 `NO_OUTPUT` 或
  `SUPPRESSED`。运行清单绑定报告 SHA，不向每个事件重复嵌入报告摘要。

### 审查纠偏与最小处置

- 重新审查确认，先前建议把 Observation 改成五值可用性并另加独立缺口维度属于超出阶段 0
  获批合同的设计；当前继续保留六值 `MarketDataQuality` 和门 A 保守因果外壳，不提前冻结
  门 B 的完整 Observation。
- 重新审查确认，当前 `SourceEvidenceRefV1` 已保存来源种类、源原生身份、排序坐标及物理
  清单/文件 SHA，而事件 ID 有意排除物理重分片信息。无需聚合多份物理来源或添加 Parquet
  行号；完全相同的语义坐标继续作为输入完整性冲突失败可见。
- 从 `EventLineageV1` 和 `MarketResolutionTerminalV1::Resolved` 删除
  `derivation_digest_sha256`，旧字段因 `deny_unknown_fields` 被明确拒绝；没有向
  `LevelExecutionTransitionV1` 添加替代摘要。
- 把通用 `contract_digest<T>` 收窄为类型化
  `gate_a_capability_matrix_digest`；同时拒绝携带 `blocking_reason` 的 `PASS` 能力项，未扩展
  通用规范 JSON 框架。
- 新增有界固定全序字节向量，覆盖六个来源固定序、`i64` 边界、两类排序坐标、NUL 转义、
  缺少/存在源原生身份及四种源原生身份变体；未展开为无必要的笛卡尔积。
- 更新三份黄金编码，并补齐终态兑付/水位线、精确十进制溢出、能力矩阵、回执内容身份、
  能力摘要、覆盖日期及伪装状态负例。当前定向核心测试为 16 项通过；全部定向验证与任务
  自审已完成，阶段 0 仍须独立阶段审查后才能关闭。
- 独立阶段审查中间发现两个获批合同内的因果缺口：接纳坐标路径没有验证
  `received_at_utc_ms <= admitted_at_utc_ms`，市场侧 Observation 可见组件也没有验证
  `as_of_available_at_utc_ms <= through_total_order_key.available_at_utc_ms`。前者与上游六类
  schema 的接收/接纳顺序不一致，后者可能暴露最后已最终化事件之后的状态；现已各增加一条
  局部校验和负例，不改变六值质量、谱系或摘要边界。修复后核心 16 项和定向 Clippy 通过。
- 自审时曾把三件套首屏状态改写为“阶段 0 独立审查中”，文档权威守卫以 2 项通过、2 项
  失败拒绝该未登记短语，并精确要求原合同短语“阶段 0 实施准入复核中”。本次撤回三处首屏
  改写，不修改已关闭的 `data-collect` 守卫；实际游标只在计划第 9 节和本记录正文更新。守卫
  随后重跑为 4 项通过。
- 独立阶段审查还发现，门 A 的整体 `FAIL` 被错误限制为能力矩阵中没有任何 `PASS`。需求只
  约束每次评估都能得到 `PASS|PARTIAL|FAIL`，致命错误可能在部分能力已经通过后发生；现已
  最小放宽为“矩阵并非全通过、无发布产物”，并增加混合 `PASS/FAIL` 得到整体 `FAIL` 的
  正例，以及全通过矩阵伪装成 `FAIL` 的负例。修复后核心 16 项、定向 Clippy、格式和差异
  检查通过。

### 待用户决定的合同冲突

- 获批需求和计划只定义门回执的 `PASS|PARTIAL|FAIL`、逐能力“通过/未通过”和固定 16 项
  能力集合，没有定义逐能力第三状态；当前 `contracts/src/lib.rs` 却公开冻结了
  `CapabilityStatus::NotEvaluated`，核心测试又把它作为 `PARTIAL` 正例。其是否必须携带
  `blocking_reason` 或证据、与整体 `PARTIAL/FAIL` 的组合规则均没有获批合同。
- 现实选项是：删除 `NotEvaluated`，以 `Fail + blocking_reason` 表示 `PARTIAL` 中未通过的
  能力；或者先在需求和计划中明确批准并定义 `NotEvaluated` 语义后再保留。独立阶段审查在
  此处停止；用户决定前不改该枚举、不关闭阶段 0、不创建本阶段提交。
- 获批需求要求每次门 A 评估都能输出 `FAIL` 回执，同时规定日期下限、未知 schema、路径
  逃逸、输入摘要不符和故障不得发布成功回执或成功清单；计划的原子签发步骤则只明确
  `PASS/PARTIAL`。当前 `GateAReceiptV1` 对所有结果统一强制输入集合/清单身份、三个输出
  清单摘要、能力矩阵摘要和运行清单摘要。测试辅助函数以固定占位摘要让 `FAIL` 通过。
- 进一步复核确认，只把三个输出摘要改为可选仍不足：在路径逃逸、清单不存在或非法等
  早期失败时，输入集合身份和输入清单集合摘要未必已有真实前像；能力矩阵还依赖上述
  `NotEvaluated` 决策；崩溃或同盘磁盘满时也未必已有原子失败运行清单。当前合同没有定义
  门 A 评估从何时开始，以及不同失败阶段必须或允许绑定哪些身份。
- 现实选项是：把 `FAIL` 定义为独立失败评估记录，只强制启动时已经确定的共同身份，并按
  失败阶段绑定实际存在的失败证据；或者把门 A 回执收窄为候选评估阶段的结果，早期准入/
  崩溃只留下失败证据，并明确 `FAIL` 只用于全部必填前像形成后的整体否决。前者必须逐项
  定义可缺字段，后者必须收窄需求中“每次评估”的范围。用户决定前不修改回执合同、测试或
  黄金文件。

## 2026-08-04：批准成功回执单态合同并从本次修正开始执行 TDD

### 用户决定

- 用户批准把 `GateAReceiptV1` 收窄为只代表全部必需能力通过并原子发布的
  `REAL_EXECUTION_TAPE_READY`；`PARTIAL`、准入失败、构建失败、内部不变量失败和运行中断
  统一进入能力/失败诊断，不签发门 A 回执。
- 用户批准删除没有实际消费者需求的 `CapabilityStatus::NotEvaluated`。逐能力只保留
  `Pass|Fail`；`Fail` 表示能力尚未满足或尚未由证据证明，不声称检查一定已经执行，并强制
  通过非空 `blocking_reason` 说明阻断原因。
- 成功回执继续绑定真实输入、三个输出清单、能力矩阵和运行清单摘要；这些摘要均有独立、
  可持久化和可重算的前像，不属于已经删除的无独立前像派生摘要。
- 用户同时批准从本次未决合同修正开始保留真实红灯证据。此前阶段 0 把实现和测试成批
  修改后直接验证绿灯，不得追溯写成 TDD；本次先同步获批文档，再新增针对旧合同必然失败的
  测试并实际运行，确认失败原因后才能修改实现。

### 当前执行门

- 本次修正不新增失败回执、可选摘要、失败阶段枚举、诊断类型或诊断 crate；阶段 1/2 只按
  需求已有的能力/质量/隔离报告、运行清单和临时失败证据边界实现失败可见性。
- 当前游标回到阶段 0 合同修正。依次完成“文档同步 -> 红灯测试 -> 最小实现 -> 绿灯与全部
  定向验证 -> 任务自审 -> 独立阶段复审”后，才能关闭阶段 0 并创建本地提交；推送仍未授权。

### TDD 红灯证据

- 只修改 `data-pipeline/core/tests/phase0_contract.rs` 后运行
  `cargo test -p data-pipeline-core --locked --offline --test phase0_contract`，结果为
  `15 passed; 3 failed`。三个失败均由旧合同仍允许本次已禁止行为直接触发：
  `not_evaluated_is_not_a_capability_status` 证明解码器仍接受 `NOT_EVALUATED`，
  `gate_a_receipt_schema_is_success_only` 证明成功回执仍编码 `tape_assessment`，
  `gate_a_receipt_encoding_matches_golden_and_states_cannot_masquerade` 证明校验器仍接受
  `PARTIAL` 回执。
- 红灯发生时尚未修改 `contracts/src/lib.rs`、`data-pipeline/core/src/lib.rs` 或回执黄金文件；
  下一步只做消除上述三类旧行为所需的最小实现。

### 聚焦红灯、最小实现与任务复审

- 初次整体红灯中的大测试在第一个编码字段断言处停止，因而不能证明后续旧字段解码断言
  实际执行。生产代码修改前又分别运行 `NOT_EVALUATED`、旧 `PARTIAL`、全 `FAIL` 和混合
  `PASS/FAIL` 场景，均取得由旧合同语义直接造成的独立红灯；没有把未执行到的旧字段解码
  断言追溯写成红灯证据。
- 生产代码修改前的五项聚焦红灯原始记录如下；每项均单独运行，结果均为
  `0 passed; 1 failed`：
  1. `cargo test -p data-pipeline-core --locked --offline --test phase0_contract
     not_evaluated_is_not_a_capability_status -- --exact` 在当时的
     `phase0_contract.rs:1227:5` 失败：
     `assertion failed: decode_gate_a_capability_matrix(&encoded).is_err()`。
  2. `cargo test -p data-pipeline-core --locked --offline --test phase0_contract
     gate_a_receipt_schema_is_success_only -- --exact` 在当时的
     `phase0_contract.rs:1207:5` 失败：
     `assertion failed: !fields.contains_key("tape_assessment")`。该次运行停在编码断言，
     没有执行到旧字段解码断言。
  3. `cargo test -p data-pipeline-core --locked --offline --test phase0_contract
     gate_a_receipt_rejects_partial_capability_matrix -- --exact` 在当时的
     `phase0_contract.rs:1173:5` 失败：
     `assertion failed: validate_gate_a_receipt(&receipt, &partial).is_err()`。
  4. `cargo test -p data-pipeline-core --locked --offline --test phase0_contract
     gate_a_receipt_rejects_all_fail_capability_matrix -- --exact` 在当时的
     `phase0_contract.rs:1180:5` 失败：
     `assertion failed: validate_gate_a_receipt(&receipt, &fail).is_err()`。
  5. `cargo test -p data-pipeline-core --locked --offline --test phase0_contract
     gate_a_receipt_rejects_mixed_fail_capability_matrix -- --exact` 在当时的
     `phase0_contract.rs:1193:5` 失败：
     `assertion failed: validate_gate_a_receipt(&receipt, &mixed_failure).is_err()`。
- 最小实现只删除 `CapabilityStatus::NotEvaluated`、`TapeAssessment`、
  `TapeArtifactKind` 及 `GateAReceiptV1` 的两个旧状态字段；能力矩阵校验使用穷尽匹配，
  `Fail` 继续要求非空 `blocking_reason`，成功回执要求 16 项能力全部为 `Pass`。没有新增
  失败回执、可选摘要、失败阶段枚举、诊断类型、crate 或依赖。
- 只更新受稳定编码影响的 `real_execution_tape_receipt_v1.json`；新的 `receipt_id` 为
  `1b2bf20a7dbab794a7303cd52cebf4a3e4ffca41ebb5ac847f5e7b677a8d880a`，真实输入、三个
  输出清单、能力矩阵和运行清单摘要仍为必填。
- 任务级独立审查发现一个 Important：旧字段解码测试把两个字段放在同一输入中且只断言
  任意错误，可能由另一个未知字段或 `receipt_id` 错误造成假绿。修复轮次 1 将编码字段缺失
  与两个单字段解码场景拆成三个测试；两个解码测试均精确匹配对应的
  `ContractError::Json` 未知字段信息。范围复审裁定该 Important 已解决，且没有新
  Critical/Important。
- 审查同时指出 `Fail` 缺少 `blocking_reason=Some("")` 的直接回归。生产校验已经拒绝空
  字符串，因此阶段自审只增加一条覆盖现有合同的最小测试，没有修改生产逻辑，也没有把这条
  事后补充的绿灯回归冒充为实施前红灯。

### 当前绿灯与阶段自审证据

- `cargo test -p contracts --locked --offline`：构建、单元测试入口和文档测试通过。
- `cargo test -p data-pipeline-core --locked --offline`：22 项合同测试通过；其中成功回执只
  接受全 `Pass`、全 `Fail` 和混合失败均拒绝、两个旧回执字段逐项因未知字段失败、
  `NOT_EVALUATED` 解码失败，`Fail` 的缺失和空 `blocking_reason` 均被拒绝。
- `cargo test -p data-pipeline-training --locked --offline`：5 项有界排序试验通过。
- `cargo test -p data-collect --locked --offline --test consumer_format_compatibility_contract`
  与 `--test training_consumer_boundary_contract`：既有模型训练消费边界 6 项和 4 项通过。
- `cargo test -p data-collect --locked --offline --test document_authority_contract`：4 项通过；
  三件套首屏状态、事项编号、引用和当前权威图保持一致。
- `contracts`、`data-pipeline-core`、`data-pipeline-training` 的
  `cargo clippy --all-targets --locked --offline -- -D warnings` 全部通过；
  `cargo fmt --all -- --check` 与 `git diff --check` 通过。
- `cargo tree --locked --offline --depth 1` 重新证明：`contracts` 只直接依赖 `serde`；
  `core` 只直接依赖 `contracts`、`rust_decimal`、`serde`、`serde_json`、`sha2`；
  `training` 独占 Arrow/Parquet 和排序编排。完整依赖树负向检索没有发现计划禁止的网络、
  SDK、`data-collect`、模拟器、Ledger、Python 或 DataFusion 路径。
- 阶段自审未发现当前需求、计划、代码和测试之间的新冲突，也未发现为本次合同修正新增的
  抽象或范围扩张。阶段 0 仍未关闭、未提交、未推送；下一步必须由独立阶段审查代理全面
  复核，不能用本任务范围复审替代阶段审查。

### 独立阶段审查第一轮

- 阶段审查代理使用 `gpt-5.6-sol` 和 `ultra` 推理等级，完整读取当前三件套、三个 Rust
  单元、测试、黄金文件、工作区差异和依赖边界；结论为
  `Critical 0 / Important 5 / Minor 0`，阶段 0 必须修复后复审。
- 两项文档 Important 是：当前执行门措辞弱于批准计划，以及五次聚焦红灯只存在于将被清理
  的临时交接报告。前者已按批准计划收紧，后者已把准确命令、测试名、失败断言和结果持久化
  到本 changelog。
- 三项代码 Important 是：独立 `ReceivedOnly` 全序键未拒绝接收时间晚于可用时间；未被
  选为全序来源的 RTDS 证据可以晚于事件可用时间；生命周期资产集合和终态兑付向量没有
  拒绝重复 `outcome.index`。三者均属于已批准因果/唯一身份合同内的实现缺陷，不要求新增
  字段或改变业务语义。
- 审查没有发现 Critical、需用户重新裁决的现行合同冲突、影子语义核心或越界依赖。阶段 0
  仍未关闭、未提交、未推送；代码 Important 必须先取得独立聚焦红灯，再做最小实现和受影响
  验证，随后重新执行完整阶段验证与独立复审。

### 审查修复轮次 1：代码红灯与最小实现

- 只增加六个独立负例、尚未修改生产代码时，逐项运行
  `cargo test -p data-pipeline-core --locked --offline --test phase0_contract <测试名> -- --exact`；
  六项均为 `0 passed; 1 failed`，真实失败记录如下：
  1. `total_order_key_bytes_rejects_received_only_after_availability` 在当时的
     `phase0_contract.rs:615:5` 失败；断言要求
     `total_order_key_bytes(&key)` 返回字段
     `total_order_key.ordering_coordinate.received_at_utc_ms` 的 `InvalidField`，旧实现却接受该键。
  2. `observation_through_total_order_key_rejects_received_only_after_availability` 在当时的
     `phase0_contract.rs:635:5` 失败；断言要求 Observation 校验返回同一精确字段的
     `InvalidField`，旧实现却接受该截止键。
  3. `terminal_watermark_rejects_received_only_after_availability` 在当时的
     `phase0_contract.rs:663:5` 失败；断言要求事件校验返回同一精确字段的 `InvalidField`，
     旧实现却接受该终态水位线。
  4. `unselected_rtds_source_evidence_rejects_received_only_after_event_availability` 在当时的
     `phase0_contract.rs:684:5` 失败；断言要求事件校验返回字段
     `lineage.source_evidence[1].ordering_coordinate.received_at_utc_ms` 的 `InvalidField`，
     旧实现却接受未选中的未来 RTDS 证据。
  5. `lifecycle_assets_reject_duplicate_outcome_index_for_distinct_tokens` 在当时的
     `phase0_contract.rs:705:5` 失败；断言要求事件校验返回字段
     `payload.event.assets[1].outcome.index` 的 `InvalidField`，旧实现却接受重复结果索引。
  6. `terminal_payout_vector_rejects_duplicate_outcome_index_for_distinct_tokens` 在当时的
     `phase0_contract.rs:724:5` 失败；断言要求事件校验返回字段
     `payload.event.payout_vector[1].asset.outcome.index` 的 `InvalidField`，旧实现却接受重复
     结果索引。
- 六项红灯全部确认后，只在 `data-pipeline/core/src/lib.rs` 增加三处既有合同校验：
  独立 `ReceivedOnly` 全序键拒绝接收时间晚于键可用时间；每个 `ReceivedOnly` 来源证据拒绝
  接收时间晚于事件可用时间；生命周期资产集合和终态兑付向量拒绝重复 `outcome.index`。
  没有新增合同字段、公共 API、固定 `0/1` 索引、固定结果标签或通用映射抽象。
- 六项聚焦测试随后分别重跑为 `1 passed; 0 failed`；
  `cargo test -p data-pipeline-core --locked --offline` 为 `28 passed; 0 failed`，核心严格
  Clippy、全工作区格式和 `git diff --check` 均通过。本条只记录受影响验证；完整阶段验证和
  独立复审仍须随后执行。

### 修复范围复审与完整阶段验证

- 原阶段审查代理对五项 Important 做范围复审，逐项裁定全部 `ADDRESSED`，且修正范围没有
  新 Critical/Important；该结论只关闭修复轮次，不替代最终广泛阶段复审。
- 随后重新运行阶段 0 完整定向矩阵：`contracts` 构建、单元测试入口和文档测试通过；
  `data-pipeline-core` 28 项通过；`data-pipeline-training` 5 项通过；既有模型训练消费边界
  6 项和 4 项通过；文档权威守卫 4 项通过。
- 三个新 crate 的 `cargo clippy --all-targets --locked --offline -- -D warnings` 全部通过；
  `cargo fmt --all -- --check` 与 `git diff --check` 通过。
- `cargo metadata --locked --offline --no-deps` 与三个实际依赖树再次确认工作区和职责边界
  不变：`contracts` 只直接依赖 `serde`；`core` 无 I/O、网络、Arrow/Parquet 或下游依赖；
  `training` 独占 Arrow/Parquet，且没有网络、SDK、`data-collect`、模拟器、Ledger、Python
  或 DataFusion 路径。
- 当前仍未关闭阶段 0、未提交、未推送。下一步必须基于修正后的完整工作树重新执行广泛独立
  阶段审查；只有该审查通过并完成最终即时验证，才可创建阶段 0 本地提交。

### 修复后的广泛独立阶段复审

- 新的 `gpt-5.6-sol`、`ultra` 审查代理从头读取完整三件套、三个 crate、1,399 行核心合同
  测试、三份黄金文件和跨模块边界，并在隔离 `/tmp` 构建目录独立重跑完整测试、严格
  Clippy、格式和差异矩阵；结果均通过且工作树未改变。
- 审查确认此前五项 Important 均已关闭，没有现行合同冲突、影子实现、越界依赖或阶段 1/2
  功能偷跑；同时发现一个新的 Important，因此结论为
  `Critical 0 / Important 1 / Minor 0`，阶段 0 仍须修复后复审。
- 新问题位于 `data-pipeline/training` 的狭义自有行缓冲：`SortRow::resident_bytes()` 为每个
  已存在行计入一个描述符和两个内层 `Vec` capacity，却没有计入外层 `Vec<SortRow>` 的
  `capacity - len` 个预留描述符。现实探针得到 `SortRow=48B`，首次压入后
  `len=1/capacity=4`；当前指标为 123B，而外层描述符分配加两个内层 capacity 已为 267B，
  `external_sort` 在 `max_run_bytes=123` 时仍错误成功。该数字不包含 RSS、Arrow 临时分配或
  分配器隐藏开销。
- 该缺口使变更记录此前所称“归本试验所有的行描述符缓冲硬上限”不能成立，属于实现缺陷和
  测试缺口，不是需求或计划冲突。必须先以初始 `pending_rows` 和 `RunWriter.buffer` 两个
  独立红灯复现，再把外层实际 capacity 与内层 capacity 共同纳入预算和峰值；不得借机扩展
  为通用内存管理或进程总内存声明。

### 审查修复轮次 2：外部排序容量 TDD

- 生产代码修改前，分别实际运行以下两个历史测试版本；命令没有使用
  `--locked --offline`，本记录不补写未实际使用的参数：
  1. `cargo test -p data-pipeline-training --lib tests::initial_pending_rows_flushes_before_outer_descriptor_capacity_exceeds_run_budget -- --exact`
     在当时的 `data-pipeline/training/src/lib.rs:1171:9` 失败，`initial_runs` 实际为 `1`、
     期望为 `2`，结果为 `0 passed; 1 failed`。
  2. `cargo test -p data-pipeline-training --lib tests::run_writer_buffer_flushes_before_outer_descriptor_capacity_exceeds_run_budget -- --exact`
     在当时的 `data-pipeline/training/src/lib.rs:1206:9` 失败，写缓冲峰值行数实际为 `2`、
     期望为 `1`，结果为 `0 passed; 1 failed`。
- 两个历史测试都从运行时实际 `Vec::capacity()`、`size_of::<SortRow>()` 和两行内层
  `Vec` capacity 构造预算，分别真实执行到初始 run 数和写缓冲峰值行为断言。历史初始测试
  当时仍使用 `batch_rows=4`；历史 writer 测试在行为断言前执行了 `finish()`。在生产实现
  完成后，测试又收紧为容量受控的两行，初始测试改用 `batch_rows=1`，并把
  `capacity > len`、旧公式仍在预算内、实际公式已经超预算三项证明断言移到目标行为之前；
  writer 测试改为刷新后直接检查 `rows=1` 和 `buffer.len()=1`。这个最终收紧版本没有在旧
  生产实现上重新运行，因此只能记录其绿灯，不能冒充上述历史红灯。
- 最小实现只修改 `data-pipeline/training/src/lib.rs`：初始 `pending_rows` 与
  `RunWriter.buffer` 使用受配置行数约束的显式容量；统一预算公式为外层实际
  `capacity * size_of::<SortRow>()` 加当前各行两个内层 `Vec` capacity。候选行加入前按该
  公式复算，超限时先切 run 或刷新；空缓冲仍容不下则以既有 `RowMemoryLimit` 失败关闭。
- `validate_config` 新增 `max_run_rows` 描述符容量门；原批次描述符门保持不变。初始缓冲首次
  创建和每次 `replace`、写缓冲首次创建和每次 `clear()` 后都重新读取实际 capacity、检查
  上限并更新峰值；空输入也记录两个固定缓冲的描述符基础高水位。旧 `buffer_bytes` 状态已
  删除，没有新增公共 API、配置字段、指标字段、错误类型、crate 或通用内存抽象，也没有把
  范围扩大到 RSS、分配器隐藏开销、Arrow 临时分配、读取游标或进程总内存。
- 主代理在最终代码上重新运行两项带 `--locked --offline` 的精确测试，各为
  `1 passed; 0 failed`；`cargo test -p data-pipeline-training --locked --offline` 为
  7 项单元测试全部通过且文档测试入口通过；
  `cargo clippy -p data-pipeline-training --all-targets --locked --offline -- -D warnings`、
  `cargo fmt --all -- --check` 和 `git diff --check` 均通过。
- 独立范围复审使用 `gpt-5.6-sol` 与 `ultra` 推理等级，裁定
  `Critical 0 / Important 0 / Minor 0`。复审确认统一预算、实际容量失败关闭、重建/清空后的
  基础计量、空输入、配置门、测试隔离和范围控制均正确；同时裁定两个历史真实行为红灯足以
  满足任务 0.7，但要求永久保留最终测试体只取得绿灯的事实。该结论只关闭任务 0.7 修复范围，
  不代表阶段 0 已完成；下一步仍须重新执行完整阶段验证和广泛独立阶段复审。

### 修复轮次 2 后的完整阶段验证与自审

- 基于同步后的当前三件套和最终代码重新运行阶段 0 完整定向矩阵：
  `cargo test -p contracts --locked --offline` 的单元测试入口和文档测试入口通过；
  `cargo test -p data-pipeline-core --locked --offline` 为 28 项合同测试通过；
  `cargo test -p data-pipeline-training --locked --offline` 为 7 项排序试验通过，文档测试入口
  通过。
- 既有消费边界继续通过：
  `cargo test -p data-collect --locked --offline --test consumer_format_compatibility_contract`
  为 6 项通过；`--test training_consumer_boundary_contract` 为 4 项通过。文档权威守卫在本次
  文档同步后重跑为 4 项通过。
- `contracts`、`data-pipeline-core` 和 `data-pipeline-training` 分别运行
  `cargo clippy -p <crate> --all-targets --locked --offline -- -D warnings`，三者均通过；
  `cargo fmt --all -- --check` 与 `git diff --check` 通过。
- `cargo metadata --locked --offline --no-deps --format-version 1` 确认工作区仍只有
  `contracts`、`data-collect`、`data-pipeline-core`、`data-pipeline-training` 四个成员。
  三个新 crate 的一级依赖边界不变：`contracts` 只直接依赖 `serde`；核心只直接依赖
  `contracts`、`rust_decimal`、`serde`、`serde_json`、`sha2`；training 只在核心合同之上
  增加 Arrow/Parquet、`sha2` 和测试用 `tempfile`。完整依赖树负向检查没有发现核心中的
  I/O、网络、Arrow/Parquet 或下游依赖，也没有发现 training 中的网络、SDK、
  `data-collect`、模拟器、Ledger、Python 或 DataFusion 路径。
- 阶段自审确认任务 0.7 的每行改动均可追溯到审查发现：没有新公共 API、第二预算路径、
  配置泛化、读取路径扩张或阶段 1/2 功能；需求、计划、变更记录和实现之间没有新的现行
  冲突。当前仍未关闭阶段 0、未提交、未推送；完整阶段验证不能替代下一轮广泛独立阶段复审。

### 广泛独立阶段复审第三轮与方案裁定

- 新的 `gpt-5.6-sol`、`ultra` 主审从头复核完整三件套、所有当前代码、黄金文件、依赖边界
  和未跟踪交付文件，并派出合同/核心与 training 边界两个只读子审查。独立 `/tmp` 构建重新
  验证核心 28 项、training 7 项、消费/权威守卫 6+4+4 项、三个严格 Clippy、格式与差异；
  结果全部通过且工作树未改变。
- 合同/核心子审查为 `Critical 0 / Important 0 / Minor 0`；主审整体结论为
  `Critical 0 / Important 1 / Minor 0`，阶段 0 仍不能关闭。唯一 Important 是
  `sort_rows_with_arrow` 同时保留 `slots: Vec<Option<SortRow>>` 和
  `sorted: Vec<SortRow>`，随后 `sorted.into_iter()` 的底层分配又与
  `RunWriter.buffer` 并存。它们是真实 Rust 自有行描述符容器，不是已经排除的 Arrow 数组、
  读取游标、RSS 或分配器隐藏开销。
- 当前工具链同布局探针得到 `SortRow=48B`、`Option<SortRow>=48B`；审查场景中的
  `slots cap=4` 与 `sorted cap=2` 同时需要 288 字节描述符，而任务 0.7 记录的单一初始
  缓冲 `cap=4` 只有 192 字节。内层键/载荷所有权只转移一次，不属于该差额。现有指标仍只
  报告单个缓冲，因此全部测试绿灯不能覆盖这项并存容量。
- 该发现不产生用户决策门：`DPT-BND-001~003` 与任务 0.7 已要求狭义自有行缓冲有硬上限，
  上述容器明确属于该集合；这是实现和测试没有满足既有语义，不是需求、计划或上下文之间
  存在两个可选权威。审查同时确认不得把范围扩到读取游标、合并堆、Arrow/Parquet 内部、
  RSS、分配器隐藏开销、进程总内存或通用预算器。
- 独立方案审查比较了两条路径。保留所有容器再合并计量会改变 `max_run_bytes` 和现有两个
  峰值指标的含义，并会让任务 0.7 的紧预算初始 run 在排序阶段直接失败；还容易把范围扩到
  合并读取状态。采用原地复用则保留现有预算和指标语义，不新增失败路径或公共字段。因此
  选择最小方案：只消费 Arrow 索引，以安全 O(n) 置换复用同一 `Vec<SortRow>`；初始 run
  直接按 `batch_rows` 写出而不分配 `RunWriter.buffer`；合并和空输入路径保持不变。
- 当前仍未修改生产代码。任务 0.8 必须先让“Arrow 排序复用原分配”测试在旧生产代码上取得
  红灯并完成原地置换，再单独让“单一初始 run 无自有写缓冲”测试在仍保留旧写缓冲的代码上
  取得第二个红灯；两轮不能合并，避免一个修正或失败遮蔽另一个行为。

### 任务 0.8 第一轮：Arrow 索引原地置换

- 只新增 `arrow_sort_reuses_input_sort_row_allocation` 后、生产代码修改前，实际运行
  `cargo test -p data-pipeline-training --lib tests::arrow_sort_reuses_input_sort_row_allocation -- --exact`；
  命令未使用 `--locked --offline`。测试在当时的
  `data-pipeline/training/src/lib.rs:1183` 以 `0 passed; 1 failed` 取得红灯：旧路径返回的指针
  与输入指针不同，返回 capacity 为 7，而输入实际 capacity 为 32。
- 该测试使用 7 个乱序行，Arrow 排序索引包含一个六节点置换环和一个不动点；输入只显式
  预留小型 capacity 32，不硬编码 `SortRow` 大小、分配器地址或扩容倍率。组合断言同时比较
  指针和 capacity，直接证明旧 `slots`/`sorted` 路径没有复用原行描述符分配。
- 红灯确认后，只把 `sort_rows_with_arrow` 改为可变接收原 `Vec<SortRow>`：仍由
  `lexsort_to_indices` 唯一决定目标位置到原位置的映射；先验证索引长度、界内、唯一和完整，
  再用一个受行数约束的 `Vec<bool>` 访问位图与安全 `swap` 完成 O(n) 环置换。没有调用 Rust
  第二排序实现、使用 `unsafe` 或建立新的 `Vec<SortRow>`/`Vec<Option<SortRow>>`。
- 主代理在最终第一轮代码上使用带 `--locked --offline` 的同一精确测试重新验证为
  `1 passed; 0 failed`。实现代理同时验证既有 Arrow 顺序测试、配置无关摘要测试、training
  8 项完整测试、严格 Clippy、格式和差异均通过。
- 本轮没有修改 `flush_initial_run`、`RunWriter` 或中途切 run 的 `replace`，因此初始写入仍
  会分配第二个行缓冲；下一轮必须在当前原地排序代码上先取得独立红灯，不能把本轮绿灯当作
  整个 Important 已解决。

### 任务 0.8 第二轮：初始 run 无自有写缓冲

- 在第一轮原地排序已经转绿、生产写入路径尚未修改时，只新增
  `single_initial_run_writes_without_owned_writer_row_buffer`，并实际运行
  `cargo test -p data-pipeline-training --locked --offline --lib tests::single_initial_run_writes_without_owned_writer_row_buffer -- --exact`。
  测试在当时的 `data-pipeline/training/src/lib.rs:1238` 以 `0 passed; 1 failed` 取得独立红灯：
  前置的输出顺序、输入/输出行守恒、`initial_runs=1` 和 `merge_passes=0` 断言均已通过，唯一
  失败是写缓冲峰值实际为 `(1, 272)`、期望为 `(0, 0)`。因此该红灯没有被第一轮排序分配行为
  遮蔽，也没有把后续收紧的测试冒充为历史证据。
- 红灯确认后，只修改外部排序试验的内部所有权和写入路径：中途切 run 使用
  `std::mem::take`，完成旧 run 写入后才重新预留下一只 `pending_rows`；初始 run 复用原地排序
  后的同一 `Vec<SortRow>`，通过私有无缓冲构造入口按 `batch_rows` 借用分片并复用既有
  ArrowWriter、配额错误映射、行计数和原子改名路径。合并仍使用原 `RunWriter.buffer`，空输入
  仍走原有带缓冲发布路径。没有新增公共 API、配置/指标/错误字段、第二个排序实现或通用写入
  抽象，也没有扩大到读取游标、合并堆、Arrow/Parquet 内部、RSS 或进程总内存。
- 同一精确命令在最小实现后转为 `1 passed; 0 failed`。主代理随后独立重跑第二轮与第一轮
  两个新测试、任务 0.7 两项原测试、既有 Arrow 顺序、多 run 多轮合并和配置无关逻辑摘要
  七项精确测试，均为 `1 passed; 0 failed`。当前仍须完成任务 0.8 专项独立复核、阶段 0 完整
  验证和新的广泛独立阶段复审；本节不宣称阶段 0 已完成，不提交且不推送。

### 任务 0.8 专项独立复核与测试强化

- `gpt-5.6-sol`、`ultra` 专项审查独立核验实现、计划和测试，并在 `/tmp` 构建目录运行
  training 9 项完整测试、七项逐一精确测试、严格 Clippy、格式和差异检查；全部通过。审查还
  对 0 至 8 行的全部 46,234 个有效置换作穷举推演，确认目标位置到原位置的环置换正确。
- 专项结论为 `Critical 0 / Important 0 / Minor 1`；此前重复 `SortRow` 容器的 Important 已
  关闭。唯一 Minor 是既有多 run/多轮合并测试只验证写缓冲峰值不超过上限，没有直接锁定
  计划步骤 0.8.5 要求的“发生合并时峰值大于 0”。当前生产路径本身正确，缺口只在集成测试
  的回归锁定强度。
- 按最小建议只在既有 `forces_multiple_runs_and_multiple_merge_passes` 增加写缓冲行数和字节
  峰值均大于 0 的断言，同时保留原上限断言；没有修改生产代码，也没有固定分配器相关精确
  字节值。该最终实现上的测试强化首次运行即为 `1 passed; 0 failed`，只属于绿灯回归证据，
  不冒充新的历史红灯。
- 空输入当前仍静态走原带缓冲发布路径；缺少专门的空输入动态用例只登记为剩余测试风险，
  不在本轮新增范围或生产行为。专项审查者复核精确测试后裁定原 Minor 已关闭，结论为
  `Critical 0 / Important 0 / Minor 0`；正峰值断言没有固定容量或精确字节值，不构成过度
  校验。当前须完成阶段 0 全量验证和新的广泛独立阶段复审。

### 任务 0.8 后的完整阶段验证与自审

- 基于当前三件套和最终 Task 0.8 代码重新运行阶段 0 完整测试矩阵：`contracts` 的单元测试
  入口和文档测试入口通过；`data-pipeline-core` 为 28 项合同测试通过；
  `data-pipeline-training` 为 9 项排序试验通过且文档测试入口通过；既有
  `consumer_format_compatibility_contract`、`training_consumer_boundary_contract` 和
  `document_authority_contract` 分别为 6、4、4 项通过。
- `contracts`、`data-pipeline-core` 和 `data-pipeline-training` 分别运行全部目标的严格
  Clippy 并使用 `-D warnings`，三者均通过；`cargo fmt --all -- --check`、已跟踪
  `git diff --check` 及 10 个未跟踪交付文件的逐文件空白检查均通过。
- `cargo metadata --locked --offline --no-deps --format-version 1` 仍只列出 `contracts`、
  `data-collect`、`data-pipeline-core` 和 `data-pipeline-training` 四个工作区成员。一级依赖仍为：
  `contracts` 只直接依赖 `serde`；核心只直接依赖 `contracts`、`rust_decimal`、`serde`、
  `serde_json`、`sha2`；training 只在核心合同之上增加 Arrow/Parquet、`sha2` 和测试用
  `tempfile`。完整依赖树负向扫描没有发现核心中的 I/O、网络、Arrow/Parquet 或下游 crate，
  也没有发现 training 中的网络、SDK、`data-collect`、DataFusion 或 Python 路径。
- 源码负向扫描确认三个新 crate 没有 `serde_json::Value` 任意探测、`unsafe`、Rust 第二排序、
  `NotEvaluated` 或旧回执状态类型；`CapabilityStatus`、`GateAReceiptV1`、全序编码和能力矩阵
  摘要仍各有唯一权威定义。阶段自审没有发现 Task 0.8 引入公共 API/配置/指标/错误扩张、影子
  路径、阶段 1/2 偷跑或新的需求/计划/代码冲突。
- 当前验证通过不能替代项目要求的广泛独立阶段复审；阶段 0 仍未关闭、未提交、未推送。

## 2026-08-04：任务 0.9 审查修复波次

### 用户裁决与边界

- 用户批准推荐方案 2：历史名称 `ConsumerKind::Training` 继续表示下游 `model-train`。
  `model-train` 进程可直接读取 `manifest`、`source_coverage`、`source_health`、
  `source_gap`、`reconnect_trace`、`backpressure_state` 六类角色，但它们只能用于血缘、
  审计和质量诊断；不得用于训练准入、切分、样本选择或决策触发，也不得进入 Observation、
  特征、标签、训练样本或模型输入。训练数据和模型输入只来自版本化 DPT 输出。
- 当前仓库尚无 `model-train` 实现，因此本轮只复用 `data-collect` 已有精确允许、必需和禁止
  角色集合形成机械边界守卫；不新增无法由实际路径执行的字段、抽象或占位实现。

### Gate A 三项真实红灯与最小修正

三项测试均先只加入 `data-pipeline/core/tests/phase0_contract.rs`，再在生产代码修改前逐项
运行。共同命令前缀为：

```text
CARGO_TARGET_DIR=/tmp/sim-system-task09-target cargo test -p data-pipeline-core --locked --offline --test phase0_contract <测试名> -- --exact
```

1. `capability_matrix_rejects_whitespace_only_contract_field_path`：退出码 `101`，
   `0 passed; 1 failed; 30 filtered out`；失败断言为
   `called Result::unwrap_err() on an Ok value: ()`。
2. `pass_capability_rejects_whitespace_only_evidence_id`：退出码 `101`，
   `0 passed; 1 failed; 30 filtered out`；失败断言同样证明旧校验返回 `Ok(())`。
3. `fail_capability_rejects_whitespace_only_blocking_reason`：退出码 `101`，
   `0 passed; 1 failed; 30 filtered out`；失败断言同样证明旧校验返回 `Ok(())`。

三项红灯全部确认后，只把 `validate_sorted_unique_strings` 的条目空串判断和 `FAIL` 的
`blocking_reason` 判断收紧为 `trim().is_empty()`；没有新增类型、帮助抽象或改变排序、唯一
性语义。三项使用相同精确命令分别转为 `1 passed; 0 failed`。

### Observation 两项真实红灯、构造器与黄金文件

生产代码和测试构造器修改前，使用上述同一命令格式逐项运行：

1. `base_observation_visible_component_matches_tick_size_change_event`：退出码 `101`，
   `0 passed; 1 failed; 32 filtered out`；失败明确为
   `left: PolymarketL2`、`right: CurrentTick`。
2. `base_observation_visible_component_is_not_older_than_its_event`：退出码 `101`，
   `0 passed; 1 failed; 32 filtered out`；失败消息为
   `visible component precedes its visible event`。

随后只把 `base_observation()` 的存在组件改为冻结顺序索引 2 的 `CurrentTick`，把
`as_of_available_at_utc_ms` 改为事件可用时间 `1200`、`information_age_ms` 改为 `50`，并
同步既有负例引用的可见组件索引。两项精确测试分别转为 `1 passed; 0 failed`。当前编码输出
只改变黄金文件中的 `polymarket_l2/current_tick` 可见状态、对应缺失掩码和
`canonical_hash_sha256=cfb0c4eee045a9df34c200dc3cd06f74f9683fcdaef98e069fd1d5e7e5fca788`；
九个组件顺序、事件 ID、最终全序键和因果截止点保持不变。黄金合同精确测试转为
`1 passed; 0 failed`，没有新增跨对象生产校验。

### 模型训练元数据边界与当前验证

- `data-collect/src/manifest.rs` 只为现有三组角色常量补充用途注释；原
  `training_consumer_boundary_contract` 直接断言六类允许集合、四类版本化 DPT 必需输出
  集合和八类禁止集合。该断言首次运行即为 `1 passed; 0 failed; 3 filtered out`，只记作
  现有行为的特征化绿灯，不冒充实施前红灯。
- 当前阶段验证快照中，`cargo test -p data-pipeline-core --locked --offline` 为
  `33 passed; 0 failed`；`consumer_format_compatibility_contract`、
  `document_authority_contract`、`manifest_contract`、`training_consumer_boundary_contract`
  分别为 `6`、`4`、`5`、`4` 项通过。
- 严格 Clippy 命令
  `CARGO_TARGET_DIR=/tmp/sim-system-task09-target cargo clippy -p data-pipeline-core -p data-collect --all-targets --locked --offline -- -D warnings`
  与 `cargo fmt --all -- --check`、已跟踪 `git diff --check` 已通过；三个受影响未跟踪文件的
  `git diff --no-index --check /dev/null <文件>` 均无空白错误输出。独立阶段复审仍须完成；
  本节不宣称阶段 0 或任务 0.9 已最终通过，不提交且不推送。

### Task 0.9 范围复审关闭

- 首轮范围复审结论为 `Critical 0 / Important 0 / Minor 1`；唯一 `Minor` 是任务简报仍把六类
  `data-collect` 角色写成可用于“准入”元数据，与已落盘的训练准入禁止边界不一致。
- 只对 `.superpowers/sdd/data-pipeline-training-plan/task-0.9-brief.md` 做机械文字修正：改为
  “血缘、审计和质量诊断元数据”，并明确不得用于训练准入、数据切分、样本选择或决策触发；
  没有修改代码、测试、需求或窄事项三件套。
- 修正后范围复审最终结论为 `Critical 0 / Important 0 / Minor 0`，Task 0.9 范围复审关闭。
  该结论不代表阶段 0 已通过或关闭；下一步仍是完整阶段验证和新的广泛独立阶段复审。

### Task 0.9 后的完整阶段验证与自审

- 契约入口测试通过；`data-pipeline-core` 为 `33/33`，`data-pipeline-training` 为 `9/9`。
- `data-collect` 的 `consumer_format_compatibility_contract`、
  `source_format_contract`、`training_consumer_boundary_contract`、
  `manifest_contract`、`document_authority_contract` 分别为 `6/6`、`6/6`、`4/4`、`5/5`、
  `4/4`。
- `contracts`、`data-pipeline-core`、`data-pipeline-training`、`data-collect` 四个
  crate 的全部目标均通过 `-D warnings` 严格 Clippy；`cargo fmt --all -- --check`、已跟踪
  `git diff --check` 和 10 个未跟踪文件的空白检查均通过。
- `cargo metadata` 仍显示四个工作区成员；三个新 crate 的一级依赖边界保持不变。
- 生产源码负向扫描未发现 `unsafe`、第二套排序、旧回执类型、禁用依赖、核心 I/O 或
  `serde_json::Value`。测试模块中的两处 `sort_by` 只用于独立全序验证，不是生产影子排序。
- 上述证据只表示 Task 0.9 后的完整阶段验证与自审通过，不代表新的广泛独立阶段复审已经
  通过，也不关闭阶段 0；本轮不提交、不推送。

### 阶段 0 最终广泛独立审查与关闭

- 最终审查重新全新执行完整验证：`contracts` 入口通过，`data-pipeline-core` 为 `33/33`，
  `data-pipeline-training` 为 `9/9`；`data-collect` 的
  `consumer_format_compatibility_contract`、`source_format_contract`、
  `training_consumer_boundary_contract`、`manifest_contract`、
  `document_authority_contract` 分别为 `6/6`、`6/6`、`4/4`、`5/5`、`4/4`。
- `contracts`、`data-pipeline-core`、`data-pipeline-training`、`data-collect` 四个 crate 的全部
  目标均通过 `-D warnings` 严格 Clippy；格式检查、已跟踪差异检查、10 个未跟踪文件的空白
  检查、`cargo metadata`、一级 `cargo tree` 依赖边界和生产源码负向扫描全部通过。
- 生产源码负向扫描未发现 `unsafe`、第二套排序、旧回执类型、禁用依赖、核心 I/O 或
  `serde_json::Value`；测试模块中的两处 `sort_by` 仍只用于独立全序验证，不是生产影子排序。
- 首次全新验证因 `/tmp` 配额不足而失败；释放约 `18.7G` 旧的可再生 `/tmp` 构建缓存后，
  同一验证重新执行并通过。子审查另有一次漏设 `CARGO_TARGET_DIR`，因此触碰工作树中被 Git
  忽略的 `target/` 构建缓存；该事件没有产生交付物或 Git 变化，`target/` 保留且不清理。
- 最终广泛审查唯一 `Minor` 是变更记录中的两个精确标识笔误：
  `source_format_compatibility_contract` 与 `data-pipeline-contracts`。两处已分别机械修正为真实
  测试目标 `source_format_contract` 和真实包名 `contracts`；最终复核结论为
  `Critical 0 / Important 0 / Minor 0`。
- 最终审查未发现过度设计或需要用户决定的新冲突，阶段 0 实质关闭门已满足。阶段 0 本地阶段
  提交只归档已经审查的差异，当前外部推进门为推送授权；未推送、未进入阶段 1。

## 2026-08-04：Task 0.10 重新打开阶段 0，恢复 tick 的上游事件权威

### 用户决定与错误复核

- 用户明确纠正：`sim-system` 冷启动不得包含 tick。逐资产初始 tick 必须来自
  `data-collect` 冷路径保存的训练证据，paper 热路径则由 `data-collect` 实时交接进入在线
  `data-pipeline`；两者最终都只经 `ExecutionMarketEvent` 传给 `sim-system`。用户同时批准
  其余最小合同修正：动态旧 tick 合法未知、三个平台交易状态独立、公开成交身份来源可辨，
  以及历史规则证据、私有校准证据、固定二元兑付和 `delayed` 可见性文字修正。
- 用户随后批准删除 `InitialL2SnapshotV1` 中无真实消费者的初始/重同步共用分支。复核确认
  `L2SnapshotPurpose::{InitialLoad, Resync}` 由阶段 0 提交 `2b18359` 自行引入，获批需求只
  要求首个 `book` 装载基准、后续 `book` 形成确认或唯一净差额；仓库没有任何 `Resync`
  构造、测试或消费者，既有 `BookResyncDeltaInference` 和质量边界已经分别承担后续净差额与
  重同步状态。此前变更记录所称“初始/重同步快照”是过度设计，本节明确取代该表述。
- 只读复核证明该纠正正确。四日样本中，全部 156 个发生 tick 变化的市场在首次变化前均有
  metadata `orderPriceMinTickSize="0.01"` 证据；624 条物理 `tick_size_change` 行按
  `(market, asset, old, new)` 去重后是 312 个状态变化键。当前合同只表达动态变化，尚未表达
  上游初始 tick；当前代码并没有已经实施的模拟器冷启动 tick。
- 错误根因追溯到提交 `3145a635`：当时把 CLOB `mts` 与真正的低频规则一并纳入
  `PlatformRuleSnapshot`，又保留动态 tick 事件，形成双权威。审查中“当前 HTTP 值不能还原
  历史初始 tick”的发现正确，但据此继续设计模拟器冷启动 tick 是错误归属；本节取代此前
  相关决定，旧条目只保留历史事实。

### 冻结的最小修正与当前进度

- tick 唯一权威链冻结为
  `data-collect -> data-pipeline -> ExecutionMarketEvent -> sim-system`。初始 L2 快照增加
  必填正数逐资产初始 tick，并删除 `L2SnapshotPurpose` 与 `purpose`，只表示首次装载；后续
  完整 `book` 只沿既有净差额转换和质量边界处理。动态旧 tick 允许缺失。规则快照的
  `CURRENT_QUERY` 与 `HISTORICAL_AS_OF` 两种证据模式都不包含、不查询也不交叉校验 tick。
- 三个平台布尔状态使用独立事件载荷，不扩展生命周期枚举或建立组合状态机；公开成交继续
  使用字符串 `trade_id`，只增加平台交易哈希或派生源证据的必填来源枚举。既有能力枚举、
  `data-collect` schema、网络边界和未来映射器均不扩展。
- 本事项需求、计划及审核中的 `sim-system`、`rebuild-core` 文档已开始同步，阶段 0 重新打开。
  本节记录时尚未修改生产代码；后续必须先逐行为取得互不遮蔽的真实红灯，再做最小实现、
  完整验证和新的独立阶段复审。推送仍未获授权。

### Task 0.10 独立红灯与最小实现证据

生产代码修改前，以下六项分别使用
`CARGO_TARGET_DIR=/tmp/sim-system-task010-target cargo test -p data-pipeline-core --locked --offline --test phase0_contract <测试名> -- --exact`
运行，均以退出码 `101`、`0 passed; 1 failed; 38 filtered out` 形成互不遮蔽的红灯：

1. `initial_l2_snapshot_decodes_source_initial_tick`：明确失败于未知字段
   `initial_tick_size_pusd_per_share`；
2. `initial_l2_snapshot_requires_source_initial_tick`：明确证明缺失初始 tick 仍被接受；
3. `tick_size_change_accepts_unknown_old_tick`：明确失败于 `null` 不能解码为字符串；
4. `market_trading_state_roundtrips_independent_flags`：明确失败于不存在
   `market_trading_state` 事件变体；
5. `public_trade_id_source_roundtrips`：明确失败于未知字段 `trade_id_source`；
6. `public_trade_id_source_is_required`：明确证明缺失成交 ID 来源仍被接受。

先只增加对应字段、枚举和载荷后，上述六项分别转为 `1 passed; 0 failed`。随后新增
`initial_l2_snapshot_rejects_nonpositive_source_initial_tick`，在初始 tick 尚未进入正数校验时
以退出码 `101`、`0 passed; 1 failed; 39 filtered out` 形成独立红灯；只复用既有
`validate_positive_decimal` 后转为 `1 passed; 0 failed`。平台三状态逐字段可空、成交来源未知
枚举负例和已知旧 tick 非正数拒绝作为新类型或既有不变量的补充回归覆盖直接通过，不冒充
实施前红灯。

用户批准删除多余用途分支后，又在删除生产类型前分别执行：

1. `initial_l2_snapshot_decodes_without_purpose`：退出码 `101`，明确失败于缺少必填
   `purpose`；
2. `initial_l2_snapshot_rejects_legacy_purpose`：退出码 `101`，明确证明旧合同仍接受
   `purpose`。

两项均为 `0 passed; 1 failed; 43 filtered out`。只删除 `L2SnapshotPurpose`、`purpose` 和测试
构造器中的旧值后，两项分别转为 `1 passed; 0 failed`；没有添加兼容旧字段的双路径。当前
`phase0_contract` 全集在补齐已知旧 tick 正数回归后为 `45 passed; 0 failed`，稳定基础 tick
事件黄金编码未变化。该结果是
Task 0.10 的定向绿灯，不代替后续完整阶段验证和独立阶段复审。

### 文档权威守卫的阶段游标修正

- 完整验证首次运行 `document_authority_contract` 时为 `2 passed; 2 failed`。两项失败都源于
  守卫仍要求三件套状态包含旧终态“阶段 0 已通过；停在推送授权门”，而三件套已按本次批准
  的修正如实标为阶段 0 重新打开；事项编号、生命周期、执行权威、执行授权和互引路径均未
  发生冲突。
- 守卫改为只在三件套状态中钉住稳定事实“用户已批准”，继续逐字段精确校验
  `APPROVED_REQUIREMENTS`、`APPROVED_ACTIVE_PLAN`、`ACTIVE_IMPLEMENTATION_CHANGELOG`、
  `plan/data-pipeline-training-plan.md` 和 `LOCAL_IMPLEMENTATION_ONLY`。既有负例仍把“用户已批准”
  替换为“需求草案”并要求拒绝，因此没有把草案、无权威或越权授权放行，也没有为临时阶段游标
  增加解析器或状态机。
- 自审同时发现计划任务 0.2 仍只写动态 tick 和当前时点冷启动，虽然后续任务 0.10 已取代该
  语义，但会在同一活动计划内形成歧义。现已把任务 0.2 同步为“初始与动态 tick 都归事件带，
  当前查询与历史截至时点规则快照均不含 tick”，没有改变实施范围。

### 首轮独立阶段复审问题

- 首轮独立复审结论为 `Critical 0 / Important 1 / Minor 1`，因此没有关闭阶段或提交。
- `Important` 是 `data-collect/docs/current/acceptance.md` 仍把 `orderPriceMinTickSize` 归为
  模拟器冷启动规则的直接重叠候选，并遗漏本次当前文档验收快照。现已按最新决定追加明确
  取代条目，并同步 `data-collect` 主三件套、`CURRENT`、当前合同和变更记录；旧测试计数只作
  当时快照保留。
- `Minor` 是动态旧 tick 测试只覆盖显式 `null`，没有直接覆盖源字段缺失。生产类型
  `Option<ExactDecimal>` 已正确接受缺失；现于同一测试补充缺字段解码、校验、规范编码为
  `null` 和往返断言。该断言在生产实现已存在后增加，只记为直接绿灯回归，不冒充实施前红灯。

### 首轮复审修正后的验证

- 动态旧 tick 缺字段回归以 `1 passed; 0 failed; 44 filtered out` 直接通过；完整核心合同为
  `45 passed; 0 failed`，训练排序试验为 `9 passed; 0 failed`。
- `data-collect` 的 `consumer_format_compatibility_contract`、`source_format_contract`、
  `training_consumer_boundary_contract`、`manifest_contract`、`document_authority_contract`
  分别为 `6/6`、`6/6`、`4/4`、`5/5`、`4/4`。四个包的全部目标通过 `-D warnings` 严格
  Clippy；格式、差异、工作区元数据和三个一级依赖树检查通过。
- 第一次重跑三个核心包全集时，链接器在写旧测试二进制过程中以
  `ld terminated with signal 7 [Bus error]` 退出；同一 `/tmp` 挂载尚余约 12 GiB，独立复审
  的新构建目录此前也出现过 `Disk quota exceeded`。没有删除或改写任何工作树、共享缓存或
  用户数据；立即单独重跑核心合同 `45/45` 通过，随后原三个包命令完整通过。该环境异常不冒充
  测试失败，也不从记录中隐藏。
- 第二轮复审发现当前合同文档的一处编辑残留把 metadata v5 的
  `source_evidence_payload_json.orderPriceMinTickSize` 错写成归属
  `polymarket_orderbook_v3`。现已只修正归属名；orderbook v3 仍只负责
  `old_tick_size/new_tick_size` 动态事件字段，没有改变 schema 或代码。
- 同轮完整当前文档复查又发现 `data-collect/docs/current/known-limitations.md` 仍把所有
  `sim-system` 新运行笼统写成查询当前平台。现已同步为只有当前平台时点运行形成
  `CURRENT_QUERY`、历史运行只绑定 `HISTORICAL_AS_OF`，并明确两种规则证据均不含 tick。

### 第二轮独立阶段复审与关闭

- 第二轮最终复审结论为 `Critical 0 / Important 0 / Minor 0`。复审确认首轮两项问题、
  metadata/orderbook 字段归属残留和当前限制文档残留均已关闭。
- 最终差异没有 `L2SnapshotPurpose`、`InitialLoad`、第二条完整快照应用路径、影子映射器、
  网络查询、能力枚举扩张或组合状态机；没有发现过度校验或过度设计。当前仅冻结阶段 0
  公共合同，尚未实现映射器、真实事件生产、模拟器消费或规则加载器，这些是计划内后续工作，
  不是本轮缺陷。
- 阶段 0 修正关闭门已经满足。下一动作只允许创建本地阶段修正提交；推送仍未获授权，不能
  进入阶段 1。

## 2026-08-04：退役跨模块文档守卫

### 用户决定与原因

- 用户确认 `data-collect` 不属于当前 DPT 开发目标，仓库此前也不依赖该文档守卫完成业务
  开发；守卫反而多次因跨模块文档和阶段叙事变化制造伪冲突。
- 用户批准完整删除守卫、清理当前引用、保留历史运行记录且不建立替代守卫；该决定不授权
  修改 `data-collect` 生产代码、schema、数据、运行时、部署或远端状态。

### 删除前证据

- 退役验收命令要求守卫文件和现行依赖均不存在；修改前它按预期退出 `1`，形成真实红灯。
- 既有 `document_authority_contract` 随后使用当前工作树构建缓存运行，结果为
  `4 passed; 0 failed`。首次尝试使用新的 `/tmp` 构建目录时在依赖编译阶段因
  `Disk quota exceeded` 退出；该目录由本次命令创建且已删除，测试改用当前工作树缓存后
  正常通过。删除守卫不是对测试失败的规避。

### 最小实现与验证

- 删除 998 行的 `data-collect/tests/document_authority_contract.rs` 和仅为其存在的 118 行
  `data-collect/docs/current/document-authority-audit.md`。
- 从 DPT 与 `data-collect` 当前需求、计划、验收命令和路由中删除现行守卫要求；已完成事项
  和带日期验收中的旧测试结果不改写，只由新的退役条目明确取代其当前执行含义。
- `cargo metadata --locked --offline --no-deps --format-version 1` 已确认不再存在
  `document_authority_contract` 测试目标；聚焦退役验收通过。
- `cargo test -p contracts -p data-pipeline-core -p data-pipeline-training --locked --offline`
  通过，其中核心合同 `45 passed`、训练 `9 passed`，其余为无单元测试的合同包和文档测试。
- 三个 DPT 包全部目标的严格 Clippy、`cargo fmt --all -- --check` 和 `git diff --check`
  通过。没有运行或声称 `data-collect` 生产回归；当前改动不触及其生产实现。

## 2026-08-04：测试职责与文档职责收敛

### 用户决定

- 需求只保留产品经理层面的宏观业务目标与验收；计划成为当前业务语义和技术实施的唯一
  规范来源；changelog 只记录当时事实和后续取代关系。
- 新决策直接覆盖或删除当前需求与计划的旧内容；禁止 Rust 测试搜索 Markdown 固定文本，
  不建立测试登记表、文档守卫或新治理系统。
- 网络帧、Parquet、JSON、manifest 和跨模块持久产物继续按不可信边界处理。测试采用真实
  样本正例、每类可达损坏的一个代表性输入和真实事故回归，不按字段、枚举、`NaN`、无穷或
  其他理论非法数值排列组合。
- tick 只保留一个“非正值不能通过 JSON 解码进入规范事件”的代表性边界测试；删除分别在
  初始 tick 和动态旧 tick 内部构造非法状态的两个重复测试。

### 修改前证据与实施范围

- 聚焦清理验收在修改前因 `deploy_readme_contract` 仍存在、两个旧 tick 测试仍存在且新边界
  测试尚不存在而退出 `1`。
- `deploy_readme_contract` 删除前为 `6 passed; 0 failed`；两个旧 tick 测试分别以
  `1 passed; 0 failed; 44 filtered out` 通过。它们证明原测试可运行，不构成保留理由。
- 本轮删除整个 `deploy_readme_contract` 测试目标，把两个非正 tick 测试合并为一个经
  `decode_execution_market_event` 的 JSON 边界测试，并覆盖需求、计划、仓库规则和当前验收
  中的旧穷举或文档权威要求。没有修改生产合同或运行时行为。

### 验证结果

- 新边界测试直接为 `1 passed; 0 failed; 43 filtered out`。它复用既有生产校验，只是把测试
  放回真实 JSON 解码边界，因此属于特征化绿灯，不冒充 TDD 实施红灯。
- 首次合并运行三个 DPT 包时，核心合同 `44/44` 通过；训练包有两项通过、七项均因操作系统
  返回 `Disk quota exceeded` 失败。只读检查确认 `/tmp` 为带 `usrquota` 的 tmpfs；最小
  1 MiB 实写在 `/tmp` 复现相同错误，而 `/dev/shm` 成功，证明失败来自临时目录用户配额，
  不是本轮合同或断言变化。探针文件和目录随后逐一删除。
- 仅把本次命令的 `TMPDIR` 指向独立 `/dev/shm` 临时目录后，三个 DPT 包合并验证为
  `contracts 0/0`、`data-pipeline-core 44/44`、`data-pipeline-training 9/9` 通过；临时目录
  在运行后删除，未清理共享 `/tmp`、构建缓存或用户数据。
- 三个 DPT 包全部目标的严格 Clippy、`cargo fmt --all -- --check`、`git diff --check`、
  Cargo 退役目标检查、旧 tick 测试名清除检查和测试代码 Markdown 搜索检查均通过。
- 未运行或声称 `data-collect` 生产回归；本轮没有修改其生产实现。

### 后续审查、职责迁移与取代关系

- 后续独立审查确认，最初 `44/44` 的绿灯只能证明当时聚合测试的首个断言：多个不同损坏
  类别串在同一测试时，前置失败会遮蔽后续行为；直接篡改已经构造的强类型对象也绕过了真实
  JSON 边界。该结果仍作为当时事实保留，但不再作为本轮最终验收结果。
- 在删除技术正文前先完成语义迁移：DPT 需求由 740 行压缩为 129 行，只保留宏观业务目标、
  范围和 19 项结果级验收；当前技术语义全部迁入计划，计划由 519 行调整为 595 行并成为唯一
  规范来源。`data-collect` 主需求由 129 行压缩为 48 行，主计划由 72 行补足为 111 行；模型训练
  消费边界需求由 96 行压缩为 40 行，其计划保留六类诊断元数据和消费权限的精确规范。
- `data-collect/docs/current/` 的五份派生页由 `CURRENT_CONTRACT` 改为
  `CURRENT_REFERENCE`，执行 authority 设为 `none`，并明确它们不能新增或改变主计划语义。
  当前需求与计划删除或覆盖旧职责文本；changelog 只保留当时事实和本段取代关系。
- 核心合同测试删除同类排列和理论极值正例，把不同业务边界拆成独立 JSON 解码测试，并将
  冻结排序向量改为现实正 Unix 毫秒和普通 epoch。测试函数由 45 个变为 68 个，但断言行由
  100 行降为 78 行、直接篡改强类型后断言内部校验失败的 34 处降为 0，文件净减少 65 行。
  函数数量增加来自不同业务边界独立执行，不代表新增损坏排列。
- 训练测试把六种资源失败中的五类可达边界拆开，删除第二个同类 `InvalidConfig` 排列、锁定
  `Vec` 指针和容量的私有实现测试，并让两项容量事故回归使用真实 `row(1)` / `row(2)`；
  写入器回归改为公开高水位指标和 Parquet 回读。测试函数由 9 个变为 12 个，断言行由 49 行
  降为 43 行。

### 最终本地验证

- `TMPDIR=/dev/shm cargo test -p contracts -p data-pipeline-core -p data-pipeline-training
  --locked --offline` 通过：`contracts 0/0`、核心合同 `68/68`、训练 `12/12`，全部文档测试通过。
- `TMPDIR=/dev/shm cargo check -p data-collect --locked --offline --tests` 通过；该命令只确认删除
  自动发现的测试目标后现有目标仍可编译，不构成生产运行或部署验收。
- 三个 DPT 包全部目标的严格 Clippy、`cargo fmt --all -- --check`、`git diff --check` 通过。
  Cargo metadata 中已无 `deploy_readme_contract` 和 `document_authority_contract`；静态检查确认
  旧 tick 测试名、测试代码 Markdown 路径读取、训练测试私有容器断言和 `.superpowers`
  治理目录均不存在。
- 本轮只修改规则、需求/计划/changelog、派生参考和测试；没有修改生产逻辑、schema、数据、
  运行时、部署或远端状态，也未创建提交或推送。

### 独立审查结论

- 稳定快照的最终独立审查结论为 `Critical 0 / Important 0 / Minor 0`，阶段 0 本轮完成门通过。
- 审查确认六项 Markdown/systemd/废弃入口测试和跨模块文档守卫均已退役；核心与训练测试
  不再保留同类排列、不可能内部状态、私有容器形状或具体 `serde` 错误文字耦合。
- 审查确认需求、唯一规范计划、历史 changelog 和非权威派生参考的职责一致，且本轮没有
  生产合同、schema 或运行时改动。当前仍未提交、未推送，也未进入阶段 1。

## 2026-08-04：批准阶段 0 决策向后续阶段的最小计划修正

### 用户决定与审查结论

- 用户批准修正阶段 0 新决策对阶段 1 至阶段 5 的实际影响，并同时批准修复审核中的
  `sim-system` 与 `rebuild-core` 文档；本次仍只处理文档，不进入阶段 1 代码实现，不授权
  提交、推送、发布、部署或远端状态变化。
- 只读纵向审查确认，DPT 的 19 项宏观验收和业务范围没有改变，因此需求保持不动；当前计划
  仍有两处已删除的 `PARTIAL` 状态残留，并缺少后续 TDD 顺序、阶段 0 已选技术路线、当前
  V1 真实映射与能力归属、联合纵向回执身份、门 B 运行谱系和新增资源边界的明确执行步骤。
- 审查还确认，联合纵向回执可以直接使用已有不可变联合清单的内容身份与 SHA-256，不需要
  新增登记系统或重复回执载荷；门 B 只直接绑定有独立持久前像的模拟运行清单、候选/最终
  训练数据集清单，不恢复此前删除的冗余派生摘要。

### 当前计划修正

- 删除阶段 1 和阶段 3 的 `PARTIAL` 残留，统一为失败诊断不产生门 A 回执、部分能力或失败
  诊断不满足阶段 3 入口；阶段 1 验收映射补回已经由资源任务实际承担的 `DPT-AC-010`。
- 在计划总则只增加一次适用于新增或改变可执行行为的“聚焦红灯 -> 最小实现 -> 受影响回归”
  顺序，并明确已有行为特征化、文档核对和资源测量不得强造红灯；没有建立测试登记表、文档
  守卫或逐字段负例矩阵。
- 把阶段 0 已验证的当前技术路线迁入唯一规范计划：继续使用锁定的单一 Arrow/Parquet 类型
  族、Arrow 索引排序、有界 Parquet run 和固定扇入堆合并，当前不引入会形成第二套 Arrow
  `RecordBatch` 类型和旧 `thrift` 的 DataFusion 路径；精确依赖版本和时点证据仍由 Cargo
  配置、`Cargo.lock`、构建清单与历史记录承担。
- 阶段 2 增加一个真实代表市场链路验收，覆盖初始 tick 建立 `CurrentTick`、后续 `book` 不
  再产生初始快照、未知旧 tick、独立交易状态和成交 ID 来源，并把这些语义映射到四项现有
  门 A 能力；没有增加能力枚举、Observation 组件、用途分支或逐字段测试矩阵。
- 阶段 3 把现有不可变联合清单的内容身份与 SHA-256 定义为 DPT/sim 联合真实纵向回执，
  阶段 4 候选绑定本次 `SimulationRunManifest` ID/SHA 并首次实测门 B 新增资源路径，阶段 5
  复用既有模型训练消费边界并让最终回执直接绑定候选和最终训练数据集清单 ID/SHA。

### 当前边界

- 本节只记录批准决定和实际文档修改；DPT 需求、生产代码、schema、数据、测试实现、运行时
  和部署均未改变。`sim-system` 与 `rebuild-core` 仍保持草案、`authority=none` 和
  `执行授权=none`，其文档修复不构成实现批准。
- 本轮完整静态核对、语言自检和独立审查尚未完成；完成前计划状态保持“文档修正中”，不得
  写成已通过或进入阶段 1。

## 2026-08-05：阶段 0 本地提交事实与文档迁移审查收口

### 已核实事实与取代关系

- 阶段 0 合同已在本地提交 `2b18359`；后续 tick 权威修正已在本地提交 `1817f87`。当前分支
  相对 `origin/sim-system` 为 `ahead 3`；本轮文档收口本身尚未提交或推送，且尚未进入阶段 1。
- DPT 后续计划独立审查结论为 `Critical 0 / Important 0 / Minor 0`。
- `sim-system` 文档迁移初审中关于 `orderPriceMinTickSize` 的意见被用户判定为过度收紧并驳回；
  随后的定向复审结论为 `Critical 0 / Important 0 / Minor 0`。
- `rebuild-core` 文档迁移初审的 `Important 3` 已按用户批准完成三项修复；随后的定向复审结论为
  `Critical 0 / Important 0 / Minor 0`。
- 上述三组结论只记录各自当前审查范围。全局最终独立审查尚未执行，不得由这些结果推导为
  全局终审完成、文档收口已提交/推送或已进入阶段 1。本条取代本文件中与上述当前提交和审查
  游标不一致的旧叙述；当前执行权威和游标仍仅由对应计划承担。

### 全局最终独立审查

- 九份文档的全局最终只读独立审查已完成并通过，结论为
  `Critical 0 / Important 0 / Minor 0`。
- 终审确认 DPT 的 19 项、`sim-system` 的 10 项和 `rebuild-core` 的 10 项验收均有覆盖；用户
  保留的 `orderPriceMinTickSize` 诊断边界准确，`rebuild-core` 三项修复已关闭，执行权威、
  路径和 Git 事实准确。
- 终审确认没有新增文档守卫、读取 Markdown 的 Rust 测试、登记系统或理论负例矩阵；最终
  `git diff --check` 退出码为 `0`。
- 本轮文档收口仍未提交、未推送，且未进入阶段 1。

## 2026-08-05：阶段 0 收口提交、推送与阶段 1 开发获批

- 用户明确授权创建阶段 0 完成提交并推送当前 `sim-system` 分支，随后进入阶段 1 本地开发。
  本次授权不延伸到阶段 1 的推送、发布、部署或远端状态变化。
- 提交前验证最初使用 `cargo test --workspace --all-targets --locked --offline`，该命令会执行现有
  `real_source_alignment_probe` 基准程序，并按其默认配置访问真实外部数据源、持续 45 分钟；
  这不是阶段 0 测试失败，也不是合适的本地提交门。该验证进程已中止，其生成的三个未跟踪
  临时产物已按精确路径清理，未纳入提交。
- 改用明确排除基准程序的 `cargo test --workspace --lib --bins --tests --examples --locked
  --offline` 后，全工作区库、二进制、示例和集成测试通过；严格 Clippy、格式检查和
  `git diff --check` 同步通过。阶段 0 收口提交只允许显式暂存已审查文件。

## 2026-08-05：阶段 1 任务 1.1 首个路径资格切片及审查修正

### 首次实现与审查结论

- 首次实现曾在路径检查后读取清单 JSON 的 `utc_day`，并返回名为
  `AdmittedSourceManifest` 的对象。该对象没有验证任务 1.1.2 才负责的 schema、`state`、
  关闭原因和覆盖对象，不能代表真实成功准入；本轮审查后不再把它记述为有效准入切片。
- 独立审查结论为 `Critical 1 / Important 2 / Minor 0`。Critical 是非法日期目录会失败开放，
  且词法合格日期可通过根内符号链接解析到日期下限前文件；Important 1 是提前解析 JSON 并
  返回语义不真实的“已准入”对象；Important 2 是原先使用本机真实旧日期路径，不能证明
  日期下限在候选文件 I/O 前拒绝。

### 审查修正的测试驱动开发事实

- 真实非法目录回归先用
  `TMPDIR=/dev/shm cargo test -p data-pipeline-training --test phase1_admission_contract
  invalid_date_directory_cannot_fail_open --locked --offline -- --ignored --exact` 运行，当前实现
  错误返回成功并触发断言，得到真实 `0 passed / 1 failed` 红灯。
- 根内跨日期符号链接回归的首条命令因 `--exact` 缺少 `tests::` 前缀而执行 `0 tests`，不作为
  证据；纠正为 `TMPDIR=/dev/shm cargo test -p data-pipeline-training --lib
  tests::canonical_manifest_path_rejects_symlink_to_older_day_inside_root --locked --offline --
  --exact` 后，当前实现返回规范旧日期路径并触发 `expect_err`，得到真实
  `0 passed / 1 failed` 红灯。两条红灯均在生产修正前取得。
- 日期下限前的集成测试已改用确认不存在的合成合法路径
  `/home/chillchill/new-datasets/1900-01-01/.../manifest.json`，避免本机真实旧数据的存在掩盖
  “候选 I/O 前拒绝”语义。

### 修正后的实现边界与验证

- 唯一公共入口改为 `qualify_direct_source_manifest_path`，只返回路径资格对象
  `QualifiedSourceManifestPath`，不读取或解析 JSON，不签发成功准入或门 A 回执。直接
  `serde`、`serde_json` 依赖及其锁文件依赖记录已移除。
- 词法路径先拒绝 `..`，并强制把首分量分类为合法 UTC 日；普通日期下限前路径在候选 I/O
  前失败。规范化后再次确认物理路径仍在固定根内，并从规范相对路径重新强制分类和执行日期
  下限；返回的 `utc_day` 只来自这条规范安全路径。非法、非 UTF-8 或缺失日期分量均以
  类型化错误失败关闭。
- 修正后可移植集成测试为 `2 passed / 2 ignored`，两个本机真实路径测试分别显式通过，
  `data-pipeline-training` 库回归为 `14 passed`，`data-pipeline-core` 阶段 0 合同回归为
  `68 passed`。严格 Clippy、格式检查、锁定离线元数据、一级依赖树和 `git diff --check`
  均通过；一级依赖树不含直接 `serde` 或 `serde_json`。
- 本切片仍未进入 schema、`state`、覆盖对象、清单文件集合、任务 1.2、副本协议或门 A，
  也不表示任务 1.1、阶段 1、提交或推送已经完成。

### 第二轮审查与修正

- 第二轮独立审查结论为 `Critical 0 / Important 1 / Minor 1`：路径资格对象的两个公共字段
  可被调用方绕过资格函数伪造；缺失、非 UTF-8 和格式非法日期使用三个错误变体，超过当前
  消费者需要的诊断粒度。
- 真实合格路径测试先改用 `manifest_path()` 和 `utc_day()`，聚焦运行因两个方法不存在得到
  编译红灯；随后字段改为私有，并只增加两个只读访问器，唯一构造仍由路径资格函数持有。
- 真实非法日期目录测试先改为单一 `InvalidDatasetDate { path }` 并因变体不存在得到编译红灯；
  随后原三个过细变体合并为该变体，缺失、非 UTF-8 和日期分类失败均保留候选路径，不新增
  理论排列测试。
- 第三轮需求符合性复审和独立代码质量审查均为
  `Critical 0 / Important 0 / Minor 0`；主代理重新执行可移植测试、两个本机真实路径测试、
  训练库与核心合同回归、严格 Clippy、格式、元数据、依赖树和差异检查，结果全部通过。
- 修正后默认集成测试为 `2 passed / 2 ignored`，两个真实本机测试与两个符号链接测试分别
  通过，训练库回归为 `14 passed`，阶段 0 核心合同回归为 `68 passed`；严格 Clippy、格式、
  差异、锁定离线元数据和一级依赖树检查均通过。

## 2026-08-05：阶段 1 步骤 1.1.2 每日清单窄头部校验

### 测试驱动开发事实

- 真实 `2026-07-30` 清单正例先调用尚不存在的
  `validate_direct_source_daily_manifest_header`，聚焦命令因 `E0432` 缺少公共入口得到真实
  编译红灯；最小实现先复用 `qualify_direct_source_manifest_path`，再读取并解析清单窄头部，
  同一真实正例随后通过。
- 旧 schema 使用合格日期 `2026-07-30` 的合成字节；生产校验前，解析器错误返回
  `schema_version=data_collect_daily_manifest_v2` 的成功对象并得到真实运行红灯。只增加
  `data_collect_daily_manifest_v3` 校验后聚焦测试通过。
- `state=open`、仅含空白的 `closure_reason`、`ended_known` 覆盖首末时间倒序，以及清单
  `utc_day=2026-07-31` 与安全路径日期 `2026-07-30` 不一致，均分别在对应生产校验前运行
  聚焦测试并得到“错误返回成功对象”的真实红灯；每项只增加对应最小失败关闭校验后转绿。

### 实现边界与验证

- 新公共结果 `ValidatedDailyManifestHeaderV3` 的字段保持私有，只提供只读访问器；该结果仅
  代表每日清单头部已验证，不代表完整输入准入，也不签发门 A 回执。
- 顶层窄 DTO 只反序列化 `schema_version`、`state`、`utc_day`、`closure_reason` 和
  `collection_day_coverage`。覆盖对象只反序列化 v1 的六个字段；`files`、`gaps`、来源健康、
  摘要和围栏等后续字段仍未解析，未知顶层字段由 `serde` 忽略。
- `closure_reason` 只要求存在字符串且去除首尾空白后非空，返回值保留原始字符串；没有新增
  关闭原因枚举，也没有建立关闭原因与覆盖状态的映射。
- 已关闭清单拒绝 `active` 覆盖；`not_started` 要求三个时间均为空且未重新激活；
  `unclean_end_unknown` 要求首末时间存在、有序、位于该 UTC 日内且结束时间为空；
  `ended_known` 要求三个时间存在、`first <= last <= end`、首末启动时间位于该 UTC 日内且结束
  时间允许等于次日零点。`reactivated_within_day=true` 本身不构成失败。UTC 日界使用锁定的
  `time 0.3.53` 计算，没有手写历法。
- 默认 `data-pipeline-training` 测试为库测试 `19 passed`、集成测试
  `2 passed / 3 ignored`；本机真实 `2026-07-30` 窄头部测试显式运行并通过；阶段 0 核心合同
  回归为 `68 passed`。严格 Clippy、格式检查、锁定离线元数据、一级依赖树和
  `git diff --check` 均通过。
- 本步骤没有解析或校验清单文件集合、SHA-256、业务缺口、围栏或副本协议，没有进入步骤
  1.1.3、任务 1.2 或门 A；提交和推送均未执行。

### 独立审查与最小修正

- 规格审查结论为 `Critical 0 / Important 2 / Minor 0`。第一项真实问题是初版
  `ended_known` 校验允许 `last_started_utc_ms` 等于次日零点，与 `data-collect` 当前生产
  合同“首末启动时间均位于半开 UTC 日区间内”不一致；第二项是五个损坏代表只断言任意错误，
  不能持续证明各自命中了正确分支。
- 覆盖损坏的最终代表改为“最后启动时间等于次日零点”。生产修正前运行
  `tests::collection_last_start_at_next_midnight_is_rejected`，实现错误返回成功头部并得到真实
  `0 passed / 1 failed` 红灯；随后只增加 `last_started_utc_ms < day_end`，聚焦测试转绿。
  原真实清单正例仍证明 `ended_utc_ms == day_end` 合法，没有增加时间排列矩阵。
- 原有五个损坏测试均改为精确匹配对应错误变体和关键值，没有增加损坏测试数量。代码质量
  复审进一步确认，仅有精确失败分支仍不能防止覆盖校验未来错误拒绝全部合法 `ended_known`；
  因此增加一个非忽略的共享夹具成功正例，只证明合法覆盖可通过，不增加字段排列矩阵。
- 代码质量审查另报一个真实 TOCTOU：路径资格完成后再以字符串路径调用 `fs::read`，可在两次
  路径解析之间把当前用户可写的日期目录替换为根外符号链接。该问题属于步骤 1.1.1 已冻结的
  路径逃逸边界，不由步骤 1.2.2 的同一文件内容冻结替代。
- 受控回归先在临时根中完成路径资格，再把日期目录替换为根外符号链接并打开替换后的文件；
  聚焦测试因缺少句柄绑定读取函数得到真实 `E0425` 编译红灯。最小修正只使用标准库：打开
  `File` 一次，通过 `/proc/self/fd/<fd>` 取得该句柄的实际路径，重新核对固定根、日期和初始
  资格，再从同一 `File` 读取；受控竞态测试和真实清单正例随后转绿。
- 未引入 `openat2`、`rustix` 或新的抽象层，也未在本步骤冻结同一 inode 的并发原地写入；后者
  仍由步骤 1.2.2 的输入摘要和运行中变化检测负责。
- 代码质量复审指出初版错误名 `ManifestSourceChanged` 会暗示同路径 inode 或内容变化也由本
  步骤检测；该名称已收窄为 `OpenedManifestPathMismatch`，只描述实际核对的规范路径和日期。
- 最终规格复审和代码质量复审均为 `Critical 0 / Important 0 / Minor 0`。最终默认训练库测试为
  `21 passed`，集成测试为 `2 passed / 3 ignored`；本机真实 `2026-07-30` 窄头部正例和阶段
  0 核心合同回归分别为 `1 passed` 与 `68 passed`。严格 Clippy、格式和差异检查均通过。

## 2026-08-05：阶段 1 步骤 1.1.3 清单文件集资格

### 合法文件集的首个红灯

- 只新增可移植的合成清单和一个清单内最终 `.parquet` 路径测试，文件内容特意不是
  Parquet，用于证明本步骤不提前解码文件内容。生产代码修改前运行
  `TMPDIR=/dev/shm cargo test -p data-pipeline-training --lib
  tests::listed_final_parquet_path_is_qualified_without_reading_its_content --locked --offline --
  --exact`，结果因缺少 `qualify_direct_source_daily_manifest_files_at_root` 以 `E0425`
  编译失败，退出码为 `101`。
- 该红灯只证明当前缺少清单文件集资格入口；本记录时尚未修改生产代码，也尚未校验
  路径逃逸、重复、临时对象或清单外文件。

### 文件相对路径逃逸红灯

- 合法文件集转绿后，只新增一个 `../outside.parquet` 文件条目，并在清单 UTC 日目录的
  父目录写入实际候选文件。生产路径校验修改前运行
  `TMPDIR=/dev/shm cargo test -p data-pipeline-training --lib
  tests::manifest_file_parent_escape_is_rejected --locked --offline -- --exact`，当时实现错误
  返回成功文件集，其规范目标已位于 UTC 日目录之外；结果为 `0 passed / 1 failed /
  22 filtered out`。
- 最小修正只允许非空且由 `Component::Normal` 组成的清单成员相对路径，拒绝该代表中的
  父目录组件及绝对路径。相同聚焦命令随后通过，结果为
  `1 passed / 0 failed / 22 filtered out`，并精确命中 `UnsafeManifestFilePath`。

### 文件符号链接逃逸红灯

- 父目录逃逸转绿后，只新增一个路径字面安全、但最终符号链接目标位于 UTC 日目录外的
  `.parquet` 条目。生产规范目标边界校验修改前运行
  `TMPDIR=/dev/shm cargo test -p data-pipeline-training --lib
  tests::manifest_file_symlink_escape_is_rejected --locked --offline -- --exact`，当时实现错误返回
  成功文件集，返回的规范路径明确位于临时数据根外；结果为 `0 passed / 1 failed /
  23 filtered out`。
- 最小修正只要求每个清单成员的规范目标仍位于规范 UTC 日目录内，错误保留清单相对路径与
  规范目标。相同聚焦命令随后通过，结果为 `1 passed / 0 failed / 23 filtered out`，并精确
  命中 `ManifestFileEscapesDay`。

### 清单相对路径重复红灯

- 符号链接逃逸转绿后，只新增一个将同一合法 `.parquet` 相对路径列出两次的清单。生产去重
  修改前运行 `TMPDIR=/dev/shm cargo test -p data-pipeline-training --lib
  tests::duplicate_manifest_relative_path_is_rejected --locked --offline -- --exact`，当时实现错误
  返回含两个相同成员的成功文件集；结果为 `0 passed / 1 failed / 24 filtered out`。
- 最小修正只在规范目标解析前用一个相对路径集合拒绝第二次相同路径；相同聚焦命令随后
  通过，结果为 `1 passed / 0 failed / 24 filtered out`，并精确命中
  `DuplicateManifestRelativePath`。

### 清单规范目标重复红灯

- 相对路径去重转绿后，只新增一个不同相对路径通过日目录内符号链接指向同一规范文件的
  清单。生产规范目标去重修改前运行 `TMPDIR=/dev/shm cargo test -p
  data-pipeline-training --lib tests::duplicate_manifest_canonical_target_is_rejected --locked
  --offline -- --exact`，当时实现错误返回两个相对路径、同一规范目标的成功文件集；结果为
  `0 passed / 1 failed / 25 filtered out`。
- 最小修正只增加一个规范目标集合并拒绝第二个相同规范目标；相同聚焦命令随后通过，结果为
  `1 passed / 0 failed / 25 filtered out`，并精确命中
  `DuplicateManifestCanonicalTarget`。

### 运行中 Parquet 红灯

- 规范目标去重转绿后，只新增一个清单列出并落盘当前写入器真实命名形态
  `.parquet.running` 的代表。生产最终化名称检查前运行 `TMPDIR=/dev/shm cargo test -p
  data-pipeline-training --lib tests::running_parquet_manifest_entry_is_rejected --locked
  --offline -- --exact`，当时实现错误返回成功文件集；结果为 `0 passed / 1 failed /
  26 filtered out`。
- 最小修正识别当前生产代码可达的临时文件后缀，并要求普通清单成员以 `.parquet` 结尾；
  相同聚焦命令随后通过，结果为 `1 passed / 0 failed / 26 filtered out`，并精确命中
  `TemporaryManifestObject`。自审发现助手初版提前包含下一项 `.rsync-partial` 分类；为保留
  下一行为的真实红灯，该两条分类在新增下一测试前移除，不影响本项 `.running` 结果。

### rsync 临时对象红灯

- 在确认生产助手不含 `.rsync-partial` 分类后，只新增一个清单列出并落盘
  `source_kind=.../.rsync-partial/upload.parquet` 的代表。生产分类修改前运行
  `TMPDIR=/dev/shm cargo test -p data-pipeline-training --lib
  tests::rsync_partial_manifest_entry_is_rejected --locked --offline -- --exact`，当时实现错误返回
  成功文件集；结果为 `0 passed / 1 failed / 27 filtered out`。
- 最小修正只补回路径组件等于或包含 `.rsync-partial` 的临时对象分类；相同聚焦命令随后
  通过，结果为 `1 passed / 0 failed / 27 filtered out`，并精确命中
  `TemporaryManifestObject`。

### 清单外最终 Parquet 红灯

- rsync 临时对象转绿后，只新增一个当前真实布局代表：同一层
  `source_kind=binance_aggtrade` 目录同时存在已列和未列的普通最终 `.parquet`。生产目录扫描
  修改前运行 `TMPDIR=/dev/shm cargo test -p data-pipeline-training --lib
  tests::unlisted_finalized_parquet_in_one_level_source_directory_is_rejected --locked --offline --
  --exact`，当时实现错误返回只含已列成员的成功文件集；结果为 `0 passed / 1 failed /
  28 filtered out`。
- 最小修正只扫描规范 UTC 日目录及其一层真实子目录的名称和文件类型；目录项复用同一临时
  分类，普通最终 `.parquet` 以相对路径集合核对是否列出，不打开 Parquet 内容，也不递归
  更深。相同清单外文件聚焦命令随后通过，结果为 `1 passed / 0 failed / 28 filtered out`，
  并精确命中 `UnlistedFinalizedParquet`。
- 最终 `.running` 与 `.rsync-partial` 代表按实际残留形态收窄为“清单未列、日目录中实际
  存在”，不再把临时对象伪造成清单成员；测试数量不变。前述红灯仍准确记录分类校验尚未
  实现时的历史事实。
- 自审删除了扫描初版附带的“清单成员最多两层、父目录必须是真目录”限制：有界扫描只描述
  本步骤主动检查的日目录范围，不授权冻结未来清单成员目录深度或拒绝根内非逃逸别名。
  保留的路径合同只有安全相对路径、最终 `.parquet`、目标仍位于规范日目录内且是文件。

### CurDir 静默规范化红灯

- 全量验证前自审确认 `Path::components()` 会消去路径中间的 `.`，因此初版“全部组件为
  `Normal`”不足以落实不静默规范化 `CurDir` 的已批准边界。只新增
  `source_kind=..././curdir.parquet` 且规范目标真实存在的代表后运行
  `TMPDIR=/dev/shm cargo test -p data-pipeline-training --lib
  tests::manifest_file_curdir_component_is_not_silently_normalized --locked --offline -- --exact`，
  当时实现错误返回成功文件集；结果为 `0 passed / 1 failed / 30 filtered out`。
- 最小修正直接对 DTO 原始字符串按 `/` 分段，拒绝空段、`.` 和 `..`，同时保留
  `Component::Normal` 检查；相同聚焦命令随后通过，结果为
  `1 passed / 0 failed / 30 filtered out`，并精确保留原始 `./` 路径。

### 有界目录布局绕过红灯

- 对照当前生产者后确认，`data-collect::finalized_file_facts` 只从 UTC 日目录的一层真实
  `source_kind=*` 子目录收集普通文件，四日 491 个真实条目也均为“一个目录组件 + 文件名”。
  因此前述删除路径层级限制的自审判断被本项证据取代；不限制 listed 深度会使深层 listed
  与 unlisted 文件集合一起绕过一层扫描。
- 只新增一个深层目录同时含清单内与清单外 `.parquet` 的代表。生产布局校验前运行
  `TMPDIR=/dev/shm cargo test -p data-pipeline-training --lib
  tests::listed_path_outside_bounded_day_layout_is_rejected --locked --offline -- --exact`，当时实现
  错误返回成功文件集；结果为 `0 passed / 1 failed / 31 filtered out`。
- 最小修正新增单一 `ManifestFileOutsideBoundedLayout`，要求清单字面相对路径及其规范目标相对
  UTC 日目录均为“一个目录组件 + 文件名”；不校验首组件名称或 source 业务语义。相同聚焦
  命令随后通过，结果为 `1 passed / 0 failed / 31 filtered out`。

### 最终实现边界与验证

- 公共窄结果 `QualifiedDailyManifestFileSetV3` 和 `QualifiedManifestParquetPath` 字段保持私有，
  只提供清单路径、已验证头部和清单成员路径的只读访问器；类型名与说明均不声称完整准入或
  门 A。`files` 窄 DTO 只解析 `path`，合成正例中的 `source_kind`、`rows` 等未知字段被忽略。
- 清单头部校验和 `files` 解析复用同一次 `File` 打开、经 `/proc/self/fd` 重新核对后的同一份
  字节；没有第二次读取清单。目录扫描只读取规范 UTC 日目录及一层真实子目录的名称和文件
  类型，不打开 Parquet 内容，也不递归更深。
- `files` 非数组代表直接精确命中 `Json`。该行为由首个正例所需的 `Vec<{path}>` 窄 DTO 已先
  失败关闭，因此没有伪造独立红灯；测试结果为 `1 passed / 0 failed / 29 filtered out`。
- 最终 `.running` 与 `.rsync-partial` 测试均使用空 `files` 清单和目录实际残留，分别精确
  命中 `TemporaryManifestObject`；普通清单外 `.parquet` 精确命中
  `UnlistedFinalizedParquet`。符号链接逃逸、重复相对路径、重复规范目标、CurDir 和有界布局
  代表均精确命中各自错误分支。
- 四日真实忽略测试逐日核对 `2026-07-30/31`、`2026-08-01/02` 的文件数分别为
  `121/127/121/122`，总计 `491`，显式运行结果为 `1 passed / 0 failed / 5 filtered out`；本步骤
  未核对行数、字节、SHA-256、围栏、业务质量或 Parquet 内容。
- 最终默认 `data-pipeline-training` 测试为库测试 `32 passed`、集成测试
  `2 passed / 4 ignored`；`data-pipeline-core` 阶段 0 合同回归为 `68 passed`。严格 Clippy、
  格式、锁定离线元数据和 `git diff --check` 均通过。本步骤未提交或推送。

### 独立审查后的普通文件边界修正

- 独立审查确认初版有界扫描仍忽略一层来源目录中的嵌套目录。现有层级测试先改为只在
  manifest 列出合法浅层 `listed.parquet`，同时在同一来源目录留下
  `nested/unlisted.parquet`；生产修改前运行 `TMPDIR=/dev/shm cargo test -p
  data-pipeline-training --lib tests::nested_directory_with_unlisted_parquet_is_rejected --locked
  --offline -- --exact`，实现错误返回成功文件集，结果为 `0 passed / 1 failed /
  31 filtered out`。
- 旧规范目标重复测试改为 manifest 只列同日来源目录中的 `alias.parquet`，该路径是指向普通
  非 Parquet 文件 `ordinary-target.bin` 的符号链接。生产修改前运行 `TMPDIR=/dev/shm cargo
  test -p data-pipeline-training --lib
  tests::manifest_parquet_symlink_to_non_parquet_file_is_rejected --locked --offline -- --exact`，实现
  错误返回成功文件集，结果为 `0 passed / 1 failed / 31 filtered out`。
- 旧 `DuplicateManifestCanonicalTarget` 及其规范目标集合只处理两个清单相对路径解析到同一
  目标的情况；当前 `data-collect::finalized_file_facts` 只产出一层真实目录中的普通文件，
  因此该别名判断由更直接的普通文件合同取代。对应错误变体、显示文本、规范目标集合和旧
  canonical duplicate 测试均删除，不保留不可达分支。
- 独立复审确认，`data-collect/src/publish.rs:258-267` 只是发现侧名称分类器，不能证明
  `.partial`、`.incomplete` 是当前 `typed_raw` 写入路径或真实样本中的可达名称；训练准入器
  删除这两个未证实分支，不把未列的普通非 Parquet 对象升级为阻断语义。计划明列且当前
  分类器仍识别的 `.rsync-partial`，以及现有写入路径可达的运行中/临时名称继续保留。
- 最小生产修正复用 `ManifestFileOutsideBoundedLayout`：UTC 日目录根只允许普通文件或真实
  目录，一层来源目录只允许普通文件；临时名称仍先命中 `TemporaryManifestObject`，且扫描
  不递归。两个聚焦命令随后分别通过，均为 `1 passed / 0 failed / 31 filtered out`。
- 修正后默认 `data-pipeline-training` 测试仍为库测试 `32 passed`、集成测试
  `2 passed / 4 ignored`；四日真实文件集为 `1 passed / 0 failed / 5 filtered out`，阶段 0
  核心合同回归为 `68 passed`。严格 Clippy 首次只报告等价布尔表达式可简化，按其建议做
  不改变行为的机械改写后通过；随后重新运行完整训练测试仍全部通过。格式、锁定离线元数据、
  旧 canonical duplicate 符号搜索及 `git diff --check` 均通过。本步骤未提交或推送。

## 2026-08-05：阶段 1 直接源范围收敛与任务 1.1 关闭

- 欠规范证据：计划 2.2 第 3 项仍把训练服务器副本及其传输清单、逐文件 SHA-256 身份证明列为
  当前准入语义，但任务 1.1 已实现、测试和复审的范围只有固定根
  `/home/chillchill/new-datasets` 的直接源；该副本语义没有对应的当前传输合同或已批准实现。
- 用户批准当前阶段只支持该固定根的直接源，删除任务 1.1 验证中的“副本身份不符”；列投影、
  分批扫描及其过滤条件、投影列和跳过列写入运行清单的要求不变。
- 原“训练服务器副本可凭传输清单和逐文件 SHA-256 准入”的当前语义被取代。未来确有远端训练
  副本需求时，必须另行冻结并取得批准的传输合同，不能由本次修正或既有任务 1.1 证据推导。
- 任务 1.1 关闭依据：三个步骤已有代码与测试证据；当前训练库列出 32 个库测试，默认集成测试
  为 `2 passed / 4 ignored`，四日真实文件集显式通过；三个步骤的独立复审记录及其修正后验证
  均已完成。计划游标据此推进到任务 1.2.1。
- 本次决定不新增远端同步、训练服务器副本、传输合同、提交、推送、发布或运行环境状态变化
  权限；阶段 1 本地开发继续沿用既有授权，任务 1.2.2 仍按计划顺序执行。

## 2026-08-05：阶段 1 任务 1.2.1 完整 Parquet 准入与 manifest v3 围栏

### 欠规范事实与用户决定

- 原计划的“适用围栏证据/围栏回执”没有定义首版成功准入可接受的关闭终态和精确证据集合，
  不能据此把任意已关闭清单解释为成功准入。
- 用户批准：首版只接受 `closure_reason=utc_day_barrier`、
  `collection_day_coverage.state=ended_known`、精确无重复的四个 producer barrier
  （`binance_aggtrade`、`binance_depth`、`polymarket`、`polymarket_rtds`）及六个当前
  source kind 的 writer finalize ack。4/6 只证明 manifest v3 结构性围栏，不是独立运行时或
  密码学回执；其他当前关闭原因失败关闭，只产生分类诊断。

### 测试驱动开发事实

- 身份元组测试先运行 `TMPDIR=/dev/shm cargo test -p data-pipeline-training --lib
  tests::manifest_file_identity_tuple_must_match_its_derived_layout_identity --locked --offline
  -- --exact`，因缺少 `validate_daily_manifest_admission_at_root` 与
  `ManifestAdmissionValidationError` 得到 `E0425/E0433` 红灯；最小实现后同一命令为
  `1 passed / 0 failed`，错误保留完整派生/清单 source、schema、UTC 日、小时和 part 元组。
- 真实可达的 `unclean_rollover + unclean_end_unknown` 终态代表先因缺少
  `AdmissionTerminalMismatch` 得到 `E0599` 红灯；增加精确成功终态限制后同一聚焦命令转绿。
  未采用 `operator_shutdown`，因为该值不是已核实的 closed-manifest 关闭原因。
- 同一持久 Parquet 的 footer 行数、字节和完整文件 SHA-256 分支均从干净 base 清单单独写入
  错误事实；每个分支加入前均以缺少对应错误变体的 `E0599` 取得红灯，随后同一
  `persistent_file_integrity_mismatches_are_rejected` 命令转绿。摘要值另有大写十六进制格式
  代表：先 `E0599`，再拒绝非 64 位小写十六进制。
- `data_collect_daily_file_fact_v1` footer 窄 DTO 的 part 交叉代表、非规范
  `-part-01`、缺失 writer finalize ack 分别先以 `E0599` 红灯，再以同名聚焦命令转绿；前者
  同时要求三项原始 footer key 和 daily file fact 各恰一次且有值。
- 四日真实正例先在集成测试中导入尚不存在的
  `validate_direct_source_daily_manifest_admission`，得到 `E0432` 红灯；随后公共入口返回 DPT
  自有的私有字段验证输入/文件事实，而非上游 DTO 或 `()`，对四日 491 个文件执行完整流式
  SHA-256、footer 与 metadata 交叉验证。

### 实现与验证边界

- 每个 Parquet 只打开一次，通过 `/proc/self/fd` 反查并绑定已验证 canonical path；同一对象的
  metadata 提取字节数、固定 64 KiB 缓冲流式计算 SHA-256、seek 回起点后读取 footer。返回后的
  输入冻结变化仍由任务 1.2.2 负责。
- footer 只读取成熟 Parquet crate；要求
  `parquet_raw_source_kind`、`parquet_raw_layout_version`、
  `data_collect_artifact_kind=typed_raw_parquet` 和 `data_collect_daily_file_fact_v1`，后者窄交叉
  source/schema/UTC 小时/part/rows。允许其他合法 metadata，不自建 Parquet 解析器。
- `cargo test -p data-pipeline-training --lib --locked --offline` 最终为 `39 passed`。本任务未修改
  `data-collect`、需求、当前游标、远端状态、提交或推送。

### 独立审查第一轮后的 I1/I2 修正

- 独立审查指出两个可执行问题：I1 是只在完整 admission DTO 反序列化后检查成功终态，导致真实
  恢复形态在缺少成功专用 `producer_barriers` / `writer_finalize_acks` 时先落入泛化 `Json` 错误；I2
  是外层数据集日期与 `typed_raw/utc_day=*` 内层分区尚可不一致。审查同时确认 M0：公共结果仍为
  DPT 自有私有字段事实，未暴露上游 DTO 或 `()`。
- I1 使用真实恢复形态：`closure_reason=reconciled_collection_not_started`、
  `collection_day_coverage.state=not_started`、三个覆盖时间均为 `null`、
  `reactivated_within_day=false`，并删除成功专用 barriers、acks 和文件。生产修正前，
  `tests::success_admission_requires_the_exact_closed_terminal_tuple` 精确运行后返回
  `Json(missing field producer_barriers)`；把精确成功终态检查移到完整 admission DTO 解析之前后，
  同一命令通过并命中 `AdmissionTerminalMismatch`。
- I2 使用外层目录和 header 均为 `2026-07-30`、内层路径为
  `typed_raw/utc_day=2026-07-31` 的合成清单。生产修正前，
  `tests::manifest_inner_utc_day_partition_must_match_outer_dataset_day` 返回合格路径而触发
  `expect_err`；路径资格层在词法和规范路径上都核对该内层分区后，同一命令通过并拒绝为
  `InvalidDatasetDate`。
- 上述两项聚焦回归各为 `1 passed / 0 failed`。I1/I2 修正后，主代理显式运行
  `TMPDIR=/dev/shm cargo test -p data-pipeline-training --test phase1_admission_contract
  real_four_day_daily_manifests_pass_complete_admission --locked --offline -- --ignored --exact`，退出码为
  `0`，结果为 `1 passed / 0 failed / 6 filtered out / 182.26s`。这次结果替代 I2 修正前的四日
  admission 证据；当前游标仍为任务 1.2.1，未推进、未改 `data-collect`、未提交或推送。

### 独立审查第二轮：恢复完整 admission 身份元组回归

- 首轮“用 I2 内层分区测试替换 identity test”的建议不完整：内层路径分区属于路径资格边界，
  不能覆盖任务 1.2.1 明列的“清单文件项身份元组与由文件布局派生元组”完整 admission 边界。
  I2 继续保留，且本轮不新增逐字段矩阵。
- 恢复
  `tests::manifest_file_identity_tuple_must_match_its_derived_layout_identity`：只把最终 Parquet
  文件名小时从 `00` 改为 `01`，同步清单 `path` 而不改清单 `utc_hour`，经完整 admission 入口精确
  命中 `FileIdentityMismatch`。断言同时固定 expected/found 的 source、schema、UTC 日、UTC 小时和
  part 五字段完整元组。
- 该测试首轮实现前的 `E0425/E0433` 红灯和最小实现后的绿灯已在本 changelog 的任务 1.2.1 记录；
  本轮只是恢复被错误删除的覆盖，不伪造新的红灯。恢复后的聚焦命令为 `1 passed / 0 failed / 39
  filtered out`。未改生产代码、计划语义或游标，未重跑 9.2GB 四日测试，未提交或推送。

### 最终定向复审与任务 1.2.1 关闭

- 最终定向复审确认首轮 finding 已处理，新结论为 `Critical 0 / Important 0 / Minor 0`；需求符合性
  为 `PASS`，代码质量为 `APPROVED`。
- 关闭证据包括：默认包测试库 `40 passed`、集成 `2 passed / 5 ignored`，以及生产修复后的四日真实
  完整准入退出码 `0`、`1 passed / 0 failed / 6 filtered out / 182.26s`。
- 任务 1.2.1 至此关闭，计划当前游标推进至任务 1.2.2 的输入摘要冻结与运行中变化检测。本记录仅
  保存事实，不承担当前权威；未改代码、需求、`data-collect`、远端状态，未提交或推送。

## 2026-08-05：任务 1.2.2 历史事故归属纠正

- 任务预检核实，历史 T08 对象是日期下限前
  `2026-07-09T08` 只写入 `PAR1` 四字节且从未最终化的 `.running` 文件；它没有进入正式
  reader，也不是已准入最终化输入在运行中被截断或改写的事故。
- 当前四日验收基线的 491 个清单成员全部是最终化文件，目录与清单均没有 `.running`、
  `.tmp`、`.digesting` 或 `.rsync-partial`。T08 事故形态归步骤 1.1.3 的临时对象拒绝边界，
  现有测试已经证明 `.running` 不能取得准入资格，不增加重复事故测试。
- 用户批准删除任务 1.2.2 中“已发生的截断事故另作回归”的错误归因。任务 1.2.2 只用一个
  已准入最终化输入在运行中发生字节变化的代表，证明重新校验失败并终止构建；该代表是当前
  合同的故障注入，不冒充历史事故。
- 本次纠正不修改需求、`data-collect`、真实数据或既有任务 1.1/1.2.1 实现，也不形成提交、
  推送、部署或远端状态变化权限。

## 2026-08-05：任务 1.2.2 输入集合冻结与变化检测实现

### RED 与最小实现

- 先增加唯一聚焦行为测试
  `tests::frozen_input_set_rejects_a_changed_but_independently_admissible_input`，再执行
  `TMPDIR=/dev/shm cargo test -p data-pipeline-training --lib
  frozen_input_set_rejects_a_changed_but_independently_admissible_input --locked --offline`。生产 API
  尚不存在时，编译以退出码 `101` 失败，精确报告两处缺少
  `freeze_direct_source_input_set_with_admission` 的 `E0425` 和一处缺少
  `InputSetSnapshotError` 的 `E0433`；该失败只由任务 1.2.2 能力缺失导致，构成预期红灯。
- 最小生产实现增加 `FrozenDirectSourceInputSetV1`、`freeze_direct_source_input_set` 和窄
  `verify_unchanged`。公开冻结入口逐个调用任务 1.2.1 的
  `validate_direct_source_daily_manifest_admission`；快照私有持有已准入每日输入，只暴露只读切片、
  `input_set_identity_sha256` 与 `input_manifest_set_sha256`。
- 两类摘要使用独立 v1 域。输入集合身份只编码排序后的 UTC 日、source、schema、UTC 小时、part、
  bytes、rows 与文件 SHA-256；不编码调用参数顺序、绝对路径、inode、mtime 或其他机器本地事实。
  清单集合身份独立编码排序后的 UTC 日和精确 manifest SHA-256。快照内每日输入排序仅用于稳定只读
  暴露；绝对路径不参与任一摘要。
- `verify_unchanged` 重新对冻结的 manifest 路径执行同一完整准入流程，再同时比较两类摘要。当前
  准入失败直接返回带 manifest 路径的 `Admission` 错误；任一摘要不一致返回同时携带两类预期值与
  当前值的 `DigestMismatch`，成功只返回 `()`，不签发任何回执。

### GREEN、回归与边界

- 聚焦测试先冻结有效合成 Parquet/manifest，重复冻结证明两类摘要确定且均为 64 位小写 SHA-256；
  随后把 Parquet 从 `[1, 2]` 改为不同的有效内容 `[2, 1]`，同步更新 manifest 的 bytes 和 SHA-256，
  并先证明变化后的输入仍独立通过任务 1.2.1 完整准入。冻结快照的重新校验随后精确返回
  `DigestMismatch`，证明拒绝来自冻结身份比较，而非损坏 Parquet 准入。
- 同一聚焦命令在最小实现后退出码为 `0`，结果为 `1 passed / 0 failed / 40 filtered out`。
  `TMPDIR=/dev/shm cargo test -p data-pipeline-training --lib --locked --offline` 为
  `41 passed / 0 failed`；默认包命令
  `TMPDIR=/dev/shm cargo test -p data-pipeline-training --locked --offline` 为库测试
  `41 passed`、集成测试 `2 passed / 5 ignored`、文档测试 `0`。
- 本实现没有增加 watcher、轮询、inode/mtime 权威、长持有文件描述符、输入文件副本、构建管道、
  收据、网络或配置；没有增加或改名 T08 测试，也没有修改依赖、需求、计划游标、`data-collect`、
  真实数据或远端状态，未提交或推送。

### 独立审查、主代理复验与任务关闭

- `gpt-5.6-sol/ultra` 独立任务审查结论为规范符合性 `PASS`、代码与测试质量 `APPROVED`，
  `Critical 0 / Important 0 / Minor 0`。审查确认两类摘要字段完整且独立域分离，冻结与复核均
  复用任务 1.2.1 准入入口，变化测试先证明新输入独立合格再命中冻结摘要不一致，且没有影子
  准入、回执、监听、轮询、网络、配置或 T08 范围扩张。
- 主代理在审查前从当前代码重新运行聚焦测试、默认包测试、严格 Clippy、格式和差异检查，全部
  退出码为 `0`：聚焦变化测试 `1 passed`；库测试 `41 passed`；默认集成测试
  `2 passed / 5 ignored`；Clippy 无警告。五个忽略测试保持既有真实数据依赖，本任务没有把它们
  写成已运行。
- 任务自审确认，`input_set_identity_sha256` 绑定排序后的语义文件事实，
  `input_manifest_set_sha256` 独立绑定精确清单事实；两者在每次复核时同时比较。当前任务不增加
  空日、重复调用参数或逐字段排列测试，也不把未来运行配置职责提前放入冻结原语。
- 任务 1.2.2 至此关闭，计划游标推进至任务 1.3 的四日真实样本基线。本轮仍未修改需求、
  `data-collect`、真实数据或远端状态，未创建 `.superpowers`，未提交或推送。

## 2026-08-05：任务 1.3 四日真实样本基线

- 本任务只增加真实数据和既有准入行为的特征化测试，没有新增或修改生产 API、质量分类器、
  需求、`data-collect` 或真实数据。按计划第 1.1 节第 8 项，没有为已有行为伪造 TDD 红灯。
- 现有四日完整准入测试在每个日期唯一一次调用
  `validate_direct_source_daily_manifest_admission`，直接复用已准入文件事实逐日断言文件、行数和
  字节，并合计得到 491 个文件、151,056,246 行和 9,202,902,694 字节。该测试不枚举输入目录，
  也没有增加第二次四日完整扫描。
- 新增测试私有强类型 DTO，把 manifest 顶层 `gaps` 只读取为不透明 `Vec<String>`。真实
  2026-07-30 同时为 `closed`、`gaps=0`、未重新激活；2026-07-31 同时为 `closed`、
  `gaps=6`、`reactivated_within_day=true`。测试不解析自由文本时间、不构造精确缺口区间，也不
  硬编码六条诊断全文。
- 实施代理唯一一次运行四日完整准入测试，退出码为 `0`，结果为 `1 passed / 0 failed / 7
  filtered out`，测试本体耗时 189.46 秒；逐日事实分别为 121/43,741,059/2,658,237,003、
  127/42,259,888/2,604,557,207、121/31,336,754/1,892,535,675、
  122/33,718,545/2,047,572,809。默认包测试为库测试 `41 passed`、集成测试
  `2 passed / 6 ignored`，严格 Clippy、格式和差异检查通过。首次格式检查只提示新增断言换行，
  机械格式化后复核通过。
- `gpt-5.6-sol/ultra` 独立任务审查结论为规范符合、任务质量 `Approved`，
  `Critical 0 / Important 0 / Minor 0`；审查没有重复运行四日扫描。
- 主代理随后重新运行小型质量事实测试、默认包测试、严格 Clippy、格式和差异检查，全部退出码
  为 `0`；同时独立读取四份 manifest，重新得到相同逐日文件、行数、字节、`gaps` 和重新激活
  事实。按“一次四日完整扫描”边界，没有重复执行 9.2 GB SHA/footer 准入。
- 任务 1.3 至此关闭，计划游标推进至任务 1.4 的资源预算实测。本轮未提交、推送、部署或执行
  远端操作；阶段 1 推送仍未获授权。

## 2026-08-05：资源预算执行顺序冲突与用户决定

- 任务 1.4 实施前核查确认，原计划要求阶段 1 立即用真实输入实测并冻结分批解码、语义排序、
  业务队列、市场热状态、常驻内存和总内存；但六类真实 schema 解码、规范全序和市场状态归约
  分别尚待任务 2.1 至 2.3 实现。当前代码只有清单准入与不透明 `SortRow` 通用外部排序器，
  无法在不建立第二语义路径或冒充指标的前提下完成原任务 1.4。
- 该不一致属于计划执行顺序错误，不是需求冲突、真实样本混入 `.running`、历史 T08 或其他
  工作树造成。需求 `DPT-AC-010` 只要求门 A 发布前基于真实输入形成资源硬上限，没有要求
  完整生产管道预算必须在阶段 1 冻结。
- 主代理与独立 `gpt-5.6-sol/ultra` 只读核查一致判定：原任务 1.4 不可按当前顺序完整验收；
  合成排序行、局部 `max_run_bytes` 或临时解码/市场状态实现均不能替代真实生产路径。
- 用户批准推荐调整：阶段 1 的任务 1.4 只实测并冻结当前已存在的清单读取、完整文件 SHA-256
  与 Parquet footer 准入路径；阶段 0 排序器局部硬上限继续有效，但不声明为完整管道预算。
  六类真实解码、规范全序、业务队列、市场热状态、常驻内存、总内存及临时磁盘的最终预算，
  移至任务 2.1 至 2.3 接通后的新任务 2.4，并须在门 A 输出任务开始前完成。
- 当前计划已直接替换相冲突的旧任务 1.4 和阶段 1 完成门措辞，原门 A 输出顺延为任务 2.5；
  需求保持不变。获得本次决定后，执行游标恢复至调整后的任务 1.4；本轮决定本身不授权提交、
  推送、部署、远端操作或真实数据修改。

## 2026-08-05：任务 1.4 每日 manifest 资源合同最终决定

- 调整后的任务 1.4 初版细化曾提出独立的每日成员数硬上限。复核当前 `data-collect` 后确认，
  同一小时合法重启会继续生成 `part-N`，四日 `121/127/121/122` 只是观测值而不是业务上限；
  因此删除独立成员数上限，也不增加 `129` 成员测试。成员集合和单日准入结果只由每日
  manifest 原始字节上限自然约束，真实成员高水位 `127` 仅作为测量事实。
- 当前准入实现使用无上限的 `read_to_end` 完整读入 manifest。用户先质疑根据四日样本冻结
  `1 MiB` 缺少依据，随后作出最终精确决定：每日 manifest 原始字节上限为 `10 MiB`
  （`10 * 1024 * 1024` 字节）；超过上限必须在 JSON 解析前失败。TDD 只增加这一个代表性
  超限红灯，不建立成员数或资源排列矩阵。
- 用户进一步确认，保留当前 `64 KiB` SHA-256 读取缓冲、串行准入、一次处理一个目标
  Parquet 和准入路径零临时磁盘边界。这些内容只描述 DPT 内部从最终化 manifest/Parquet
  生成已验证文件事实的准入实现，不改变下游消费合同：阶段 2 尚未实现的规范核心消费准入
  结果，`sim-system` 只消费门 A 规范事件，`model-train` 只消费门 B 成功产物。
- 本次决定不授权独立成员上限、并发准入、通用资源管理器、新配置系统、第二语义核心、
  `data-collect` 修改、真实数据修改、提交、推送、部署或远端操作。

### TDD、审查修正与真实资源试验

- 生产代码修改前只增加
  `tests::manifest_larger_than_10_mib_is_rejected_before_json_parsing`。首次测试写法因 Rust 模式
  中含算术表达式而发生语法错误，不作为有效红灯；只修正测试写法且仍未修改生产代码后，
  聚焦命令以退出码 `101` 精确失败于缺少
  `ManifestHeaderValidationError::ManifestBytesLimitExceeded`，构成实际 RED。
- 最小实现增加私有 `DAILY_MANIFEST_MAX_BYTES = 10 * 1024 * 1024`、结构化超限错误，并用
  `File::take(10 MiB + 1)` 实际限制读取。第一个超限检测字节到达后立即失败，JSON 解析不可达；
  文件在读取期间增长也不能绕过上限。唯一聚焦测试转为 `1 passed / 0 failed`，默认包为库测试
  `42 passed`、集成测试 `2 passed / 6 ignored`，严格 Clippy、格式和差异检查通过。
- 独立规范审查为 `PASS`、`Critical 0 / Important 0 / Minor 0`。首轮独立代码质量审查发现
  `Vec::with_capacity(10 MiB + 1)` 会让约 `350 KiB` 的正常清单无条件预分配约 30 倍内存，
  污染本任务要测量的常驻内存，因此结论为 `Critical 0 / Important 1 / Minor 0`。最小修正只
  改为 `Vec::new()`，保留 `take(10 MiB + 1)`；复审为 `Approved`、
  `Critical 0 / Important 0 / Minor 0`，全部轻量验证重新通过。
- 唯一一次四日真实完整准入资源试验直接运行已构建的集成测试二进制，避免把 Cargo 编译内存
  混入准入进程。代码身份为基准提交 `a5db59d6b114df852f6cbd7d171f68a4d4d15ba5` 加
  `data-pipeline/training/src/lib.rs` SHA-256
  `274f046c3d9bcd7c88b2bc223f3af205b0c0b9ae92bc1bf7e97470b547fa7088`；四份 manifest
  SHA-256 依次为 `4b2cd62cea8740bf4cdb29971941118e70d869835496aece714458d12ea96557`、
  `4fac5a9440c14dbb8acfca18c7a944b2fc33f7f67b5866caeb89d646b87f2296`、
  `e31b122f6debad9863fac973903714baa739282e4ebba1c9093d9972618a3d30`、
  `2ea1a7460cf7fde05a498d0b3e338eb3c10a305333fd3a7ddeca46a4a9e19889`。
- 真实 manifest 字节为 `353,534 / 367,637 / 353,128 / 355,114`，成员为
  `121 / 127 / 121 / 122`；成员最大值 `127` 只记录为高水位。试验结果为
  `1 passed / 0 failed / 7 filtered out`，491 个文件、151,056,246 行和
  9,202,902,694 字节全部通过；测试耗时 `182.33s`，进程墙钟 `3:02.33`，用户态
  `181.25s`，系统态 `0.94s`，最大常驻内存 `282,484 KiB`，交换 `0`，文件系统写出 `0`，
  退出码 `0`。该试验没有执行阶段 2 解码、排序或状态归约，也不把页缓存命中统计冒充实际
  输入字节。

## 2026-08-05：阶段 1 完成门关闭

- 主代理对任务 1.1 至 1.4 的全部当前差异完成阶段自审。改动只涉及训练 crate 的依赖、准入
  实现、一个阶段 1 集成测试、计划、changelog 和锁文件；需求与 `data-collect` 无差异，
  `.superpowers` 仍为 Git 忽略的过程目录。代码中不存在每日成员数硬上限、`129` 成员测试、
  并发准入、新配置系统、文档文本守卫、测试登记表、网络、钱包、签名或凭据路径。
- 新鲜轻量验证均以退出码 `0` 完成：超限聚焦测试 `1 passed`；训练 crate 库测试
  `42 passed`，集成测试 `2 passed / 6 ignored`；核心合同测试 `68 passed`；`contracts`
  测试通过；三个 DPT crate 的严格 Clippy、全仓格式、`git diff --check`、Cargo metadata 和
  `data-pipeline-training` 正常依赖树检查通过。六个真实数据测试保持忽略，没有重复执行四日
  9.2 GB 完整准入扫描。
- `gpt-5.6-sol/ultra` 最终独立阶段审查及其代码边界、文档合同交叉审查结论均为
  `Approved / PASS`，`Critical 0 / Important 0 / Minor 0`。审查确认固定直接源、日期下限、
  manifest v3 终态与围栏、文件身份与完整性、输入集合冻结和变化复核符合当前计划；任务 1.4
  精确保持 `10 MiB`、解析前失败、`Vec::new()`、`64 KiB` SHA-256 缓冲、串行单文件准入和
  零准入临时磁盘边界，没有额外成员合同或阶段 2 实现。
- 任务 1.4 与阶段 1 完成门据此关闭。阶段 1 只冻结准入路径资源预算，未宣称完整满足
  `DPT-AC-010`；真实解码、全序、状态归约和完整生产路径资源预算仍归任务 2.1 至 2.4。
  本轮未提交、推送、部署或进入阶段 2，当前停在阶段 1 收口提交、推送及阶段 2 本地开发的
  明确授权门前。

## 2026-08-05：阶段 1 收口提交与推送授权

- 用户明确批准创建阶段 1 收口提交并推送当前 `sim-system` 分支。提交范围只包含阶段 1 已经
  完成自审、独立审查和最终复验的训练准入代码、测试、依赖锁定、计划与本 changelog。
- 本次授权不包含阶段 2 本地开发、PR、部署、远端运行、真实数据修改或其他工作树操作；
  推送完成后仍停在阶段 2 明确授权门前。

## 2026-08-06：双时间轴决定与阶段 0R 定向重开

- 用户确认公开市场数据必须同时保留两种独立时间语义：数据源事件发生时间用于训练与
  offline-evaluation 中的隐藏平台状态回放，采集端 `timestamp_received` 用于决定交易侧模型
  何时能够看到对应公开数据。当前 `timestamp_received` 的定义获确认，即采集应用已经取得并
  解析消息的时刻；采集器后续接纳坐标只保留为谱系与摄取证据。
- 用户进一步明确，`sim-system` 的模拟撮合订单及其相关平台响应使用 Polymarket orderbook
  数据的数据源事件发生时间推进。完整隐藏平台回放不得提供给 paper 或 live。私有订单响应
  可以先于对应公开市场消息对交易侧可见，但此时只允许暴露响应自身及其明确携带的订单/账户
  事实，不得暴露尚未接收的公开消息、盘口或隐藏平台状态。
- 受隐藏平台回放能力覆盖的事件缺少数据源事件时间时，该能力必须失败或按已冻结范围隔离，
  不得用采集端接收时间或接纳坐标补齐平台时间。
- 当前 V1 公共合同不能满足双时间语义，且不得静默改义。阶段 0 因此只在公共时间、投影、
  Observation 和回执合同范围内定向重开为阶段 0R；阶段 1 已完成的准入实现、四日样本基线和
  资源试验结果保留，只在阶段 0R 实现后执行受影响回归。阶段 2 仍须等待阶段 0R 完成门关闭
  并由用户另行明确授权。

### 真实数据核实

- 对 `/home/chillchill/new-datasets/2026-08-04/typed_raw/utc_day=2026-08-04/source_kind=polymarket_market`
  的只读 Parquet
  元数据扫描覆盖 25 个文件、39,908,199 行；`timestamp` 与 `timestamp_received` 的空值数均
  为 0，没有行组缺少空值统计，schema 数量为 1。因此当前数据不支持把这两个字段的可空性
  写成现实数据缺口。
- 同日 manifest 为 `data_collect_daily_manifest_v3`，状态为 `closed`，关闭原因为
  `utc_day_barrier`，`collection_day_coverage.state=ended_known`，并记录日内重新激活、6 个
  writer ack 和 4 个 producer barrier。该核实不自动把 2026-08-04 分配给某次门 A 运行，也
  不产生 `data-collect` schema 或运行时修改事项。

### 本轮修改边界

- 用户批准按上述方案执行，并批准把其已确认的手工文件修改与本轮成果合并为一次提交后推送。
  本授权不包含阶段 2、`sim-system` 实施、部署或远端运行状态变化。
- 聚焦测试先以编译失败构成 RED；复核后将测试范围收窄为已批准的双时间索引与共享
  `event_id` 边界。最小实现随后把当前公共事件、Observation、能力矩阵和门 A 回执升级为 V2，
  增加一份规范事件到源事件时间索引和接收时间可见索引的投影、编码与校验；旧 V1 回执只保留
  为版本拒绝夹具。
- `data-pipeline-training` 的既有外部排序能力只改为消费调用方提供的不透明字节排序键，不解释
  双时间业务语义；阶段 1 准入实现、输入合同和忽略的真实数据测试保持不变。本轮没有物化
  双索引、实现阶段 2 解码/状态归约、修改 `data-collect` 或实现 `sim-system`。
- 当前聚焦结果为：`data-pipeline-core` 48 项合同测试全部通过；训练库 42 项测试全部通过；
  阶段 1 集成测试 2 项通过、6 项按既有设置忽略。四日 9.2 GB 完整准入资源试验未重复运行。
  `cargo check --workspace --all-targets`、全工作区严格 Clippy、格式、差异、黄金 JSON、Cargo
  metadata 和核心依赖边界检查均通过；核心正常依赖仍不含 Arrow、Parquet、网络或 I/O。
- 两次额外扩大范围的 `cargo test --workspace --all-targets` 在前一次尚未结束时重叠运行，
  共同触发 `data-collect` 的 `real_source_alignment_probe` bench，并共用其默认产物目录和固定
  写入文件名。后一次运行在已有 `running.parquet` 后退出，发送端只显示二级错误
  `writer chunk channel disconnected` 和退出码 1；该结果属于测试编排造成的 bench 运行隔离
  问题，不是生产写入器故障或当前开发遗留。该 bench 不属于阶段 0R 完成门，本轮没有修改或
  重跑 `data-collect`，也不能把此次扩大范围命令写成通过。
- 阶段 0R 当前等待独立阶段审查，尚未关闭，也尚未签发新的门 A 回执。

### 第一轮独立审查与修正

- `gpt-5.6-sol/ultra` 第一轮独立审查结论为
  `Critical 0 / Important 3 / Minor 1`，阶段 0R 未通过。三个 Important 分别是事件包络与
  来源证据重复保存采集器接纳坐标而允许多来源事件产生非唯一身份、Observation 缺少与贡献
  事件可见索引条目的联合截止校验，以及阶段 0R 计划错误提前声明完整双索引扰动验证。
- 事件包络中的重复采集器接纳坐标已删除，接纳坐标只保留在
  `lineage.source_evidence`；事件包络双时间只保存源事件时间和接收时间，二者仍分别等于组成
  来源证据的最大值。对应黄金事件及由其派生的 Observation 身份已按修正后的唯一前像更新。
- 增加最小的 Observation 与贡献事件可见索引条目联合校验：条目集合必须与
  `contributing_event_ids` 一一对应，任一贡献事件的接收时间晚于 Observation 可见截止点即
  失败。代表性回归同时证明单独结构校验不能替代该关系校验；本修正不物化完整索引或
  manifest。
- 阶段 0R 的验证描述已收窄为单事件双投影、编码往返、派生双时间聚合、Observation 联合
  截止关系和 Polymarket orderbook 源时间必需性；文件、行组、批次、线程、等价重分片以及
  完整索引摘要不变性仍只属于阶段 2 的任务 2.2.2。
- Minor 指出无执行权威的 `rebuild-core` 草案仍引用 V1 公共合同。该草案不属于本次批准修改
  范围，也不阻断阶段 0R 代码，但在未来恢复审核或申请实施权威前必须先同步 V2；本轮不修改
  该事项文档。
- 修正后的聚焦测试为阶段 0 合同测试 `48 passed`、双时间测试 `1 passed`。阶段 0R 继续等待
  独立复审，复审通过前不关闭，也不签发新的门 A 回执。

### 第二轮独立复审与阶段 0R 关闭

- 第二轮复审中发现一处旧 V1 文档术语：计划任务 0.8.1 仍以组件 `as_of` 描述模型可见截止
  关系。该处已改为只要求可见组件 `received_at_utc_ms` 不晚于模型可见截止点；
  `source_event_at_utc_ms` 只按双时间和跨时钟合同处理，不再被误写为必须早于该截止点。
- 修正后，`gpt-5.6-sol/ultra` 第二轮完整独立阶段复审结论为
  `Approved / PASS`、`Critical 0 / Important 0 / Minor 0`。复审确认首轮三个 Important 均已
  关闭；本轮没有物化阶段 2 双索引、修改 `data-collect` 或实现 `sim-system`。
- 最终受影响验证为：阶段 0 合同测试 `48 passed`、双时间聚焦测试 `1 passed`、训练库
  `42 passed`、阶段 1 集成测试 `2 passed / 6 ignored`；全工作区 `cargo check`、严格 Clippy、
  格式、差异、黄金 JSON、Cargo metadata 和核心依赖边界检查均通过。上述重叠运行事实只按
  上一节保留，不属于阶段 0R 完成门或后续开发事项。
- 阶段 0R 完成门据此关闭。阶段 1 已完成成果继续保留；完整双索引、真实六类映射、状态归约
  和门 A 产物仍属于阶段 2。当前没有签发新的门 A 回执，也没有开始阶段 2；下一游标停在阶段
  2 的用户另行授权门前。

## 2026-08-06：普通开发验证排除专用 bench

- 对原始命令记录、临时产物时间和 bench 写入路径的只读复核确认：此前的
  `writer chunk channel disconnected` 来自两次扩大范围命令重叠运行并共用默认产物路径，
  不是独立运行下复现的生产写入器故障。
- 用户同意把全工作区普通回归固定为
  `cargo test --workspace --lib --bins --tests --examples --locked --offline`，不得再把
  `cargo test --workspace --all-targets` 用作普通开发、提交或阶段完成门；`data-collect` 的 bench
  只在当前计划要求或用户明确批准后单独运行，并使用独立产物目录。该规则已写入根目录
  `AGENTS.md`。
- 本次只修正仓库级开发约束和历史事实定性，没有修改 `data-collect` Cargo 配置、实现、需求、
  计划或验收，也没有修改 DPT、门 A 或 `rebuild-core` 的需求与计划。

## 2026-08-06：阶段 2 授权与相同数据源事件时间消息归约复核

- 用户同意独立复审与主代理交叉验收结论，并明确批准执行阶段 2。本授权覆盖当前工作树内的
  本地代码、测试和不可变本地产物，不包含 `sim-system` 实施、推送、发布、部署、远端操作、
  敏感材料、真实下单或真实资金。
- 复审纠正了先前“删除旧总序后，同一毫秒事件没有还原顺序”的过度判断。真实
  `polymarket_orderbook_v3` 保存 `source_sequence_epoch_id`、严格递增的
  `collector_admission_sequence`、单条消息边界及嵌套 `price_changes` 顺序；V2 规范谱系仍
  保存该接纳坐标。删除的是旧跨来源业务总序，不是已采集 WebSocket 流的先后证据。
- 对 2026-08-04 的 25 个 `polymarket_market` Parquet 只读扫描覆盖 39,908,199 行和两个
  epoch；各 epoch 内接纳序号连续、无倒退、重复或缺口，同一源时间、同一档位存在连续不同
  绝对数量。该证据足以确定性还原实际采集且通过质量门的消息流，但不证明上游从未丢消息；
  连接缺口、epoch 切换和完整 `book` 重同步继续按既有质量规则处理。
- 计划已补清阶段 2 的最小实现边界：平台生效时间仍取数据源事件时间；同一 epoch 内数据源事件时间
  相同的消息只按接纳序号恢复采集先后，接纳序号不得改写平台时间或模型可见时间；不得恢复
  `TotalOrderKeyV1`、以 `event_id` 充当盘口因果顺序或把同毫秒独立消息合并。对应验证只增加
  一个真实相同数据源事件时间回归和一个新 epoch 完整 `book` 基准边界；回归必须断言每次
  连续转换及其顺序，不能只比较最终盘口而放过中间消息合并。事件时间索引在数据源事件时间
  相同时保留归约顺序，不按 `event_id` 哈希重排。接纳序号不得跨 epoch 比较；每个受影响资产
  都必须用新 epoch 的合法完整 `book` 建立新基准，不得继承旧盘口状态。
- 同步修正了计划中遗漏的 `GateACapabilityMatrixV2` 名称、四项门 A 输出被误写为“三者”及
  已有规范事件 ID 被误写为重新实现的措辞。需求宏观语义和 V2 公共合同无需修改。
- 阶段 1 已完成成果继续保留；完整生产路径资源预算仍归任务 2.4。阶段 2 当前游标进入任务
  2.1。阶段 2 前既有五文件文档和验证规则修改已经独立审查通过并形成提交 `854a6aa`，尚未
  推送。

## 2026-08-06：阶段 2 任务 2.1 实现收口与后续决策门

### 六类无损解码实现

- 聚焦测试先以 `data_pipeline_training::typed_raw` 不存在的编译错误形成真实红灯；随后只在
  `data-pipeline-core` 增加六类纯 Rust typed_raw DTO、精确十进制和行内校验，在
  `data-pipeline-training` 增加 Arrow/Parquet 物理 schema 校验、逐行解码及受阶段 1 准入类型
  约束的有界批次流。核心不依赖 Arrow、Parquet、文件系统或 `data-collect`，训练 crate 不
  依赖 `data-collect`，没有第二套 SDK、网络或浮点规范真相路径。
- 六类当前物理 schema 按生产端权威逐字段映射：orderbook v3 为 34 列、metadata v5 为
  69 列、RTDS v4 为 15 列、depth snapshot v3 为 16 列、depth update v4 为 23 列、
  aggTrade v7 为 35 列。orderbook 保留外层消息、epoch、接纳序号、数据源事件时间、接收
  时间和嵌套列表顺序；价格、数量、费率和值保留原始十进制文本，并以 `ExactDecimal` 规范
  解析验证，不使用 `f32`/`f64` 改写规范真相。
- 核心层为六类来源增加了可枚举的字段语义映射：Polymarket 价格为
  `PusdPerShare`、数量为 `Shares`、费率为 `BasisPoints`；RTDS 值为 `UsdPerBtc`；
  Binance 价格为 `UsdtPerBtc`、数量为 `Btc`。标识映射区分 `SourceNative`、
  `CollectorDerived` 和 `EvidenceDigest`；`age_ms`、`freshness_status`、
  `snapshot_status` 与 `stitch_*` 等采集器派生证据也不再依赖读取方猜测。
- 第一轮独立复审发现上述映射仍有错路径和过宽语义：orderbook 与 aggTrade 标识没有按枚举
  分支展开，metadata `winning_asset_id` 在 WebSocket 终态是源原生、在 Gamma 终态则由结果
  价格派生，不能固定标成 `SourceNative`；双时间、方向、metadata 独立状态及 Binance
  衔接坐标也有遗漏。修正后路径与真实 DTO 分支一一对应，毫秒时间和经济单位分开标明；
  `snapshot_last_update_id` 为源快照坐标，`local_u_before` 为采集器状态坐标。
- metadata 的 19 个上游封闭枚举以及 aggTrade 的 `invalid_reason_code` 和
  `payload_encoding` 曾以任意字符串进入核心，未知值可以穿过不可信 Parquet 边界。
  当前已按上游权威枚举逐值强类型化；`uma_resolution_status`、原始时间文本、自由文本和结果
  标签等开放字段保持原样，没有误收紧为本地枚举。metadata 的 `asset_ids`、`outcomes_raw`
  与 `outcome_prices_raw` 保留原始 JSON，同时在核心边界按索引建立资产、结果和精确价格
  的一对一证据；获胜资产必须属于该固定资产集，且归属根据
  `resolution_evidence_source` 逐行解析。
- 第三轮独立复审发现，前述索引边界仍会放过非二元、重复或非规范 JSON 编码的 `asset_ids`，
  并会在没有 `outcomes_raw` 时放过空的 `winning_outcome`。这与 metadata v5 生产端固定身份
  合同不一致，也会使按资产查找获胜结果不再唯一。当前已在真实解码可达的核心边界要求
  `asset_ids` 为规范 JSON 编码的两个互异十进制 ID，并要求存在的 `winning_outcome` 非空；
  一个重复资产 ID 和一个空获胜结果作为各自的代表性损坏回归。
- 首版 metadata DTO 曾错误遗漏 v5 字段，并把合法的仅摘要跨行引用判为无效。真实
  schema 复核后，DTO 改为完整 69 列；`source_evidence_payload_json` 与
  `source_evidence_raw_sha256` 同时存在时必须按原始 UTF-8 字节核对 SHA-256，
  仅摘要引用合法保留，仅有载荷而无摘要时拒绝。对 2026-07-30 至 2026-08-04 的 6,958 条
  真实 metadata 只读核实结果为：3,504 条载荷与摘要同时存在、3,452 条仅有摘要、2 条两者
  均无、0 条仅有载荷，3,504 个可核对摘要全部匹配。
- 真实生产枚举核对补回了 orderbook schema 明确允许的 `side=UNKNOWN`。Binance depth 的
  JSON 顶层序列化空白不进入语义 DTO，但每个价位的价格/数量原文和列表顺序完整保留；这与
  “无语义物理布局不改变身份”的计划一致，不建立 JSON 字节往返影子合同。

### 任务 2.1 验证与主代理自审

- `cargo test -p data-pipeline-core --locked --offline`：新增 typed_raw 单元测试 9 项、既有
  阶段 0 合同 48 项和双时间测试 1 项全部通过。
- `cargo test -p data-pipeline-training --lib --locked --offline`：既有 42 项及新增的错误物理
  schema、准入后同字节数替换边界测试共 44 项全部通过；
  `cargo test -p data-pipeline-training --test phase2_typed_raw_adapter_contract --locked --offline`
  为 `0 passed / 2 ignored / 0 failed`，两个集成项只保留必须经过阶段 1 准入类型的真实测试。
- 六类真实测试先对 2026-08-04 完整清单及文件执行阶段 1 准入；为避免只用首批数据
  证明新增封闭枚举，metadata 的 25 个文件、1,182 行全部解码，其他五类各以 64 行上限解码
  首批，其中 depth snapshot 的首文件实际为 1 行。结果 `1 passed / 0 failed`，最新复验耗时为
  `53.99s`。固定真实同源
  毫秒回归分别以
  批次大小 `1` 和 `8192` 读取相同 orderbook 文件，均保留接纳序号
  `140535301 -> 140535304 -> 140535305`、数量 `210 -> 160 -> 110`、接收时间
  `.435Z/.436Z/.436Z` 及消息内列表顺序，结果 `1 passed / 0 failed`，最终耗时 `55.20s`。
- 两个 crate 的库与测试目标严格 Clippy、全工作区格式和 `git diff --check` 通过。任务自审
  未发现 data-collect 依赖、浮点规范字段、无界整文件物化、未知 schema 放行、同毫秒消息
  合并或 `event_id` 哈希重排路径。完整生产资源预算仍归任务 2.4，任务 2.1 不提前宣称该门。
- 普通全工作区回归
  `cargo test --workspace --lib --bins --tests --examples --locked --offline` 通过；该命令按已冻结
  规则排除 bench，未触发已隔离的 `data-collect` 吞吐 bench。
- 最终独立审查发现，首版把内存 `RecordBatch` 解码入口作为正常库的公开函数，仅靠“仅测试”
  命名约束，生产调用方可以绕过 `ValidatedDirectSourceParquetFileV3`。最小修正删除该公开
  入口，把错误 schema 测试下沉为模块内单元测试；生产 API 现在只保留从已准入文件开始的
  有界 Parquet 流及其首批读取辅助函数。
- 同一审查还指出生产流只复核字节数、footer 行数和 schema，没有把读取内容与阶段 1
  准入的 SHA-256 绑定。修正后，流式读取在同一已打开文件描述符上以 64 KiB 缓冲区重算
  SHA-256，匹配准入摘要后回到文件起点建立 Parquet 流。首个回归手工构造了不可能经阶段 1
  准入的非 Parquet `aaaa`，只证明摘要分支早于 Parquet EOF，不是合法边界证据，已删除且不计为
  红灯。最终回归使用当前精确 aggTrade schema 和 footer 元数据生成合法微型 Parquet，经真实
  阶段 1 准入入口取得 `ValidatedDirectSourceParquetFileV3`，再翻转文件中部一个字节并保持文件
  长度不变，精确命中 `AdmittedFileDigestMismatch` 及替换前后摘要。这会使每次打开文件先增加
  一次有界缓冲的顺序读取，完整资源和吞吐影响仍由任务 2.4 使用真实全链路实测冻结。
- metadata 固定身份和空获胜结果修复后的第四轮独立只读复审结论为代码实现切片
  `PASS`、`Critical 0 / Important 0 / Minor 0`。最新核心测试 9 项、既有阶段 0 合同 48 项、
  双时间测试 1 项、训练库测试 44 项、六类真实准入解码以及排除 bench 的普通全工作区回归
  均通过；严格 Clippy、格式和 `git diff --check` 通过。该结论只关闭当前可执行缺陷，不替代
  下述两项任务 2.1 完成门决策和任务 2.2 公共合同决策。

### 任务 2.2 前发现的现行合同冲突

- 两名独立只读代理分别核对计划、公共类型、核心身份函数和真实样本，得到相同结论：当前
  `ExecutionMarketEventV2` 强制携带只适用于 Polymarket 的 `MarketIdentityV1`，其九种载荷
  也没有 RTDS 值、Binance 深度或 Binance 成交；但 `MarketObservationCausalShellV2` 又有
  `ChainlinkBtcUsd`、`BinanceBtcUsdtDepth`、`BinanceBtcUsdtTrade` 组件，并要求贡献事件 ID
  与可见索引可追溯。计划同时要求从同一规范事件集合产生规范事件存储、双索引与
  Observation，当前代码无法同时满足这些语义。
- 该冲突不影响已经完成的六类无损解码，但阻断完整任务 2.2。实现层不得给参考数据伪造
  Polymarket 市场身份、借用 `QualityBoundary` 保存参考值、按重叠市场复制全局参考事件，或
  生成在事件存储中没有实体的悬空 ID。
- 当前真实可选项为：一，明确参考数据只形成内部规范参考状态，不进入
  `ExecutionMarketEventV2`/双索引，并同步修改 Observation 的身份与追溯合同；二，把参考
  更新提升为一等规范事件，增加非 Polymarket 作用域、RTDS/Binance 载荷及相同规范 ID/双时间
  谱系。第二项更符合现有“唯一事件带、同源追溯”目标，但属于公共合同修改，必须由用户决定。
  Binance snapshot 本身没有交易所事件时间；无论选择哪项，都不得为它伪造事件时间索引。
- 当前还不能进入任务 2.2。任务 2.2 至 2.5 尚未实施，本轮尚未提交、推送、发布、部署或
  执行远端操作。

### 任务 2.1 独立审查发现的完成门不一致

- 最终只读审查指出：步骤 2.1.2 明确要求用“实际浮点转换边界的一个代表性失败输入”验证
  浮点编码、溢出或舍入不会反写规范真相；当前实现只有原始十进制文本到 `ExactDecimal` 的
  精确解析、规范化和超范围失败，没有模型浮点编码函数或实际转换边界。由于计划同时要求
  模型浮点编码留在后续边界，不能把“不存在浮点路径”解释成该验收已通过，也不能在没有
  编码类型、精度和失败语义的情况下凭空新增编码器。
- 现实选项为：一，把实际模型浮点转换及其代表性失败验证明确移交给后续真正拥有编码合同的
  任务，本任务只验收原始文本、`ExactDecimal`、规范范围失败及不反写；二，先由用户冻结当前
  阶段可实施的模型编码类型、精度和失败语义，再补实际边界。第一项更符合现有职责分层，但会
  改变已批准计划中步骤 2.1.2 的验收归属，仍必须由用户决定。
- 同一复审又发现，步骤 2.1.1 把“完整实现第 2.3 节及第 2.4 节第 6、7 项的公开事件
  映射”列为六类无损解码的当前验证，但计划又把双时间规范身份、状态归约和最终产物生成
  分别安排在任务 2.2、2.3 和 2.5。当前代码只产生 `TypedRawRow`，不存在
  `typed_raw -> ExecutionMarketEventV2` 映射；因此不能把该句默认解读为“仅约束后续任务”。
  建议把该验证明确移交到任务 2.2、2.3 和 2.5 各自的产物边界，但这会改变已批准计划中的
  完成门归属，仍必须由用户决定。
- 因此，前述实现和测试事实保持有效，但任务 2.1 完成门尚未关闭。当前游标停在任务 2.1
  完成门决策；任务 2.2 的参考事件合同决策紧随其后。

## 2026-08-06：用户批准验收归属与参考事件方案，随后发现来源 ID 闭包缺口

- 用户批准独立复审后的纠正结论：实际模型数值编码验证移交任务 4.2.2，完整公开事件映射
  分配到任务 2.2、2.3 和 2.5；任务 2.1 的六类无损解码切片据此关闭。RTDS/Binance 参考更新
  确认为同一唯一规范事件存储中的一等全局事件，Binance REST snapshot 只进入模型可见索引，
  不伪造数据源事件时间。
- 主代理据此同步了 DPT、sim-system、rebuild-core 的未提交文档草案；宏观需求没有变化，
  但 sim-system 与 rebuild-core 需求中写死的旧公共版本引用需要机械同步。相关修改尚未提交，
  也没有开始参考事件合同代码。
- 编码前闭包复核新发现：`ExecutionMarketEventV2` 的九类载荷均要求
  `source_event_ids: Vec<CanonicalEventId>`，但现有类型没有这些 ID 对应的原始 Polymarket
  来源事件实体，也没有规定它们解析到 `EventLineageV2.source_evidence`。核心只校验 64 位
  十六进制、排序和去重，阶段 0 黄金测试使用任意占位哈希，未验证跨记录闭包。
- 三名独立只读代理确认“命名空间和解析目标未定义”是真实合同缺口；其中对抗审查同时确认，
  仅凭字段类型不能直接推断它必须成为唯一事件存储外键。现实选项为：在统一存储内增加来源
  事件实体；或升级市场事件合同，删除/替换载荷内来源事件 ID，由 envelope lineage 成为唯一
  来源真相。前者增加来源事件语义层，后者会升级模拟器消费的市场事件版本，均不能由实现层
  静默选择。
- 按方案不一致决策门，任务 2.2 合同编码暂停，等待用户决定上述来源引用语义。未修改
  contracts 代码，未运行新增测试，未提交、推送、发布、部署或执行远端操作。

## 2026-08-06：用户批准 ExecutionMarketEventV3 来源追溯方案

- 用户明确选择升级市场事件合同，不增加原始来源事件实体。当前市场合同升级为
  `ExecutionMarketEventV3`，所有原含 `source_event_ids` 的载荷同步升版并删除该字段；
  `EventLineageV2.source_evidence` 成为市场事件和参考事件唯一的来源前像与追溯真相。
- 本决定不把完整 `SourceEvidenceRefV2` 复制进载荷，也不新增另一种来源 ID。事件身份继续
  绑定排除物理清单和文件 SHA 的稳定语义来源证据，清单与文件 SHA 只保留为可审计的物理
  谱系，因此等价重分片不得改变 `CanonicalEventId`。
- 任务 2.2 的合同决策门据此解除；当前执行顺序为先同步三份当前计划和受影响需求，再以
  失败测试驱动 V3 合同、参考事件、统一记录、Observation V3、Gate A 回执 V3、六类来源
  双时间提取与统一双索引投影。阶段 2 其余授权边界不变。

## 2026-08-06：任务 2.2 可独立实施切片通过审查

### V3 合同与唯一来源追溯

- 先以缺少 V3 公共类型和构建入口的编译失败形成聚焦红灯，随后发布
  `ExecutionMarketEventV3` 及九类同步升版载荷；所有载荷均删除 `source_event_ids`，顶层
  `EventLineageV2.source_evidence` 成为唯一直接来源真相。市场事件身份使用新的 V3 哈希域，
  继续排除物理清单与文件 SHA；等价重分片不改变 ID，语义证据或最终载荷变化会改变 ID。
- 同步发布 `CanonicalReferenceEventV1`、仅含市场事件和参考事件两个已知变体的薄
  `CanonicalEventRecordV1`、`MarketObservationCausalShellV3` 与 `GateAReceiptV3`。统一记录
  使用装箱持有两个完整事件，保持 JSON 线格式不变并避免大枚举栈布局；记录拒绝未知变体和
  额外顶层字段。Observation 与门 A 回执显式绑定统一记录 V1、市场事件 V3、参考事件 V1 和
  双索引 V2。
- V3 市场事件只接受 Polymarket orderbook 或 metadata 直接证据。orderbook 证据固定为无
  原生身份且具有完整接纳坐标；metadata 固定为语义证据摘要且不虚构接纳 epoch。RTDS 和
  Binance 只能形成参考事件，不能伪造 Polymarket 市场身份或进入模拟器市场事件视图。

### 六类双时间与参考事件映射

- 六类 `TypedRawRow` 的逐行映射已经接通：orderbook 使用 `timestamp`；metadata 只有明确
  `market_resolved_ws` 平台时间时使用 `ws_platform_timestamp_utc`；RTDS 使用
  `payload_timestamp` 并在参考载荷保存 `message_timestamp`；Binance depth snapshot 不生成
  数据源事件时间；depth update 使用 `event_time`；合法 aggTrade 使用 `trade_time` 并在参考
  载荷保存 `event_time`。所有公开可见时间均使用各行 `timestamp_received`。
- 当前没有完整来源 epoch 的 metadata 与 RTDS 不生成虚构接纳坐标；前者优先使用已核对的
  原始证据摘要，无摘要的本地行使用版本化稳定语义定位摘要，后者使用权威原始载荷的版本化
  摘要。orderbook、Binance snapshot/update/aggTrade 使用真实完整接纳坐标；合法 Binance
  原生 ID 与参考载荷必须一致。
- Chainlink 与合法 aggTrade 行级参考更新均要求恰好一条直接证据和真实数据源事件时间；
  Chainlink 不接受虚构接纳坐标，aggTrade 必须保留当前 v7 已有接纳坐标。Binance snapshot
  证据必须保持数据源事件时间为空，因此只产生可见索引条目；depth update 必须具有
  `event_time`，且深度状态的 `last_update_id` 必须由直接 snapshot/update 终端 ID 支撑。
- `CanonicalEventRecordV1` 已统一产生单记录事件时间和模型可见索引条目。该切片不对集合
  重新排序；`event_id` 只用于引用，不能成为因果顺序键。

### 验证、复审与剩余完成门

- V3 市场事件、Observation 和门 A 回执新增稳定黄金文件；旧 V2 文件只保留为拒绝测试。
  `phase0_contract` 为 `50 passed`，新增任务 2.2 合同测试为 `10 passed`，核心 typed_raw 单元
  测试为 `9 passed`，双时间测试为 `1 passed`，训练库测试为 `44 passed`。
- 2026-08-04 六类真实 schema 经完整阶段 1 准入后执行来源证据映射，结果 `1 passed`，耗时
  `53.49s`；固定真实同源同毫秒 orderbook 回归结果 `1 passed`，本轮耗时 `54.58s`。标准
  全工作区库、二进制、测试与示例回归退出码为 0；严格 Clippy、格式和 `git diff --check`
  全部通过。
- `gpt-5.6-sol/ultra` 独立只读复审在修正统一记录未知字段、参考事件不可能证据组合、来源
  形状、黄金身份及 rebuild-core 一处 V2 当前陈述后，结论为 `PASS`、
  `Critical 0 / Important 0 / Minor 0`。
- 当前只关闭任务 2.2 可独立实施的合同、逐行映射、身份与单记录投影切片。步骤 2.2.4 的
  完整文件/行组/批次/线程扰动、索引摘要和同源时间最终归约顺序，必须在任务 2.3 形成归约
  结果、任务 2.5 物化完整索引后验收；不能用当前单记录测试提前宣称整项完成。当前游标进入
  任务 2.3。本轮没有提交、推送、发布、部署或远端状态变化。

## 2026-08-06：任务 2.3 纠偏、已完成切片与生产语义决策门

### 用户纠正及当前计划同步

- 用户确认 `start_at` 就是 metadata v5 的 `market_start_utc`。重新核对公共类型、真实字段和
  当前计划后确认该纠正成立：`MarketWindowV1.start_at_utc_ms <- market_start_utc`、
  `end_at_utc_ms <- market_end_utc`，不存在另一套开始触发条件。先前把 `start_at` 列为缺口的
  判断已撤回。
- 生命周期继续使用现有 `MarketLifecycleTransitionV2`，三个官方布尔状态继续使用现有
  `MarketTradingStateV2`；不新增盘前、盘中、盘后、结算后的四阶段公开状态机，也不新增
  第十个 Observation 组件。`current_to_resolution_pending` 只改变采集运行角色，不产生退休；
  只有合格终态的 `active_to_retired` 才进入 `Retired`。`active`、`closed`、
  `accepting_orders` 原样独立，不参与上述阶段推断。
- 上述已批准语义已直接同步到当前计划第 2.4 节和步骤 2.3.4；需求的宏观目标与验收标准没有
  改变，因此没有增加需求条目。

### 跨模块陈旧叙事清理

- `plan/rebuild-core-plan.md` 已删除 DPT “当前任务 2.2 实施中”的陈旧游标，并把笼统的“公共
  Rust V2 顶层类型”改为引用其第 1.6 节列明的当前公共类型；对应事实已记录到
  `changelog/rebuild-core-changelog.md`。
- `plan/sim-system-plan.md` 已删除 DPT “当前任务 2.2”的陈旧游标副本，只保留稳定的 V3 公共
  合同依赖事实；对应事实已记录到 `changelog/sim-system-changelog.md`。两份跨模块计划均不
  复制 DPT 当前执行游标。

### 已完成且不依赖新决策的归约修正

- `canonical_reduction_report_v1` 校验现在拒绝操作和处置的不可能组合：有输出只允许
  `Merge/Overwrite/Delete/Reject + Produced`，无输出只允许 `Reject + NoOutput`，抑制只允许
  `Confirm/Suppress + Suppressed`。新增聚焦回归证明错误组合不能通过编码或解码边界。
- Polymarket 后续完整 `book` 回归已补齐：与现状完全相同的独立快照只生成
  `POLYMARKET_IDENTICAL_BOOK_CONFIRMATION` 确认报告；下一份缺少既有价位的完整快照生成
  `BookResyncDeltaInference` 删除转换、`ResyncBoundary`、精确数量守恒和指向结果事件的
  `Delete + Produced` 报告。该测试不把独立快照误当重复消息。
- 当前保留改动下执行
  `cargo test -p data-pipeline-core --tests --locked --offline`，结果依次为核心单元 9 项、阶段 0
  合同 51 项、双时间 1 项、Binance 归约 15 项、规范合同 10 项、Polymarket 归约 10 项、终态
  合并 3 项全部通过，0 项失败；`cargo fmt --all -- --check` 与
  `cargo test -p data-pipeline-training --tests --locked --offline` 通过，后者普通测试为 46 项通过、
  0 项失败、11 项按真实数据门忽略。随后显式运行这些真实数据测试：阶段 1 准入 6 项、真实
  Binance 归约 1 项、typed_raw 真实适配与终态合并 4 项全部通过，0 项失败；
  `git diff --check` 通过。这些真实回归只证明现有切片，不代表尚未接通的完整生产路径。

### 真实 metadata 复核

- 主代理只读扫描 2026-08-03 与 2026-08-04 的 49 个 metadata v5 Parquet。两日分别为
  1,156 与 1,182 行；两日合计 575 个 `prefetched_to_active` 均满足
  `lifecycle_scheduled_at_utc == market_start_utc`，575 个
  `current_to_resolution_pending` 均满足
  `runtime_role_scheduled_at_utc == market_end_utc`。2026-08-04 少一个开始/结束转换，是当日重启
  后恢复的当前市场快照，证明生产路径不能假设一定看到原始开始转换。
- 两日各有 288 个 `active_to_retired`，均由合格终态形成；Gamma 普通观察为
  `(active=true, closed=false, accepting_orders=true)`，三个 Gamma 兜底终态为
  `(active=true, closed=true, accepting_orders=false)`。该事实再次证明 `active=true` 在盘前、
  盘中、盘后及结算后都可能出现，不能承担生命周期判断。
- 两日终态分别为 287/286 个 WebSocket 终态和 1/2 个 Gamma 兜底终态。三个 Gamma 终态的
  `closed_time_utc` 均早于采集接收约 342 至 399 秒，但这只是当前样本事实，不足以把字段冻结
  为通用平台判定生效时间。

### 发现冲突后的回退与当前决策门

- 实现 Gamma 兜底终态时发现现行计划、统一来源映射和上游合同不能同时支持“直接把
  `closed_time_utc` 写入 `source_event_at_utc_ms`”。相关临时代码和测试已完整撤回；当前
  `source_evidence_from_typed_raw` 仍只允许明确的 WebSocket 平台时间成为 metadata 来源事件
  时间。
- 当前官方 Gamma 市场接口只给出 `closedTime` 字段及类型，没有定义它等于市场判定的发布或
  平台生效时刻；上游当前合同还明确允许该值晚于响应接收时间。因此不能从三个当前样本反推
  通用语义，也不能使用接收、请求、验证、本地调度或 `end_at` 时间替代平台来源时间。
- 生产 `MarketIdentityV1` 仍缺少 `window_id` 的规范前像、领域分隔和版本规则。活动树只有字段、
  任意测试值及两处“不得作为生产派生规则”的注释；`start_at/end_at` 的来源已经明确，并不能
  自动确定 `window_id` 的身份语义。
- `TerminalWatermarkV2.canonical_event_root_sha256` 与
  `lineage_identity_sha256` 仍只有公共字段和测试占位，没有生产计算前像、截止范围及是否包含
  当前终态的规则。直接使用最终事件存储根会与包含该字段的终态事件身份形成循环依赖。
- 六质量状态的类型已经存在，但 `Missing/UnknownNotObserved` 的生产边界、Binance
  `confirmed-hole` 如何进入模型可见的唯一事件存储、以及两类终态等待耗尽状态的对应质量仍
  需最终收口。当前确认缺口只产生 `NO_OUTPUT` 报告并清空盘口，尚不能满足任务 2.3.5。
- 独立质量审查进一步确认：连续必需状态尚无基准或丢失基准应为 `Missing`，尚未发生的稀疏
  事件应为 `UnknownNotObserved`；Binance 陈旧重复更新只抑制，不能冒充模型可见的 `Stale`，
  当前可生成 `Stale` 的真实强类型来源是 RTDS `freshness_status=stale`。推荐把
  `budget_exhausted_after_valid_nonterminal` 映射为 `Missing`、把
  `budget_exhausted_without_valid_response` 映射为 `UnknownNotObserved`，两者都使用既有
  `WaitingExhaustedQuarantine` 且不得结算；该精确映射尚未写入当前计划，等待用户决定。
- 当前终态合同校验还会拒绝“等待耗尽事件自身无数据源事件时间、但终态水位保留此前合法
  数据源事件时间”的真实可达组合；首次终态声明冲突也被错误固定为 `Fresh -> Invalid`，而
  完整流起点应是 `UnknownNotObserved -> Invalid`，只有已经输出合格终态后的矛盾声明才是
  `Fresh -> Invalid`。这些属于任务 2.3.5 的真实实现缺口，不要求新增公共质量枚举。
- 真实生产连接层尚不存在：当前真实回归仍由测试手工选择文件、注入 `window_id` 和终态水位
  摘要后调用各归约器。步骤 2.4.1 明确要求任务 2.1 至 2.3 的真实解码、双时间和状态归约使用
  同一路径接通，因此任务 2.4 尚未开始；不得用组件测试或第二条语义路径冒充完整生产资源
  试验。
- 当前游标保持 `阶段 2 -> 任务 2.3`。上述事实首次触发方案不一致决策门；本轮尚未提交、
  推送、发布、部署或执行远端状态变更。

### 决策门外可独立修复的质量边界与归约顺序

- 对任务 2.3.5 的可执行切片继续核对后，已修正此前“Binance 已确认缺口只产生
  `NO_OUTPUT`”的实现缺口：`not_applied_confirmed_hole` 现在以最后一个可信完整盘口形成
  `ConfirmedGap + Required` 参考深度事件和 `Reject + Produced` 报告，随后清除可继续推进的
  当前盘口；只有新的完整 snapshot 才能以 `Fresh + ResyncBoundary` 恢复。新采集纪元在尚未
  取得本纪元 snapshot 时，会把上一纪元的最后可信盘口标成 `Missing + Required`，而运行起点
  在从未取得任何基准时只保留无输出拒绝报告。陈旧重复更新仍然只抑制，不冒充模型可见的
  `Stale`。
- 公共 `QualityBoundaryV2` 校验已补上包络一致性：边界的 `current_quality` 必须精确等于外层
  `EventQualityV2.availability`。这防止质量载荷与统一事件包络对同一时刻给出两种状态。
- Polymarket 跨 `source_sequence_epoch_id` 的旧实现会保留上一纪元盘口，并把新纪元首次完整
  `book` 错误解释为旧数量到新数量的连续变化。聚焦红灯证明该行为与步骤 2.3.1 冲突；最小
  修正现在在纪元切换时按资产清空 L2/BBA，仅为原本 `Fresh` 的资产生成
  `Fresh -> Missing + Required`，拒绝新纪元完整 `book` 之前的绝对 `price_change`，再由本纪元
  首个完整 `book` 从空状态重建价位，最后形成 `Missing -> Fresh + ResyncBoundary`。同一纪元的
  相同完整 `book` 仍只确认；跨纪元的相同完整 `book` 必须重建，空完整 `book` 也能建立合法
  空基准。
- 补齐步骤 2.3.2 的完整盘口路径后，`trade -> book` 与 `book -> trade` 两种事件时间顺序均只
  产生一条公开成交和一条 `250 -> 200` 盘口转换；转换的队列消耗为 `50`，已归因成交量与已
  覆盖量均为 `0`，质量保持 `Unattributed/DeterministicInferred`，不会因邻接建立精确因果或
  双算。此前同类 `price_change` 双顺序测试继续保留。
- 只读扫描 2026-08-04 的 39,908,199 条真实 Polymarket orderbook 行，按
  `(source_sequence_epoch_id, market)` 分为 291 组：物理采集顺序中的
  `collector_admission_sequence` 没有倒退，但 290 组都至少发生一次相邻数据源事件时间倒退，
  合计 464,975 次。一个已核实实例为同一市场、同一纪元中接纳序号 `138738963` 的来源时间
  `1785801600021` 紧邻接纳序号 `138738964` 的来源时间 `1785801600012`。因此把数据按来源
  事件时间重排后，不能再要求接纳序号全局递增；步骤 2.3.1 只要求它在相同来源毫秒内保存
  采集先后。
- 对应修正把 Polymarket 归约器的“上一接纳序号”改为当前纪元内已见坐标集合：不同来源
  毫秒先按数据源事件时间推进，同一来源毫秒再按接纳序号推进；同一接纳坐标仍只能标识一条
  消息，完全相同的重试仍显式抑制，跨纪元仍不比较。新增回归以来源时间
  `1099/1100`、接纳序号 `302/301` 证明先产生 `250 -> 200`、再产生 `200 -> 210`，并保留同一
  接纳坐标绑定不同来源时间时失败关闭的原有回归。
- 完成上述修复后，`cargo test -p data-pipeline-core` 为 106 项通过、0 项失败；其中 Polymarket
  归约合同为 14 项通过。`cargo clippy -p data-pipeline-core --all-targets -- -D warnings`、
  `cargo fmt --all -- --check` 与 `git diff --check` 均通过。该结果只关闭不依赖四项当前语义
  决策的代码切片，不代表任务 2.3、任务 2.4 或阶段 2 已完成。

## 2026-08-06：四项过度设计复核、metadata 生产归约与剩余等待耗尽冲突

### 用户质疑与复核结论

- 用户指出 `window_id` 当初可能属于多写字段、`market_end_utc` 已存在、两个终态摘要可能
  多余，并要求重新证明等待耗尽与六类质量建议是否真实。主代理与两名独立只读审查代理逐项
  复核当前计划、合同、生产代码、真实 metadata v5 和上游当前合同后确认：
  - `window_id` 没有生产者、消费者或验收语义，只有任意测试占位；当前公共身份改为
    `MarketWindowV2/MarketIdentityV2`，只保留 `market_start_utc/market_end_utc` 的直接映射，
    不增加替代字段。
  - `market_end_utc` 是市场交易窗口结束时间，不是平台判定发布或生效时间。只有
    WebSocket `market_resolved.timestamp` 能形成终态事件时间投影；Gamma 兜底不得用
    `market_end_utc`、`closed_time_utc`、接收、请求、调度或应用时间补造该坐标。
  - `TerminalWatermarkV2` 的时间与外层 `EventTimeV2` 重复，两个摘要没有生产前像或消费方，
    其中事件根摘要还会形成循环依赖；该类型和终态字段已完整删除，没有增加替代摘要。
  - 先前把 `budget_exhausted_after_valid_nonterminal` 映射为 `Missing`、把另一原因映射为
    `UnknownNotObserved` 的建议没有证据支持，现已撤回。两类上游摘要都只表示采集跟踪预算
    结束，不是平台终态、市场数据丢失或模型可见质量转换，不生成 `QualityBoundary`。
- 上述决定改变当前合同和实现细节，但没有改变需求文档的宏观业务目标与验收，因此只同步
  当前计划、代码和本历史记录，没有给需求文档增加字段级声明。

### 任务 2.3.4 生产归约

- 新增 `PolymarketMetadataReducer`：首个完整 Gamma `metadata_snapshot` 直接构建
  `MarketIdentityV2`、`MarketWindowV2`、固定资产/outcome 映射和源证据初始 tick；
  `start_at_utc_ms/end_at_utc_ms` 分别精确取 `market_start_utc/market_end_utc`。后续 metadata
  强校验固定身份和资产顺序，显式生命周期转换校验前态，三个交易状态字段保持独立
  `Option<bool>`，相同三元组只确认，不推导盘前、盘中、盘后或结算后状态机。
- `prefetched_to_active/candidate_to_active` 只按显式 metadata 转换产生生命周期事件；
  `current_to_resolution_pending` 只是采集运行角色变化，在 `end_at` 不产生退休。通用 metadata
  归约器拒绝直接生成 `active_to_retired`，该事件只由通过同一资格校验的终态合并器生成。
- WebSocket 合格终态现在同时产生公开 `Resolved` 和 `Active -> Retired`；Gamma 合格终态产生
  公开 `Resolved`、`Active -> Retired` 及原样的独立交易状态。WebSocket 两项保留平台事件
  时间；Gamma 三项都保持 `source_event_at_utc_ms=None`，只按接收时间进入模型可见投影，不能
  驱动隐藏平台退休、资金释放或结算。
- 首次冲突终态声明从 `UnknownNotObserved -> Invalid`；已经输出合格终态后的矛盾声明才从
  `Fresh -> Invalid`。相同终态重投仍只抑制，不重复产生退休或结算输入。
- 真实 Gamma 回归首次暴露并纠正了一项审查建议：固定样本的
  `source_evidence_channel=None`，不是 WebSocket 的 `market` 频道。生产资格校验现按真实 HTTP
  Gamma 证据要求空值，没有复制错误建议。
- 官方 Polymarket 市场频道文档只把 `new_market` 定义为“市场创建”，并不等于
  `market_start_utc` 或 `Active`。2026-08-03/04 当前清单也没有 `new_market` 计数；因此通用
  orderbook 归约器只用它确认已由 metadata 建立的固定市场身份并形成无输出报告，不生成生命周期
  事件。核对入口：`https://docs.polymarket.com/market-data/websocket/market-channel`，访问日期
  `2026-08-06`。

### 质量、诊断与验证

- RTDS `freshness_status=stale` 已增加聚焦合同测试，明确映射为现有 `Stale`；没有为了覆盖六个
  枚举而制造 `UnknownNotObserved` 或其他伪事件。
- “有界首分歧诊断”需要真实生产编排器同时拥有输入证据、两个实际游标和前后状态；若在任务
  2.3 预先增加独立结构只会形成无生产者、无消费者的闲置合同。当前计划已把实际诊断记录移到
  步骤 2.4.1 的唯一生产路径，仍保留第 2.6 节与验收项的内容要求，且明确不得写成规范事件、
  Observation 组件、终态摘要或水位。
- 当前代码执行 `cargo test -p data-pipeline-core --locked --offline` 为 114 项通过、0 项失败；
  `cargo clippy -p data-pipeline-core --all-targets --locked --offline -- -D warnings`、
  `cargo fmt --all -- --check` 与 `git diff --check` 均通过。固定真实 Gamma 回归
  `real_gamma_terminal_is_visible_but_has_no_event_time_projection` 通过，证明接收时间
  `1785831360020` 可见而事件时间投影为空；固定真实 WebSocket 终态回归
  `real_ws_terminal_metadata_pair_merges_once` 通过。

### 尚未自行调和的等待耗尽冲突

- 上游 `resolution_followup_summary` 两类等待耗尽行真实存在，必须用于运行隔离审计；但当前
  DPT 计划明确它们只进入审计、不得成为模型可见质量或平台终态。与此同时，公共合同仍保留
  `MarketResolutionTerminalV3::WaitingExhaustedQuarantine`，`sim-system` 计划仍保留
  `EXHAUSTED_QUARANTINED` 输入状态，而模拟器正式入口只消费唯一事件存储及双索引。
- 若把该变体写入现有模型可见索引，会让仅采集器/运行控制信息进入模型可见事件集合；若不写
  索引，模拟器现有正式入口无法取得它。当前没有第三个已批准控制索引，也不应为保留一个闲置
  枚举而新增。相关实现继续暂停，等待用户决定删除陈旧公共变体及模拟器输入状态，或明确批准
  一条与 Observation 隔离的版本化控制入口；任务 2.4 在该冲突关闭前不开始。

### 独立审查纠正与最终聚焦回归

- 三名使用 `gpt-5.6-sol/ultra` 的独立只读审查代理分别核对身份/水位删除、Gamma 与
  `end_at`、等待耗尽与跨模块入口。审查确认 `window_id` 与 `TerminalWatermarkV2` 删除完整，
  Gamma 没有把 `market_end_utc`、`closed_time_utc` 或接收时间冒充平台事件时间，
  `new_market` 只确认固定身份。对 2026-08-03/04 真实 metadata 的扫描显示 576 个终态市场
  恰有 576 条唯一终态归属方记录，其中 573 条为 WebSocket、3 条为 Gamma，同市场双归属方为
  0；三个 Gamma 市场也没有对应 WebSocket `market_resolved`。因此“Gamma 先到、同结果
  WebSocket 后到”没有当前生产合同或真实样本证据，不增加跨来源归属方状态机或理论冲突
  测试。
- 审查发现两个真实不可信边界缺陷。第一，WebSocket 终态资格校验虽然随后无条件生成
  `Active -> Retired`，却没有复核上游完成终态转换已经要求的生命周期状态、生命周期转换、
  运行角色及运行角色转换。新增一个损坏 metadata 行回归先证明旧实现会错误接纳，再补齐
  四项既有资格条件；没有为四个字段穷举排列组合。第二，Gamma
  `uma_resolution_status` 以及 WebSocket/Gamma outcome 比较没有遵循采集端已有的
  `trim() + ASCII 大小写不敏感` 语义。一个组合回归先证明合法空白会被错误拒绝，再统一比较
  语义；规范输出继续使用固定身份中的 outcome 标签。
- 修复后 `cargo test -p data-pipeline-core --locked --offline` 为 116 项通过、0 项失败；
  `cargo test -p data-pipeline-training --tests --locked --offline` 的非忽略项为 46 项通过、0 项
  失败。固定真实 Gamma 与 WebSocket 终态回归各 1 项通过，核心严格 Clippy、全工作区格式和
  `git diff --check` 通过。
- 一次未带具名跳过项的扩大工作区测试在 UTC 12:00 前命中 `data-collect` 当前验收文档已经
  登记的时钟依赖夹具：它把当日 `12:00:00Z` 固定成接收时间，触发
  `invalid_real_admission_clock` 后超时。这不是本次改动或已证明的生产故障。随后按仓库普通
  回归边界运行
  `cargo test --workspace --lib --bins --tests --examples --locked --offline -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second`
  通过；其中 `data-collect` 库为 386 项通过、1 项忽略、1 项具名跳过。
- 能力枚举旧名称 `QualityGapResyncAndWatermark` 属于既有序列化合同值。本轮不在任务 2.3
  无升版静默改名；任务 2.5 定稿能力矩阵时再按当前质量/隔离语义决定版本化名称。归约器尚无
  生产编排器消费是任务 2.4.1 明确要完成的下一项工作，不是当前组件测试可以替代的证据。

### 用户批准关闭等待耗尽外部合同冲突

- 用户批准推荐方案：删除公共
  `MarketResolutionTerminalV3::WaitingExhaustedQuarantine` 及模拟器外部生命周期
  `EXHAUSTED_QUARANTINED`，不增加第三控制索引。上游
  `resolution_followup_summary` 继续作为 typed_raw 隔离审计事实保留，不转换为规范市场事件、
  `QualityBoundary`、数据源事件时间或模型可见状态。
- 公共终态现在只表达合格 `Resolved`。若请求范围包含因等待耗尽而缺少合格市场判定的市场，
  `MarketResolutionTerminal` 能力必须为 `FAIL` 且不得签发门 A；只有显式排除该市场的新范围
  运行才能重新验收其余范围，不得在原运行中静默丢弃。
- `sim-system` 计划同步删除外部耗尽分支。输入结束仍无合格判定时，只使用模拟器内部运行失败
  封存与隔离，不生成公共市场终态、订单终态、查询空页、资金释放、结算或赎回。
- 本决定不改变 DPT 与 sim-system 需求文档已经冻结的宏观失败关闭语义，因此只修改当前
  DPT/sim-system/rebuild-core 计划、公共合同、对应测试和三份变更记录；历史条目继续保留当时
  的决策过程，不承担当前权威。

### 等待耗尽审计闭环、上游身份语义修复与任务 2.3 验收

- 主代理通过生产准入和 Parquet 解码入口核实了闭合的
  `/home/chillchill/new-datasets/2026-08-05/typed_raw/utc_day=2026-08-05/manifest.json`；其
  `state=closed`、`closure_reason=utc_day_barrier`。前一小时身份快照文件
  `polymarket_market_metadata_14.parquet` 的 SHA-256 为
  `b1dbc1462b24856a6402eed60ecd6d1b4625c19f148070e4847d847276d85746`，含唯一等待耗尽摘要的
  `polymarket_market_metadata_15.parquet` 的 SHA-256 为
  `8718d8af818172214abf5a77762f90fe3d8fb4905d4218917e7c162704cdfefe`。真实摘要归属
  `btc-updown-5m-1785941700`，接收时间为 `1785942480031`，状态为
  `budget_exhausted_after_valid_nonterminal`，记录 6 次尝试、6 次有效非终态响应、0 次传输不可用
  和 0 次错过调度槽；该本地调度摘要没有数据源事件时间。
- typed_raw 边界现在强制 `resolution_followup_summary` 只能搭配两种预算耗尽状态，并拒绝把
  预算耗尽状态放入其他行种类。`PolymarketMetadataReducer` 对这两种真实摘要分别生成精确原因
  `POLYMARKET_RESOLUTION_WAIT_BUDGET_EXHAUSTED_AFTER_VALID_NONTERMINAL` 或
  `POLYMARKET_RESOLUTION_WAIT_BUDGET_EXHAUSTED_WITHOUT_VALID_RESPONSE` 的
  `Reject + NO_OUTPUT` 报告；报告保留固定身份与输入来源证据，但不生成规范事件、质量边界、
  数据源事件时间、退休、判定或任何经济副作用。
- 独立审查发现并由聚焦红灯证明三项与上游现行边界不一致的身份处理：`new_market` 只校验长度
  会放过重复资产集合，标签比较没有采用上游首尾空白规则，WebSocket 原始平台时间也没有先
  去除首尾空白再解析。最小修正现在要求资产 ID 精确集合相等，并统一采用上游已有的
  `trim() + ASCII 大小写不敏感` 标签比较及 `trim().parse()` 时间解析；没有增加新类型、配置
  或第二条身份路径。
- 新增真实回归 `real_waiting_exhaustion_summary_remains_audit_only`，固定上述闭合清单、两个文件
  SHA 和真实市场，通过生产准入、Parquet 解码、来源证据映射、身份快照构造与 metadata 归约
  入口证明该摘要只形成审计拒绝。任务 2.3 的全部 6 项 Polymarket 真实回归为 6 项通过，耗时
  113.10 秒；真实 Binance 归约回归为 1 项通过，耗时 61.70 秒。
- 最终本地验证为：`data-pipeline-core` 119 项通过、0 项失败；
  `data-pipeline-training` 非忽略测试 46 项通过、0 项失败，另有 13 项真实数据门按默认规则忽略；
  普通工作区回归按既定边界具名跳过
  `four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second` 后全部通过，
  其中 `data-collect` 库为 386 项通过、1 项忽略、1 项具名过滤。核心与 training 的严格 Clippy、
  全工作区格式检查和 `git diff --check` 全部通过。
- 任务 2.3 已据此关闭，计划当前游标前移至任务 2.4。该关闭只证明各真实解码、双时间和归约
  组件及其真实样本回归；任务 2.4.1 仍须用唯一生产编排路径接通这些组件并实测资源，不能以
  当前测试手工选取文件和市场的路径冒充生产资源证据。
- 两名 `gpt-5.6-sol/ultra` 独立只读审查代理完成关闭复核。身份与完整任务审查结论为
  `PASS / Critical 0 / Important 0 / Minor 0`；等待耗尽与跨模块边界审查结论为
  `PASS / Critical 0 / Important 0 / Minor 1`。后一项 Minor 指出计划首屏曾同时写“任务 2.2
  已关闭”和“步骤 2.2.4 待任务 2.5”，现已手术式改为任务 2.2.1 至 2.2.3 已关闭、步骤
  2.2.4 集合级验收待任务 2.5；该修正不改变代码或后续范围。两份审查均确认任务 2.3 可以
  关闭并进入任务 2.4。

### 任务 2.4 初次真实试验与 Polymarket 空盘口边界决策

- 任务 2.4.1 已开始接通唯一生产编排路径。第一次真实试验暴露准入清单中的 `utc_hour` 为
  `YYYY-MM-DDTHH`，而试验编排器错误按裸小时解析；修正后聚焦回归证明精确小时区间映射
  正确。第二次试验在完整解码小时文件时命中 `price_changes[1].best_bid=0`，旧 typed_raw
  校验将其错误判为必须为正数，运行因此在归约前失败。这两次失败均未形成任务 2.4 通过证据。
- 对真实文件
  `/home/chillchill/new-datasets/2026-08-04/typed_raw/utc_day=2026-08-04/source_kind=polymarket_market/polymarket_market_18-part-1.parquet`
  的只读核查显示：市场
  `0x242b1d2421714f5f93a1e30239e732bca05ced7c86393b7b204a32eb0f615bee` 在接收时间范围
  `[1785867275000,1785867421000)` 内共有 3935 行，其中 3897 条 `price_change` 的两项 BBA
  分别稳定为 `0.99/1` 与 `0/0.01`；14 条独立 `best_bid_ask` 也呈现相同组合。同期完整
  `book` 分别显示空 asks 与空 bids，证明 `best_ask=1` 与 `best_bid=0` 是对应盘口侧为空的
  来源边界哨兵，而不是可成交价。
- 官方 Rust SDK 的下单构造器只接受 `[tick_size, 1-tick_size]` 内价格，进一步排除把 `0/1`
  解释为可成交价。用户批准：typed_raw 原样保留合法十进制 `0/1` 来源证据；唯一 core 归约
  路径将 `best_bid=0` 和 `best_ask=1` 映射为现有 BBA 可空字段的 `None`，同时覆盖
  `price_change` 内嵌 BBA 与独立 `best_bid_ask`。不得在训练范围过滤器中另写转换，不新增
  状态、质量枚举或第二语义路径。宏观需求不变，当前计划已同步这项技术语义。

### 任务 2.4 唯一生产路径、资源硬门与最终真实试验

- `data-pipeline/training/src/canonical_run.rs` 已把冻结输入准入、六类范围内 Parquet 解码、统一
  外部排序、任务 2.3 归约器、`CanonicalEventRecordV1` 封装、双时间投影和四类逻辑摘要接入
  同一生产函数。`data-pipeline/training/src/typed_raw.rs` 的范围读取只在当前生产编排器内部按
  精确市场和接收时间左闭右开区间选择业务行；物理 schema 和选择坐标仍对每个物理批次严格
  校验，公共完整文件读取入口没有放宽。当前暂存结果仍明确不是门 A 产物、manifest 或回执。
- 独立资源审查发现 `peak_hot_markets=1` 只约束单市场数量，不能约束单市场内部的累计状态。
  Polymarket 的纪元/接纳序号/活动盘口集合和 Binance depth 的纪元/活动价位/删除证据集合会
  随输入增长。最小修正只增加统一的 `max_retained_reducer_entries`、
  `peak_retained_reducer_entries` 和 `RetainedReducerStateLimit`；它们统计当前仍保留的累计集合
  条目，不表示字节、RSS 或增长速率，也没有按来源建立配置矩阵。Arrow 批次字节采用
  `RecordBatch::get_array_memory_size()` 报告的物理数组大小，在转换业务行前检查；该口径不
  表示分配器精确值、RSS 或进程总内存，批次形成时的瞬时分配由外层 cgroup 硬门兜底。
- 最终试验范围为 `2026-08-04` 合格清单中的市场
  `0x242b1d2421714f5f93a1e30239e732bca05ced7c86393b7b204a32eb0f615bee`，接收时间
  `[1785867275000,1785867421000)`。`input_set_identity_sha256` 为
  `a84276dc04344fcb8c634f60b0c01dda097a15a5ae924e8f196c0b3d5a45044a`，
  `input_manifest_set_sha256` 为
  `c0a4d65bc097d658cedbac11af3be4af60b3b3f36aaf35fa63bcc84173f4ae51`，每日 manifest SHA-256
  为 `f5b4a00f5aacf1bd3f15c2d54ec85276ca893c2bb43608dbf52a2181ccbf5624`。选中的 11 个真实文件
  身份为：metadata part 0/1 分别是 `c5c65351811e7717bbb1781c73f5926a16dc1f9c481bcf414217138cd916ed3f`
  和 `75d63e62b6b42f228ab950eddcea2ac0fc02f0a2d7cefee6b934a97481d3764a`；Polymarket orderbook
  part 0/1 分别是 `79e7064df1b0e8087fdd4d27284c1c03fdde46c9b46f91887dc3485ffa6dccb7` 和
  `76bfcc1129f665f9e212a432d43c0fa931d2302b169b4d62878fd063b7647533`；RTDS part 0/1 分别是
  `45ab0c61f1e744ec2d0985b7569f6a908ec97663103c52ace60252c6d0dca3c1` 和
  `5688a7155b78a8a578ddc3a76fb611ee31c115bb7d3deb6becb7471d2f9653e4`；Binance depth snapshot
  是 `87ed05f5e9a2d757b55cd360af2824eac8dce34a9d7e3db3f30bcae63a634775`；depth update part 0/1
  分别是 `0c73f67feff706382e6f65a3079e119edc2fead7f7f113f93fea9ab39697b9c2` 和
  `dcbc05e742a1ca029c479b788c451337a286da82010e29896b2c4cbe1a6709c1`；aggTrade part 0/1 分别
  是 `845b786982bc6446b291390f68f6a3707fd6b8e235384a7ab835b2abe7868246` 和
  `d89bac33f10a3e33539ceba44893ed6a6e29ce8eac8e38e3691b6356fa3ef9ff`。
- 试验直接执行已构建测试二进制，外层命令为
  `systemd-run --user --unit=dpt-task24-final --property=MemoryAccounting=yes --property=MemoryMax=1073741824 --property=MemorySwapMax=0 --wait --pipe /usr/bin/time -v /home/chillchill/sim-system/target/debug/deps/phase2_canonical_run_contract-9456c258d8c7efd1 real_scoped_six_source_run_uses_one_deterministic_production_path --ignored --exact --nocapture`。
  二进制 SHA-256 为 `23eee197a8a990b805dd3082953c60fa72377465e6592c11cce737d493f1c1f4`，基线
  `HEAD=854a6aabb8ff70ef01bd86f31bf96fd06b5b878e`，`Cargo.lock` SHA-256 为
  `1122bc61e47c1b91099e62572ad2a33e9b668008545c5a90e2d7ef9a0cf1f825`，工具链为
  `rustc 1.95.0 (59807616e 2026-04-14)` 与 `cargo 1.95.0 (f2d3ce0bd 2026-03-21)`。未提交工作树
  的 `HEAD` 不被冒充为试验代码身份；实际代码身份由上述直跑二进制摘要固定。
- 唯一生产配置为：解码 `8192` 行、Arrow 报告批次 `64 MiB`、临时排序行 `128 KiB`、每来源
  epoch `1`、待配对 WS 终态 `1`、保留归约状态 `32768` 条；排序 run `2048` 行/`32 MiB`、
  临时文件 `8`、扇入 `4`、打开排序文件 `5`、读写批次 `1024` 行、临时磁盘 `4 MiB`。第二次
  运行只把解码改为 `4096` 行、排序 run 改为 `4096` 行、读写批次改为 `2048` 行，其余硬门
  相同；它只用于物理扰动，不形成第二生产配置。
- 两次运行均选中 9379 行：Polymarket orderbook 3935、metadata 3、RTDS 143、Binance depth
  snapshot 1、depth update 1443、aggTrade 3854。两次结果均为规范记录 13245 条，其中市场
  事件 7809、参考事件 5436；事件时间投影 13242、模型可见投影 13245、归约报告 20928。
  四类摘要完全相同：规范记录
  `6201d3cff4147f98daf816fc7d9ba8418ddc8e5edbbff6b19155c1f5f7c8ed9f`、事件时间投影
  `08bcdb6e9e2eba2f7d43febb5e4a5c256190b62f0042d7fb4aa3955bebd91e39`、模型可见投影
  `399bde97295b2f85e1a6bf474a1960fa1e83018d18ed269cdc9f137a73820ab9`、归约报告
  `f515529de5978b3226af7b850674569e084ec01143933fb4583fa9248c14c481`。测试结果为
  `1 passed / 0 failed / 1361.14s`，墙钟 `22:41.15`。
- 唯一生产配置的实测高水位为：物理解码批次 8192 行、Arrow 报告物理数组 16221452 字节、
  选中业务行批次 3934 行、临时排序行 102808 字节、Polymarket/Binance epoch 各 1、直接来源
  输入文件 1、待归约业务输入行 1、热市场 1、待配对 WS 终态 1、保留归约状态 24684 条、
  业务处理并行度 1。排序器产生 5 个初始 run、2 轮合并，run 峰值 2048 行/11009024 字节，
  写缓冲 1024 行/4786084 字节，临时文件 6、排序器打开文件 5、堆条目 4、临时磁盘 2322936
  字节。这里的业务处理并行度不等于进程线程数，来源输入文件与排序器文件也不冒充进程全部
  文件描述符。
- 单条规范记录、投影和归约报告的编码峰值分别为 946318、157 和 47806 字节；它们只作为
  任务 2.5 正式写入批次边界的真实依据，本任务没有为三类输出分别增加配置。直接二进制的
  最大 RSS 为 290020 KiB，冻结外层验收上限为 320 MiB；cgroup v2 总计费内存峰值为
  278515712 字节，`memory.max=1 GiB`，`memory.swap.max=0`，交换峰值为 0。运行中末次读取的
  `memory.events` 为 `low/high/max/oom/oom_kill/oom_group_kill=0`，服务最终以
  `status=0/SUCCESS` 退出。没有增加进程内 RSS 轮询器或通用资源管理器。
- 同一真实测试随后把 Arrow 报告批次字节上限降为 `1`，断言命中带路径、批次、观测值和上限
  的 `TypedRawReadError::DecodeBatchBytesLimit`，并证明失败工作根为空；整个测试通过，因此该
  代表性超限没有留下半成功暂存结果，也没有为其他资源建立超限排列矩阵。
- 长测完成后，独立审查发现待配对 WS 终态在集合已满且 claimant 键重复时会先误报容量
  超限。主代理先把既有顺序抽为窄判定函数并运行聚焦回归，旧顺序按预期失败；随后只把重复
  判定移到容量判定之前，回归转绿。该修正不改变成功输入的 `pending_ws_terminals` 映射、状态、
  配置、持久分配、输出或资源水位；上述长测二进制不包含该后续失败分类修正，成功长测本身
  证明真实范围未命中该分支，不把长测误写成修正后源码的完整重跑。
- 最终受影响回归为：`data-pipeline-core` 120 项通过、0 项失败；
  `data-pipeline-training --tests` 的库测试 48 项和普通集成测试 2 项通过，14 项真实数据门按
  默认规则忽略；严格 Clippy、全工作区格式检查和 `git diff --check` 全部通过。任务 2.4 只
  关闭 `DPT-AC-010/011` 在唯一生产路径资源与代表性失败清理上的当前切片；正式写入、损坏
  检查、原子发布和完整门 A 验收仍归任务 2.5。

### 任务 2.5 实施前方案不一致决策门

- 任务 2.5.1 当前列出唯一事件存储、市场事件视图、参考事件视图、事件时间索引、模型可见
  索引和 Observation 六个名称，却要求验证“四项输出”；`GateAReceiptV3` 只绑定唯一事件
  存储、事件时间索引、模型可见索引和 Observation 四个 manifest。当前代码只有统一事件
  存储中的两个类型变体，没有第二份市场/参考事件带。独立审查推荐把它冻结为“四个物理产物
  组、两个逻辑类型化视图”，市场/参考视图都读取同一存储，不复制事件或增加 manifest；用户
  尚未决定，不能由实现自行调和。
- `MarketObservationCausalShellV3` 只冻结外壳和校验，没有冻结何时生成 Observation、九组件
  分别由哪些可见事件更新、全局参考事件作用于哪些市场、`contributing_event_ids` 是全历史
  还是最小状态前沿，以及组件/市场状态摘要的前像。推荐方案是每个改变九组件之一的可见事件
  处理后立即为覆盖市场物化一条 Observation，同一接收毫秒不合并，贡献事件只保留各组件
  当前状态或质量的最小因果前沿；按任意可见截止点即时构造需要改变当前产物与消费者措辞，同一
  毫秒合并会丢失可见顺序。该选择会改变因果语义，必须由用户决定。
- 首个正式门 A 的覆盖范围也未冻结。推荐先选择一个已经证明具有完整事件链的代表市场，run
  manifest 明确市场集合、接收时间左闭右开区间、纳入的全局参考范围和输入日；任务 2.4 的
  146 秒资源窗口在证明完整事件链前不能直接晋升。若选择整日全部市场，现有单市场归约器和
  资源证据不足，必须先扩展计划、实现与资源试验。任务 2.4 至此关闭，当前游标停在上述用户
  决策门；需求文档的宏观 `DPT-AC-010/011` 无需修改，任务 2.5 尚未开始编码或签发回执。

### 任务 2.5 方案复核与实施授权

- 2026-08-07 复核确认，需求文档已经准确要求门 A 生成唯一规范事件集合、两种时间投影、
  Observation、完整谱系、确定性与原子回执，保持产品经理层面的宏观表述即可，不需要为了
  任务 2.5 的物化细节修改需求。上一节把若干已能由现有合同、代码和计划确定的普通实现选择
  写成了四项额外用户决策，范围过宽；其“每个事件后立即生成 Observation、同一毫秒不合并”
  的推荐也会把采集时间戳分辨率无法证明的跨来源先后暴露给模型，不再作为当前方案。
- 用户批准以收窄后的当前计划直接实施：模型可见索引逐条保留所有规范事件；Observation
  只对运行清单显式覆盖的市场生成，并在同一 `received_at_utc_ms` 中所有能改变九组件之一的
  可见事件全部应用后，最多生成一条该市场 Observation。同毫秒内可使用确定性内部顺序形成
  唯一状态，但该顺序不进入公共合同、事件身份或模型可见事实。全局参考事件只存储一次，
  在其可见后组合进本次运行显式覆盖市场，不向数据集全部市场隐式扇出，也不为会话复制状态。
- 四份主要数据产物固定为唯一事件存储、事件时间索引、模型可见索引和 Observation；
  `ExecutionMarketEventV3` 与 `CanonicalReferenceEventV1` 是唯一事件存储上的两个类型化逻辑
  读取入口，不重复物化、不增加 manifest。使用现有 Arrow/Parquet/Zstd 与版本化 JSON
  manifest；分页仅使用绑定不可变 manifest 和下一逻辑位置的有界强类型游标，不增加不透明
  令牌或通用查询层。
- `contributing_event_ids` 固定为九组件当前非空锚点事件 ID 的排序去重并集，最多九项，不
  冒充累计状态的完整历史谱系；完整重建继续依赖唯一事件存储、模型可见索引 manifest 与
  Observation 截止点。组件摘要绑定版本化组件标识、当前规范领域状态、质量和锚点；市场摘要
  绑定市场身份以及固定九组件顺序的质量、缺失状态和组件摘要。
- 首个正式门 A 使用能让全部必需能力真实通过的最小完整单市场范围。任务 2.4 的既有范围
  只有在真实数据证明其覆盖完整能力后才能复用，否则扩展合格输入范围并重新测量受影响资源
  门。发布路径只实现同文件系统候选目录、完整重验、一次原子重命名与失败清理；不增加常驻
  发布服务、通用完成器协议或人工批准字段。用户已批准该修订并授权进入任务 2.5 实施。

### BBA epoch 误判复核与任务 2.5 恢复

- 实施前审查一度把 Polymarket 归约器在 epoch 切换时执行的 `state.bba=None` 解释为公开
  BBA 被静默清除，并推荐新增 `POLYMARKET_BBA_EPOCH_BASELINE_REQUIRED`。重新核对全部读写
  路径后确认，该字段只承担当前 epoch 内的同值确认、前一直接证据和报告基线；清空它会让
  新 epoch 首条数值相同的绝对 BBA 重新形成规范事件，不等于模型公开 BBA 变为 `Missing`。
- 当前计划已经规定只有 `BestBidAskV2` 更新 BBA、资产级当前 `QualityBoundaryV2` 只更新
  `PolymarketL2`，且仅采集器可见的重连信息不能进入 Observation。现有跨 epoch 回归也明确
  只生成 L2 缺失边界，并允许 BBA 在没有完整 `book` 时独立更新。因此新增 BBA 缺失事件会
  把采集任务换代误写为平台市场事实，删除模型最后真实收到的价格，并同时污染事件时间投影
  与模型可见投影；该推荐不成立，现已撤回。
- 只读真实数据复核确认 BBA-before-book 合法可达；2026-08-04 重启边界的 1,859,801 行中，
  新 epoch 的 11 个市场、22 个资产均为完整 `book` 先到再到 BBA。事件顺序事实既不能把
  collector epoch 解释为平台 BBA 缺失，也不改变最后实际可见状态语义。用户同意本次纠正并
  批准继续执行：不新增质量原因、不升级合同或 `canonical_rule_version`；只收窄任务 2.3.1
  中“盘口状态”为“L2 数量归约状态”，并把 BBA 保留、重新锚定和 L2 独立恢复并入任务 2.5
  现有 Observation 聚焦测试。

### 任务 2.5 正式前真实样本门与物理扰动边界纠正

- 正式前独立审查发现，计划曾要求首个真实代表市场链证明未知旧 tick。两次独立只读扫描
  复核 `2026-07-30` 至 `2026-08-05` 的合格日期数据，共有 1,136 条
  `tick_size_change`，没有 `old_tick_size` 为空或空白的行，实际均为
  `0.01 -> 0.001`。这不否定合同允许未知旧 tick；它证明把可空合同变体强制写成真实范围
  必须出现的事实无法由现有数据满足，也不属于宏观需求。用户批准真实门只验证实际动态
  old/new tick 及两 token/asset 链路，未知旧 tick 继续由既有合同边界测试和阶段 3 黄金合同
  序列验证，不伪造真实样本。
- 同轮审查确认，计划曾把文件、行组、批次、线程和等价重分片混写成全部产物摘要不变。
  当前生产核心固定业务处理并行度为 `1`；输入清单和完整来源谱系又必须如实绑定物理
  manifest/file SHA。真实源文件重分片时，规范事件 ID 应保持稳定，但输入集合、完整记录流
  和产物谱系身份必须改变，不能把两类身份强制相等。用户批准把 `DPT-AC-002` 收窄为计划
  实际支持的无语义物理执行变化，并按三层证据验收：合同测试证明物理 SHA 不进入事件语义
  身份；正式同输入运行扰动 Parquet 解码批次、外排 run、读写批次和输出行组；固定并行度
  `1` 只作资源事实断言，不为验收新增并行或源文件重分片路径。
- 需求、计划、当前游标、阶段 3 黄金/真实证据归属和验收映射已经按上述批准决定直接覆盖
  旧语义。该修正删除无法满足和会制造第二路径的过度验收，不改变双时间、事件语义身份、
  真实物理谱系或门 A 成功条件。

### 任务 2.5 正式范围资源诊断与有界 Parquet 行组修复

- 首个正式范围使用 `2026-08-03` 合格清单、市场
  `0xd4346bcf419856fa3261d3c0705597d5c909ead020b9f1a0de468092704fc65f` 和接收时间左闭右开
  区间 `[1785715201837,1785715888412)`。扩大范围后的首次诊断先证明原 `131072` 条归约器
  保留集合上限不足，真实峰值在下一条输入到达时为 `131082`；该上限据真实范围提高为
  `163840`，没有改变归约语义或新增按来源配置。
- 随后的诊断在规范记录捕获阶段命中原统一 `2 MiB` 单行载荷门。专用只读探针通过同一生产
  归约路径精确测得：最大规范记录为 `2896901` 字节，对应来源接纳序号 `2515346`、直接证据
  `4544` 项、`1362` 个 bids 和 `1563` 个 asks。该序号是来源接纳坐标，不是规范记录发射位置
  或输出行数；一度据它推导数百万规范记录和 `1024` 个临时 run 文件的判断错误，随后已撤回。
  独立语义审查确认该载荷是当前 Binance 完整状态与去重直接证据前沿的合法结果，不是报告
  重复或递归历史复制。统一单行载荷门因此只提高到 `3 MiB`；没有拆出逐产物配置或改变公共
  事件合同。探针当时报告的归约报告峰值 `1616` 字节不是完整正式范围最终高水位；后续完整
  主配置实测以 `55836` 字节取代该局部数值，仍远低于同一统一载荷门。
- 早期 release 诊断在 `MemoryMax=1 GiB`、`MemorySwapMax=0` 下被 cgroup OOM 终止，但随后
  核对发现其 `TempDir` 位于 `/tmp`，而本机 `/tmp` 是 `tmpfs`；实时 `memory.stat` 的
  `file == shmem` 证明候选文件本身被计入内存，不能据此把 OOM 归因于正式 `ext4` 路径的页
  缓存，也不能把该次运行作为资源门证据。核对后只删除了精确失败目录 `/tmp/.tmpNCHdNo`
  和 `/tmp/.tmpFto612`，分别约 `473 MiB` 和 `226 MiB`；它们没有成功 manifest 或回执，
  属于不可恢复但可由同一输入重现的诊断产物。后续全部资源诊断改用正式发布根同类
  `/home/chillchill/sim-system/.local-artifacts/data-pipeline-training/gate-a-diagnostics` 的
  `ext4` 文件系统。
- 最小修复在两条 Parquet 写入路径的每个既有有界批次后关闭行组、同步文件并提示内核回收
  已落盘缓存；外排读取器改为一次只打开并解码一个行组，解码后仍按真实 `SortRow` 驻留分配
  检查 `64 MiB` 门。没有新增 Parquet 物理字节上限；独立审查确认
  `RowGroupMetaData::total_byte_size()` 是未压缩列字节，不能与行对象驻留内存直接比较。该修复
  是对既有批次和读取内存门的正确有界实现，但不再声称它单独解释了上述 `tmpfs` OOM。
- 两次后续 release 诊断没有再发生 OOM，交换峰值均为 0，但先后暴露两项自产物口径错误：
  一次以 Parquet 未压缩列字节 `67113597 > 67108864` 错误拒绝合法行组；删除该异构口径后，
  一次又以解码后行驻留量 `67149720 > 67108864` 拒绝捕获写入器按纯
  `key.len() + payload.len()` 放行的边缘批次。后一项通过把捕获缓冲统一为已有
  `Vec<SortRow>`，复用与外排相同的容量和行分配口径修复；近边界回归证明旧口径会放行
  `65456` 字节的两行原始载荷并在计入 `96` 字节描述符后自拒绝，当前实现会在第二行前刷新。
- 首次正确使用 `ext4` 的完整主配置诊断没有 OOM，最大 RSS 为 `451924 KiB`、cgroup 总内存
  峰值为 `685621248` 字节、交换为 0；它继续推进到结构化
  `TempRunFileLimit { needed: 129, limit: 128 }`。把统一临时 run 文件门改为 `512` 后，下一轮
  真实运行进入 `sort-visibility-material` 第二轮合并，再以
  `TempByteLimit { limit: 536870912 }` 失败；该轮最大 RSS 为 `647932 KiB`、cgroup 总内存
  峰值为 `690204672` 字节、交换为 0。现场连续文件编号精确还原该最重排序为
  `194 -> 49 -> 13 -> 4 -> 1`，主配置文件峰值为 `195`；`4096` 行物理扰动最多把每个主 run
  拆为两个，文件峰值上界为 `389`，所以 `512` 是覆盖两种配置的最小清晰二进制门，不需要
  改写合并生命周期或提高到 `1024`。
- `sort-visibility-material` 的最终文件为 `454862895` 字节；最后一轮必须让上一代完整输入和
  新完整输出同时存在，结构占用接近其两倍，`768 MiB` 没有可靠余量。统一临时排序字节门据此
  提高为 `1 GiB`，不改变合并扇入 `4`、同时打开文件 `5` 或候选目录 `4 GiB` 门。下一轮主
  配置在该门下完整构建并复核成功：规范记录 `213458`、事件时间投影 `213454`、模型可见投影
  `213458`、Observation `71114`、归约报告 `418877`；临时排序峰值 `904511002` 字节、文件
  峰值 `195`、Observation L2 保留层级峰值 `240`、候选目录上界 `1377583253` 字节、含复核
  控制文件的正式工作目录上界 `1431060629` 字节。四类规范流摘要分别为
  `949e621b694fae8ec9a74f6dda0d7dc2c2ffe16280bc8ef6c4675e0fc596e277`、
  `bb8ac3ca9c59e88f8ccc0a698058e0e5dca2556ae680598c6ac76eababce5566`、
  `c70df7b7777063a24e930f3a6552106576a56b7cd41f1b0d14e2b23531990181`、
  `a47dd5b33fa5fbc5e5f96543652a45729a08e5108488a7a6bd318ae0d82505f3`，候选复算完全一致。
- 上述成功诊断墙钟 `1208.32s`，最大 RSS `642092 KiB`，cgroup 总内存峰值 `693571584`
  字节，交换为 0，测试结果 `1 passed / 0 failed`；临时测试目录按 `TempDir` 生命周期清理，
  没有签发正式回执。当前 Observation L2 门据 `240` 收紧为 `256`，外层 RSS 门据相邻真实
  诊断提高为 `768 MiB`；临时 run 文件 `512`、临时排序字节 `1 GiB`、单行载荷 `3 MiB`、
  共享捕获 `1 GiB` 和候选目录 `4 GiB` 保持统一配置。完整 `4096` 行物理扰动、主配置逻辑
  一致性、能力矩阵和原子发布仍须由正式干净提交命令完成，不能用本次主配置诊断替代。
- 当前源码的 `data-pipeline-training` 库测试 `66/66`、命令行测试 `1/1` 通过；计划冻结的
  `cargo test --workspace --lib --bins --tests --examples --locked --offline -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second`
  以退出码 0 通过。`cargo clippy -p data-pipeline-training --all-targets --locked --offline -- -D warnings`、
  `cargo fmt --all -- --check` 和 `git diff --check` 均通过。
- 干净提交后的首个正式 release 二进制构建暴露了测试依赖图掩盖的 `rustix` 特性缺口：
  普通测试和全目标 Clippy 会经开发依赖间接启用 `rustix/std`，而只构建正式二进制时，原显式
  `features=["fs"]` 无法让 `std::fs::File` 和 `OsStr` 实现 `rustix` 的适配特性，编译以 7 个
  `E0277` 失败。锁定的 `rustix 1.1.4` 本地包清单确认默认特性就是 `std`；当前依赖只显式
  增加 `std`，保持 `default-features=false` 和已有 `fs`，没有改动文件缓存或原子重命名语义，
  也没有引入新依赖。修复后正式 release 在 `14.28s` 内构建成功，二进制 SHA-256 为
  `ce9016ec8d7a03978954332ecf99c614c32daa0c79f1b4c5d2a8e83160e6280f`；训练库测试 `66/66`、
  命令行测试 `1/1`、严格 Clippy、格式和差异检查再次通过。
- 两轮独立 `gpt-5.6-sol/ultra` 复审均确认上述修复不改变排序、载荷或摘要，没有新增第二套
  Arrow/Parquet 路径。关于恶意主体同时改写 Parquet、manifest 和 SHA 后用 Thrift 集合计数
  放大分配的发现，源码事实成立，但当前批准的本地产物损坏模型没有敌对本地写入者；普通
  单文件损坏会先被完整文件 SHA 拒绝。当前不增加无效 footer 魔数或自研 Thrift 解析器；若
  未来扩大威胁模型，应先冻结外部可信摘要和受限解析进程，而不是在本轮形成影子解析。

### 任务 2.5 全工作区回归命令冲突修正

- 2026-08-07 正式执行前复核发现，根 `AGENTS.md` 把普通全工作区回归写成无过滤命令，但
  `plan/data-collect-plan.md` 与 `data-collect/docs/current/acceptance.md` 已一致要求精确跳过
  `four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second`。实际无过滤
  运行也只在该测试失败：测试夹具把当日 `12:00:00Z` 固定为接收时间，在 UTC 12:00 前会因
  晚于真实准入时钟而触发 `invalid_real_admission_clock` 并最终超时。这是测试夹具的当前限制，
  不是已证明的生产运行时失败，也不是此前 bench 的 `writer chunk channel disconnected`。
- 用户批准最小修正：根 `AGENTS.md` 和本计划任务 2.5.3 统一采用
  `cargo test --workspace --lib --bins --tests --examples --locked --offline -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second`，并要求验收证据明确记录
  该项被跳过，不得写成该测试通过。此次决定不修改 `data-collect` 代码、计划、验收文档或
  DPT 需求，也不增加 `#[ignore]`、等待 UTC 中午执行或其他测试旁路。
