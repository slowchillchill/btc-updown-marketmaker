# data-pipeline(training) 变更记录

> 事项编号：`DATA-PIPELINE-TRAINING-20260803`
>
> 生命周期：`ACTIVE_IMPLEMENTATION_CHANGELOG`
>
> 当前执行 authority：`plan/data-pipeline-training-plan.md`
>
> 对应需求：`demand/data-pipeline-training-demand.md`
>
> 对应计划：`plan/data-pipeline-training-plan.md`
>
> `data-collect` 当前权威入口：`data-collect/docs/CURRENT.md`
>
> 执行授权：`LOCAL_IMPLEMENTATION_ONLY`
>
> 状态：`用户已批准；活动实施记录；阶段 0 实施准入复核中`

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
