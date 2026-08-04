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
> 状态：`用户已批准；阶段 0 已通过；停在推送授权门`

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
