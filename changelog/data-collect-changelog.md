# data-collect 当前变更记录

> 事项编号：`DATA-COLLECT-CURRENT`
>
> 生命周期：`CURRENT_MASTER_CHANGELOG`
>
> 当前执行 authority：`data-collect/docs/CURRENT.md`
>
> 对应需求：`demand/data-collect-demand.md`
>
> 对应计划：`plan/data-collect-plan.md`
>
> 状态：`COMPLETE_CURRENT_SNAPSHOT`

本文件只保留当前最终状态和仍影响后续模块的决策。逐阶段实现、失败尝试、远端命令、
运行采样和审查时间线已由 `data-collect/docs/archive-manifest.tsv` 指定的 Git
标签、路径和 blob 保留，不在活动文档重复。

## 当前实现快照

- Cargo 定义训练、模拟盘热路径和实盘热路径三个二进制程序。
- 当前只有训练程序连接真实 Polymarket、RTDS 和 Binance 数据源；模拟盘和实盘程序只
  提供启动回执、Unix 套接字与心跳、状态。
- Polymarket 使用官方 Rust SDK `0.7.0` 加仓库唯一 `0.7.0-p1` 补丁。
- 当前新写入 schema 为订单簿 v3、元数据 v5、RTDS v4、Binance 深度快照 v3、
  深度更新 v4、`aggTrade` v7。
- 存储按接收时间划分 UTC 日、数据源和小时，写最终化 Parquet、每日唯一清单 v3 与
  采集日覆盖信息 v1。
- 默认容量为 50GB；启动时已达上限则不启动数据源或产物而直接退出 75，运行时扫描达到
  上限则协调最终化后退出 75；两者都不自动删除最终化原始数据。
- RTDS 使用稳态 300 秒、首次有效消息 60 秒的完整替换；退避基数为 1 秒且每级 ×2，
  应用 ±20% 确定性抖动、单次上限 300 秒、无独立尝试耗尽门，有效新鲜消息后重置，
  并严格先断开再连接。
- 固定身份信息、E+3..E+8 市场判定、重启时失败即关闭与确定性 UTC 日切换已实现并有测试。

## 用户最终决策

- 2026-07-28 事故事项已由用户最终验收。
- 真实 UTC 日切换和完整健康 UTC 日不再是完成门；文档不反向声称未记录
  的运行证据已经发生。
- 2026-08-03 用户纠正并取代 2026-08-02 的当前跨模块顺序：已完成
  `data-collect(training)` 后，先冻结 `data-pipeline(training)` 需求和共享合同，并完成
  “最终化类型化原始数据 -> 规范全序 -> ExecutionMarketEvent”最小真实纵向链路；随后
  由 `sim-system` 同时使用黄金事件和真实管道事件开发、验收；再补齐同一
  `data-pipeline(training)` 的 Ledger 投影合并、完整 `Observation`、标签、切分、
  归一化与训练产物闭环，最后进入 `model-train`。
- 模拟盘和实盘的真实外部数据源适配器不属于 `model-train`，保持延期；必须先
  完成 `model-train`（模型训练模块，不是 `data-collect-training-collector`）；之后
  仍只有在用户决定返回 `data-collect` 时，才以独立三件套开发。该延期不阻断
  `data-collect` 完成。
- `data-collect` 当前没有 Rust 实现、部署或远端操作游标。

## 2026-08-04 模拟器低频规则责任与存储复核

- 用户确认：CLOB 最小订单数量、完整 `feeSchedule`、`itode` 等低频平台规则不再要求
  `data-collect` 补齐；它们由 `sim-system` 每个新运行在冷启动阶段通过官方 Rust SDK 按
  目标市场查询，并在运行内冻结。
- 当前 v5 没有 `min_size`、`fee_schedule_raw`、`feeSchedule`、`itode` 或
  `rebateRate` 专用列，删除这类专用列的真实收益为 0 字节。旧同名字段只存在于旧布局或
  发现阶段内存结构，不是当前 Parquet 列。
- 只读审查仅使用 `/home/chillchill/new-datasets` 中 2026-07-30 至 2026-08-02 四日，合计
  491 个 Parquet、151,056,246 行、9,202,902,694 字节；metadata 为 97 个 v5 文件、
  4,620 行、4,119,473 字节。
- 1,167 个 Gamma 原始证据载荷中存在十个同名或相似键。全部删除的逻辑字节减少为
  255,582，按相同每小时分片和 ZSTD-3 的反事实压缩收益约 9,317 字节，即 metadata 约
  0.226%、全部合格数据约 0.000101%。删除还会破坏原始载荷和关联摘要，因此决定不删键、
  不重写既有数据。`current/contracts.md` 已逐键记录
  `orderMinSize`、`orderPriceMinTickSize`、`feesEnabled`、`makerBaseFee`、
  `takerBaseFee`、`makerRebatesFeeShareBps`、`secondsDelay`、`negRisk`、
  `enableOrderBook`、`rfqEnabled` 的观测值和空间反事实；它们都只是原始 JSON 键，不是
  可删除的类型化专用列。
- 十键中只有 `orderMinSize`、`orderPriceMinTickSize`、`feesEnabled`、`negRisk` 是直接
  重叠候选；三个旧手续费/返佣键只是主题近似，`secondsDelay`、`enableOrderBook`、
  `rfqEnabled` 不是本次配置参数。十键合计只是空间反事实上限，不是“十个重复字段”。
- 完整 `feeSchedule`、`feeSchedule.rebateRate`、`itode` 和 `oas` 在当前 v5 载荷中不存在，
  因而其字段重叠和删除收益均为 0。四日范围的公开成交虽有 2,039,650 个非空
  `fee_rate_bps` 且均为字符串 `"0"`，但这是逐成交事件证据，不是冷启动规则副本。
- 四日数据另有 624 条 `tick_size_change`，涉及 156 个市场，均为 `0.01 -> 0.001`；这是
  运行中动态事实，继续保留。`last_trade_price.fee_rate_bps` 也是公开成交证据，不是
  `feeSchedule` 配置副本。
- 本次只同步当前文档责任边界与只读测量结论；没有修改 Rust、schema、Parquet、服务、
  部署或远端状态，也没有产生新 `data-collect` 实施游标。
- 独立审查进一步确认：模拟器当前 SDK 不仅缺少 `itode` 和
  `feeSchedule.rebateRate`，`mos/nr/fd.r/e/to` 的默认反序列化也无法审计字段存在性；这是
  `sim-system` 的 SDK 类型覆盖门，不改变 `data-collect` 已关闭状态或采集 schema。

## 2026-08-04 跨模块三件套治理补齐

- 为保持需求、计划和变更记录一一对应，`rebuild-core-demand` 与
  `SIM-SYSTEM-20260731` 各新增无执行权威的 plan/changelog 审核草案；这些文档不改变
  `data-collect` 当前合同、代码、完成状态或部署状态。
- `data-collect` 文档权威守卫把三套跨模块三件套统一归类为
  `authority=none`、`执行授权=none`，并机械检查同一事项编号和相互引用。活动 Markdown
  显式处置集合由 28 份增为 32 份；唯一 `CURRENT` 入口及当前主三件套的路径、身份和数量
  不变，归档标签、快照和历史文档数量不变。
- 首次检查暴露两份新增 `rebuild-core` 文档尚未进入显式处置集合；补齐归类后，
  `document_authority_contract` 为 `4 passed; 0 failed`，`deploy_readme_contract` 为
  `6 passed; 0 failed`，`deployment_binary_contract` 为 `3 passed; 0 failed`，
  `cargo fmt --all -- --check` 通过。
- 本次仅修正文档治理完整性；没有修改生产运行时/schema、正式数据、服务或远端状态，
  也没有产生部署、推送/发布、敏感材料、真实下单或真实资金授权。

## 2026-08-03 跨模块顺序纠偏

- 用户确认此前先独立完成 `sim-system`、再建设 `data-pipeline(training)` 的顺序有误，
  并批准修改旧活动文档的顺序以及开始独立的 `data-pipeline(training)` 需求草案。
- 新顺序把同一 `data-pipeline(training)` 分为两个交付门：模拟器之前的上游投影门只
  建立清单准入、唯一规范全序、`ExecutionMarketEvent` 和同源市场侧 Observation；
  模拟器之后的训练闭环门再接入归一化反馈/Ledger 投影，补齐完整 Observation、标签、
  切分、归一化和训练产物。该分层不允许形成两套语义规范化器或两条影子管道。
- 黄金事件仍是 `sim-system` 组件符合性、边界和确定性回归的必要证据，但不能替代至少
  一条“最终化类型化原始数据 -> 规范全序 -> ExecutionMarketEvent -> sim-system”的
  真实纵向验证链路。完整归一化层、Ledger 和 RewardContract 仍不是模拟器开发前置。
- 本次只调整文档顺序、增加审核中草案及其文档治理分类；不重开已完成的
  `data-collect`，不修改采集运行时/schema，不授权部署或远端状态变化。
- 最终文档权威测试为 `4 passed; 0 failed`，部署说明测试为 `6 passed; 0 failed`，
  `cargo fmt --all -- --check` 与 `git diff --check` 通过；修正后的独立终审结果为
  `Critical 0 / Important 0 / Minor 0`。

## 2026-08-02 跨模块顺序同步

- 复审确认 `data-collect` 当前只需同步跨模块主线，并继续明确传输层和数据源层的覆盖、
  健康状态、缺口、恢复与重连证据，与 `data-pipeline` 规范层之间的分工；其他模拟和
  重建澄清不改变 `data-collect` 运行时职责或完成状态。
- `CURRENT`、主三件套、当前合同、验收、限制文档和两份跨模块需求已同步当前主线；
  模拟盘和实盘适配器的既有延期门保持不变。
- 首次运行文档权威契约测试为 `3 passed / 1 failed`：活动文档
  `demand/sim-system-demand.md` 已提及 `data-collect`，但旧权威图只枚举一份
  跨模块草案。随后将该文件明确分类为第二份无 `data-collect` 权威的审核中草案，
  并同步契约测试；这不是生产运行时、schema 或部署变更。
- 修正后文档权威契约测试为 `4 passed / 0 failed`，`git diff --check`
  通过。随后扩展的部署说明契约测试首次为 `5 passed / 1 failed`，原因是同步
  时误删了“`model-train` 不是 `data-collect-training-collector`”的防误读措辞；现已
  恢复该区分而未恢复旧顺序。最终部署说明契约测试为 `6 passed / 0 failed`，
  文档权威契约测试为 `4 passed / 0 failed`；精确证据记录在
  `data-collect/docs/current/acceptance.md`。
- 独立最终审查发现 `sim-system` 第 3 节仍把模拟盘和实盘写成无条件第 5 至 8 项；已将
  当前主线明确止于 `model-train`，并把后续路径改为只有用户届时决定返回
  `data-collect` 才能启动的条件性后续路线。复审结果为
  `Critical 0 / Important 0 / Minor 0`（三个等级均无问题）。

## 2026-07-30 文档权威整理

- 先完成 61 份相关 Markdown 全量审计，修正旧 SDK、schema、RTDS、保留策略、部署、
  尝试游标与模拟盘、实盘能力冲突。
- 带注释标签 `data-collect-history-pre-current-authority-20260730` 冻结审计完成状态，
  解引用提交为 `4ed31ad6abe7237d92cc99a4857979d8ae26d373`。
- 45 份历史沿革长文从活动树移除；归档清单保存原路径与 blob。
- 新增唯一 `data-collect/docs/CURRENT.md` 和六个单主题当前归属文档。
- README 与旧当前文档原路径压缩为仅路由文档，不再复制规范或命令。
- 四份跨模块错误记录保持仅作证据，不能形成当前执行结论。
- 自动权威守卫覆盖唯一入口、三件套、归档对象、历史隔离与负例。
- 模拟盘和实盘的心跳帧、套接字辅助函数只有组件测试，完整二进制程序长运行循环尚无
  端到端测试；热路径运行标识的回执路径限制也尚未失败即关闭，详见当前验收与限制文档。

## 当前证据入口

- 当前实现与边界：`data-collect/docs/current/contracts.md`
- 当前 CLI 与产物：`data-collect/docs/current/interfaces.md`
- 当前运行权限边界：`data-collect/docs/current/operations.md`
- 当前验收命令与真实结果：`data-collect/docs/current/acceptance.md`
- 当前限制：`data-collect/docs/current/known-limitations.md`
- 文档治理结论：`data-collect/docs/current/document-authority-audit.md`

## 2026-08-04 模拟器冷启动来源纠正

- 用户批准 `sim-system` 每个新运行通过固定的官方公开 HTTPS 端点和窄类型响应加载器查询
  低频平台规则。本决定取代本文件此前“模拟器通过官方 Rust SDK 冷启动”以及“SDK 类型覆盖
  缺口阻断模拟器冷启动”的表述；旧段落保留为历史决策记录。
- 官方 Rust SDK 类型覆盖缺口不再阻断模拟器。平台响应缺少必需字段、字段非法、身份不一致
  或来源冲突时，受影响的模拟器能力仍失败即关闭；未消费的 `oas` 只登记为未消费字段。
- 该例外只属于 `sim-system` 冷启动，不改变 `data-collect` 必须使用官方 Rust SDK 的合同，
  不授权采集器使用原始 REST，也不改变当前 schema、原始证据保留、模块完成状态、authority
  或执行授权。
- 本次只修正文档语义；没有实现加载器、修改 Rust/schema/数据、操作服务或远端、部署、
  提交或推送，也没有把既有测试结果冒充为加载器验证。

## 2026-08-04 DPT 任务 0.10 tick 权威归属同步

本节取代此前把 `orderPriceMinTickSize` 归为模拟器冷启动规则重叠候选、以及把全部新运行都
写成查询当前平台的当前含义；旧段落的测试计数和测量结果仍只作为当时历史快照保留。

- `orderPriceMinTickSize` 是采集时点的初始 tick 源证据，orderbook v3 的
  `old_tick_size/new_tick_size` 是动态 tick 事件证据；两者都经 `data-pipeline` 进入规范
  事件链，不属于 `PlatformRuleSnapshot`。
- `sim-system` 当前时点运行使用官方公开 HTTPS 查询形成 `CURRENT_QUERY` 快照；历史运行只
  绑定适用时点合法的 `HISTORICAL_AS_OF` 同 schema 快照，禁止查询当前平台回填。两种模式
  都不包含、查询或交叉校验 tick。
- 本次只同步主三件套、`CURRENT`、当前合同、当前限制、验收快照和文档权威守卫；不重开采集器运行时，
  不修改生产 Rust、schema、Parquet、清单、服务、部署、远端状态、模块 authority 或执行
  授权。唯一 `data-collect` Rust 变化是把 DPT 三件套状态标记从临时阶段整句收窄为稳定的
  “用户已批准”，生命周期、执行权威、执行授权和负例仍保持精确校验。
