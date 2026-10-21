# 公共行情统计 v1

本接口落实 [#340 / D-0366](https://github.com/slowchillchill/btc-updown-marketmaker/issues/340#issuecomment-6263400469)
的 B 公共分段。定义版本为 `public-market-statistics-v1`。它提供公共数值、历史和同截止冻结读取；
A 实际经营接入、B 模型编码/动作/训练，以及完整 100ms 宿主仍在各自任务中。旧 A1/R3 的采样和结果身份保留。
底层检查点保留已启用的公共统计历史，不代表 B 模型与学习器已实现协调恢复。

## 所有者与调用链

`contracts::PublicStatisticsConfigV1`、`PublicStatisticsIdentityV1` 和 `PublicMarketStatisticsV1` 定义参数、
输入身份与结果。F0 继续引用 `MarketBusinessObservationV1`，规则继续引用 `PublicTradingRuleContextV1`。
`source_applied_at_utc_ms` / `source_received_age_ms` 的固定顺序为深度、聚合成交、spot、TWAP30、TWAP60；
`basis` 的固定顺序为 spot、TWAP30、TWAP60。
`data_pipeline_core::public_statistics` 维护公共历史和 F1—F8 纯计算；BLB 和 spot 逐对数学原语也位于该中性模块。
`baseline_mm::blb_research` 与旧 spot 入口只重导出这些原语。旧 A 的 received-time 接纳、决定采样、权重和回退仍由旧调用方维护。

宿主在供料前显式创建 `PublicStatisticsStreamV1::new(identity, config)`，将同一个 `Arc` 交给同输入、
同参数、同规则和同交付时序的各个 `PublicConsumptionStateV1::enable_public_statistics`。
该入口核对实际执行规则的 SHA-256。消费完整 F1/F2/F6/F7 时，公共生产者使用 `PublicObservationRequirementsV1::FULL`；
本轮合成源如此设置，旧 A 的低物化分支保持原身份。共同身份必须来自实际输入及公共 `PublicInputStreamHeaderV1`；
`delivery_policy` 记录真实延迟和单调化参数，不使用生产者来源可见下界代替应用时间。

原 Parquet 路径继续为 `DayProducerV1 → PreparedPublicGroupV1::from_day → DayPublicSourceV1`。
`DayPublicSourceV1` 先应用 `DeliveryTimes` 的延迟与单调化，然后 SIM 应用公共组。只有
`apply_public_group` 成功返回 receipt 后，`PublicConsumptionStateV1` 才用其
`execution_time().logical_time_utc_ms()` 更新统计。它不依赖研究导出、策略健康开关或决定次数。
本轮提供显式接入 API 及合成消费程序；既有 A 日 CLI 不会自动启用新采样。

同一个 prepared 组缓存对应显式 stream 的统计前缀。另一账户消费该组时复用相同的 `Arc`。
不同配置使用不同 stream；同一个 stream 若被错误用于不同实际应用时间或不同组引用，会返回明确错误。
没有全局隐式缓存，也没有逐帧内容哈希。各账户保留自己的已消费前缀；准备队列仍持有慢账户尚需的组。

`PublicObservationFrameV1.statistics` 同时引用本账户 receipt 的公共前缀和
`TrustedGroupDecisionCutoffV1.decision_time()`。`shared-run` 的借用读取和 `to_owned_input` /
`strategy_observation_input` 保留该结果，与规则、原观察、Ledger/Registry 和经济读取一起冻结。
未来预取组尚未应用时不会进入历史；后继组与同毫秒末值替换也不会修改旧 `Arc` 结果。

## 数值与时间语义

| 量 | 实现与精度 | 缺失和边界 |
| --- | --- | --- |
| F0、F9 | 原观察/规则类型、规范来源锚点、组引用、前缀、完整共同配置与版本 | 原事件、接收和来源质量不改写；新增首次应用时间及随查询推进的接收年龄 |
| F1、F2 | 深度一次解析为 checked `Decimal`；最佳正价正量档、中点和价差；BLB 沿 `blb-visible-interval-v1` | 缺正式 K、`at_anchor`、未物化、可见空区间及覆盖未知保持原区别；不作汇率校正 |
| F3 | 配置正毫秒窗口的中点 as-of 差，`Decimal` | 两端都需支撑；不插值、不在静默查询中造样本 |
| F4 | spot 使用现有 `f64` 逐对平方差/时长原语 | 按右端 `>= t-h` 纳入完整价对；缺时长为 `None`，真实同价可为零 |
| F5 | spot 分段持价积分，`f64`，单位 USD/BTC·s | 按终局窗口裁剪；缺左支撑保持缺失，同时保留部分覆盖时长；不是官方 TWAP 或连续行情证明 |
| F6、F7 | 全物化可见深度数量和、无权重压力，checked `Decimal` | 分母零为未定义；缺深度的最新成交压力保持缺失；最新成交不会因查询再次变成新消息 |
| F8 | spot/TWAP30/TWAP60 各自独立 checked `Decimal` 基线与保存结果 | 先算旧基线，只有该参考的新接纳消息才更新基线；查询持有最近相关组结果和证据时间；等数值投影仅为数值代理 |
| PM 数量和 | 每资产、每侧可见 shares 的 checked `Decimal` 和 | 未观察为缺失；有效空侧为零；直接 BBA 不冒充簿派生值 |

同毫秒的新合法样本替换当前前缀的时间桶末值；旧前缀仍保留原值。
新同价消息有新来源锚点，能形成零变化价对；公共归约拒绝/去重的消息不更新统计。
来源接收时间不决定统计采样时钟。静默查询按当前合法 `t` 移动 F3/F4 窗口及 F5 持价路径，F8 不重算。

`momentum_windows_ms`、`spot_variance_windows_ms` 和 `terminal_integral_windows_ms` 均显式配置，
单位为正 `i64` 可表示的毫秒。空集合表示该类结果没有窗口请求；没有隐含策略默认窗口、模型维数、
初始 sigma、健康门、权重、归一化或风险参数。配置必须在该流开始消费前确定。

## 保留与成本边界

中点和 spot 历史使用持久 `imbl::Vector`。每次修改共享未变节点，保留最大请求窗口和必要左支撑，
不为每组或每账户复制完整窗口。查询结果按此前缀的市场和查询时刻复用；每个市场只缓存最近查询，
已经交给消费者的冻结结果由其 `Arc` 保持。

市场结束时，在历史回收前保存每市场、每请求终局窗口的一份积分标量及覆盖信息。
它供该输入流后续的旧市场义务和公共读取使用。此状态随已公开市场数和窗口数增长，空间为
`O(市场数 × 终局窗口数)`，并随显式 stream/输入及仍持有的前缀生命周期释放。
本轮没有另设长期在线市场释放政策，也没有复制完整市场观察或全日特征。
窗口点数还取决于真实消息频率；窗口长度本身不是固定字节容量承诺。

F4 当前按请求窗口读取相邻价对并复用原数学函数；F5 在市场未结束时按必要持价段积分。
测量应包含这些查询、完整输入冻结、公共/SIM/NOE/Ledger 记录和输出成本。
高频深度、更多活动市场/窗口、私有订单与经济事实及模型 GPU 投影还需各自的代表性验证。

## 可显式调用的合成程序

```bash
cargo build -p backtest --example public_statistics_fixture --release --locked --offline -j 20
<产物绝对路径> <仓库外配置.json> <尚不存在的输出目录>
```

配置示例为 `backtest/examples/public-statistics-fixture.json`。复制到仓库外后修改已有窗口、组数、
消费者数、进度频率或输出频率，无需修改源码或重编译。程序只构造合法合成来源，不打开真实行情文件。
它经原 prepare/core/from_day/SIM/shared-run 链，使用真实 `ModelTrain` 身份的现有公共政策；
不提交订单、不模拟训练、不改变 Maker/Gamma/EOF 政策。

程序保存完整生效配置、程序 SHA-256、定义/输入/规则身份、实际公共更新和账户冻结次数、
保留点数、输出字节、墙钟和吞吐。进程峰值 RSS 可用操作系统进程统计包围程序测量；
CPU/GPU 传输为零，GPU/显存为不适用。实际测量的构建 profile、命令、编译成本与运行成本分别交回。

合成测量包含初始化、公共归约、交付、统计、每账户完整冻结和配置要求的样本输出、记录稳定化。
同一因果公共流和各账户调用由夹具主线程顺序推进，原 SIM 后台记录线程仍运行；
它不证明多账户并行加速或真实 Parquet、全日经营、完整模型和训练吞吐。

## 定向验收

`cargo test -p backtest --example public_statistics_fixture --locked --offline` 覆盖：
压力 5/3→0.25 与 q2/深度8→0.2、PM shares、F8 的 0→3→1 及查询持有/通道隔离、完整输入冻结（后继 Ledger 预留使一个账户可用现金从 100 变为 93，另一账户保持 101，旧冻结仍为 100）、
同毫秒末值、真实同价/重复/静默、窗口左界完整价对、缺失/真零、未来预取及慢消费者、
不同配置/应用时间、统计历史序列化和终局积分回收后的读取。
BLB 原有手算、正式锚缺失、`at_anchor`、未物化和可见空区间继续由旧研究入口的原测试覆盖。

积分测试的独立时间线：PM 组的 collector admission 为 22,001ms；后到的 22,000ms spot 组
按单调交付应用于 22,001ms。终局窗口起点 22,000ms 至观察时刻 23,000ms 的积分为
`100 × 0.001 + 103 × 0.999 = 102.997`。该算式检验应用时钟与持价区间，不能改用消息接收时钟得到 103。
EOF 后再追加元素不符合此有限夹具的输入方式；测试先预取未来合法组，再读取更早静默截止，
遵循 [Rust 1.95.0 Peekable 源码](https://github.com/rust-lang/rust/blob/1.95.0/library/core/src/iter/adapters/peekable.rs)
的缓存语义，不修改生产有限 EOF 行为。

## 显式 F2 端点回看

[#350](https://github.com/slowchillchill/btc-updown-marketmaker/issues/350) 为 C0/C1 消费者增加可选 `PublicStatisticsConfigV1.blb_lookback_windows_ms`，本次声明 `[200]`，空集合不物化。`PublicMarketStatisticsV1.blb_lookbacks` 提供声明左查询时点及该前缀的 left/right asof 端点。它不是任意过去时点读取接口。

`PublicBlbEndpointV1` 保存同一合法应用组的 F2 必要标量、F1 正量最佳价、正式 `official_anchor`（含 `direct_anchors`）、深度质量／事实引用及市场／输入／参数身份。只有真实消费推进历史；纯查询不新增点。相同 Q 的状态、锚或 F1／来源变化仍保留，不把组身份当锚身份。同毫秒替换新前缀末点而不污染旧冻结。迟到 K 不回填旧端点。持久节点裁剪到最长声明回看及左支撑，旧冻结仍可读自己的节点。

该有限消费者只在市场存续期建立 F2 端点；市场结束后当前流释放该市场节点，结束外回看返回缺失。旧冻结不受释放影响。没有完整盘口、档位数组或整日观察副本。C0/C1 的字段映射、不可比行为和库／CLI 使用见[候选说明](../baseline-mm/docs/strategies/complementary-pair-public-candidates.md)。
