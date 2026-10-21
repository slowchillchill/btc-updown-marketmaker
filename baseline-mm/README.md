# 人工做市策略库

`baseline-mm` 是 A 路线的人工盈利策略业务线，维护具体交易规则、参数、必要算法记忆和策略业务分析。回测、PAPER、LIVE 的上层入口组合策略库与公共运行能力；公共运行核心不依赖具体策略。

首个设计为 BTC 五分钟 Up/Down 的 BUY-only 互补配对做市。其[候选计算规格与研究方案](docs/strategies/complementary-pair-v1.md)说明费用归属、修复利润占用、报价与数量、FAK、风险和生命周期调用需求。当前 workspace 成员提供可调用 Rust 候选库，已交付配置解析、构造与公共策略适配；未提供已校准参数、盈利证据或三环境宿主。候选计算方法不等于正式运行参数。

任务范围、批准状态和推进位置维护在 [策略合同 #185](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185)，公共历史调用边界与运行装配维护在 [公共回测 #186](https://github.com/slowchillchill/btc-updown-marketmaker/issues/186)。完整业务合同以 #185 为准，本目录不保存本地任务镜像。A 内部按[总体合同 #1](https://github.com/slowchillchill/btc-updown-marketmaker/issues/1)的串行任务交接推进。

策略按具体业务名组织，导出策略构造、参数定义及必要适配；不预建三环境消费循环或另一层 CLI 配置覆盖。参数选择由所属上层入口按“显式 CLI > 配置文件 > 策略默认值”合并，布尔开关区分未指定与显式关闭。运行元数据记录策略身份、规则版本、代码版本和完整生效参数。不同策略和对照运行使用各自实例与独立账户状态，同账户多策略隔离不属于首版能力。

公共行情、模拟时序与撮合、订单提交、NOE、Ledger、NAV 和结算由原模块维护。策略只派生其决策与诊断，不复制现金、持仓或订单账本。公共缺口由所属模块补齐，公共回测负责历史装配与通用结果，模型开发竞赛和独立留出评价仍由其所属模块负责。

入口为 `baseline_mm::ComplementaryPairV1`，业务身份 `complementary_pair`、规则版本 `candidate_v3`。宿主提供完整 `ComplementaryPairConfigV3`、封存公共规则、独立请求命名空间与 `StrategyContinuityV2`；新账户用 `new_account()`，原账户接续用原高水位／终止相关窗口／请求序号／未决利润占用，不能当新账户重置。配置解析 `parse_toml` 只读完整策略文件，不实现另一层 CLI 合并；当前没有正式风险默认值。

```rust,ignore
let config = ComplementaryPairConfigV3::parse_toml(&configuration_text)?;
let mut strategy = ComplementaryPairV1::new(config, sealed_rules, namespace, continuity)?;
let input = shared.strategy_input(&mut driver)?.expect("原公共可决策组");
let calculation = input.calculate(&mut strategy, measured_start)?;
let completed = calculation.complete(measured_finish);
let dispatch = shared.dispatch_strategy_qualified(&mut decision, &mut strategy, completed).await?;
```

宿主依获批时序推进到发送点；本策略选用来源健康，宿主用 `dispatch_strategy_qualified` 逐项复核，并将真实批次结果或明确失效通知原实例。策略仅对真实已发或结果未知请求保留利润占用。低层 `execute_batch_reserved` 的直接消费者需自行调用同一 `StrategyV1::dispatched` 通知，不能把形成候选当已发请求。泛型诊断随冻结计算保存在内存，后来的账户／行情归约不能改变旧诊断；本库不落盘 JSON 或数据库。

[合成配置](configs/strategies/complementary_pair/synthetic-engineering-v3.toml) 全部参数显式且无校准资格。规则、公式、公共成本界与限制见规格第 11 节；集成测试经原 DPT／SIM／NOE／Ledger 构造费用、平均成本池、预留和撤单反馈，不建策略影子账本。

### 初始双腿价格参考

[#337](https://github.com/slowchillchill/btc-updown-marketmaker/issues/337) 增加必填第 42 字段 `quote.initial_pair_price_reference`。`fair_value` 保留原公平价、spread 和库存偏移；`best_bid` 取同一冻结观察中各腿正量 bid 的最高价，再沿原 tick 向下取整、`1−tick` 和可见 `ask−tick` 裁剪。合法空 ask 不单独阻止报价。未知、空或只有零量 bid 时，该侧目标未知，不填零或回退公平价；任一侧目标未知不生成新配对。概率仍参与原经营评分和修复，库存仍参与原数量、现金、投入和风险判断。

两种模式都复用不高于目标的最低旧价。因此实际请求可能低于买一，买一上移不会自动追价。一般维护与 `new_pair` 内的价格判断处理全部当前挂单；缺买一逐侧撤单，另一侧继续按自身目标判断。一般维护保留原局部 `qualified` 修复例外；`new_pair` 不继承该豁免。`repair` 内部继续用原公平价公式及成本、撤换和优先调度；历史修复来源单失去当前修复用途后也可能被买一目标撤销。保留孤腿但没有修复动作或 `Maintain` 等阻断结果时，仍可按原条件新配对。

`pair_entry.price_reference` 保存参考；`target_prices` 按 Up/Down 保存两个可空目标，`target_unavailable` 区分 `MissingPositiveBid`、`EstimateRequired` 和 `NoLegalMakerPrice`；未执行该分支的 `evaluated` 仍为 false。`reused_prices` 及原始请求可连接目标、复用价和实际请求价；继续使用 `minimal-research-v1`，不复制全市场盘口。读取旧诊断时，旧 `target_prices` 的两项非空数字字符串保持可读。

完整 42 字段工程示例为[公平价](configs/strategies/complementary_pair/synthetic-engineering-v3.toml)和[买一](configs/strategies/complementary_pair/synthetic-engineering-v3-best-bid.toml)，两份仅参考字段不同。它们未经校准，不是 R3 实验包。原 `synthetic-engineering.toml` 保留旧 41 字段身份；当前解析器拒绝缺少新字段的配置，当前构造器拒绝旧 `candidate_v1` / `candidate_v2` 连续状态。历史源码、参数及结果沿原提交和元数据追溯，不能更改标签后当作新结果。

配置文件和 CLI 由 `backtest-cli` 合并：

```bash
backtest-cli --mode day --config <完整宿主配置.json> \
  --strategy.quote.initial_pair_price_reference best_bid
```

同一二进制可显式覆盖为 `fair_value`；宿主仍需提供其原规则、账户、输入、输出与时序配置。该命令只说明使用入口，不签发历史回放。

### 来源健康与冻结动作

[策略合同 #185 任务3§3.5](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185)规定健康策略归本库。公共来源进展、同步状态、合法观察/发送截止、整批预留及停批释放沿原公共类型，健康判断不会改写公共质量或 NAV。历史缺少连接证据时保持未知；不从没有消息推定断连，不增加固定恢复帧数或冷却。

`health` 区段的十六个必填字段均为非负整数毫秒，没有正式默认值。原八项 `*_silence_ms` / `*_event_delay_ms` 保持最大期限；对应新增 `*_silence_normal_ms` / `*_event_delay_normal_ms` 为正常上界。normal 不超过对应 max，允许零与相等。接收静默 S 与事件年龄 A 都严格小于 normal 时为正常；都严格小于 max、至少一项达到 normal 时为延迟升高但可接受；任一项达到 max 即超限。中间态继续按原条件交易，不单独暂停、撤单或限制补腿。缺少有效业务消息或事件时间、时间不一致、来源不可用或明确要求重新同步独立表达，不填成正常。

生产与既有紧凑消费者复用 `source_health_state_v1`。新列式研究按同一三态定义向量化计算，并用小型跨语言对照核对该生产函数。冻结诊断的 `source_health_states` 表达三态及独立异常，原 `source_health` 只保存阻断项，原 `is_empty()` 动作许可不被中间态改变。正常与最大边界均可无消息重评，预约只返回严格未来时刻，零/等边界不反复预约当前时间。无效帧、PONG、重复接收或其他资产消息不能覆盖对应流事实。
| 流 | 接收静默阈值 | 原事件滞后阈值 | 范围 |
|---|---|---|---|
| Polymarket 盘口 | `polymarket_silence_ms` | `polymarket_event_delay_ms` | 每个 condition/token |
| Chainlink 现价或本次估计实际采用的 TWAP | `chainlink_silence_ms` | `chainlink_event_delay_ms` | `btc/usd`，具体来源独立 |
| Binance 成交 | `binance_trade_silence_ms` | `binance_trade_event_delay_ms` | `BTCUSDT` |
| Binance 深度 | `binance_depth_silence_ms` | `binance_depth_event_delay_ms` | `BTCUSDT` |

Chainlink 与两个资产盘口是新增配对的依赖。Binance 只按实际非零算法系数启用；成交压力使用深度分母，所以 `binance_trade_pressure_weight_usd != 0` 时成交与深度都参与，即使独立深度系数为零。被禁用的修正不读取其来源。固定市场锚点不是持续行情流，不使用这些静默期限。

辅助来源退化时停止新增风险并撤掉不再合格的风险挂单；真实缺口仍可按原成本、已有承诺和资金边界独立修复。概率不可用时，独立比较依赖持有腿的公共标记及必要盘口；有效空买盘 `Some(None)` 按公共零标记语义处理，标记不可用不会伪装为零。Maker/FAK继续使用当前成本、真实承诺及配置残余风险。FAK按同截止公开卖档估计本金、数量与费用，实际成交由SIM返回，不要求全域未来量保证。

`uses_source_health` 显式选择事实投影；`next_health_evaluation_at` 同时覆盖当前经营与待发动作冻结的必要流，返回下一正常或最大边界时刻，全部静默仍能唤醒重评。当前估计从 TWAP 切换到现价也不会遮蔽原待发动作的 TWAP 期限；没有待发决定时继续按当前依赖重评。计算期间、待发期间以及每项提交前均通过 `qualify_intent` 复核冻结的动作依赖。诊断保存请求关联的来源范围、独立修复持有腿及支撑价量/成本/容量的市场标量与已有承诺；不保存另一份盘口或复制账本。孤腿含费成本在决定与资格中共用原owner市场成本池；未闭合修复通过 `ActionBeforeReadV2::request_trade_economics` 读取同截止原成交，区分经济应用与成功配平，不开启全历史逐笔投影，也不在策略构造第二事实来源。正利润支持复核复用原利润池与未决占用读取。相关事实发生变化时明确失效、停下未尝试后缀并重新计算，不改冻结价格、数量或原诊断；本批已发请求不会被误当作外部新增承诺。现金准入仍由原整批预留及请求身份校验承担。

`invalidated` 只登记实际已发前缀的利润占用，未尝试项没有占用；原结果未知、撤单竞速、反馈等待、高水位与 Exit 继续沿原合同。撤单来自策略已知的原目标；之后目标进入终态不会阻止同批其他目标的撤单。直接低层消费者需选用等价的公共资格路径并通知原实例，不能省略这些回调。

配置在启动时完整解析与 CLI 覆盖并记录生效值，不支持热加载。合成配置的健康数值只验证工程机制，不代表已校准运行参数。`cli/tests/source_health.rs` 经真实 typed Parquet、原准入与原历史入口组合本策略验证静默撤单、事件滞后/同值恢复、代次同步、计算/待发失效、真实部分成交后的独立补腿与必要行情失效、连续高水位和 Exit；策略测试覆盖启用依赖、持有标记复核、真实承诺变化及已发前缀利润通知。公共输入损坏、竞速和未知请求边界复用原所属测试。

### 首折 Chainlink 来源统计

`research_initial_sigma` 示例的 `prepare CONFIG OUTPUT` 与 `replay CONFIG PREPARED OUTPUT` 分别复用公共有限录制和原稳定前缀重放。显式所选文件、范围、原资源字段及选源身份全部保存在配置；此方式保留完整公共 record，不把零市场来源组伪造成市场观察。

生产 `SignalMemory` 与来源统计共用现价质量/接收时点接纳及逐对方差原语。生产仍按 condition 保存记忆，在原 decide 内独立接纳自身合格的 Chainlink spot，不受 Binance 综合健康阻挡；综合健康仍限制概率与交易，原窗口清理和无有效价对时的配置回退保持。计算等待期间不逐组补充历史；来源统计不采用回退，不把全局三日候选当作每市场 sigma 或完整六源可见历史。每次原重放应用后即时借用 core 当前完整现价，再推进下一输入；该投影不是任意历史组的快照。统计只保存前一个接纳点及日/全程标量，完整来源仍留在原公共材料。

### 首折 Chainlink 来源健康统计

`research_source_health CONFIG OUTPUT_DIRECTORY` 只重放既有公共录制。先核捕获文件长度和 SHA-256，在首输入前启用原来源进展，沿原 `PublicReplayV1::next_prepared_applied` 应用后借用 `ChainlinkSpot / None / btc/usd`，下一输入前完成读取；不依赖市场观察，也不使用 sigma 点接纳计数。公共 core 只提供按范围借用的读取口；策略与研究复用同一个单流健康判断，保持原未知、同步、时间异常、静默、事件年龄的判断顺序与 `>=` 边界。

构建命令为 `cargo build -p baseline-mm --example research_source_health --release --locked --offline -j 8`。配置显式提供 `public_root`、固定 `prefix`、接收窗口 `received_start_utc_ms / received_end_utc_ms`、捕获 `expected_capture_bytes / expected_capture_sha256`、`read_page_rows`、`max_read_batch_bytes`、原 `selected_source_identity_sha256 / selection_evidence` 及 `implementation_identity`。进度 `progress_interval_seconds` 默认30秒，0关闭；页数和字节预算控制原读取批次，不等于进程RSS上界。输出目录必须新建，生成原始 `result.private.json` 及紧凑标量 `segments.jsonl`，私有配置与机器路径不进入公共报告。

方法在回放前固定：有效非负接收间隔及可观测事件年龄分别取 p50/p90/p95/p99，排序后使用秩 `ceil(p*n)`（1起算），去重笛卡尔积最多16对。零阈值保持立即到期，不加1；最大值只作尾部诊断。单次回放的标量区间在内存分析候选，按UTC半开窗口和午夜交集分桶，保留首点前未知及末点后无确认。独立静默/事件条件报告交叠和不重复联合时长；另按生产首个失败原因互斥分类并对账全窗口，缺事件时间不能遮蔽之后的静默到期。缺失与不一致时间不混入数值分位。原连接及同步缺证据保持未知；同折候选只描述来源，正式配置、完整市场与收益验证仍按 [#185任务3](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185) 后续安排。

首折实际统计、16对候选及连续时钟覆盖见[中文报告](docs/research/complementary-pair-source-health-20260909-20260912.md)和[完整机器可读结果](docs/research/complementary-pair-source-health-20260909-20260912.json)。到达样本分位不等于连续时钟健康覆盖，报告保留未知、原质量证据及准确运行版本。

### 既有两目标市场材料的逐scope健康统计

同一`research_source_health CONFIG OUTPUT_DIRECTORY`入口可显式提供`scopes`数组，每项包含`label`、原`SourceStreamScopeV1`的`scope`和半开`window`（`start_at_utc_ms / end_at_utc_ms`）。`target_markets`给目标condition及窗口，`initialization_input_positions`取原初始化证据。未提供scopes的既有单流配置继续走原三日统计，方法和结果口径保持；多scope统计辅助仅在examples内，不增加生产配置或健康政策。

每输入应用后按准确scope读取一次，多个市场的公共投影不重复确认；公共状态改变、last_valid身份改变、本次遍历首次有效身份分别计数。每scope只保留必要标量和来源身份集合用于识别非相邻重现，不篡改原公共last_valid或健康区间。CL/TWAP比较价格字段表示，Binance成交比较价/量/maker三个标量，仍分别保留真实成交身份；PM与Depth完整等值未测，顶价相同不当作整簿不变，原归约Confirm另列。

候选只使用各scope窗内合法消息：接收间隔的接收/可见端点均在窗内，可见event年龄也按该窗采样；窗前末次事实只支撑连续时钟分类，跨窗间隔不进拟合，尾段单独披露。每scope独立沿原ceil秩生成最多16对，不混合PM资产或采用共用正式阈值；没有合法样本明确不能形成候选。初始化、非目标市场、目标窗前/窗后观察分母分别保存，不以所有市场观察数替代来源样本。

本轮十二分钟投入和准确九scope由[#185当前M1—M5](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185)固定；只读原record至正常End/EOF，单次真实运行，30分钟/2GiB RSS/512MiB研究输出为本段监控界，非生产默认。公共writer合成夹具使用本地目录；构建、回归、真实测量串行，失败或超界留证交主会话，不自动扩额度或复跑。

既有十二分钟材料的两目标市场、四PM资产及五个辅助scope结果见[中文市场来源健康报告](docs/research/complementary-pair-market-source-health-20260909-0000-0012.md)和[完整分源JSON](docs/research/complementary-pair-market-source-health-20260909-0000-0012.json)。该报告独立列各scope窗口/未知证据、116对描述性候选及一次运行的版本与资源；TWAP30无样本保持未知，候选未采纳为正式健康参数。

### 当前列式来源健康研究

`research/research_source_health_columnar.py` 直接查询Parquet必要列。DuckDB按文件或必要重叠文件组形成连续区间，立即汇总每日S/A bins、有效到达样本与分母，不累计全期raw或逐消息材料。公共最小状态归 `data-pipeline/training/python/dpt_health`，候选归本库。冻结后重读验证窗，保留窗前状态和连续段。Python不逐行处理全部PM消息；原Rust初始化选择器和 Histogram/body_normal/candidates 继续复用。

当前任务与授权见[#185](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185)。十八日原始到达描述统计已完成，但不是有效消息校准结果。`fd5581c8` 的全期导入因OOM终止，study未启动；旧APPROVED不覆盖当前修正。用户批准的独立研究内存保护已实际生效。v6准备因snapshot身份筛选遗漏合法metadata而退出1，不是OOM；未发布完整材料，study未启。随后v7源码76f854e0已生成十八日有效S/A、四折冻结验证和最后开发比较；prepare/study均退出0，全程2:12:52.154384。用户取消新增小样本试验。见[十八日正式研究报告](docs/research/complementary-pair-source-health-18d-20260909-20260926.md)及[机器摘要](docs/research/complementary-pair-source-health-18d-20260909-20260926.json)。完整4倍配置仅建议作为后续经营比较起点，正式阈值尚未采用。当前健康研究以列式入口为准。Rust `research_source_health_two_layer` 仅保留 `fit-columnar-histograms`（每日桶拟合）和 `classify-columnar-facts`（生产三态对照）；旧完整core/紧凑研究命令及专属工具已删除，历史报告和归档保持原身份。依赖锁、时间语义、16字段、四折冻结、全局资源预算、复现命令和限制见[两层研究说明](docs/research/complementary-pair-source-health-two-layer.md)。

### 当前直接概率与跨来源研究

`research/research_probability_direct.py` 按原始 Parquet 必要列准备紧凑目标采样，复用公共参考、终局及深度数量状态，再按四折和最后开发窗做75次顺序拟合、冻结评价。准确查询、公式、候选、资源与一次运行命令见[方法及配置](docs/research/complementary-pair-probability-direct.md)。三次实际十八日prepare均已失败：身份前态、TWAP跨单元SQL和公共深度拼接状态分别保留独立记录；study均未启动。[第三次失败及窄范围公共修复](docs/research/complementary-pair-probability-run-20261007-repair2.md)保存准确运行身份、目录分母、深度边界及验证。修复不恢复所有未持久化reset；正式参数与完整经营比较仍未完成。

依[#318](https://github.com/slowchillchill/btc-updown-marketmaker/issues/318)，当前V2删除两个额外overfill字段；旧字段、旧接续与明确candidate_v1材料须独立迁移，旧历史材料保留。未决金额单的原时点估计随已发送或效果未知请求登记，不随更乐观盘口释放；利润只占预计新增配平亏损。此修复不提供新盈利证据，也不授权重跑旧研究。

受控研究候选 `candidate_v4_public_c0_c1` 的完整配置、公共 F0—F8 映射、200ms 端点规则及正式库／CLI 入口见[公共统计 C0/C1 候选说明](docs/strategies/complementary-pair-public-candidates.md)。它不改变旧 `candidate_v3` 的身份和实现路径。

相对C0的单一低价初始批否决研究版本 `candidate_v5_public_low_price` 及同版本off/on配置见[低价候选说明](docs/strategies/complementary-pair-low-price-candidate.md)。它保持原42参数与公共C0，不叠加BLB，不改旧V3/V4身份。
