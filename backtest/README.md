# 公共历史回测入口

`backtest` 提供 `run_historical_v1`，完成一个显式有限历史范围内的一次连续运行。它只依赖公共策略接口 `shared-run::StrategyV1`，不依赖 `baseline-mm`。独立工作区包 `backtest-cli` 才构造人工策略 `ComplementaryPairV1`，并把它交给公共宿主。两者均不提供热重载。

## 公共调用与真实材料链

```rust,ignore
let report = backtest::run_historical_v1(&run_config, sealed_rules, &mut strategy).await?;
```

调用方提供完整 `HistoricalRunConfigV1`、已通过原历史因果校验的 `SealedProcessRuleSnapshotV3`，以及一个策略实例。配置包含清单路径、接收时间左闭右开范围、已批准用途说明、DPT 资源参数、完整宿主时序、初始账户、运行与代码身份、策略版本及有效参数、原成交终态规则路径和产物位置。数值没有宿主生产默认值。策略身份描述由调用方提供，公共库不解释具体策略参数。

材料依次经过以下原所属模块：

1. DPT `freeze_public_source_input_set_v1` 完整准入显式每日清单和其列出的 Parquet，沿原输入集合摘要算法绑定来源；包括原六来源和新版 V6/TWAP。
2. DPT `prepare_historical_files_in_range_v1` 与 `prepare_historical_files_v1` 共用文件解码、来源预扫描和原排序。范围只筛选原接收事实，不拿文件路径或行号代替业务排序键。
3. 原 `HistoricalMergeV1::prepare_input` 和公共核心应用全部选定输入；正常有限输入产生真实 `EndOfInput`，由原 `PublicRecordingWriterV1` 稳定记录。
4. 正常 End 已应用且原 writer 成功稳定后，由该次完整生产 core 生成固定市场目录，并直接移交 `prepare_hidden_v1`。隐藏入口只提取固定身份、资产、初始 tick 后释放公开 core，随后完整读取同一真实稳定前缀，按原源时间顺序独立归约隐藏数量，不借公开 L2 作隐藏初始簿。独立 `PublicReplayV1` 从盘重建能力继续供消费者及代表性等价验证使用。
5. 原 `RecordedPublicGroupSourceV1` 和 `RecordedHiddenBucketSourceV1` 读取上述材料，由原 SIM 接纳完整公开组及完整隐藏桶。

新历史准备的排序版本唯一来自 DPT 的 `HISTORICAL_SOURCE_HEAD_ORDERING_VERSION`，当前为 `historical-source-head-v2`。旧元数据（V5 和 V6 Existing）按原行的真实准入时间、序号排序，与 AnchorRule 使用同一键布局；缺失 epoch 继续缺失，来源证据、消息摘要和跨来源首项归并规则保持。配置中 `preparation.header.ordering_version` 仅保留兼容输入形状，不选择算法；宿主将实际版本写入新公共流头及启动元数据的有效配置。旧已记录流不重新排序、不改写流头，也不能重标为新排序产物；需要新排序结果时须重新准备材料。

调用方记录新材料的有效配置时复用 `HistoricalPreparationConfigV1::effective_header`。材料试制工具的成功及失败报告均使用该实际流头；兼容旧标签输入不被拒绝，已有报告和已有记录保持原样。

`HistoricalBacktest` 是准确的模拟调用身份：隐藏路径驱动撮合，公开路径只提供因果观测。它不冒充训练、独立评价或 PAPER 身份。配置中的用途说明用于保留调用方已有授权，不替调用方决定数据集用途。

## 时序与连续账户

`HistoricalTimingV1` 分别配置公开交付延迟、计算开始延迟、计算耗时和完成后的调度发送延迟。各项都使用显式 `DelayScheduleV1::Fixed` 或 `Sequence`；序列耗尽返回原类型错误，不隐式重复，也不补零。

公开可见时刻来自原完整组的依赖接收时间以及闭合它的实际输入接纳时间；交付再加显式宿主延迟，并沿原输入序保持非倒退时序。SIM 的原经验代理时延与平台延迟继续使用其原规则。策略参数中的动作路径估计不决定宿主的计算耗时。

事件循环比较真实下一公开组、下一完整隐藏桶、原 SIM 到期义务、策略请求的评估时刻以及冻结计算的发送时刻。同毫秒全部公开组先按原序接纳；已经冻结的发送请求在本毫秒隐藏桶闭合前提交。当前未消费的原义务只通知一次计算，原隐藏桶或义务驱动实际完成后重新接纳触发；显式零计算/发送延迟可以在同毫秒发送，随后原义务仍会推进。计算期间可继续接收公开组，发送时仍使用原冻结意图、原观察截止和独立实际发送时刻。经营选择不会借后来的观察改价，也不会回插过去。

整个运行只创建一个 SIM 会话、一个 Ledger、一个共享执行器和一个传入的策略实例。新增市场沿原登记入口加入；旧市场的订单、预留、成交、费用、孤腿和应收保留在同一公共状态。策略实例中的高水位及业务记忆也持续保留。分派使用原 `dispatch_strategy`，把原成功或批次错误交回 `dispatched`；公共宿主不自动重试、缩单或把错误改成 NoTrade。结算继续使用原 `SettlementCoordinatorV1`；赎回请求没有确认时不会增加现金。

## 有限结束与产物

正常范围结束只推进范围内义务。完整隐藏桶给出的下一桶或真实源 EOF 证明空档，宿主据此在范围末毫秒闭合已证明的隐藏前缀，并领取该时点可见的原反馈。它不添加范围外行情或市场终态，不推进范围外义务。

历史记录使用 `FiniteHistorical` 持久角色。真实源 EOF 对该身份不要求所有市场已有经济终态，也不自动排空未来业务义务。正常范围结束使用原记录排空能力，保存 `result.json`，不会调用 `complete` 或产生 `RunSeal`。范围结束与经济终局保持区别。

策略 Exit 在其显式计算完成事件立即返回；不稳定记录、不排空、不补撤单、不等待结算，也不写最终结果文件。失败保留原策略错误或原批次执行错误并结束，不自动重试。Exit/失败返回的 `ledger_consumed_memory` 明确是已消费的内存读取；`durable_noe_prefix` 只指实际已稳定的原 NOE 前缀，不能拿内存末态当作耐久结果。

启动元数据保存完整有效配置、来源清单与集合摘要、原公共稳定前缀、规则摘要、代码身份及策略版本。通用结果复用 `AccountEconomicsReadV2` 和原 Ledger 投影，保留结束原因、真实观察时刻、记录是否排空、未闭合承诺、缺前驱的 NOE、待发送计算和下一原义务。

`account_economics_memory` 保留同一次报告构造的完整原 `AccountEconomicsReadV2`（cash、逐市场assets、成功份额与成本、配对／孤腿、分Maker/Taker执行费用、返佣预计／未成熟／确认未付／已付、结算状态与应收等）。顶层NAV/WCM兼容字段直接来自这一对象，不是独立计算；最终范围内义务可能在最后一次策略回调后改变经济事实，不能以最后回调代替此最终读取。

`observation_cutoff` 保留实际 `TrustedGroupDecisionCutoffV1`，包含原公开组receipt、受信逻辑观察时间和实际领取的私有反馈序列；`last_observed_at_utc_ms` 由它派生。`ledger_consumed_memory` 的账户身份、ledger/platform/local前缀定位已被Ledger消费的内存状态；`pending_noe_records` 说明已领取但尚待前驱的记录，`durable_noe_prefix` 仅定位实际持久的NOE前缀。这些前缀含义不同，不因正常排空或同一报告而声称观察、消费、经济与持久完全齐平。

正常RangeEnd在原报告后沿原 `stop_and_drain` 更新记录状态与持久前缀，将同一经济对象及截止写入既有 `result.json`，不新增业务推进或 `RunSeal`。Exit/失败只返回原内存报告，包含同一读取结构，仍无隐式排空或最终结果文件；不能把它当成持久末态。金额沿原ExactDecimal字符串及ExactRationalV1整数比输出，序列化不新增反序列化或账户恢复能力。

结果只保留最近发送与当前未完成计算的时序及总计算次数，不复制逐决策账本。该入口不要求逐决策 JSONL、SQLite 或诊断落盘前置。

## 上层人工策略命令

完整启动配置形状见 [合成命令配置](cli/configs/synthetic-command.json)。其中数值及代码身份是工程示例，来源位置需替换为本次已批准的实际材料，代码身份也需填写本次真实构建事实；示例没有研究校准或正式风险结论。

从工作区根运行：

```bash
cargo run -p backtest-cli --locked --offline -- \
  --config path/to/run.json \
  --strategy.repair.enable_active_repair false \
  --strategy.quote.base_pair_shares 12
```

配置支持 JSON 和 TOML。`--help` 列出全部可覆盖字段。所有策略参数使用 `--strategy.<区段>.<字段> <显式值>`；联合成交情景数组使用 JSON 数组文字。优先级为命令行、配置文件、策略原类型的已有默认。当前策略全部数值必填，没有正式数值默认，命令层不复制一套默认值。覆盖按原参数真实类型解析，文件可省略由 CLI 补齐的字段或区段，无需重复占位。未提供布尔覆盖会保留文件值，显式 `false` 会覆盖文件中的 `true`。公共运行的策略描述由命令层从实际构造参数生成，文件不需要再重复一份 `run.strategy`。

人工策略健康参数也全部支持 `--strategy.health.<字段> <毫秒>`，例如 `--strategy.health.chainlink_silence_ms 1500`。八个字段、边界及动作依赖见 [策略库健康说明](../baseline-mm/README.md#来源健康与冻结动作)，当前合成配置没有正式默认。命令启动时解析覆盖并将完整 `health` 区段写入原启动元数据，不提供热加载或跳过健康的运行开关。

## 验证

`tests/historical.rs` 用实际采集 Parquet 写入器、每日清单准入和完整生产入口检验：同毫秒双完整组、迟到公开盘口与隐藏成交隔离、计算期间更新报价后冻结发送保持原值、两个市场连续账户、一侧成交后撤单仍在途、范围尾部未完成计算、最后策略回调之后的真实成交/费用/返佣进入最终返回和result.json、立即 Exit 和原错误停止。

`cli/tests/source_health.rs` 通过同一 typed Parquet 准入与 `run_historical_v1` 装配真实人工策略，验证健康到期撤单、辅助退化后的独立修复和必要盘口失效、事件滞后与同值恢复、双资产代次同步、计算/待发失效和立即 Exit。有限测量包含真实请求与非零资格调用，分别记录期限及资格回调实耗；整轮耗时不被解释成纯检查成本。

`cli/tests/command.rs` 实际启动 `backtest-cli` 子进程，组合 `ComplementaryPairV1`，核对文件参数、Decimal/统计参数及显式 `false` 覆盖，并读取真实启动元数据中的完整有效参数。工作区回归遵守根 `AGENTS.md` 的固定命令和具名 UTC 测试跳过规则。

## 显式市场元数据初始化与经营起点

`HistoricalPreparationConfigV1::metadata_initialization` 为可选配置，省略时沿原准备、来源身份与宿主触发。启用时提供 `MetadataInitializationConfigV1`：

- `files` 指定原清单及其中的元数据相对文件路径；只提取选中文件事实，沿 `open_finite_public_input_v1` 验证身份、Footer、模式和有界解码，不准入前一日其他文件。
- `markets` 指定目标 `condition_id` 及起始真实 Snapshot 的 `snapshot_source_message_id`；消息身份沿原 DPT 计算，物理行号不代替身份。
- `received_start_utc_ms`、`received_end_utc_ms` 对应同一准备范围，`purpose` 保留调用方的已批准初始化用途。

DPT先在所选文件定位指定Snapshot，再分页读取每个目标市场从该Snapshot的原来源排序位置开始、接收时间严格早于研究起点的完整Existing片段。Snapshot、StateTransition以及原轮询/终态/汇总记录均交原归约；中间记录不跳过。同市场多个合法Snapshot选择取片段并集，以最早指定Snapshot为起点，每个指定Snapshot仍分别核验。重复文件/目标配置不重复注入同一物理记录，原文件本身重复事实仍保留。起始消息错误、文件身份错误或原业务依赖缺失在对应原边界失败，不替换Snapshot。此选择排除AnchorRule，范围外盘口、价格和TWAP仍未开放。

选中元数据与研究输入共用原历史排序及归约，完整输入、无输出输入和来源绑定由原公共记录保存。`historical-initialization.json` 只保存来源文件事实、初始化输入位置和消息身份，不保存第二份市场状态。正常 End 及原 writer 稳定成功后，将准备 core 直接交原隐藏入口；初始化与研究输入共用的固定上下文不重建，隐藏数量仍从同一真实稳定前缀独立派生。Existing元数据缺失的collector epoch保持缺失，公开可见规则不改变，初始化不制造隐藏时间。

`input_set_identity_sha256` 继续只描述原每日准入集合。启用初始化时，实际 `source_identity_sha256` 使用 `historical-metadata-initialization-source-v1` 组合原每日集合、初始化原清单及文件事实、目标Snapshot选择和所属范围/用途；组合不包含本机物理根路径。SIM来源身份使用此实际组合摘要，无初始化时与旧集合摘要一致。排序版本仍来自原DPT实现，不能用来源组合版本替代排序版本。

宿主沿同一driver、会话、Ledger及策略对象运行：严格早于经营起点的真实公开交付和隐藏桶仍登记、推进市场及消费原自动义务，但不产生经营dirty、不消耗计算序列、不调用策略或分派、不进行显式结算协调。起点处不封口或重新开户；下一真实公开组或原合法义务进入区间后按原规则经营，hidden-only消费不新增策略触发。同毫秒原义务闭合继续保留。原零金额自动 `MakerMarketFinalized` 事实仍保存在NOE/账户状态，账户读取不能被当作收益样本分母。

公开 `Sequence` 延迟按包含初始化的完整公共流 `group_position` 读取；调用方必须显式提供所有项，不补零或平移。计算三项序列仅按实际研究计算从零读取。初始化来源如果经原可见规则及显式延迟到达起点之后，届时才更新状态并按原合法事件触发，不能倒灌或永久忽略。新运行报告的 `delivery_summary` 分别汇总起点前与区间内实际交付的组位置、时间和来源类别，不存逐决策账本；旧调用省略该报告字段。

`material_trial` 始终只准备与观察，不创建账户。统计以组 `member_input_positions` 判断来源；全部成员来自初始化的组计入 `initialization_public_groups/initialization_markets`，研究成员组计入原 `public_groups/markets`，跨来源成员/闭合位置及观测中的初始化直接来源另计 `research_groups_with_initialization_dependencies`。初始化观测不进入研究首见时间或字段缺失分母；`prepared_markets` 是身份目录，不能据其出现推断完整研究窗口已覆盖。

`tests/initialization.rs` 使用真实元数据边界和标明用途的工程账户夹具，覆盖完整片段、单文件选择、重复配置、来源错误、删除原依赖、分页重放、显式非恒定延迟、经营起点、晚到初始化、早于接收起点的隐藏源时间、原同毫秒闭合和零金额生命周期。材料试制工具另验证初始化与研究观测分母。完整真实五分钟材料及资源测量在工程交付后的研究步骤执行，工程样例通过不表示材料或盈利验收。

## 准备 core 的直接移交

按 [#214 的专项批准](https://github.com/slowchillchill/btc-updown-marketmaker/issues/214#issuecomment-5976228785)（D-0282），每次 prepare 不再丢弃已完成 core 后额外执行一次空状态公共重放。此项仅替代 #196 的该生产步骤；输入初始化、正常 End、持久材料和独立从盘重放责任保持。已有 hidden 入口消费 core 的所有权，投影的是固定上下文中的初始 tick，随后释放公开数量状态，再按真实 Canonical 和原外排独立计算隐藏步骤、报告与索引；当前 tick 和公开盘口不作隐藏起点。

原输入准备／归约及 writer 写入／关闭／稳定错误仍在原步骤失败。存储 JSON、顺序、流身份、分片及 EOF 计数继续由原 hidden reader 在完整读取时核对；部分存储错误因取消前置重放而在 hidden 阶段暴露，可能留下未完成 hidden 目录，但不能返回成功材料。没有新增文件改动检测、全量重哈希或每次自产全读证明；独立 replay 的原检查保持。

小型原 prepare 产生的直接移交结果，与同记录独立空重建 core 后的 hidden 结果逐字段比较固定市场、完整隐藏步骤／报告、索引、桶及正常前缀。初始化混合与当前 tick 已变化但初始 tick 不变的代表另有明确断言；既有晚身份／规则、同毫秒桶、跨 epoch 前量冲突和缺源时间回归继续覆盖原行为，另验证隐藏读取的原 EOF 错误不会变成成功。此实现段只运行小夹具和工程检查，没有真实十二分钟／小时材料运行；消除重复调用由实际调用链确认，节省时长待后续准确全流程测量，不套用旧纯 replay 时长。


## 准备过程中交付通用统计

`prepare_historical_run_with_statistics_v1(config, progress)`与原`prepare_historical_run_v1`共用同一个准备循环。原入口不创建统计累积器；新入口借用每次原core生成的完整组、本次真实Input及当时已登记的初始化来源，就地累积原材料试制统计。正常End也按原返回的完整组迭代，当前核心的End不产生额外组。`PreparedHistoricalRunWithStatisticsV1`仅在输入归约、writer稳定、初始化来源文件与hidden准备全部成功后返回`prepared`和完整`statistics`；任何原失败不返回局部统计。

原Counts/MarketStats及分类归属backtest，字段与序列化形状不变；研究／初始化分母、跨来源依赖、已知空与Unavailable、spot重复及源时间回退仍按原口径。`statistics.markets`来自完整组实际观察，不能用固定身份目录`prepared_markets`替代。新路径只保留原计数、首例和最近来源；不保留完整组、盘口或观察历史。统计状态与准备及hidden有存活重叠，可能提高峰值RSS，未增加新的市场上限。

两路复用SIM所属`original_public_group_visible_at_utc_ms`纯计算：关闭Input的接收／接纳时间取最大，再与本组事实依赖接收时间取最大；统计沿原单调可见时间记录。`RecordedPublicGroupSourceV1`仍将同一值传给原时序接口，调用方自定义执行时序不改变。

`material_trial`改用新入口，不再独立打开统计reader，仍交付原完整`statistics`供原两市场／首小时研究消费。合并阶段计量名为`prepare_and_statistics_measurement`；独立`statistics_measurement`为null并明确标记不适用，不能把它写成0秒或声称统计没有计算。旧实测报告保持。

库进度默认为None关闭；材料示例沿原`--progress-interval-seconds`参数，默认30秒、0关闭。可选`HistoricalPreparationProgressV1`在原应用检查点报告输入／组／观察计数和耗时；稳定及hidden完成之前`published=false`；可信总量和未知项的具体口径见下节。阶段覆盖下述11个连续区间，阻塞调用内部没有心跳或独占函数耗时分解。输出失败只关闭日志，既不改变业务错误也不重试。

此工程段通过小型真实prepare与同记录独立空重放的完整统计对照及必要回归验证；没有新的真实十二分钟／小时材料运行。后续如准确签发测量，应将新合并阶段与旧525.219863807＋187.391546387＝712.611410194秒对照，研究另列；不能直接声称节省全部187.39秒，因为原统计计算仍保留。

## 原准备路径的阶段计量与进度

启用材料示例的进度时，`HistoricalPreparationProgressV1`同时保存固定的11个连续调用区间：文件准入；初始化选择及来源装配；epoch前遍；后遍供料与初始外排；各来源外排finish；公共应用与就地统计／录制（含End）；公共稳定及目录装配；hidden公共扫描与供料；hidden消息外排finish；hidden归约与派生写入；hidden最终化（derived关闭、index.finish、原sync及结果）。父级`prepare_and_statistics_measurement`不与这些子区间再次相加。后遍的惰性解码、来源构造、序列化与sorter.push交织，区间名称保留这种合并关系，不当作独占排序时间。

training的通知只承载本次历史准备的阶段和已完成工作的标量，不读取/proc，也不依赖backtest。旧入口与带通知入口共用原私有实现。文件准入仍完整验证SHA及footer，两遍原始供料各自沿原文件校验和有界解码；epoch前遍和后遍的职责未删除，解码后才按原接收范围筛选。初始化文件的读取计数与日文件分列。物理解码批次／行、区间入选行、文件完成、六来源SortMetrics、公共应用输入／生成组／观察、hidden已读取记录／成功供料消息／归约消息／实际写入步骤和报告／成功索引数分别报告，不能相加冒充独立行情样本。

检查点采用真实批次、文件、阶段边界和循环内的可控间隔；0关闭内部阶段采样及输出，普通prepare不启用通知。单次阻塞的整文件SHA、解码、外排finish或sync未返回时，没有后台心跳或可靠百分比。准入和初始化选择完成后，原footer及已读取初始化文件提供每遍的可信物理文件／行总量；各来源外排finish成功后，六路原SortMetrics的实际输入行数加原End提供公共应用输入总量。`applied_input_fraction`只表示这一阶段的已应用输入比例，不是整个prepare的完成百分比；未确定的总量和所有ETA继续为null。输出损坏后停日志，阶段计量仍可继续；计量源不可用时保留墙钟与可用性，CPU／I/O缺失为null，不改变业务控制流或重试。

`material_trial`新增`preparation_phase_measurements`，包含固定阶段、阶段成功／失败、工作量及相对合并总计的residual。阶段端点复用同一次进程计数快照形成连续差值，residual如实保留，不硬凑到某个热点；未访问阶段为空。/proc的rchar是进程读请求量，read_bytes是进程设备读取计数，均包含计量和少量输出开销，不能进一步归给区间内部函数。阶段端点RSS不是阶段峰值，也不将RSS相加；进程峰值仍由外层GNU及资源测量器记录。

准备失败后，即使结束端点计量不可用，材料示例也优先保存并返回原prepare错误，成功statistics仍为空。输入、End、writer稳定、hidden完整EOF和原write／flush／close／sync责任不变。代表小夹具核开启／关闭的完整材料字节与统计、阶段顺序及物理／入选分母、关闭不采样／输出、计量及日志故障、原准入／初始化／写入错误和既有hidden坏EOF。此工程段真实材料运行0次；旧712.611410194秒仅为后续原合并阶段的比较口径，不据代码接线虚报新的时长或端到端结果。

## 正式已准备材料的复用

`prepare_and_publish_historical_run_v1(&preparation, progress)` 是跨进程复用的正式发布入口。它调用同一原准备实现，只有公共正常 End、writer 稳定、初始化来源写入和 hidden 准备全部成功后，才将 `prepared-run-v1.json.pending` 写完并发布为材料根下的 `prepared-run-v1.json`。失败不由 trial 报告或部分目录补出描述。新准备仍要求新的工作根，不覆盖已发布材料。

描述版本为 `prepared_historical_material_v1`，保存实际有效准备配置（材料工作根以 `.` 表示，解析时取描述所在目录）、范围及用途、原来源清单、日集合 `input_set_identity_sha256`、含初始化的 `source_identity_sha256`、初始化来源、固定市场目录、完整公共 committed prefix、hidden consumer 身份及根内相对文件位置。它没有策略、账户、订单、盘口或观察历史，也没有 hidden 内容摘要；来源路径用于溯源，复用时不读取原 raw 文件。调用方应将成功描述和所属材料作为不可变产物保存，不重新编辑其中的配置。

```rust,ignore
let description = backtest::prepare_and_publish_historical_run_v1(&preparation, None)?;
// 每次提供新的策略实例、新运行输出位置及明确的会话/账户配置。
let report = backtest::run_prepared_historical_v1(
    &description, &prepared_run_config, sealed_rules, &mut fresh_strategy,
).await?;
```

`HistoricalPreparedRunConfigV1` 只包含本次时序、会话/账户初态、策略描述、代码身份、最终性规则位置、SIM/NOE/Ledger/元数据/结果输出、消费及激活身份、原账户接纳策略和原运行资源参数。它不接受第二份 `preparation`；范围、初始化、材料来源、市场目录和读取资源配置均由描述唯一提供。`From<&HistoricalRunConfigV1>` 仅提取原运行字段，便于库调用迁移，不重新准备材料。调用方仍显式加载原规则快照并传入；每次运行都会重新装配 SIM、共享执行、NOE、Ledger 和账户，创建独立 public core、候选/依赖索引和 hidden reader。库不会自动重置任意传入的 `&mut StrategyV1`，调用方必须提供新策略实例。

旧 `run_historical_v1` 保持完整配置和原 prepare 后运行方式，与新入口共用同一装配及经营循环。新入口按描述的固定凭据使用原 `open_committed`，更晚的稳定目录不进入本次视图；候选、保存依赖及历史身份修复扫描也沿同一前缀，保留原完整 EOF/计数责任。描述只做必要版本、相对定位和固定市场/流绑定结构校验，实际公共载荷仍在原读取位置校验。市场目录缺项返回材料错误，避免下标 panic。hidden 在实际索引页引用的 Step 被消费时，核对完整 `HiddenDerivedStepRefV1` 与 consumer 身份；不预扫未消费的尾部，不重hash，不声称这种绑定是不存在的 hidden 内容摘要。正常结束、策略 Exit、原材料/策略错误及不自动重试规则保持。

CLI 使用三个明确模式：

```bash
# 原方式兼容；省略 --mode 时仍为 run。
backtest-cli --mode run --config path/to/original-command.json

# 配置文件直接是 HistoricalPreparationConfigV1，不要求策略或账户字段。
backtest-cli --mode prepare --config path/to/preparation.json

# 配置文件仍含 run/rules/strategy/request_namespace，run改为HistoricalPreparedRunConfigV1。
backtest-cli --mode reuse --prepared path/to/materials/prepared-run-v1.json \
  --config path/to/independent-command.json \
  --strategy.repair.enable_active_repair false \
  --progress-interval-seconds 30
```

JSON/TOML 解析、完整策略参数覆盖和显式 `false` 沿原命令层复用；每次 reuse 子进程新建 `ComplementaryPairV1` 及 `StrategyContinuityV1::new_account()`。准备模式不加载经营策略或创建账户。prepare/reuse 工具进度默认30秒、0关闭；reuse 打印打开和结束阶段，经营重放转接原可选 replay 进度，读到记录、应用输入、生成组的含义分别保持，未知工作量和 ETA 不编造。库新运行配置的进度默认关闭，输出故障不替代业务错误。

本段只用原有效工程夹具验证：一次成功发布、原 raw 移走后两次独立复用、完整旧新报告与初始化交付相等、每次原2000初始现金与空策略状态、两个市场连续账户及 hidden 成交、输出与材料字节隔离；另核失败不发布、版本/固定 prefix/缺失市场/实际 hidden 混接。原身份修复夹具证明固定前缀不会读取更晚损坏目录，原隐藏夹具证明同 derivation_position 下其他引用字段不符也失败。既有 Exit、已消费损坏、初始化和正常结束代表继续运行。没有真实十二分钟、首小时、研究或收益运行，没有新速度比例；PR235 的材料与统计445.431763861秒、PR228完整959.236639304秒保留各自旧实测身份，不能当作本段复用性能。旧大材料/失败现场不迁移、不回填描述、不重制；完整研究容量、风险参数和盈利验收仍未完成。

## 来源健康期限与未尝试失效

`run_historical_v1` 和 `run_prepared_historical_v1` 在传入策略显式选用 `uses_source_health` 时，开启原 DPT 回放的流进展投影，并将策略提供的健康期限加入同一事件调度器。期限独立于待发计算，即使无新公开组、计算未完成或完成后仍等待发送，也会按原逻辑时钟到期观察。没有固定节拍、自动重试或公共健康阈值；期限须严格晚于调用时刻，已到期条件应在当前资格判定中处理。

选用路径使用当前受信观察读取，仍保持同刻公开组先于发送、发送先于当前隐藏桶闭合及既有因果截止。计算期发现失效先通知策略，保留原计算完成边界；完成后等待发送时失效可在发现时刻重新计算。新计算继续支付配置中的计算开始延迟、计算耗时和发送等待，不等待新行情，不复用旧后缀或覆盖原冻结诊断。发出第一项后，第二项提交前失效也停止全部后缀；实际效果、未知义务及预留释放由原共享执行处理。

到期的健康期限同时保留下一次重评需求。当前待发决定为正常 NoTrade 或所有意图仍合格时，原批次继续完成；完成后按原计算开始延迟重新计算，不等下一条行情，也不把正常批次记成失效。

只在选用时返回 `HistoricalRunReportV1::source_health`，包含实际资格调用次数、期限唤醒次数、失效批数及最近失效计算的原时序。它不保存逐决策账本；未发送失效的 `actual_sent_at_utc_ms` 为 `None`。旧消费者保留原公开组、回调和 `next_evaluation_at_utc_ms` 语义。

工程反例在 `tests/historical.rs`、`shared-run/tests/strategy_boundary.rs` 与 DPT 原测试入口覆盖静默期限、接收持续但事件滞后、已发前缀/未发后缀和无新行情重算。`source_health_finite_overhead_measurement` 是显式忽略的合成测量入口，以 `SOURCE_HEALTH_MEASUREMENT_ENABLED=0/1` 和已有 `SOURCE_HEALTH_MEASUREMENT_ROOT` 运行同一有限夹具；可用 `/usr/bin/time -v` 分进程记录 CPU 与峰值内存。其参数、输入和结果不构成正式健康阈值、历史断网证明或在线性能承诺。
