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

`HistoricalBacktest` 是准确的模拟调用身份：隐藏路径驱动撮合，公开路径默认只提供因果观测；显式Gamma政策另接纳成功消费的合格终态，仍等执行前沿闭合才应用经济。它不冒充训练、独立评价或 PAPER 身份。配置中的用途说明用于保留调用方已有授权，不替调用方决定数据集用途。

## 时序与连续账户

`HistoricalTimingV1` 分别配置公开交付延迟、计算开始延迟、计算耗时和完成后的调度发送延迟。各项都使用显式 `DelayScheduleV1::Fixed` 或 `Sequence`；序列耗尽返回原类型错误，不隐式重复，也不补零。

公开可见时刻来自原完整组的依赖接收时间以及闭合它的实际输入接纳时间；交付再加显式宿主延迟，并沿原输入序保持非倒退时序。SIM 的原经验代理时延与平台延迟继续使用其原规则。策略参数中的动作路径估计不决定宿主的计算耗时。

事件循环比较真实下一公开组、下一完整隐藏桶、原 SIM 到期义务、策略请求的评估时刻以及冻结计算的发送时刻。同毫秒全部公开组先按原序接纳；已经冻结的发送请求在本毫秒隐藏桶闭合前提交。当前未消费的原义务只通知一次计算，原隐藏桶或义务驱动实际完成后重新接纳触发；显式零计算/发送延迟可以在同毫秒发送，随后原义务仍会推进。计算期间可继续接收公开组，发送时仍使用原冻结意图、原观察截止和独立实际发送时刻。经营选择不会借后来的观察改价，也不会回插过去。

整个运行只创建一个 SIM 会话、一个 Ledger、一个共享执行器和一个传入的策略实例。新增市场沿原登记入口加入；旧市场的订单、预留、成交、费用、孤腿和应收保留在同一公共状态。策略实例中的高水位及业务记忆也持续保留。分派使用原 `dispatch_strategy`，把原成功或批次错误交回 `dispatched`；公共宿主不自动重试、缩单或把错误改成 NoTrade。历史自动模式由 SIM 在原判定、最终性／补偿和适用返佣前驱闭合后自动结算。宿主只经 NOE→Ledger 消费原经济事实，不发送赎回请求、查询或重试；自动回款增加原现金并消除对应应收和剩余成本，不在 NAV 重复加计。公共请求式结算协调器继续供其他已批准消费者使用。

## 有限结束与产物

正常范围结束只推进范围内义务。完整隐藏桶给出的下一桶或真实源 EOF 证明空档，宿主据此在范围末毫秒闭合已证明的隐藏前缀，并领取该时点可见的原反馈。它不添加范围外行情或市场终态，不推进范围外义务。

历史记录使用 `FiniteHistorical` 持久角色。真实源 EOF 对该身份不要求所有市场已有经济终态，也不自动排空未来业务义务。正常范围结束使用原记录排空能力，保存 `result.json`，不会调用 `complete` 或产生 `RunSeal`。范围结束与经济终局保持区别。

策略 Exit 在其显式计算完成事件立即返回；不稳定记录、不排空、不补撤单、不等待结算，也不写最终结果文件。失败保留原策略错误或原批次执行错误并结束，不自动重试。Exit/失败返回的 `ledger_consumed_memory` 明确是已消费的内存读取；`durable_noe_prefix` 只指实际已稳定的原 NOE 前缀，不能拿内存末态当作耐久结果。

启动元数据保存完整有效配置、来源清单与集合摘要、原公共稳定前缀、规则摘要、代码身份及策略版本。通用结果复用 `AccountEconomicsReadV3` 和原 Ledger 投影，保留结束原因、真实观察时刻、记录是否排空、未闭合承诺、缺前驱的 NOE、待发送计算和下一原义务。

`account_economics_memory` 保留同一次报告构造的完整原 `AccountEconomicsReadV3`（cash、逐市场assets、成功份额与成本、配对／孤腿、分Maker/Taker执行费用、返佣预计／未成熟／确认未付／已付、结算状态与应收等）。顶层NAV/WCM兼容字段直接来自这一对象，不是独立计算；最终范围内义务可能在最后一次策略回调后改变经济事实，不能以最后回调代替此最终读取。

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

人工策略健康参数也全部支持 `--strategy.health.<字段> <毫秒>`，例如 `--strategy.health.chainlink_silence_ms 1500`。十六个字段、normal/max 边界及动作依赖见 [策略库健康说明](../baseline-mm/README.md#来源健康与冻结动作)，当前合成配置没有正式默认。命令启动时解析覆盖并将完整 `health` 区段写入原启动元数据，不提供热加载或跳过健康的运行开关。

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

新有限历史材料按 [D-0309](https://github.com/slowchillchill/btc-updown-marketmaker/issues/62#issuecomment-6052454945) 采用 DPT 的[紧凑深度描述](../data-pipeline/training/README.md#显式有限选源的公共录制)，保留原 Input 和独立公共重放。只有 Binance 派生深度正文不再逐条持久保存；隐藏参考索引及完整桶保留，SIM 撮合、NOE／Ledger、费用和最终经济读取沿原消费链。旧材料兼容，发布和复用入口不变。

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

training的通知只承载本次历史准备的阶段和已完成工作的标量，不读取/proc，也不依赖backtest。旧入口与带通知入口共用原私有实现。文件准入仍完整验证SHA及footer，并把同一次已打开对象与footer移交首次reader；独立重开继续核验完整内容。原prepare的epoch前遍与后遍保留各自有界供料职责。初始化选择只读取一次正式Snapshot和所需前序行，后续pass复用这些行，因此只报告selected_rows，不虚报物理解码或文件完成。初始化文件的读取计数与日文件分列。物理解码批次／行、区间入选行、文件完成、六来源SortMetrics、公共应用输入／生成组／观察、hidden已读取记录／成功供料消息／归约消息／实际写入步骤和报告／成功索引数分别报告，不能相加冒充独立行情样本。

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

CLI 使用四个明确模式；原三个兼容模式如下，`day`见下节：

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

每个公开组 `next_group` 成功后，宿主借用该组调用 `qualify_intent_public`，先记录冻结动作公开依赖失效，再消费下一组。发送到期同毫秒也检查；首次失败复用原通知、计数与失效锁，恢复不复活旧决定。计算尚未完成时等原完成，已完成时在检测时刻安排最早重算；消费全部同毫秒公开组后仍按原发送／隐藏桶／账本／新计算顺序处理，新计算继续支付原配置时序。尚未分派或预留的 pending 没有资金释放，真正发送继续走原整批预留与完整资格路径。

到期的健康期限同时保留下一次重评需求。当前待发决定为正常 NoTrade 或所有意图仍合格时，原批次继续完成；完成后按原计算开始延迟重新计算，不等下一条行情，也不把正常批次记成失效。

只在选用时返回 `HistoricalRunReportV1::source_health`，包含实际资格调用次数、期限唤醒次数、失效批数及最近失效计算的原时序。它不保存逐决策账本；未发送失效的 `actual_sent_at_utc_ms` 为 `None`。旧消费者保留原公开组、回调和 `next_evaluation_at_utc_ms` 语义。

工程反例在 `tests/historical.rs`、`shared-run/tests/strategy_boundary.rs` 与 DPT 原测试入口覆盖静默期限、接收持续但事件滞后、已发前缀/未发后缀和无新行情重算。`source_health_finite_overhead_measurement` 是显式忽略的合成测量入口，以 `SOURCE_HEALTH_MEASUREMENT_ENABLED=0/1` 和已有 `SOURCE_HEALTH_MEASUREMENT_ROOT` 运行同一有限夹具；可用 `/usr/bin/time -v` 分进程记录 CPU 与峰值内存。其参数、输入和结果不构成正式健康阈值、历史断网证明或在线性能承诺。

## 原Parquet日作业、并行与日内恢复

D-0311/D-0312 下的 `run_historical_days_v1` 接收 `HistoricalDayConfigV1` 日期列表和原 `CheckpointStrategyV1` 工厂。每日期只创建一个公共 `PublicObservationCoreV1`。每候选在线程内创建自己的策略、SIM、NOE、Ledger、共享执行和时序；这些运行对象无需跨线程传递。默认日期并发按可用CPU数和最大候选数安排，调用方也可用 `run_historical_days_with_parallelism_v1` 明确日期并发。同日独立候选同时消费不可变公共组和完整隐藏桶；同一市场、同一账户的原业务因果顺序保持。

新日入口直接使用原每日清单和原Parquet，复用正式准入、解码及初始化选择。它先按文件并行读取必要窄列，形成身份／物理定位／来源顺序的最小索引。索引覆盖全部相关未读行，允许Footer时间统计缺失。索引不保存完整Canonical、盘口或观察录制；随后使用原文件／行组／组内行定位读取实际输入。同一天公共Core只归约一次，已创建各独立账户后才开始公共生产，候选不会从空Core重复归约公共前缀。组内不同市场观察及隐藏分市场计算可并行，收集后保留原顺序。该入口不调用旧全期 `HistoricalMergeV1::prepare`，不以完整公共／隐藏宽录制作为账户前置。

公共输入使用 `historical-source-head-v2`：来源内按原准入关系，六逻辑来源的头按原接收／来源／身份合流；Binance snapshot/update保持同逻辑深度流。隐藏PM先按epoch／源时间／序号／Canonical位置归约私有数量，再按全局源时间及原派生先后组成完整同刻桶。BBA、参考事实和未读下界均保留。新隐藏来源绑定为 `ClosedStreamingHiddenStepBucketV1`，排序版本 `hidden-reference-before-pm-source-v1`，同刻参考先于PM、PM保持原相对派生顺序；不伪造旧V1的全期参考数量或 `CommittedIngressPrefixV1` 存储引用。SIM沿同一隐藏算法接纳新表示；旧录制和旧V1继续读取。

`checkpoint_interval_seconds` 是完整日点的保存周期，0关闭周期保存；正常完成仍发布完成引用。`checkpoint_every_applied_inputs`、`checkpoint_every_candidate_events` 可显式指定事件周期。保存发生在各候选的安全事件边界，候选模拟时刻不必相同，公共生产可领先。周期是请求保存的时机，不是最长暂停或可重做时长承诺；实际重新执行范围是上一完整引用之后尚未提交的尾部。长期作业的 `progress_interval_seconds` 控制已处理公共输入及耗时输出，总量不能可靠确定时写 `unknown`。`producer_lead_groups` 是队列领先软目标，默认按可用CPU数取1024组／核；它控制普通生产推进，关闭消费者仍需的隐藏桶时允许继续必要供料，不拒绝输入、不丢同刻材料。公共组和隐藏桶只保留当前消费者仍需范围及最近完整点中的范围。

日点由原模块导出自己的状态：DPT保存原文件游标、预读、六头、Core、TWAP定位、私有隐藏归约和未闭桶；SIM保存订单、在途、随机、到期及反馈；NOE保留幂等、真实消费引用和尚不可见反馈；Ledger保存当前账户／成本归约与原持久位置；共享执行保存注册和已领取却缺前驱的记录、结算协调的handled状态；策略保存continuity及SignalMemory；backtest保存各消费位置、交付时序、完整待发送计算、调度和小型日内汇总。

每个候选先稳定SIM、发布原NOE、封闭原Ledger写入段，再保存模块状态。协调器写完整日状态，校验引用并同步目录，最后原子发布 `checkpoint.json`，同步日根目录。任阶段中断保留此前完整引用；恢复校验完整点全部列明文件后才重新接原写入器、处理其准确尾部。它不重扫数小时SIM／NOE／Ledger日志，也不新建账本配旧策略。新点耐久后仅回收上一清单明确列出的文件；未知或未发布文件不作为完整点，也不被猜测清理。不可变 `source-order.cbor` 只保存一次，周期状态保存其摘要引用。日根、原输入、版本、初始化、业务配置、规则／finality及原持久写入器位置共同定义恢复；进度、保存周期和生产领先等调度参数可重启调整。

日入口在全部候选声明观察需求后，按其并集配置唯一公共 Core。`StrategyV1::observation_requirements` 默认要求完整 Binance 深度；complementary_pair 的两个 Binance 权重都为零时不要求数量。仍有完整消费者时保留完整数量。省略的数量明确表示为 `BinanceDepthObservationV1::NotMaterialized`，原未知仍是 `None`；来源、质量、直接锚点、依赖前沿、组位置和时间不变。日交付的深度事实保留原 `PublicFactBindingV1` 引用，隐藏输入仍沿原完整归约材料建立参考桶。PM 的逐档 bids/asks 不省略，active repair 继续读取真实 asks。

未变的盘口投影由原事实所有者共享，数量或其归因变化后产生新投影；滞后候选仍持有自己的旧组。私有 `SharedObservationV3` 保存表示按批内实际 Arc 身份编码市场快照版本、盘口、深度正文、组事实和来源进展各一次，再保留各组的市场引用及准确前缀；旧 `SharedObservationV2` 仍在读取边界导入。不按内容哈希，也不把旧组替换为当前 Core。日队列使用整批共享表示，候选模块分别保存其最后消费和预取组。它不建立跨文件共享索引，Core 权威状态和候选状态仍由各原模块保存。

日内经济采样只读取同一 Ledger 前缀的实际预留、未返还本金与 NAV。未返还本金并入 Ledger 原经济贡献索引，沿原事件更新并在恢复时重建；NAV 每次使用实际消费组重新估值，不把报价变化误作账户不变。完整账户报告仍在整点、结果和显式完整读取时生成。采样时刻、积分、回撤和未知值保持原语义。

`stage-timing` 开关沿原整点、检查点和结束输出累计计时，补充日采样、候选编码／写盘同步／公共发布等待及 producer 解码、准备、Core、物化、隐藏归约。各线程的包含耗时不能相加当墙钟；这些数值不参与业务准入。短夹具成本不代表完整研究速度。

日恢复格式为 `original-parquet-day-checkpoint-v3`。该版本明确当前准备订单身份及其Ledger关联；旧v2完整运行不能被当前入口静默续接。SIM请求只保存旧序号／公开请求派生身份的模式，缺省旧模式仍保持原含义；旧原始事件可解释，但不从旧材料反推新关联，也不提供通用检查点迁移。内部模块字节采用 CBOR 字节串；写入时按成功写出的实际字节生成摘要，flush 和 sync 成功后才能发布引用，不在发布阶段回读完整状态文件。恢复时仍校验已发布文件。部分完成点保留运行状态；仅全部候选完成时发布计数、输入及版本身份和完整报告，省略已无消费者的 Core／供料／候选恢复文件。旧停止研究的检查点不跨源码身份续跑。有限 8 市场／12 组夹具只证明共享表示确实消除重复，不能据此推算真实日容量、保存耗时或研究 ETA。

有限宿主在运行装配或恢复后、首条公共输入前，开启原记录器的逐次交接等待。每次尚未成功的 SIM 提交遇到原通道暂满时，只等待已有待交接组；不重试已成功的关闭、观察或经济步骤，不增加容量。后台写入失败仍返回原错误。其他使用原非阻塞调用和外层供料重试的消费者保留原行为；账户角色不用于推导宿主是否允许等待。此项线程调度方式不进入业务恢复身份。

正常范围结束、失败和成功 `StrategyExit` 分别保留原原因。Exit沿原立即结束语义，不为保存追加排空等待。合法日末未闭合订单、持仓、待发送计算和应收不会被强行关闭。完成引用复用对应原报告，不重新创建策略或账户。SIM日入口显式选择 `SIMDAY01` 紧凑记录：固定版本标识后，每个原接纳组保存一个长度+CBOR帧，组顺序、输入序号、关闭依据和取消命中保持。头部保存原RunOpened及公共流、消费时序、隐藏输入集合和排序绑定；高频公共／隐藏记录保存真实驱动时刻及原输入／组／桶位置。独立重放须从原Parquet、初始化和同版本公共模块重建组ID、桶事实及PM派生材料，读取类型明确表示这一要求。请求、市场登记、结算和其他控制仍保留完整原业务载荷，NOE的 `#input` 关联不变。

日后台不再物化并保存重复PM绑定／报告；该次临时归约材料随原接纳组释放。检查点保存实际布局、固定绑定、字节位置和业务序号，按原完整点截去未提交尾部后续写，不扫描已提交长前缀。旧检查点缺布局字段默认JSON；普通历史、PAPER/LIVE及旧JSON证据不转写。`max_group_bytes` 按实际所选布局的记录字节保持原上限；帧和头部不增加业务input_sequence。紧凑格式没有证明174账户日可在现有磁盘内完成，经济记录和检查点的完整日成本仍未知。

`HistoricalRunReportV1.day_economics` 在原事件边界读取同一次 `AccountEconomicsReadV3`，累计公共NAV最大回撤和资金占用。公共NAV回撤与baseline扣除未到账确认返佣后的风险权益／30%高水位分开。资金占用明确为原实际预留（含原在途经济占用）加未赎回成功成交剩余本金，以及已经济扣款尚未成功归约的成交本金；不是另建现金或利润账本。金额按原精确数值，时间积分单位为pUSD·毫秒。未知NAV采样保留 `Unavailable`，已观察最大回撤另列为局部证据；未知资金区间不补零。峰值、积分、采样前沿和未知量均随候选点恢复。

`summarize_independent_historical_days_v1` 按策略配置、四项时序、准入、实现及规则身份汇总独立日结果。原确认净损益相加，平均日确认收益率为总确认净损益／各日原初始本金之和；另列扣除未到账确认返佣后的净结果。它保留最差每日公共NAV回撤、未闭合数、待消费NOE及Exit／未完成日数，不串接净值或算复利，不把未知净结果当零。确认结果仍以每日报告的原已消费前缀为界，未闭合日不冒充最终结算。

CLI使用 `--mode day --config <JSON或TOML> --progress-interval-seconds <秒>`。配置顶层提供 `rules`（原规则快照配置）、可选 `parallel_dates` 和 `days`。每个日期提供 `preparation`、`checkpoint_root`、`checkpoint_interval_seconds`、上述可选事件周期／领先目标，以及 `candidates`。每个候选提供原独立宿主 `run: HistoricalPreparedRunConfigV1`、完整43字段 `strategy: ComplementaryPairConfigV1` 和固定 `request_namespace`；宿主时序、规则、账户和命名空间不由策略参数文件自动补齐。CLI沿原字段类型覆盖 `--strategy.<字段> <值>`，day模式把显式覆盖应用到各候选，记录完整有效参数和请求命名空间。再次调用同一日根自动恢复或完成复用；stdout输出 `days` 及上述 `summaries`。旧run／prepare／reuse入口保持原调用。

A的36份策略配置分布在对应五折（7/8/7/7/7），每份只含原43策略字段。调用方按获批比较窗和相应冻结配置装配日期及每候选每日独立2000pUSD账户；入口不自动把36候选用于所有日期，也不自动启动经营研究。

### 日运行的经济读取与物理批量

日入口仍在每个原事件边界读取经济事实并推进统计时间。Ledger 在原事实拥有方维护成交数量、未闭合前驱、剩余费用、投入本金和返佣贡献的增量汇总；报价变化仍重估孤腿，未知估值不替换为零。人工策略只使用这些正式费用归属，不再冻结全部历史成交；需要成交详情的其他策略默认仍取得完整详情。SharedExecution 的动作快照按活动身份和本次请求引用保留原订单及闭合腿的零承诺证据。待关联经济事实只在新反馈、Ledger 应用位置或相关公共市场变化时再次尝试，平台自动结算和原反馈可见顺序保持。

日 SIMDAY01 运行的 NOE 后台将多个逻辑批写入不可变 `noe_arrow_parquet_segment_v1` 段。原数字 `.parquet` 地址是逻辑容器地址，由原 `.manifest.json` 定位段和本批行范围；批内 ordinal、来源水位和发布顺序保持。段正文、footer 和文件同步只完成一次，引用只在对应 manifest 稳定后交付。单条／单批读取仅解码所需范围，显式顺序历史读取复用当前段的 Arrow 页；普通历史／PAPER 的写入保持原单批 Parquet，旧单批 Parquet 和 JSON 仍由同一 owner 读取。运行及检查点不保留全历史耐久引用列表，实际检查消费者按需枚举已发布材料，并接收读取错误。直接对逻辑地址取文件规模的可选展示可能为未知，应由读取方经 manifest 找到物理段；本组不修改独立展示模块。

日后台以 256 逻辑批、4096 行或 4 MiB 已编码正文作为排出目标，大批不会因此被拒绝。原逐市场结束、完整检查点和正常完成仍立即排出所需材料，顺序仍为 SIM 稳定、NOE 发布、Ledger 来源引用持久化。Ledger 内存归约保持实时，Parquet 小交接批合并为有界行组，原交接批的 flush 不等于 fsync；稳定点仍封口并同步。退出不补排空。恢复只删除完整点后的逻辑 manifest，并整体回收已无保留 manifest 引用的 owner 段，不截断共享段或重写已发布前缀。上述表示变化不提供跨实现身份的旧研究续跑，也未验证完整一小时或 174 账户日的吞吐、容量和耗时。

日入口在同一次窄列遍历中定位原行坐标和来源epoch，先利用可信行组范围排除窗口外内容；统计缺失时读取真实行。只有失序内容才整理，六来源合流及隐藏源时刻下界保持。深度输入一次准备原精确数值，归约仍保留代际、stitch、质量及前量检查；外部raw入口继续验证。

恢复公共日状态和来源顺序索引时，缓冲解码与完整文件摘要同次读取，解码后的剩余字节同样计入摘要。候选文件同次缓冲读入并核验，再由原候选worker解码。全部发布文件验证成功后才挂接可能截尾的持久writer；不跨实现身份恢复旧研究点。下一研究包准备可复用已算的小文件摘要，此项不属于运行稳态提速。

### 显式连续窗口研究汇总及预算保存

旧 `run_historical_days_v1` 与默认CLI日入口保持原行为和输出。调用
`run_historical_days_with_options_v1` 或在day命令增加 `--research-summary` 才启用紧凑窗口汇总。
成功的 `calculate` 只分类一次：Continue空intents为NoTrade，Continue非空为动作，Exit单列。
三类之和是成功决定总数；健康失效、发送失败、期末pending不撤销原决定。计算报错不计NoTrade。
分类、小时游标和快照随原 `RuntimeLoopCheckpointV1` 保存；旧检查点缺少分类时保持未知。

小时边界只读此前已完成事实，在下一小时第一项真实事件前采样。边界不进入事件队列，
边界同毫秒多组不提前计入上一小时，无事件小时不造行情或决定。每张快照保留边界与真实
`observation_cutoff`；NAV变化、确认净结果、未到账返佣分别列示。金额差额用原精确有理数相减，
原资金积分和累计回撤复用 `HistoricalDayEconomicsSummaryV1`。没有区间内极值记录，不能把累计最大回撤
相减称为小时最大回撤。跨小时保持原账户、策略记忆与高水位，不重新开账户。

Maker/Taker份数、金额、费读取原executed；笔数按Ledger原trade身份的经济应用/成功状态读取，
生命周期不重复计数。有SIM回执的提交请求、其声明订单腿和撤单请求分列；回执不等于平台接受。
无回执注册单列，是否已提交保持未知；未发送的冻结pending独立保留。撤单累计遍历registry原请求与完成事实，
不把 `cancellation_requests(None).response_counts` 的最近一次响应当全窗累计。

`--wall-budget-seconds 25200 --checkpoint-reserve-seconds 900` 从CLI输入准备前开始计时，
到6h45m请求最后完整检查点。原requested→ready→publish成功后，活动候选在原save边界返回
`BudgetStop`。该检查点保留Running、源游标和pending；不执行RangeEnd尾部推进、不补发、不清仓。
部分报告仅在本次返回，不放completed。发布失败保留此前成功发布代并报错，不能虚报预算保存完成。
`published_checkpoint` 是最后成功发布引用。返回报告中最后读取的投影不因此变成独立持久事实；
分类/小时仅以该引用包含的原候选检查点为准。

原策略Exit仍立即结束，报告终止时点与未闭合状态；退出后的小时不补零。RangeEnd、StrategyExit、
BudgetStop、Failure沿原report终止原因区分。CLI硬截止仅覆盖CLI自身；若随后运行机器汇总，必须放在
同一个外层7h期限内。硬截止只承认最后成功发布代，未输出的stage-timing值为未知。
本轮具体准备、启动及证据边界见[六小时研究方案](../baseline-mm/docs/research/complementary-pair-six-hour-plan-20261012.md)。

## 新 A 复核显式选择 Gamma 政策

`HistoricalRunConfigV1` 和 `HistoricalPreparedRunConfigV1` 的可选字段
`gamma_resolution_policy` 默认 `source_time_only`。默认序列化不新增该字段，旧配置和旧检查点保持原义。
已批准的新 A 复核显式设置 `"gamma_resolution_policy": "qualified_public_consumption"`。
完整有效配置和 SIM 原输入日志共同保留本次选择；恢复沿原检查点，不重配已开始业务的会话。

新政策只使已成功公开消费的合格 Gamma 终态进入 SIM 的原自动结算链。它保留空平台源时间，
以该组实际消费的受信逻辑时刻作为最早经济生效依据。宿主继续先推进严格更早义务、消费同毫秒公开组，
再处理原隐藏桶；Gamma 等到执行前沿闭合后才应用经济，且仍等待原成交和返佣前驱。
公开盘口、成交和一般市场事实不因此驱动隐藏撮合。旧 WS 时序、保守 Maker、宿主延迟和精确经济公式保持。
窗口外终态、仅 closed、缺少合格结果、输入耗尽和冲突不能生成回款。本政策不提供策略主动赎回动作。


## 实际现金政策与历史结果边界

当前运行的 `admission_policy` 为 `ActualCashNoMarketLimit`，账户经济读取为 `account_economics_v3`。有效配置、结果的 `economics_version` 和耐久账本保存各自准确身份。现金汇总只有实际可用与实际预留；WCM 保留原归因金额与未知语义，但不证明同一交易路径能够脱离方向回款或外部入金执行。公共资金定义见 [shared-run](../shared-run/README.md#实际可用资金准入)。

当前配置解析不把 `DualCashNoMarketLimit` 转成新政策。准备脚本显式生成新政策配置；这只装配新运行，不能迁移旧账户、检查点或成绩。日恢复仍核对原候选配置、源码及检查点身份。旧 v2 结果和已停止研究保留生成时口径；新研究须使用独立输出和新政策身份，本次工程验证不恢复十二小时研究。
