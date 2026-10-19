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

JSON/TOML 解析、完整策略参数覆盖和显式 `false` 沿原命令层复用；每次 reuse 子进程新建 `ComplementaryPairV1` 及 `StrategyContinuityV2::new_account()`。准备模式不加载经营策略或创建账户。prepare/reuse 工具进度默认30秒、0关闭；reuse 打印打开和结束阶段，经营重放转接原可选 replay 进度，读到记录、应用输入、生成组的含义分别保持，未知工作量和 ETA 不编造。库新运行配置的进度默认关闭，输出故障不替代业务错误。

本段只用原有效工程夹具验证：一次成功发布、原 raw 移走后两次独立复用、完整旧新报告与初始化交付相等、每次原2000初始现金与空策略状态、两个市场连续账户及 hidden 成交、输出与材料字节隔离；另核失败不发布、版本/固定 prefix/缺失市场/实际 hidden 混接。原身份修复夹具证明固定前缀不会读取更晚损坏目录，原隐藏夹具证明同 derivation_position 下其他引用字段不符也失败。既有 Exit、已消费损坏、初始化和正常结束代表继续运行。没有真实十二分钟、首小时、研究或收益运行，没有新速度比例；PR235 的材料与统计445.431763861秒、PR228完整959.236639304秒保留各自旧实测身份，不能当作本段复用性能。旧大材料/失败现场不迁移、不回填描述、不重制；完整研究容量、风险参数和盈利验收仍未完成。

## 来源健康期限与未尝试失效

`run_historical_v1` 和 `run_prepared_historical_v1` 在传入策略显式选用 `uses_source_health` 时，开启原 DPT 回放的流进展投影，并将策略提供的健康期限加入同一事件调度器。期限独立于待发计算，即使无新公开组、计算未完成或完成后仍等待发送，也会按原逻辑时钟到期观察。没有固定节拍、自动重试或公共健康阈值；期限须严格晚于调用时刻，已到期条件应在当前资格判定中处理。

选用路径使用当前受信观察读取，仍保持同刻公开组先于发送、发送先于当前隐藏桶闭合及既有因果截止。计算期发现失效先通知策略，保留原计算完成边界；完成后等待发送时失效可在发现时刻重新计算。新计算继续支付配置中的计算开始延迟、计算耗时和发送等待，不等待新行情，不复用旧后缀或覆盖原冻结诊断。发出第一项后，第二项提交前失效也停止全部后缀；实际效果、未知义务及预留释放由原共享执行处理。

每个公开组 `next_group` 成功后，宿主借用该组调用 `qualify_intent_public`，先记录冻结动作公开依赖失效，再消费下一组。发送到期同毫秒也检查；首次失败复用原通知、计数与失效锁，恢复不复活旧决定。计算尚未完成时等原完成，已完成时在检测时刻安排最早重算；消费全部同毫秒公开组后仍按原发送／隐藏桶／账本／新计算顺序处理，新计算继续支付原配置时序。尚未分派或预留的 pending 没有资金释放，真正发送继续走原整批预留与完整资格路径。

到期的健康期限同时保留下一次重评需求。当前待发决定为正常 NoTrade 或所有意图仍合格时，原批次继续完成；完成后按原计算开始延迟重新计算，不等下一条行情，也不把正常批次记成失效。

只在选用时返回 `HistoricalRunReportV1::source_health`，包含实际资格调用次数、期限唤醒次数、失效批数及最近失效计算的原时序。它不保存逐决策账本；未发送失效的 `actual_sent_at_utc_ms` 为 `None`。旧消费者保留原公开组、回调和 `next_evaluation_at_utc_ms` 语义。

工程反例在 `tests/historical.rs`、`shared-run/tests/strategy_boundary.rs` 与 DPT 原测试入口覆盖静默期限、接收持续但事件滞后、已发前缀/未发后缀和无新行情重算。`source_health_finite_overhead_measurement` 是显式忽略的合成测量入口，以 `SOURCE_HEALTH_MEASUREMENT_ENABLED=0/1` 和已有 `SOURCE_HEALTH_MEASUREMENT_ROOT` 运行同一有限夹具；可用 `/usr/bin/time -v` 分进程记录 CPU 与峰值内存。其参数、输入和结果不构成正式健康阈值、历史断网证明或在线性能承诺。

D-0353 的失败报告保留待发原批次，以及动作批次在计算时、准入前的实际可用现金和账本序号。批次部分发送时复用原发送回执、失败项、请求效果及释放错误；不重建资金账本。没有实际发送的正常 NoTrade、零发送失败或失效批次，`actual_sent_at_utc_ms` 均为 `None`，计算完成与 `scheduled_send_at_utc_ms` 仍保留原意义。

## 原Parquet日作业、并行与从头运行

D-0311/D-0312 下的 `run_historical_days_v1` 接收 `HistoricalDayConfigV1` 日期列表和`StrategyV1` 工厂。每日期只创建一个公共 `PublicObservationCoreV1`。每候选在线程内创建自己的策略、SIM、NOE、Ledger、共享执行和时序；这些运行对象无需跨线程传递。默认日期并发按可用CPU数和最大候选数安排，调用方也可用 `run_historical_days_with_parallelism_v1` 明确日期并发。同日独立候选同时消费不可变公共组和完整隐藏桶；同一市场、同一账户的原业务因果顺序保持。

新日入口直接使用原每日清单和原Parquet，复用正式准入、解码及初始化选择。它先按文件并行读取必要窄列，形成身份／物理定位／来源顺序的最小索引。索引覆盖全部相关未读行，允许Footer时间统计缺失。索引不保存完整Canonical、盘口或观察录制；随后使用原文件／行组／组内行定位读取实际输入。同一天公共Core只归约一次，已创建各独立账户后才开始公共生产，候选不会从空Core重复归约公共前缀。组内不同市场观察及隐藏分市场计算可并行，收集后保留原顺序。该入口不调用旧全期 `HistoricalMergeV1::prepare`，不以完整公共／隐藏宽录制作为账户前置。

公共输入使用 `historical-source-head-v2`：来源内按原准入关系，六逻辑来源的头按原接收／来源／身份合流；Binance snapshot/update保持同逻辑深度流。隐藏PM先按epoch／源时间／序号／Canonical位置归约私有数量，再按全局源时间及原派生先后组成完整同刻桶。BBA、参考事实和未读下界均保留。新隐藏来源绑定为 `ClosedStreamingHiddenStepBucketV1`，排序版本 `hidden-reference-before-pm-source-v1`，同刻参考先于PM、PM保持原相对派生顺序；不伪造旧V1的全期参考数量或 `CommittedIngressPrefixV1` 存储引用。SIM沿同一隐藏算法接纳新表示；旧录制和旧V1继续读取。

日入口每次创建新账户并从原输入开始，不再写入或读取恢复检查点，不再复用完成引用，也不把来源顺序索引写成 `source-order.cbor`。来源排序索引仍在本次进程内服务实际回放。配置不再包含 `checkpoint_root`、`checkpoint_interval_seconds` 或两个 `checkpoint_every_*` 字段；旧配置须移除这些字段。运行使用新的输出路径，已有 SIM、NOE 和账本记录不能作为续跑入口。

有限范围按 `timestamp_received` 筛选研究材料，隐藏路径再按这些材料的平台源时间归约。例如一条事实的源时间为 00:59:59、接收时间为 01:00:01，日初一小时范围不含它，完整日包含它；两次运行的一小时处不天然具有相同隐藏状态。初始化材料也按其真实可见性接入，不提前放行。短范围可以检查给定材料与政策下的机制，不能默认作为整日精确业务前缀、日化收益或全天风险证据。范围内账户保持连续；另起中午会话是新账户，范围结束保留未决义务，不补未来结算或强平。

`progress_interval_seconds` 控制已处理公共输入及耗时输出，总量不能可靠确定时写 `unknown`。`producer_lead_groups` 保留原队列领先软目标及必要隐藏桶供料；公共组和隐藏桶仅保留活动消费者仍需的范围。

日入口在全部候选声明观察需求后，按其并集配置唯一公共 Core。`StrategyV1::observation_requirements` 默认要求完整 Binance 深度；complementary_pair 的两个 Binance 权重都为零时不要求数量。仍有完整消费者时保留完整数量。省略的数量明确表示为 `BinanceDepthObservationV1::NotMaterialized`，原未知仍是 `None`；来源、质量、直接锚点、依赖前沿、组位置和时间不变。日交付的深度事实保留原 `PublicFactBindingV1` 引用，隐藏输入仍沿原完整归约材料建立参考桶。PM 的逐档 bids/asks 不省略，active repair 继续读取真实 asks。

未变的盘口投影由原事实所有者共享，数量或其归因变化后产生新投影；滞后候选仍持有自己的旧组。各候选的局部批次只发布实际返回的消费前沿，队列回收仍以最慢活动候选为界。

日内经济采样只读取同一 Ledger 前缀的实际预留、未返还本金与 NAV。未返还本金并入 Ledger 原经济贡献索引，沿原事件更新；NAV 每次使用实际消费组重新估值，不把报价变化误作账户不变。完整账户报告仍在整点、结果和显式完整读取时生成。采样时刻、积分、回撤和未知值保持原语义。

`stage-timing` 特性沿原整点和结束输出累计计时，保留原业务阶段。新增运输诊断由进程启动时的 `BACKTEST_TRANSPORT_DIAGNOSTICS=1` 独立启用；默认跳过其读钟、计数和运输日志，原业务计时和进度继续工作。预算在每个原事件边界检查唯一原子标记；真实已返回游标只在补货、等待等交接边界发布。公开数据、隐藏数据和生产控制分别通知，结束及失败通知全部等待者。生产循环在正常结束、预算停止或失败时，输出公共生产的解码、准备、Core、物化和隐藏归约累计计时，以及深度状态大小。各线程的包含耗时不能相加当墙钟；这些数值不参与业务准入。短夹具成本不代表完整研究速度。

有限宿主在运行装配后、首条公共输入前，开启原记录器的逐次交接等待。每次尚未成功的 SIM 提交遇到原通道暂满时，只等待已有待交接组；不重试已成功的关闭、观察或经济步骤，不增加容量。后台写入失败仍返回原错误。其他使用原非阻塞调用和外层供料重试的消费者保留原行为；账户角色不用于推导宿主是否允许等待。此项线程调度方式不改变业务身份。

正常范围结束、失败和成功 `StrategyExit` 分别保留原原因。Exit沿原立即结束语义，不追加排空等待。合法日末未闭合订单、持仓、待发送计算和应收不会被强行关闭。SIM日入口显式选择 `SIMDAY01` 紧凑记录：固定版本标识后，每个原接纳组保存一个长度+CBOR帧，组顺序、输入序号、关闭依据和取消命中保持。头部保存原RunOpened及公共流、消费时序、隐藏输入集合和排序绑定；高频公共／隐藏记录保存真实驱动时刻及原输入／组／桶位置。独立重放须从原Parquet、初始化和同版本公共模块重建组ID、桶事实及PM派生材料，读取类型明确表示这一要求。请求、市场登记、结算和其他控制仍保留完整原业务载荷，NOE的 `#input` 关联不变。

日生产在原读取行数及消息载荷字节预算内，使用现有 Rayon 池并行准备连续普通 PM 输入。每项保存原读取位置的来源下界和初始化绑定，随后按原输入顺序提交；遇到元数据或终态边界时停止扩批，准备错误只在对应原位置返回。独立盘口侧可并行建立不可变投影，公共 Core 和隐藏状态仍分别有序推进。估值事实与盘口侧共享不可变材料；同值新来源、代际切换和质量变化仍使相关缓存失效。初始化来源在启动时完整选择，共享分类随原组发布一次，候选只统计自己实际收到的组。准备阶段的逐项经过时间可能并发重叠，不与批次墙钟相加；批次计数只表示真实工作量。

日后台不再物化并保存重复PM绑定／报告；临时归约材料随原接纳组释放。`max_group_bytes` 按实际布局的记录字节保持原上限；帧和头部不增加业务 input_sequence。移除续跑不等于关闭 SIM、NOE 和账本记录：它们仍参与原运行记录交接，本次保持原经济与反馈行为。

`HistoricalRunReportV1.day_economics` 在原事件边界读取同一次 `AccountEconomicsReadV3`，累计公共NAV最大回撤和资金占用。公共NAV回撤与baseline扣除未到账确认返佣后的风险权益／30%高水位分开。资金占用明确为原实际预留（含原在途经济占用）加未赎回成功成交剩余本金，以及已经济扣款尚未成功归约的成交本金；不是另建现金或利润账本。金额按原精确数值，时间积分单位为pUSD·毫秒。未知NAV采样保留 `Unavailable`，已观察最大回撤另列为局部证据；未知资金区间不补零。峰值、积分、采样前沿和未知量保留在本次候选成绩中。

`summarize_independent_historical_days_v1` 按策略配置、四项时序、准入、实现及规则身份汇总独立日结果。原确认净损益相加，平均日确认收益率为总确认净损益／各日原初始本金之和；另列扣除未到账确认返佣后的净结果。它保留最差每日公共NAV回撤、未闭合数、待消费NOE及Exit／未完成日数，不串接净值或算复利，不把未知净结果当零。确认结果仍以每日报告的原已消费前缀为界，未闭合日不冒充最终结算。

CLI使用 `--mode day --config <JSON或TOML> --progress-interval-seconds <秒>`。配置顶层提供 `rules`（原规则快照配置）、可选 `parallel_dates` 和 `days`。每个日期提供 `preparation`、`candidates` 和可选 `producer_lead_groups`。每个候选提供原独立宿主 `run: HistoricalPreparedRunConfigV1`、完整42字段 `strategy: ComplementaryPairConfigV3` 和固定 `request_namespace`；宿主时序、规则、账户和命名空间不由策略参数文件自动补齐。CLI沿原字段类型覆盖 `--strategy.<字段> <值>`，day模式把显式覆盖应用到各候选，记录完整有效参数和请求命名空间。每次调用从头运行；stdout输出 `days` 及上述 `summaries`。旧run／prepare／reuse入口保持原调用。

A的36份策略配置分布在对应五折（7/8/7/7/7），每份只含原43策略字段。调用方按获批比较窗和相应冻结配置装配日期及每候选每日独立2000pUSD账户；入口不自动把36候选用于所有日期，也不自动启动经营研究。

### 日运行的经济读取与物理批量

日入口仍在每个原事件边界读取经济事实并推进统计时间。Ledger 在原事实拥有方维护成交数量、未闭合前驱、剩余费用、投入本金和返佣贡献的增量汇总；报价变化仍重估孤腿，未知估值不替换为零。人工策略只使用这些正式费用归属，不再冻结全部历史成交；需要成交详情的其他策略默认仍取得完整详情。SharedExecution 的动作快照按活动身份和本次请求引用保留原订单及闭合腿的零承诺证据。待关联经济事实只在新反馈、Ledger 应用位置或相关公共市场变化时再次尝试，平台自动结算和原反馈可见顺序保持。

日 SIMDAY01 运行的 NOE 后台将多个逻辑批写入不可变 `noe_arrow_parquet_segment_v1` 段。原数字 `.parquet` 地址是逻辑容器地址，由原 `.manifest.json` 定位段和本批行范围；批内 ordinal、来源水位和发布顺序保持。段正文、footer 和文件同步只完成一次，引用只在对应 manifest 稳定后交付。单条／单批读取仅解码所需范围，显式顺序历史读取复用当前段的 Arrow 页；普通历史／PAPER 的写入保持原单批 Parquet，旧单批 Parquet 和 JSON 仍由同一 owner 读取。运行不保留全历史耐久引用列表，实际检查消费者按需枚举已发布材料，并接收读取错误。直接对逻辑地址取文件规模的可选展示可能为未知，应由读取方经 manifest 找到物理段；本组不修改独立展示模块。

日后台以 256 逻辑批、4096 行或 4 MiB 已编码正文作为排出目标，大批不会因此被拒绝。原逐市场结束和正常完成仍立即排出所需材料，顺序仍为 SIM 稳定、NOE 发布、Ledger 来源引用持久化。Ledger 内存归约保持实时，Parquet 小交接批合并为有界行组，原交接批的 flush 不等于 fsync；稳定点仍封口并同步。退出不补排空。

日入口在同一次窄列遍历中定位原行坐标和来源epoch，先利用可信行组范围排除窗口外内容；统计缺失时读取真实行。只有失序内容才整理，六来源合流及隐藏源时刻下界保持。深度输入一次准备原精确数值，归约仍保留代际、stitch、质量及前量检查；外部raw入口继续验证。

### 显式连续窗口研究汇总及预算退出

`run_historical_days_v1` 与默认CLI日入口仍按原经济规则运行。调用
`run_historical_days_with_options_v1` 或在day命令增加 `--research-summary` 才启用紧凑窗口汇总。
成功的 `calculate` 只分类一次：Continue空intents为NoTrade，Continue非空为动作，Exit单列。
三类之和是成功决定总数；健康失效、发送失败、期末pending不撤销原决定。计算报错不计NoTrade。
分类、小时游标和快照在本次候选内存中维护，返回原成绩报告；不再保存或恢复运行状态。

小时边界只读此前已完成事实，在下一小时第一项真实事件前采样。边界不进入事件队列，
边界同毫秒多组不提前计入上一小时，无事件小时不造行情或决定。每张快照保留边界与真实
`observation_cutoff`；NAV变化、确认净结果、未到账返佣分别列示。金额差额用原精确有理数相减，
原资金积分和累计回撤复用 `HistoricalDayEconomicsSummaryV1`。没有区间内极值记录，不能把累计最大回撤
相减称为小时最大回撤。跨小时保持原账户、策略记忆与高水位，不重新开账户。

Maker/Taker份数、金额、费读取原executed；笔数按Ledger原trade身份的经济应用/成功状态读取，
生命周期不重复计数。有SIM回执的提交请求、其声明订单腿和撤单请求分列；回执不等于平台接受。
无回执注册单列，是否已提交保持未知；未发送的冻结pending独立保留。撤单累计遍历registry原请求与完成事实，
不把 `cancellation_requests(None).response_counts` 的最近一次响应当全窗累计。

`--wall-budget-seconds 25200 --summary-reserve-seconds 900` 从CLI输入准备前开始计时。
保留原总墙钟上限和900秒收尾预留；预留现在只供成绩返回、输出及记录器结束使用。
到6h45m请求预算退出，各候选在原完整事件边界返回 `BudgetStop` 和实际已完成的成绩。
不执行RangeEnd尾部推进、不补发、不清仓，也不补齐未完成小时。已在同一时刻完成的Exit保留原终止原因。
不再输出 `published_checkpoint` 或候选 `reused` 字段。预算退出后的再次运行从头开始。

原策略Exit仍立即结束，报告终止时点与未闭合状态；退出后的小时不补零。RangeEnd、StrategyExit、
BudgetStop、Failure沿原report终止原因区分。CLI硬截止仅覆盖CLI自身；若随后运行机器汇总，必须放在
同一个外层7h期限内。硬截止可能发生在输入准备或结果输出过程中；未成功返回的成绩和stage-timing值为未知，不能续跑。
本轮具体准备、启动及证据边界见[六小时研究方案](../baseline-mm/docs/research/complementary-pair-six-hour-plan-20261012.md)。

## 新 A 复核显式选择 Gamma 政策

`HistoricalRunConfigV1` 和 `HistoricalPreparedRunConfigV1` 的可选字段
`gamma_resolution_policy` 默认 `source_time_only`。默认序列化不新增该字段，未选择新政策的配置保持原义。
已批准的新 A 复核显式设置 `"gamma_resolution_policy": "qualified_public_consumption"`。
完整有效配置和 SIM 原输入日志共同保留本次选择；已开始的业务会话不在运行中重新配置。

新政策只使已成功公开消费的合格 Gamma 终态进入 SIM 的原自动结算链。它保留空平台源时间，
以该组实际消费的受信逻辑时刻作为最早经济生效依据。宿主继续先推进严格更早义务、消费同毫秒公开组，
再处理原隐藏桶；Gamma 等到执行前沿闭合后才应用经济，且仍等待原成交和返佣前驱。
公开盘口、成交和一般市场事实不因此驱动隐藏撮合。旧 WS 时序、保守 Maker、宿主延迟和精确经济公式保持。
窗口外终态、仅 closed、缺少合格结果、输入耗尽和冲突不能生成回款。本政策不提供策略主动赎回动作。

## 实际现金政策与历史结果边界

当前运行的 `admission_policy` 为 `ActualCashNoMarketLimit`，账户经济读取为 `account_economics_v3`。有效配置、结果的 `economics_version` 和耐久账本保存各自准确身份。现金汇总只有实际可用与实际预留；WCM 保留原归因金额与未知语义，但不证明同一交易路径能够脱离方向回款或外部入金执行。公共资金定义见 [shared-run](../shared-run/README.md#实际可用资金准入)。

当前配置解析不把 `DualCashNoMarketLimit` 转成新政策。准备脚本显式生成新政策配置；这只装配新运行，不能迁移旧账户或成绩。旧 v2 结果和已停止研究保留生成时口径；新研究须使用独立输出和新政策身份。当前日入口不再读取旧检查点。

## 日研究失败与部分结果

D-0353 下，日入口保留第一个被该日 Hub 确认的原始错误和来源，不声称它是所有线程中最早发生的错误。已启动的候选和日期线程均收取返回结果。候选 `outcome` 区分 `Complete`、`OriginalFailure`、`CollateralStop` 和 `NoReport`；缺报告时 `report` 为 `null`。报告保留本次原观察截止、内存经济读取及耐久 NOE 前缀；失败后不补未来反馈或隐式排空。这些部分成绩不能用于续跑。

CLI 日模式失败时仍输出一次 JSON：`status="failed"`、`error`、`failure_source` 和已有 `days`，并非零退出。`scripts/run_research.sh` 在退出后提取这些已有结果，原 CLI 退出码优先于汇总错误；`scripts/summarize_research.py` 接受缺报告候选和空日期，不把部分前缀标成完整窗口。SIM 新随机序列的版本、种子及旧材料边界见 [SIM 历史消费合同](../sim-system/historical-consumption.md)。

取消续跑后，不再执行或统计检查点编码与发布。可选 `stage-timing` 保留实际运行阶段和低频汇总；同日并发候选的等待和阶段耗时不能相加当墙钟。真实研究窗口与进度输出频率由研究配置决定，有限工程测量不代表十二小时持续吞吐。

日内 Binance 深度材料采用 `BinanceDepthPositionReference`，事实位置、直接来源、两种时间和品质按同一归约步骤交付。完整来源谱系保留给录制／独立重放；日内状态引用使用 `PublicBusinessLineageV1::StateReference`，不生成深度内容 ID，也不把位置填入 `CanonicalEventId`。新日研究的观察表示使用 `market_business_observation_v2`，归约规则和行情顺序版本保持原定义。

所有候选的数量需求取并集。数量关闭时不展开完整盘口和累积来源证据；数量开启时只生成一次共享且不可变的数量投影。源时间计数索引在本次进程内随价格证据更新。接收时间沿原可见前沿并合并本次边界证据；源时间可以下降。

旧日检查点只能由对应旧程序解释，当前日入口不迁移或读取它们。旧录制的规范内容身份与重放材料保持其原格式。空材料不触发发布锁和队列通知，实际控制状态通知继续发送。

### candidate_v3 与正常资金拒绝

当前人工策略入口按[#337](https://github.com/slowchillchill/btc-updown-marketmaker/issues/337)使用 `ComplementaryPairConfigV3` 和 `candidate_v3`。完整 42 字段必须显式提供 `quote.initial_pair_price_reference={fair_value,best_bid}`；CLI 使用 `--strategy.quote.initial_pair_price_reference best_bid` 或 `fair_value` 覆盖。旧overfill字段、缺少新参考字段的旧配置和显式candidate_v1/candidate_v2运行描述明确失败，不能由CLI静默升级。历史研究包／结果保持原绑定；两份当前工程示例和价格/维护边界见[策略使用说明](../baseline-mm/README.md#初始双腿价格参考)。工程只验证能力，不自动启动 R3 或证明收益。

日入口的来源健康开、关两条分派路径均使用公共 `is_local_funds_rejection()` 分类。明确零发送的本地资金不足只停止本批，后续新公共观察仍可计算；不立即无限重试，也不伪装成来源健康失效。报告 `local_funds_rejected_batches`计数保留该差别，平台拒绝、成交与FAILED沿原SIM／NOE／Ledger和既有结果汇总表达；真实故障仍停止。

## 有限目标公共观察与 BLB 研究量导出

`export_observations` 是只读工程入口，供研究组取得原决策前缀下的事实。它复用原日输入、合法初始化和公共归约，不运行策略、意图提交、SIM 撮合或 Ledger。公共 `backtest` 库负责有限目标观察，`baseline-mm::blb_research` 负责纯研究量，CLI 组合两者。

构建与清单检查：

```bash
cargo build -p backtest-cli --bin export_observations --locked --offline
sha256sum target/debug/export_observations
target/debug/export_observations --check-targets --targets target-cutoffs.jsonl
```

`--check-targets` 只解析清单并按账户和公共流统计数量，不读取行情、不创建输出。清单沿用研究组冻结的 JSONL：每行的 `condition_id` 指定市场，`cutoff.receipt.group` 使用 `PublicGroupReferenceV1`。输出保留整条原始目标，包括 `account`、`request_id`、`decision_time`、`source`、原提交及原截止信息。相同组下的多个请求分别输出。

正式导出的 JSON 配置只有一个顶层字段 `preparations`，内容为各原公共流的完整 `HistoricalPreparationConfigV1` 数组。研究组应从原运行包提取完整对象，保留原 `header.input_stream_id`、清单、时间范围、初始化配置和读取参数；不用重造公共前缀。不同流身份分别归约。同一日期不同身份仍是不同流；本入口不证明它们可以共享前缀。`effective_header()` 沿现有实际历史排序版本装配，其他原流字段保持原值。

```bash
target/debug/export_observations \
  --config observation-export.json \
  --targets target-cutoffs.jsonl \
  --output observations.jsonl \
  --progress-interval-seconds 30 \
  --stop-file export.stop
```

输出路径必须尚不存在，避免覆盖既有研究材料。输出的每条 JSONL 按 `record_type` 区分：

- `stream_config`：该流的原准备配置及有效流头，便于核对输入来源和初始化。
- `target`：`public.target` 保存原目标；`matched_group` 保存实际命中引用；`observation` 复用 `MarketBusinessObservationV1`，包含 Binance 已维护档位、质量、来源时间、接收/依赖前沿、PM 双腿盘口及 tick、正式锚点和有效参考。`blb` 是该观察的研究量。
- `completion`：`status` 为 `complete` 或 `stopped`，包含完整落盘目标数和总目标数。这里的完成数包括已落盘的未知目标，不能作为成功命中数。写入错误时可能没有此记录；以标准错误中的最后完成数和已完整写入的目标行核对现场。

导出沿原人工策略运行启用来源进展的模式。相同 Binance 快照确认也可能形成只有来源进展的公共组，必须保留该组；FULL 与原数量省略投影在这一相同模式下保持完整组引用。

定位严格比较本次 `DayAppliedV1.applied.applied_prefix` 和关闭组的 `group_position`、`group_id`、`closed_at_input_position`，并核对观察本身的组与前缀。不会按毫秒合并、取邻近快照或从 Core 最新状态补值。无法命中的目标输出具体原因，如 `target_group_missing`、`input_stream_configuration_missing`、`input_stream_mismatch`、`exact_group_prefix_not_found`、`market_not_visible_at_group`、`observation_prefix_mismatch`；研究组应停止依赖这些未知目标的推断。JSON 损坏或正式组引用无法解析时直接返回行号错误。

BLB 定义版本为 `blb-visible-interval-v1`：M 为 Binance 最佳正量 bid/ask 的中间价；K 只取当时可见的 `official_anchor`。K 的 USD 数值沿批准口径投影到 USDT，不作汇率转换。M>K 取 bid 的 K≤p≤M，M<K 取 ask 的 M≤p≤K。`distance` 是 |M−K|；`visible_positive_levels` 保留逐档证据，`visible_level_count` 为真实正量档数 N，`visible_quantity_btc` 为 Q，`visible_notional_usdt` 为逐档 p×q 之和 V。

正式 K 缺失时主量为 `unknown`，不以 `effective_reference` 或临时参考代替。M=K 时状态为 `at_anchor`，N/Q/V 留空，避免混入一般零厚度分组。一般区间只输出可见量，`interval_coverage` 固定为 `unknown_no_coverage_evidence`：现有维护档位不提供交易所区间完整性证明。即使可见 N/Q/V 为零，也不能据此认定交易所区间零量。数值无法表示或计算溢出同样保留具体未知。

进度写入标准错误，报告阶段、当前流的已处理输入数、已落盘目标数、总目标数和耗时；输入总量未经证明时明确为未知。`--progress-interval-seconds 0` 关闭周期输出，保留阶段及最终状态。每条目标先完整序列化、写入换行并执行 `sync_data()`，成功后才增加完成数。

停止命令：

```bash
touch export.stop
# 或在另一终端对准确导出进程执行：
kill -TERM "$EXPORT_PID"
```

停止标志在输入边界检查；当前读取/归约调用会先返回。预先存在的标志会在读取行情前停止。进程只使用当前进程内的公共读取工作线程，不派生外部行情或策略作业；终止准确 PID 即终止这些线程。SIGINT/SIGTERM 硬中断可能留下末尾未完成行，该行不可使用。停止或失败均保留现场，不承诺续跑；新一轮使用新输出文件。

#328 的工程验收仅执行合成小夹具和冻结清单解析。它不代表已处理两小时真实行情、已重建 797 条观察或取得策略研究结果。工程交付后，由研究组 AGENT 沿 #327 组织准确计算包及研究预算。

### 启动期市场开仓范围与研究证据

候选 `run.entry_market_scope` 可配置 `start_offset_ms` 和 `end_margin_ms`（非负毫秒）。
输入仍处理声明的 `[S,E)`，仅允许 `S+a <= market.start < E-b` 的市场生成新配对请求。
不配置或配置为 `null` 时沿用原行为；显式 `{}` 或 `0/0` 按输入范围筛选市场开始时间。
只指定一端时，另一端先保留文件值，仍缺失则取零。CLI 的
`--entry-market-start-offset-ms`、`--entry-market-end-margin-ms` 覆盖文件；day 模式逐候选应用，
保留各候选的完整策略参数。端点不按首末消息移动，也不自动对齐市场长度。

配置在启动时冻结，日运行的 `effective_entry_market_scope` 保存政策、输入端点及准确开仓端点。
比较身份纳入政策、输入跨度和输入起点在 UTC 日内的位置；同形状不同日期可汇总。
范围只作用于真实新配对生成，旧单维护、撤单、修复、信号历史和原宿主计算时序继续执行。
例如四小时输入设置 `300000/0` 会包含最后五分钟开始的市场；不读取右端点之后的数据。

显式提供 `--research-evidence-root <尚不存在的目录>` 后，day 入口在同次公共日生产中生成
`public-<流序号>.jsonl`，并为每日期每候选生成 `candidate-<日期序号>-<候选序号>.jsonl`。
调用方须先准备证据根目录的父目录；证据根本身必须尚不存在。该选项独立申请完整 Binance 数量，
不修改策略 pressure 权重，也不增加交易门。每个日期在装配时绑定自己的观察器、缓存和文件，
不同日期的计算、编码和写出不竞争同一把锁。文件序号只用于定位；输入流身份以文件首行和完整组为准。

当前格式为 `minimal-research-v1`，首行 `format` 声明输入流和覆盖边界。它按 #335 / D-0364
定向替代 #332 的完整侧来源、完整来源进展和候选全市场重复投影要求。新格式不保存
`pm_side_sources`、完整 `SourceStreamProgressV1` 或非消费来源历史，不能仅凭这些文件恢复任意档位的
全部来源历史。公共核心、健康判定、原输入和策略读取继续保留完整语义。旧产物使用原读取器，不能静默混读。

公共记录的连接规则如下：

- `market`：首次出现时保存固定市场身份和窗口；后续状态仅引用市场ID。
- `group`：每个实际公共组保存完整 `PublicGroupReferenceV1`，包括输入流、应用前缀、组位置、组ID和
  闭合输入位置；组可见时钟复用原入口 `original_public_group_visible_at_utc_ms`（包括真实准入时间），来源事件时钟单独保存，缺失不伪造。
- `blb_state`：按市场保存 `state.blb` 的定义、状态、unknown、正式K、M、距离、方向、N/Q/V，
  以及正式锚点的值/质量/时刻、深度质量、直接来源接收与事件时刻。记录仅在消费值、质量或时刻变化时写出。
- `pm_state`：按市场保存两资产的最优正量买卖价量、两侧质量、直接 BBA 与 tick 的值/质量/时刻。
  能由这些事实恢复正量档与 BBO 对齐状态；未知和空值保持原含义，不用整侧来源替代 BBA 来源。
- `production_end`：原公共生产达到 EOF 的覆盖事实，不能据此宣告候选或整组完成。
- `original_pair_blb`：真实初始双腿请求才生成逐档 BLB 正文，保存准确冻结组与市场。相同组和市场由
  相关候选共享一次；它可以在生产 EOF 后写出，不能按文件相邻位置猜测所属公共组。

`blb_state` 和 `pm_state` 的版本键为 `(文件首行输入流, group_position, condition_id)`。
候选 `public_state[condition_id]` 只保存真实诊断涉及市场的 `input_stream_id`、`blb_group`、`pm_group`。
完整冻结组取该次 `input_cutoff.receipt.group`，与公共 `group` 逐字段相等连接。状态版本可以早于冻结组，
但不得更晚。全时段公共表仍覆盖无订单市场；不能据候选没有下单裁剪 H2。

来源年龄映射保持本轮口径：`depth_received_ms` 取 Binance depth 的 `direct_anchors` 中接收毫秒最大值；
`pm_up_bbo_received_ms` 和 `pm_down_bbo_received_ms` 分别取唯一 Up/Down 资产的
`best_bid_ask.received_ms`，该值来自 BBA 自身 `direct_anchors` 中接收毫秒最大值。没有值保持 unknown。
它们是该事实的最新直接来源接收年龄代理，不代表所有档位或全部依赖的信息年龄。
不能改用 bids/asks、valuation 或全流最新消息时刻。消费时刻减来源接收时刻得到年龄，时间流逝本身不复制状态。
来源事件时刻单独保留，不能替代可见时钟。

BLB 继续复用 `blb-visible-interval-v1`。同流深度只解析一次，各市场按自身正式K求值；普通状态不构造逐档副本，
真实请求从原组持有的冻结深度延迟生成逐档证据。缺失K、`at_anchor`、观测到零量和覆盖未知分别保留。
`interval_coverage=unknown_no_coverage_evidence` 表示维护档位没有区间完整性证明；零量不证明完整区间确实没有挂单。
临时参考不代替正式K。历史留在文件，仅当前状态和在途冻结引用驻留内存。

候选每次真实计算仍写 `calculation` 和 `scheduled`，无意图、负面和未知评估也保留。
人工策略诊断采用 `stable-diagnostics-v1`：`version` 是该候选文件内的稳定诊断版本，`definition` 仅变化时出现；
其中各市场的实际 `probability` 和完整 `urgency` 放在每次 `dynamic` 中。读取时按市场身份覆盖这两项，
即可恢复该次全部真实诊断。稳定部分先按原类型比较，避免每计算先编码完整 JSON 再去重。
不能以 choice 相同代替诊断相同。未执行分支的 `pair_entry.evaluated=false`、可选检查的 `null` 保持原意。
原意图、请求及腿序号继续保存在计算记录；公开/当前/派发失效、本地零提交、接纳/拒绝等记录保留。
成交、费用、撮合与确认时间继续按原身份连接 SIM、NOE 和 Ledger，研究文件不复制账本。

小型读取样例 `scripts/read_minimal_research.py` 按公共流和候选逐行恢复诊断及必要状态，只保留当前版本，
不会膨胀重建旧巨型来源对象：

```bash
python3 backtest/scripts/read_minimal_research.py <public-N.jsonl> <candidate-I-J.jsonl>
```

写出采用有限缓冲，空请求不访问公共写入器，普通计算不逐次刷新。真实请求与派发保留必要错误传播。
生产 EOF 不代表导出完成：全部相关候选完成最后请求后，宿主显式刷新公共输出；候选在成功返回前也显式刷新。
最终刷新失败进入原错误/部分结果路径，不能宣告整组成功；已有真实回执和业务报告仍保留其原事实。
正常完成以最终日结果和各账户报告为准；没有这些完成事实时，文件存在不代表完整。
人工强停可能丢失缓冲尾部，不要求 SIGKILL 排空，末尾不完整 JSON 行不可用，也不给中断结果补终局。
本入口仍不承诺当前 day 优雅排空、断点续跑或五分钟完整报告。
研究按获批工作量自然完成时，不提供 `--wall-budget-seconds` 或外层超时。
启用已有 `backtest/stage-timing` 构建特性时，公共观察与候选证据分别累计计算/编码/写出及锁等待的阶段耗时，
运行结束或已有整点输出；公共观察阶段嵌套于原生产交付阶段，不能重复相加或跨候选相加当墙钟。
进度由 `--progress-interval-seconds` 控制，工作总量或剩余时间未知时沿原输出标为未知。
