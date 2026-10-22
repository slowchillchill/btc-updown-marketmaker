# 有限历史消费

`contracts::SimCallerRoleV1::HistoricalBacktest` 使用原隐藏撮合路径，与训练和独立评价共享行情归约、请求和经济链。Maker模型由冻结规则及实际调用方选择。公开路径默认仅用于因果观测；显式 Gamma 政策的窄终态接纳见下文。其市场登记可以随着连续历史材料继续增加；未选择下述显式有限输入政策的训练和评价，登记冻结规则保持原有职责。

`PublicSimExecutionV1::create_new_historical` 使用 `RunDurabilityRoleV2::FiniteHistorical`。该角色表示实际有限记录前缀，不表示全部市场或经济义务已经闭合，不能签发 `CleanRun`。普通公共消费构造仍沿原持久角色运行。

针对该准确历史身份，真实隐藏或公开源 EOF 不要求每个市场具有完整终态，也不自动推进未来义务。完整隐藏桶引用仍由 DPT 提供。已知下一桶时间或已封口来源证明的空档允许原前缀驱动在宿主明确时点闭合；同毫秒未处理的隐藏桶不会被提前关闭。`FinitePublicConsumptionV1::finish_input_at` 在给定范围末毫秒领取范围内反馈并结束公开输入，不生成市场判定。宿主负责把该时点限制在其显式范围内。

`PublicGroupDecisionV1::cutoff` 保留原冻结观察。`sent_at_utc_ms` 返回请求实际使用的发送时刻，供共享分派结果读取。延迟发送不会把原观察时刻误报为发送时间，也不会在执行时重新选择策略价格。

正常历史范围结束可以调用原 `stop_and_drain` 排空已接纳记录；它不推进业务时间，不领取未来反馈，不产生 `RunSeal`。Exit 直接返回，不调用稳定、排空或完整完成。相关公共宿主与产物含义见 [backtest](../backtest/README.md)。

`PublicConsumptionStateV1::selected_frames_at_observation` 按原有序公开观察定位市场，直接生成所需帧。尚未结束市场身份只从成功消费的公共组登记，并在真实观察时间到期时移出工作集合；预开始市场仍保留。调用方提供的历史义务身份仍须在已消费观察中存在。隐藏预登记规则和领先生产者目录不补造公开输入。恢复从最后已消费组及其观察时间一次性重建身份集合，旧检查点字段保持可读。`frames` 与 `frames_at_observation` 保留完整接口。

## 历史 Maker 模型与过渡边界

新冻结配置的 `historical_backtest_model = "PUBLIC_TRADE_CONSERVATIVE_FIFO_V1"` 只切换
`HistoricalBacktest`。PAPER、ModelTrain及OfflineEvaluation继续使用
`L2_BOOK_DELTA_FIFO_ESTIMATE_V1`；后两者的CPU/GPU采用由所属事项协调。快照缺少新增字段时，
历史回测也保留原模型。`maker_model_version_for(caller_role)` 返回实际选择，backtest运行元数据保存该身份。
旧检查点与日志绑定原规则SHA；不能为续跑修改原文件或更换规则SHA。

历史新模型仅为存在本账户挂单的买价位保留Maker排队段。盘口净减只更新实际可执行深度，
不生成Maker成交，也不把未知撤单当作前方订单离开。新外部量排在既有账户单后。
SELL成交消费同资产的买队列；BUY成交映射到互补资产的买队列。当前正式SIM订单入口仅支持BUY。
一条规范成交事实按价格优先和FIFO消费一份有限数量，同一账户多单及互补映射不复制预算。
穿价成交也须扣除更好价位的可见外部量，不使用“穿价就全部成交”。

Maker排位与可执行盘口是不同事实。Required质量边界沿原逻辑清理实际深度，保留既有Maker排位；
重建量不会把账户提升到队头。只有账户单存续的价位保留排位；连续外部尾段合并，最后账户单移除后释放。
成交后的费用、返佣、最终性、私有反馈及SIM自动结算仍走原经济链，成交仍标为MakerInferred。

这些规则不证明真实FIFO、真实本账户成交或市场影响。未挂本账户单的更好价位使用当时可见深度；
成交先到、盘口后到时，该深度可能偏旧，导致保守地少分配量。不同规范记录缺少平台唯一身份时，
不能凭同量、邻接时间或互补价格去重。PAPER暂保留旧模型及原已知局限；其迟到成交、已确认撤单与资金
释放问题归后续#108/#109，本项不宣称解决在线因果。

旧研究材料需要显式生成新规则文件，才能开展新模型的历史复核。入口保留旧平台观测、观测时间、费率和
其他冻结规则，只替换本项Maker项目假设及当前实现身份；项目假设证据日期为本次决定日期，
不伪装成旧采集时已采用。新文件使用原不覆盖封存和历史装载入口，不访问网络。

```bash
cargo run -p sim-system --example derive_historical_maker_rules --locked --offline --   <旧规则JSON> <旧规则SHA256> <历史范围开始UTC毫秒> <新规则JSON> <当前实现身份JSON>
```

实现身份JSON沿原 `ImplementationIdentityV1`，含准确 `git_commit`、`source_identity_sha256`、
`cargo_lock_sha256`。命令输出新规则SHA及历史/PAPER各自模型身份。将新路径和SHA交给原backtest入口；
旧规则文件、旧结果和旧检查点原样保留。该命令不重置或授权研究预算。

## A历史显式绝对缺口模型（D-0374）

`PUBLIC_TRADE_CONSERVATIVE_FIFO_V2`只在新规则文件中显式采用。此派生入口
`derive_historical_maker_v2_snapshot_v3`只改变源快照的`historical_backtest_model`、对应模型证据和
实现身份。其他角色选择、原`model_version`、费用、tick、平台观测和全部经济/时序规则均保留源值。
静态缺省仍为V1；原五参数命令和第六参数角色数组仍沿原V1行为。

```bash
cargo run -p sim-system --example derive_historical_maker_rules --release --locked --offline -- \
  <源冻结规则JSON> <源SHA256> <输入因果开始UTC毫秒> <新规则JSON> <准确实现身份JSON> \
  PUBLIC_TRADE_CONSERVATIVE_FIFO_V2
```

命令完整输出HistoricalBacktest、ModelTrain、OfflineEvaluation及PAPER实际模型和新文件SHA。
把新路径及SHA交给原backtest的`rules`，新运行的实际角色为`HistoricalBacktest`。
新文件不覆盖旧文件，不重新查询平台。目标文件系统需支持原`renameat2(RENAME_NOREPLACE)`封存发布；
可在支持的本地工作目录生成，再将封存文件复制到归档并核验。不在运行中换模，不将旧检查点改绑新SHA。
保存/恢复继续使用原规则SHA和执行状态内的`maker_model`；旧字段缺省与旧日志身份不变。

同一会话、同一token和买价位，Q来自SIM当前实际可执行外部深度，排除账户overlay；E为该价
Maker队列全部External段剩余量。V2只补`max(Q-E,0)`到所有旧段之后：

- 原正向book分支`after>before`使用Q=after。净减、不变及纯查询不补量、不裁剪或推进Maker。
- 每张真实新订单余量入队前，按实际生效点Q补缺口，再追加该账户段。原准入、post-only、
  即时Taker和资金条件不变；不能获得生效前成交。

遍历只访问当前价位，由Q逐段扣External剩余量、至零短路，最坏O(k)，k为该价队列段数；
新单取Q还按原实际盘口段读取同价外部剩余量，m为这些实际段数，整次新入队最坏O(m+k)。
没有新增缓存或全市场扫描。真实盘口净减仍减少Taker可执行深度。Maker仅由原公共成交事实有限预算
推进；方向/互补、价格/FIFO、全部外部尾、撤单剩余量合并、末单清理及原经济链保持。
质量中断保留原Maker排位，合法恢复只补恢复Q与剩余E的缺口，不提升账户队位。

重复`100→10→100`且没有trade、可见上限不变时，E100不会重复累积90；真实增长到190仍补90。
盘口偏旧Q100但trade已使E10时，新单前会补90，因此V2可能比V1更保守。
它不保证E≤Q、真实FIFO、更多成交或收益。未知队前撤单、公开成交粒度/跨记录身份、偏旧更优
深度竞争及质量缺口期间排位仍是模型局限；跨价循环不变也不意味着各价分配结果不变。
这是新的项目估计，不将V1及旧结果重新标成违约缺陷。

正常SIM消费者的跨价见证见`run_v2`测试
`historical_maker_v2_cross_price_trade_spends_external_tail_before_lower_order`：高价0.6处E100、
账户A5、真实增长后外部尾90，低价0.5处账户B5。SELL110的预算先花高价外部100、分给A5，
再花高价外部尾5，不能错误给B5。断言沿真实订单生效和公共成交归约核对MakerInferred5份、
本金3、实际现金增量-3、低价未成交及剩余预留2.5；不从内部队列形状代替经济出口。


正式运行沿既有day CLI，配置与参数、范围、初始化、Gamma及结束政策由准确研究包冻结。
`--progress-interval-seconds`控制原阶段、输入工作量及耗时输出；未证明总量或剩余时间仍为未知。
该CLI没有优雅停止标志或自动续跑；对准确PID发送SIGTERM会终止本进程及其线程，可能丢失未落盘
队列和尾部记录。日布局即使文件名为`sim.jsonl`也使用`SIMDAY01`长度帧，不能按JSONL读取；
应按原读取器核对完整前缀。中断后仅完整已落盘记录可供现场核对，不构成完整报告或可恢复账户。
正常`RangeEnd`沿原记录排空及结果写出，不扩大为未来经济义务全部排空。
工程验证和构建不授权真实历史、策略研究、B/PAPER/LIVE或资金操作。

## 显式 Gamma 终态经济政策（D-0346）

`GammaResolutionPolicyV1` 默认 `SourceTimeOnly`，保留旧研究、旧检查点及其他消费者的含义。
已批准的新 A 复核显式选择 `QualifiedPublicConsumption`；SIM 在首个业务输入前将该选择记入原输入日志。

只有原 DPT 已通过终态资格、且 SIM 成功消费的无平台源时间 Gamma 元数据终态进入这一政策。
原来源、证据和 `source_event_at_utc_ms=None` 保持。SIM 保存实际消费该公开组的受信逻辑时刻作为
最早经济生效依据，不使用墙钟、市场截止、`closed_time` 或接收时间回填平台源时间。

成功消费先形成有限待处理终态。隐藏逻辑实际到达消费毫秒后，才将资格接入原平台市场状态；
不提前推进隐藏时间，不更新盘口或交易状态。原逐记录经济路径仍处理已知平台源时间的资格。
Gamma 只在执行前沿完全闭合消费毫秒后，沿原判定、订单释放、成交最终性／补偿和适用返佣前驱应用经济。
同毫秒原隐藏质量冲突沿原失效理由取消待处理终态。重复和恢复沿原身份与已应用状态避免重复支付。

自动回款反馈的 `resolution_source_event_at_utc_ms` 对 Gamma 为 `None`，另以
`resolution_public_consumed_at_utc_ms` 保存消费依据；WS 保持原整数源时间及原 JSON 形状。
NOE 忠实传递这些依据，Ledger 继续归约原经济事实。交易侧不发起 redeem。
这项政策是 SIM 对经济生效时点的假设，不证明真实平台兑付时间或始终比真实平台更晚。

## 新B显式公共环境（D-0367）

[采用决定 D-0367](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181#issuecomment-6266028183)
允许新运行以真实 `ModelTrain`、`HistoricalBacktest`、`OfflineEvaluation` 身份共用保守公开成交 Maker、
`QualifiedPublicConsumption` 和有限输入生命周期。角色描述实际用途；它不是启动训练、评价或数据作业的授权。

Maker 继续归冻结的 `L2QueueModelRuleV1`。`model_train_model`、`offline_evaluation_model` 为可选字段，
与原 `historical_backtest_model` 分别选择对应角色。缺字段仍使用源快照原 `model_version` 的旧模型；
PAPER 不读取新增选择。规则查询和实际执行从同一个有类型的选择取得结果，不按旧版本字符串猜测新算法。
派生新文件复用封存、历史装载和实现身份；不修改旧文件、其他平台规则及经济算法。

原五参数规则命令保持原历史用途。第六参数显式给出角色数组文件时，使用 `derive_public_maker_snapshot_v3`：

```json
["MODEL_TRAIN", "HISTORICAL_BACKTEST", "OFFLINE_EVALUATION"]
```

```bash
cargo run -p sim-system --example derive_historical_maker_rules --locked --offline -- \
  <源规则JSON> <源规则SHA256> <输入因果开始UTC毫秒> <新规则JSON> <实现身份JSON> <角色数组JSON>
```

输出列出三类离线角色及 PAPER 的实际 Maker 版本。不要只换 `caller_role` 而继续使用未采用新 Maker 的旧规则。

`PublicSimExecutionV1::configure_input_lifecycle_policy_v1(InputLifecyclePolicyV1::FiniteInput)`
在有限历史记录会话的首业务输入前绑定生命周期，沿原接纳输入日志及会话重建保存。
未配置的 `RoleDefault` 完整保留旧角色分支，PAPER 不采用这项政策。
新政策允许训练/独评沿真实输入追加登记；公开组和隐藏 step 桶的 EOF 不要求全部市场具有经济终态，
也不自动排空未来到期、反馈或付款。历史旧桶接口的既有完整性合同不因此扩展。
原完整组、隐藏桶前沿及范围内义务处理仍适用，实际来源错误不变成 EOF。

SIM 不保存第二个范围截止点。backtest 原调度只消费严格早于 `received_end_utc_ms` 的合法运行事件，
尾部使用 `finish_input_at(end-1)`。原始输入在范围内、延迟后交付越界时，不会因此获得公开消费回执。
有限 EOF 只结束输入前缀；未闭挂单、成交、预留和经济项继续如实保留。
`stop_and_drain` 仅排空已接纳记录，仍不生成 `CleanRun` 或 `RunSeal`。
这不定义账户重置、经济终值、模型记忆或学习恢复。

Gamma 直接复用前节接口及前驱规则。选择新 Maker 或有限生命周期不会隐式选择 Gamma；
新B调用方显式传入 `QualifiedPublicConsumption`，需要对照时也可显式传入 `SourceTimeOnly`。

backtest 泛型宿主在 `HistoricalSessionV1.caller_role` 传递实际身份，
`HistoricalSessionV1.input_lifecycle_policy` 传递显式生命周期；旧 JSON 缺省仍是 HistoricalBacktest/RoleDefault。
日入口及已准备入口的元数据记录实际角色、Maker版本及生命周期。
A CLI 仍使用原策略工厂；公共适配不把该命令变成 B 模型或训练程序。
合成环境调用及验收见 [公共环境夹具](../backtest/examples/finite-environment-validation.md)。

## 延迟采样与恢复版本

新运行在 `RunOpenedV2.latency_sampler_version` 记录
`xoshiro256plusplus_jump_multiply_high_empirical_rank_v2`。会话种子为32字节，配置使用64位十六进制文本，
开户时解析一次。每个会话使用锁定 `rand_xoshiro 0.7.0` 的 `Xoshiro256PlusPlus`；请求发出、
直接响应、生命周期反馈分别使用原始序列、一次jump、两次jump后的序列。全零种子沿上游构造语义转换，
不截断、补零或计算哈希。经验直方图与整数乘法取高位的秩映射保持。

请求编号仅负责身份及重送关联。API接收前拒绝不推进序列，首次登记请求才提交新状态，
重复请求复用原回执。平台接纳后拒单仍是已登记的请求。原生命周期事件生成一次才抽样一次，
一个事件产生多条NOE记录时复用其接收时间。不同策略后续请求可以自然分叉。
显式结算的真实发送或查询沿同一发出域；无新发送的重送不推进。A继续使用平台自动结算。

随机状态随原会话进入日志重建和检查点恢复；不因显示名称、目录或仅延长输入范围而重置。
旧任意字符串种子和旧采样日志、检查点由旧程序解释，新程序不静默迁移。
当前 backtest 日入口每次创建新账户并从原输入开始，不保存或读取整日恢复检查点，也不复用完成引用。
上述随机恢复只描述底层组件能力，不表示历史日宿主可以续跑；旧发布点须保留原版本解释。
