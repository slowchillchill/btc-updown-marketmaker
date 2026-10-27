# 新 B 模型消费者

任务入口为 [#181](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181)。
本目录实现真实模型消费者，以及[D-0371](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181#issuecomment-6287942857)采用的公共经济目标、整组PPO/TBPTT学习和事务恢复。
学习接口、原工程合成成本及边界见[学习交付说明](LEARNING.md)。#377 的显式执行模式、缓存、冷恢复与有界验证入口见[性能候选说明](PERFORMANCE.md)。后继已分别获准并完成[最小真实链验证](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181#issuecomment-6305408177)和[有限两臂机制研究](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181#issuecomment-6339458665)，原工程合成证据与各轮真实研究分别解释。
两臂研究六个作业正常完成，共49,088次主GRU调用、4次更新、2次提交和2次导出；本窗信用全为0，无`alpha1`优势依据，跨运行微差及冻结K/J、请求原因投影的局限仍保留。独立审查仅接收实际运行及有限证据，任务4整体验收、完整PPO正确性、正式训练、独立评价、全日容量和盈利均未因此成立；模型及专项奖励机制尚未采用。当前停点、后继数据用途和预算由#181维护，已结束预算不转为新作业。
模型、订单和修复政策采用 [D-0370](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181#issuecomment-6272123144)，
时序沿用 [D-0369](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181#issuecomment-6270139404)。
合成权重仅证明接线，不证明模型学会交易、盈利、应对积压或达到100ms容量。

## 公共调用链

`原Parquet → backtest原日供料/公共归约/统计 → 原100ms FIFO → StrategyInputV1 → ModelConsumer → ReservedIntentV1 → 原Bridge/SIM/NOE/Ledger → 后续合法反馈`。

Rust保持精确动作、预留及公共环境。Python常驻进程持有网络、每账户隐藏状态、采样状态和本次动作临时上下文；显式学习入口还持有联合optimizer与组事务候选。
公共模块不依赖模型。合成夹具和显式原日配置程序调用同一消费者；没有影子撮合、现金或统计。
同日多个账户沿原日生产者共享公共页，各账户保留自己的经济状态。模型传输是当次必要投影，不保存完整行情或特征副本。

## 输入、记忆与动作

`b-public-token-log1p-v1`投影包含F0报价、深度、参考、公开状态/规则及证据，公共F1—F9全部字段、参数、质量/缺失，
当前现金、终局净差、经济暂记份额、原承诺残量和原订单反馈。它不传原组事件载荷或全账本。
市场截止和处理开始账户截止分开。帧年龄为处理开始减市场截止，等待为处理开始减最早开始；最早开始仍是配置语义，
不是实测帧生成耗时。原来源接收年龄直接从证据派生，不刷新证据时间。

数值使用固定`sign(x)*log1p(abs(x))`后转FP32，不拟合数据或裁剪。
每个标量附字段桶、类别桶及数值/空值/布尔/文字类别。稳定FNV-1a桶的大小由编码配置及模型共同绑定；
哈希和FP32都是有损神经投影，可能发生类别碰撞或大数精度损失。真实零、缺失和未物化保留不同类别输入。
资格、价格、数量、资金及请求身份始终使用Rust原精确事实，不能从有损张量反推其支持域。
变长行情字段先经共享标量编码器和均值池化，再合并市场及账户编码进入单向GRU。

账户主GRU每次实际调用只前向一次。一次输出供全部条件头复用。
动作内部的GRUCell只更新该次条件解码临时上下文，不写账户隐藏状态。
每账户隐藏/采样状态独立；市场切换不重置。原宿主串行消费该账户全部合法帧，独立就绪账户可合批；
批大小是上限，不等待凑满，不把物理等待变成SIM延迟。

顶层选择NoTrade或一个意图。配对的互补两腿等量，各腿依次选择`post_only`、完整tick价格，最后选择共同份额。
修复选择对应风险方向单腿GTC及其属性/价格/份额。NoTrade不提交意图。
没有模型撤单、SELL、自动撤换或宿主代选。预开始市场按已可见身份和公开交易状态处理；
新配对按市场开始时刻的UTC允许集合00:05—23:50与声明输入短窗相交。
原day展开`effective_entry_market_scope`后交消费者；默认零起点偏移和零终点扣除，窗外预取next市场不能新配对。
修复及旧单处理继续消费原观察，不删除短窗首尾。
未知交易状态不伪造“已关闭”；实际SIM仍可拒绝请求。旧帧动作按真实到达时刻执行，不回写旧时刻。

修复资格使用公共`repair_gap_shares`的终局净差和Registry可信`remaining_target_shares`，
后者已经包含未终局贡献，不能再加经济暂记成功池。对应侧的可达净差为正才使互补侧有资格。
已赎回持仓不再制造资格。未知残量保留未知，不据未知产生资格；已知事实能独立证明风险时仍可取得资格。
资格读取排除原订单数量已低于同一不可变公开MOS的必拒请求；原10份单剩0.01仍可能成交，不按残量套MOS。
这项风险证明不影响Actor选择低于MOS的新动作及原真实拒绝。资格不生成数量上限：允许超过缺口、反向残余和旧单重叠；无风险不得主动单边发起。

数量以centishares（shares×100）精确编码。令`M=2^96-1`，唯一排名依次覆盖：

- `1..M`中的每个整数；
- `(M,10M]`中的每个10倍数；
- `(10M,100M]`中的每个100倍数。

这些正好是Decimal两位及以下小数可以表达的全部正份额，不是稀疏数量表或u64上限。
Rust按原`limit_buy_cost_v1`逐腿计算本金/费用并分别舍入，二分可用现金下的完整排名域；
模型按整数前缀条件头选择唯一排名，不枚举全域，不在采样后吸附或缩量。
平台最低份数仍为输入及原SIM拒绝规则，不变成Actor最低量掩码。
实际路径的每个条件头保存原logits、掩码、选择及log_probability，联合值为实际路径之和。
之后资金或行情变化的失败保留原公共结果，不转换成NoTrade。

完整排名指SDK份额表示，不保证每个值都能通过全部公共金额边界。已有极值限制：
某些可负担的大份额在SDK金额乘法中发生原Decimal精度收缩，公共BigUint成本仍精确，
但原Ledger无法将该成本字符串解析为Decimal。消费者不舍入该金额，也不把它改成NoTrade；
原SharedExecution返回原Ledger错误并保留失败。它是公共数值能力限制，不能称为已学到的正常平台拒绝。
本段小额合成验证不证明该极值可执行；无签名或LIVE订单验证。

## 构建、依赖和显式调用

Rust包加入根workspace，复用根`Cargo.lock`。
`requirements-cu128-py312-linux.lock`是CPython3.12/Linux x86_64的全依赖新锁，
所有制品使用官方精确URL和SHA-256。新venv安装时使用`pip install --require-hashes -r <锁>`；
Torch不需要NumPy完成此推理路径。CUDA运行依赖目标主机兼容驱动和GPU，不以镜像预装替代项目锁。
所有缓存、虚拟环境、临时目录和构建目标按执行主机要求放在仓库外数据盘。

```bash
cargo build -p model-train --release --bins --examples --locked --offline -j <可用编译并行数>
<python> <model_worker.py> initialize <初始化.json> <新模型.pt>
<model_consumer> print-config <原日消费者配置.json>
<model_consumer> run <原日消费者配置.json> <新summary.json>
```

`ConsumerRunConfig`直接组合原`HistoricalDayConfigV1`、明确规则来源和显式worker配置。
新B已接入CURRENT_QUERY/显式HISTORICAL_AS_OF/RECOVERY，业务前一次解析并同句柄进入采集和公共再生；完整配置、新/恢复命令见[RULES.md](RULES.md)。
每个原候选的`strategy.effective_parameters.model_consumer`接受`ConsumerConfig`：
`account_key`、`sampling_seed`、`request_namespace`、`progress_every`。账户键必须在同一worker中唯一；
显式新运行初始化自己的隐藏状态，不承诺跨日或中断恢复。
`worker`指定Python、脚本、模型、设备、线程、最大合批数及编码桶。
入口使用准确ModelTrain角色、fixed100ms_fifo及显式公共统计配置；A工厂和默认时序保持原样。
`print-config`只解析和打印，不打开行情、不启动模型。
已有支持参数的改变不要求修改源码或重新编译；模型形状/编码桶改变需产生对应的新模型文件。

## 推理合成接口与历史成本边界

当前`examples/model-initialization.json`明确48宽、1层Actor/Critic的423856参数，种子1810371只供工程验证。
当前模型格式为`b-gru-actor-critic-gtc-v2`；PR347的旧纯Actor权重不含价值头，不能作为此格式的检查点。
`controlled_synthetic_v1`通过初始化条件头偏置增加特定价格/数量被抽到的机会；
它仍运行真实GRU和条件网络，不按时刻或账户脚本发单。下列`model_consumer_fixture run`只推理；学习必须使用显式`model_learner`入口。
`examples/model-fixture-config.json`声明两账户、现金2000/0.02、独立采样种子及2000ms成交终局延迟。
其12秒合成Parquet短窗保留原公开/隐藏时差、部分Maker成交、原单及EOF义务。
新增参考通过既有合成来源生成器进入原流，不另算统计。

```bash
<model_consumer_fixture> prepare <合成配置.json> <新准备目录>
<model_consumer_fixture> run <合成配置.json> <新输出目录>
<python> <run_fixture_budget.py> --wall-seconds 300 --evidence <新测量目录> -- \
  <model_consumer_fixture> run <合成配置.json> <新输出目录>
```

准备模式只生成合成材料及完整消费者配置，不前向。运行模式的计时包含材料准备和完整公共链。
PR347历史推理验收的两个合成账户功能批总计最多256次真实前向；另做同短窗1/10独立运行成本档，各档最多5分钟墙钟。
监督器到限保留失败/部分输出并停止本次工程作业；这个限制不进入策略、服务或SIM时间。
不自动扩大工作量或重跑凑通过。编译和依赖成本另列并纳入交付总成本。

worker记录真实CUDA同步后的前向/条件解码时间、主GRU调用/批次、动作头数、应用张量H2D/D2H字节及次数、
模型装载字节和PyTorch显存峰。应用字节不等于驱动全部PCIe流量。
Rust逐次计编码、完整往返和精确域/意图构造；worker在实际收发缓冲边界计IPC完整字节/往返次数，包含协议封装和换行；原日结果继续记录公共准备、供料和执行经济成本。
监督器按50ms采样父Rust及Python进程组RSS，分别报告进程峰和共时观测峰；采样峰是下界。
输出保存动作、必要反馈与概率路径，不保存完整观察或特征时间序列。

真实数据扫描、账户回放、训练、独立评价、PAPER/LIVE和部署需各自授权。
公共经济目标、整组PPO/TBPTT学习、日账户有限终值及整组学习事务恢复已实现，详见[学习交付说明](LEARNING.md)；规则加载与恢复见[规则接口](RULES.md)。这不提供准确日内账户续跑；有限EOF不排空范围外义务，不强平或补到账。


## PR347历史推理工程证据的适用范围

以下成本绑定PR347原纯Actor产物，不是当前Actor/Critic学习成本；当前证据见[学习交付说明](LEARNING.md)。

本次合成功能累计256次真实主GRU前向，不再补跑。原236次整链记录暴露了必拒本地请求误算风险的资格偏差；
修正后低现金定向实际6次前向，看到5帧必拒承诺仍可见但可信风险为零，修复资格关闭，新Pair仍可选。
短窗在900ms计算尚未结束时到达有限EOF，原报告保留6帧队列；不能把它改写成队列已经排空。
原功能批验证独立100ms帧、双截止、真实Maker/Taker反馈、部分成交及未闭义务。
当前源码另有纯Rust原请求最小量/合法微小残量、来源条件隔离和终局净差反例。

批处理诊断分两次保留原身份。首次8前向的完整状态差`0.00014656782150267167`超过预设`1e-5`，
断言失败，且断言前未落完整向量。这项证据不足保留为缺口。最后6前向在断言前保存完整向量、概率头及运行设置：
账户0推进时，账户1完整隐藏状态、调用数和RNG摘要完全不变；批大小达到2。
批/逐个状态最大差`0.00010640174150466919`仍超过同一数值检查，结果字段明确为false。
该阈值是工程诊断期望，不是D-0370业务政策。没有改精度或放宽阈值以产生通过结果。
[PyTorch 2.8数值说明](https://docs.pytorch.org/docs/2.8/notes/numerical_accuracy.html#batched-computations-or-slice-computations)
说明批/逐个浮点结果可有差异；它不自动证明本实现正确。身份隔离证据与数值一致性检查分开交独审。
运行使用FP32、matmul TF32=false、cuDNN TF32=true，未修改默认精度设置。

同一12秒合成窗口的完整成本为：

| 成本档 | 独立环境运行 | 主GRU前向 | 外部进程墙钟 | 有效前向/秒 | 完整输出字节 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1次 | 1 | 118 | 6.346176s | 18.594 | 4,299,538 |
| 10次 | 10 | 1180 | 45.470733s | 25.951 | 42,939,480 |

10次档在一个常驻worker中使用10个独立账户/环境及初始记忆，模型只装载一次；不是10次冷启动。
各档均仅执行一次，没有到限，模型准备、公共供料、编码、头、IPC、精确解码、执行经济与输出都在墙钟内。
单次档编码累计1.147s、模型完整IPC往返2.657s、精确域/意图构造0.267s；10次档相应为10.488s、25.494s、2.523s。
这些是嵌套阶段，不能相加为总墙钟。公共准备/运行其余成本保留在外部总量，不以GRU算子替代。
单次/10次档父Rust峰80,348/98,228KiB，Python峰1,004,728/1,004,944KiB，
共时50ms采样峰1,050,460/1,076,392KiB；这些观测峰不保证真实数据峰。
PyTorch分配峰约18.7MB、缓存峰27.3MB，不等于驱动全显存。
IPC请求/响应为5,912,429/355,720B和59,069,624/3,545,186B；
输入张量H2D为4,205,808/42,058,080B，每个进程另有模型装载1,695,228B。
完整条件头带有多次GPU/CPU同步；没有把这些成本隐藏为单次GRU时间。

首次未缓冲IPC曾产生3,480,790次写调用；整行缓冲后成本1/10档原父进程写调用为6,933/68,790。
前后工作量不同，这不是受控加速倍数。依赖取得347.3s、安装64.5s和首次失败构建84.889s单列，
修正后构建及定向检查保留原日志；阶段可重叠，不能简单相加冒充整轮墙钟。
本段材料不证明真实密集行情、100ms整日容量、训练收敛或盈利。

有限前缀零更新诊断见 [诊断入口说明](DIAGNOSTIC.md)；真实数据与模型补证仍须另行签发。

当前B公共输入需求统一在 `src/public_input.rs`：FULL数量，不选用完整source_progress。
采集与正式学习再生共用该定义；必要F9仍来自原事实和统计。新轨迹版本为
`learning-trajectory-v2`，旧学习事务不会自动迁移，详见[学习接口](LEARNING.md#公共输入与旧身份)。
[当前需求诊断示例](examples/diagnostic-current-consumer.json)只提供未执行入口，不构成真实作业授权。

## 首观察物理耗时

`run_consumer` 和 `run_learning_job` 的结果新增 `first_observation`：对象中的
`elapsed_seconds` 是从本次组合入口、配置绑定之前的单调物理起点，到第一次合法
`StrategyInputV1` 进入 `ModelConsumer::decide` 的秒数。终点在编码和worker请求之前；
即使后续编码或模型请求失败，这次交付也已发生。对象同时保留原 `account_key`、`call`、
`processing_start_utc_ms` 和紧凑 `market_frame` 引用，不保存完整观察。

这一点值包含本次规则打开、worker握手及输入准备，learner还包含采集之前的 `learn_open`。
CLI参数/JSON读取发生在组合入口之前，外部进程启动、监督和准备也不包含；这些成本仍用外部完整墙钟。
原 `elapsed_seconds` 和吞吐分母保持原区间：consumer原起点在公共输入配置绑定之后，
learner原起点在配置复制/绑定之后，learner内部collection原计时另从采集装配前开始。
新首事件始终共用顶层起点，不把这些原区间改名或拼接。
低层 `run_with_worker(..., started)` 使用调用者显式给定的 `started`，不能推断其之前的启动成本。

同一次运行的所有日期和账户共享一份 `OnceLock`；先进入首事件初始化的实际调用记录一次，
不按配置顺序或完成顺序挑选。它不承诺并发调度的确定性。后续调用只读取初始化状态，
本观测不额外逐帧取时、复制观察或写日志；初次记录才取时、构造紧凑对象并向原stderr进度流写一条
`{"event":"first_observation","first_observation":{...}}`。原summary/attempt报告携带同一快照。
没有调用（包括N0）、本次仅复用完成采集、或已消费确认时，本次字段为 `null`，不能把0当作实测。
历史 `completed-collection.json.summary.first_observation` 若存在，仍只属于原采集；本次不复制它。
consumer无正常summary的错误可查已经输出的一次进度事件；learner现有failed_collection、
failed_learning、commit_reply_failed及成功/N0落盘报告按本次实际路径保留该字段。
在事件之前退出时没有首事件；强杀或原报告写入失败不保证存在最终报告。

此观测不进入配置、组、轨迹或checkpoint身份，不改SIM时间、FIFO、双截止、动作或经济事实，
不新增超时/停止门，也不依赖 `stage-timing` feature。额外成本是一份共享记录、每次调用一次初始化状态读取，
以及首次紧凑JSON/日志和报告中的常数大小引用；未把这一开销外推为完整负载容量或100ms服务保证。
