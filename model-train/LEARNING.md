# 新 B 整组学习接口

本段交付[#181](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181)任务3及任务2步骤7，采用[D-0371](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181#issuecomment-6287942857)。
D-0369全部帧FIFO和双截止、D-0370模型1/订单1/修复3继续生效。
规则来源后继已接入[CURRENT_QUERY与恢复接口](RULES.md)，同一冻结身份进入采集及公共再生。
本段完成工程合成验证；真实行情扫描、账户回放、真实数据学习、独立评价及PAPER/LIVE仍需相应授权。

## 公共经济与尾部

`Ledger.confirmed_economics_v1 → SharedExecution.learning_account_at_v1 → ModelConsumer私有轨迹 → model_learner冻结目标 → Python联合optimizer`。
原Parquet日供料、公共组和F0—F9、Bridge/SIM/NOE/Ledger继续承担原职责。
公共核心不读取模型参数，不实现训练奖励。B只组合原确认分量：

`K = Gpair + RM − FT + RT_effective − Fother − Lorphan`。

Gpair从原累计配对份额和成本池读取；赎回后保留。Lorphan只计判定后的净亏损，不奖方向盈利。
费用单列，不从份额扣费。返佣资格直接复用原driver已闭经济前缀的日权益读取，确认权益一次，付款不再计奖、不提前成为现金。
同市场/同日聚合身份稳定，累计值替换后取差，允许负修正。未变分量缓存复用，轨迹只在实际变化时写确认分量。
旧A默认不请求这份读取。原`MarketFundsNumbersV1.total()`修正了platform_pending本金/费重复相加，原分项语义保持。

有限终值为`Jend=K(PH)−Uposition−Uobligation`。未判定剩余持仓与已应用但未终局本金归Uposition。
原Ledger必要读取的principal+local_principal形成未完成义务本金；原预留→平台待经济→应用归约已把已应用本金从此项扣除。
缺成交详情时，原Lifecycle.applied_fills仍有按trade/path身份的精确本金和费用，直接补到持仓风险与同市场FT聚合；详情到达后原池承担同一值，补偿则撤回。
本段删除了多余的Registry逐订单金额推导。费用不充当第二份本金。
必要负成本没有合法金额/上界时标签待定。仅有待处理NOE计数不使标签待定，未知正权益不提前授信用。

每个日账户显式声明经营截止E和证据截止H，H≥E。E后不调用模型、不发送新意图，不补NoTrade、不推进GRU。
跨E冻结未发送计算、未调用FIFO及原旧单义务保留；旧已发送义务继续沿原引擎到H，H外不排空。
完整UTC日及市场开始集合仍按原声明处理，短窗不机械删首尾。
`learning_account_start`保留Pstart；`unit-objectives.json`分别报告Pstart、首调用前Bpre、Jend−Pstart及N0。
N0不造Actor样本。消费者装配时即建立空轨迹，完成采集读取以原实际调用计数核对，不把丢失/截断的非空轨迹当N0。
全N0组报告Pstart/J/N0并跳过学习更新和提交；混合组保留零调用单元经营报告，只让真实样本进入Actor。实际调用最后一条转移吸收E—H尾部和有限终值。

## 轨迹与学习

轨迹只保存必要私有投影、账户/市场双前缀、实际SIM开始时刻、原动作、冻结条件域/完整数量排名、版本和行为logp/V。
公共市场输入按原Parquet/公共组/查询截止再生；账户私有输入直接读原轨迹，不重跑账户或SIM。
同一日内仅统计参数和公共交付时序均相同的身份共享一次再生；异构配置各自保留原采集语义，不套用首账户参数。
显式typed帧访问者保留合法可见的预开始市场，旧compact导出保持其原窗口筛选。
不保存完整公共行情、观察或特征副本，不增加运行时逐帧/配置哈希证明。

`r_i=K_(i+1)−K_i`，末条为`Jend−K_last`；Critic监督为完整剩余`Jend−K_i`。
γ=1；GAE的λ为`exp(−实际SIM开始间隔秒/300)`，不使用训练墙钟或原市场截止间隔。
old joint logp、old V、r/A/G在本组冻结。首次生成标签后落盘；初次与恢复均读取同一不可变标签快照，
不重新计算浮点值并以逐位相等作为恢复门。

单向GRU共享给Actor/Critic，Critic梯度反传共享编码器和GRU，一个Adam联合optimizer。
每轮从合法起点以当前参数顺序重建，精确teacher-forcing原动作/条件域，完整joint logp求PPO ratio/clip。
条件头不另行clip；单值域logp=0仍推进原动作条件上下文。主GRU每条真实样本只重算一次。
Q块反传后hidden数值连续、梯度detach，块间不step，整轮末恰好1次联合step，2轮后提交整组。
未采用熵奖励、KL停机、逐字段clip或旧B超参数。

轨迹页行数和缓存页数控制实际LRU私有页池；仅保留行偏移及小型标签/时间索引。
`training_sequence_batch`控制就绪独立账户的RPC请求合批，当前Python仍逐序列计算可微GRU，
不宣称已实现密集训练序列GPU向量化。公共再生每轮重读原流，其准备、扫描及归约成本计入完整进程成本。
公共再生进度复用当日首候选的`model_consumer.progress_every`，按应用组计数；0关闭进度，避免逐组无用途日志。

## 参数与调用

当前工程配置为48宽×1层、423856参数、FP32，初始化seed1810371。
编码桶4096/4096，torch线程4、就绪批上限2；这些是显式工程参数，不是正式训练容量或账户数政策。
[完整学习参数示例](examples/learning-parameters.json)：Adam lr0.0003、ε0.15、cV1、共同金额尺度2000、τ300秒、Q4、2轮、私有页4行×2页、序列请求批2。
Adam的其余生效参数为betas=(0.9,0.999)、eps=1e-8、weight_decay=0、amsgrad=false、maximize=false、foreach=None、capturable=false、differentiable=false、fused=None（锁定PyTorch2.8默认值，checkpoint原param_groups保留）。
cV使用half MSE；Actor优势与Critic金额使用同一金额尺度。
显式参数可以在仓库外配置，不要求改源码或重编译。形状/编码桶改变须使用对应的新模型。
当前格式`b-gru-actor-critic-gtc-v2`，不能加载PR347无价值头的纯Actor权重。

```bash
cargo build -p model-train --release --bins --examples --locked --offline -j <可用并行数>
<python> <model_worker.py> initialize <初始化.json> <新模型.pt>
<model_learner> print-config <LearningJobConfig.json>
<model_learner> run <LearningJobConfig.json>
```

`LearningJobConfig`包含完整原`ConsumerRunConfig`、`parameters`、`group_id`、`output_directory`、
`active_end_utc_ms`、`audit_before_update`、`gradient_checks`、`fault`和独立`attempt_report`路径。
单日job用`active_end_utc_ms`；多日job须按`consumer.days`顺序显式提供`active_end_by_day_utc_ms`，不能把同一绝对E套给后一天。
也可逐单日job顺序使用同一checkpoint目录。每组新账户必须有自己的原运行身份，不能借此宣称日内环境续接。
`audit_before_update`和`gradient_checks`是显式有成本的验收请求；正常学习关闭，所有网络调用仍计入所签预算。
`print-config`只解析打印，不打开行情或启动模型。
原fixture基础consumer配置仍可写learning:null及“无学习”，表示供料模板；learner在实际collection配置中注入E/H/轨迹/组身份，并准确更新purpose。
旧执行证据保留原基础配置和当时注入配置，不改写为新版本。

## 事务恢复

`checkpoint.pt`的单文件原子替换是整组提交边界，包含模型、optimizer、Torch/CUDA RNG、必要账户采样RNG、版本和已消费组身份。
已完成的原组配置和冻结私有轨迹不可变；已成功提交再丢回执，只确认消费身份，零重复采集/GRU/step。
未完成采集从单元合法起点重采；未提交的学习从上次commit重做整个2轮组，不从半轮optimizer继续。
每次提交保留`committed-NNNNNN.pt`硬链接供准确版本引用，不复制张量。
同一checkpoint目录由一个学习协调进程顺序使用；当前不交多进程同时写同目录的分布式提交。
完整标签、实际版本及消费身份同事务使用；没有scheduler时显式记录None。
必要随机状态是全局Torch/CUDA与本进程存在的采样账户状态；完整采集在新进程复用时无需恢复已结束账户的环境/采样续跑。

新进程可仅改变检查点存放目录：将原 `A/checkpoint.pt` 的字节复制到新目录 `B/checkpoint.pt`，
配置 `parameters.checkpoint_directory=B`，随后沿原学习入口打开B、向B提交。没有新增源目录字段，
不修改原检查点或消费历史；现有同版本格式直接兼容，不升级旧v1状态。数值兼容比较只排除 `checkpoint.config.checkpoint_directory`，
已消费组比较只排除 `group.binding.parameters.checkpoint_directory`，完成采集绑定比较只排除
`completed-collection.binding.parameters.checkpoint_directory`。没有递归忽略其他路径或同名字段，
学习参数、版本、模型严格状态、输入/账户/时间/规则和轨迹身份仍按原判定。
已消费同组保持既有提交序号与确认语义，不重复采集、再生、更新或提交；未提交的原完成采集按同一语义绑定复用。
同一存活学习器内切目录仍不支持，原配置保护保持。轨迹、规则封存及其他consumer路径必须仍在原处可读；
这不是全套研究目录搬迁或准确日内环境续接接口。

原子替换/目录fsync不能替代存储设备的持久性保证；本段测试覆盖三处逻辑故障，不宣称断电或分布式事务验收。
本段首个工程checkpoint曾把`torch.__version__`的TorchVersion子类落盘，weights_only读取失败。
新版本存原生str；仅为这份已生成工程产物局部允许官方TorchVersion类，仍用weights_only=True。
采用依据为[PyTorch2.8序列化说明](https://docs.pytorch.org/docs/2.8/notes/serialization.html#torch-load-with-weights-only-true)。
模型/产物的离线版本SHA用于交付身份；不添加业务逐帧哈希。

## 本段合成证据

功能档两个账户、每账户最多8次实际采集，原Parquet18条应用输入、17个公共组，12秒短窗。
E为起点+1000ms，H为起点+12000ms；public/start/dispatch延迟100/10/30ms，计算序列为200/20/20/20/20/20/20/400ms。
该序列保留短时积压、恢复及跨E冻结，时长是显式SIM执行语义，不冒称当前GPU实测耗时。
原动作由真实GRU/条件头生成，工程控制初始化不代发单。专项只记录资格和原动作/成交归属，不加数值特殊奖励。

功能实际累计36采集、152重算、6联合step、2commit、3个计划故障：
采集中断后合法起点重采、2轮学习后未提交故障、commit成功丢回执后只确认身份。
已有G0采集/提交均未重复。独立联合概率最大误差4.44e-16，old-teacher joint logp最大差1.77e-7（明确夹具容差1e-4）。
联合PPO clip/完整剩余half MSE通过；第二轮ratio改变；Critic-only共享GRU/encoder梯度非零。
完整BPTT与Q4 TBPTT的隐藏数值最大差0、GRU梯度差0.00103919，说明截梯度保留数值连续。
批与逐个浮点不保证逐位相同；不据此宣称数值、轨迹或训练确定性。

| 成本档 | 采集/重算/step/commit | 外部完整墙钟 | 全部主GRU/秒 | 逻辑文件字节 | 唯一inode文件字节 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1次 | 16/32/2/1 | 3.701423s | 12.968 | 11,069,780 | 5,929,829 |
| 3次 | 48/96/6/3 | 11.297046s | 12.747 | 33,211,173 | 17,791,320 |

两档都包含合成准备、冷worker装载、原Parquet供料/公共统计、编码/条件头、IPC、执行经济反馈、公共再生、两轮学习、checkpoint和输出。
3次档逐次启动3个独立进程/账户环境/初始模型及新checkpoint，装载3次；OS/GPU缓存仍可能温热。
共时进程组RSS采样峰分别1,419,492/1,422,492KiB（50ms采样，观测下界）；CUDA分配峰约35.17MB、缓存峰54.53MB，非驱动全部显存。
本次未采集设备进程/NVML显存峰，不能把allocator峰当作设备峰或全量容量保证。
应用输入H2D为291,984/875,952B；初始模型装载1,695,424/5,086,272B；checkpoint有效张量D2H为5,086,272/15,258,816B。
RPC请求/响应为549,321/88,151B及1,647,935/264,141B。应用字节不等于驱动总PCIe传输。
逻辑字节包含checkpoint硬链接的重复名字，唯一inode字节用于区分实际文件增长。
1次档checkpoint占唯一文件5,139,951B（两个硬链接名）；私有轨迹90,021B，NOE/Ledger/SIM197,700B，合成准备124,808B，标签/目标7,713B，配置/报告369,636B。
3次档对应为15,419,853/270,068/593,100/374,424/23,140/1,110,735B。模型由显式外部产物复用，不在每个成本运行中复制。

功能监督分段实测墙钟总26.020951409s，另一次零前向checkpoint格式检查未预置精确外部计时，保守扣43秒授权预算，
43秒包括工具间隔，仅是预算扣减上界。功能原上限20分钟、成本1/3各10分钟均未超限。
全轮实际100采集/280重算/14step/6commit/3个计划故障，未用剩余额度追加probe。
保留三次原非预期失败：预开始公共再生筛选错误、冻结标签重新计算逐位比较、TorchVersion checkpoint格式。
前两次只有16/0次新采集，无重算；第三次G0已完成72重算/2step/1commit，G1尚未做新模型工作。
最终适用证据分别绑定修正后的产物版本。
后续本金读取改为原Ledger权威分项、缺详情负项补原生命周期读取。零模型核对覆盖100条原采集前缀（含失败4条）及12个已完成账户的H：
K_i全部精确0，旧/新义务本金逐条相等，H原trade_count均0、Uposition/Uobligation/J均0；6份冻结标签保持原字节，故r/A/G及既有真实更新证据保持适用。
本段模型整链没有确认成交或非零经济收益，非零优势来自oldV。非零金额及补偿的公共链/标签语义由纯Rust真实NOE反例验证；其实际公共投影保存在tests/fixtures/confirmed-original-noe-v1.json并直接传入正式freeze_labels函数。
费用.084/残余本金3/补偿、配对毛利.3/残余本金.5分别按手算核对r、完整G、GAE及末端；不把这项纯验证说成非零目标GPU经营训练。

复用既有官方依赖锁/venv，本段未下载安装。首次check43.49秒、首次release95秒及后续迭代构建/测试在独立执行记录中保留，失败构建成本也保留。
这些阶段可能嵌套或重叠，不能简单相加为整轮交付墙钟；没有准确整轮会话起止，不声称精确总交付耗时。
既有极大SDK份额成本超出Ledger Decimal解析范围的typed错误仍是公共边界，不伪装成正常拒绝或负奖励。
本段小额合成不证明真实密集行情、全日容量、训练收敛、盈利或正式参数适用性；任务4及正式作业预算仍待后继。

## 提交前复核及历史证据边界

提交前发现并修复两项缺陷：异构统计/公共交付时序错误复用首账户输入；零调用单元未创建空轨迹。
新增纯Rust回归使用4份离线输入配置身份，验证3个原公共再生流及同身份共享；没有创建SIM账户或模型。
全N0/混合N0通过正式完成采集读取与目标报告入口验证，并覆盖非空轨迹丢失/截断错误，不执行模型或学习提交。

v1—v4未独立保存历史Rust未提交源码全集或patch；源集合hash不能还原源码，因此不能完整机械比对历史Rust与最终候选。
3.701423366/11.297046047秒只属于保留的实际执行程序、脚本与原配置，不是最终新二进制实测。
Python核心比较及100个Pi/12个H的数值核对只证明各自窄范围，不能替代完整历史源码绑定或最终版本运行成本。
v4目录旧artifact-hashes.json是复制旧清单，保留原件；更正清单区分原会话当时已打印的程序/worker/engine/model哈希与当前保留文件哈希。
当时suite没有独立打印执行前哈希，不以当前hash反写历史。诊断小源码与能找回的原命令独立保留；当时未打印的rlib展开路径/哈希保持未知。

## 公共输入与旧身份

当前B的公共需求由 `public_input` 唯一声明：FULL数量，`source_progress=false`。
消费者的策略需求与正式学习公共再生复用这一声明。未选择完整来源进展集合不等于删除F9：
F0的quality、direct_anchors、缺失及证据时间和统计coverage/source时间仍进入原编码。
显式来源进展需求通过独立 `requires_source_progress` 与observer/健康消费者需求合并，
不启用B健康调度，也不让统计FULL覆盖独立证据请求。

生效参数记录 `model_public_input`，沿既有consumer/group绑定比较保持语义身份。
正式轨迹行、学习读取、group与checkpoint采用 `learning-trajectory-v2`，表示再生按同一B需求。
原v1完成组和轨迹不静默升级；原完成记录binding不符时沿原比较拒绝，旧v1行在正式读取时拒绝。
Python仍在恢复模型/优化器状态之前核对原checkpoint的trajectory_version；更换group id不能绕过此比较。
已消费同id仍比较完整group，不重复消费。旧v1仅允许显式只读诊断引用；需要重新学习时应另建
当前语义的采集组和checkpoint目录，不将旧学习checkpoint重新标为v2。
`confirmed-finite-j-v1`、`b-public-token-log1p-v1`和模型格式均未改变。

本次工程只通过合成Parquet/stdlib替身核对采集请求与生产再生，未执行模型初始化、前反向或更新。
实际数值及最早call25/26重验仍需独立签发；CPU输入一致不等于CUDA/完整PPO验收。
