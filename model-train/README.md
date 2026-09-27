# model-train 纯计算组件与两图

## 全档盘口执行与性能复跑

`native_inputs_v5` 五输入、六侧顺序和全部有效档位保持。盘口图先按 PM 四侧／Binance 两侧的
布尔掩码收集有效行，分别经过原两组 `9→32→32` 权重，再散回原位置。
sum／mean／max 仍沿原坐标计算；内部空洞、补齐和空侧不改变相邻配对树及并列 max 的首项梯度。
`BookLinear` 将乘积的逻辑布局改为 `[行, 输入, 输出]`，使 ONNX／ORT 的连续末轴承载输出通道，
减少切片复制成本；这不承诺 PyTorch eager 中间张量的实际 stride。逐输出的 float32 乘法、
相邻配对加法及偏置保持，首层直接计算相邻输入对的乘积之和，奇数尾项继续原树，
未改用普通 GEMM 或任意并行归约。最终状态图只含标准 ONNX 算子。

原生投影的路径索引和重复直接时间计算只在当前冻结侧内复用，重复锚点仍按原规则表示时间不唯一。
精确对数的 `ln(2)` 初始有理区间为不可变常量；需要精度加倍时继续使用原区间算法与舍入判据。
每个根／市场／侧还保留上一帧实际使用的精确对数输入及其 f32 结果；当前价格、数量和距离先形成
准确有理表达式，再按完整数值查表。每帧重建当前输入集合，消失的键立即退出，未知／空侧清空，
市场退役随原历史删除；没有固定容量、截断或时间行缓存。表是可丢弃的纯计算结果，不是盘口事实。
距离键只构造一次，拥有的价格／数量／距离键按原结果批量建树。六侧独立准备和每块 64 行的逐档纯输出
复用同一 Rayon 持久池，结果按原侧序写回；历史、GRU 和同根帧仍顺序推进。历史队列实际追加或淘汰时
重算四个派生值，未变队列复用结果；六条历史共 24 个 f32 随原市场历史初始化和释放，不复制历史队列。
`RAYON_NUM_THREADS` 是原生池的线程设置，与 ORT 算子内线程分别控制；未设置时沿 Rayon
的可用逻辑 CPU 默认值。增加线程是否有利须用实际工作量测量，线程数不改变盘口档位数。

下列测量入口读取固定真实帧包（`metadata.json`、`frames.json`、`inputs.f32`）及已归档的配置／权重。
每配置 `benchmark_configuration.json` 保存 `Configuration` 字段及来源；只重导出状态图，动作图保持
原文件。输出位置应在任务工作树之外，以便审查和工作树清理后仍保留原始性能数据。

```bash
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 .venv/bin/python -m model_train.benchmark_state export \
  --graphs "$BASELINE_GRAPHS" --output "$CANDIDATE_GRAPHS"
.venv/bin/python -m model_train.benchmark_state matrix \
  --baseline "$BASELINE_GRAPHS" --candidate "$CANDIDATE_GRAPHS" \
  --packed "$SAMPLED_INPUTS" --output "$STATE_MEASUREMENTS"
.venv/bin/python -m model_train.report_state_benchmarks \
  --measurements "$STATE_MEASUREMENTS" --output "$STATE_REPORT"
```

矩阵为 MAIN 层数 `1/2/3/10/20/30`、宽度 `256/512/768/1024`、ORT 线程 `1/4/8/16`；
每版 96 组。各组合基线／候选配对，前后次序在每种线程及配置内平衡，全部串行执行。
每组 16 次预热，每个真实帧正式调用一次；16 个均匀位置另做 PyTorch／ORT 容限检查。
结果保留逐次纳秒、P50/P90/P95/P99/最大值、最大值帧身份、模型／输入哈希和线程／环境记录。
正式计时仅覆盖 `session.run`（包括 Python 调用边界）；读取、补齐和零 hidden 准备另列。
每帧 hidden 为零，权重未训练；这是固定真实帧抽测，**不属于五分钟完整连续回放**，也不包含原生投影、
动作图或完整交易链。样本最大值不保证最坏情况，不能把分开实验的分位数相加。

原生完整输出路径的有界真实录制复跑入口为 `native-inputs/examples/replay_projection.rs`：

```bash
RAYON_NUM_THREADS=4 cargo run --release --locked --offline \
  --manifest-path native-inputs/Cargo.toml --example replay_projection -- \
  "$REPLAY_CONFIGURATION" "$PUBLIC_RECORDING_ROOT" "$NATIVE_MEASUREMENTS" decisions 32
```

公共录制根包含既有 `recording/`、`scenario.json`、`record-completion.json`。
配置沿录制准备产物提供公共解码参数、规则快照及哈希、终局策略、原生根开始时间、目标 condition、
初始模拟现金、来源身份与输出容量参数。例程完整恢复公共前史，调用原 `NativeInputRoot::consume`，
到目标累计 `contexts` 或 `decisions` 行数后停止并排空持久输出；不省略未入选帧的特征投影。
`groups.jsonl` 可用于独立逐帧递推验证，`completion.json` 的原生计时包括完整 `consume`，不等于仅盘口
投影或端到端延迟。原生代表成本样本、真实连续段及合成生命周期测试须分别报告覆盖范围。
另设 `PROJECTION_CACHE_DIAGNOSTICS=1` 可从同批原始精确观测重建查键序列，记录前帧命中率、
最大键数及键值载荷字节；载荷不包含树节点、分配器容量和双表临时重叠，不等于堆峰值。
诊断会改变机器缓存状态，应独立运行，不能混入正式计时结果。进程 RSS 包含整个回放宿主。

长时间测试按用户最新要求每 15 分钟查看进度。正式性能期间不并发其他竞争 CPU 的测试或构建。

本轮完整结果、内存取舍和验证边界见[全档编码实测报告](validation/full-depth-encoding-168.md)，
192 行分位数与最大样本身份见[配对结果 CSV](validation/full-depth-encoding-168.csv)。

后续 #170 保留原生缓存、六侧投影和历史特征的独立对照，并评估保持归约顺序的
BookLinear 首层改写。[第二轮优化报告](validation/issue-170/report.md) 提供真实前史后的
原生片段、24 个架构 × 4 种线程的两轮结果、CPU／内存取舍和最大值诊断；
[完整配置表](validation/issue-170/state-matrix.md) 列出全部 P50／P90／P95／P99／最大值，
[复跑入口](validation/issue-170/README.md) 说明独立诊断与正式计时的边界。

`benchmark_state profile` 可在独立会话诊断算子时间；其 Loop 容器与子事件分列，避免重复归因。
不得将分析器会话的数据重标为正式延迟。真实连续段可使用既有 Rust ORT 窄验证入口复跑：

```bash
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 .venv/bin/python -m model_train.validate_replay_state \
  --groups "$NATIVE_MEASUREMENTS/groups.jsonl" --graphs "$CANDIDATE_GRAPHS/MAIN-1x256" \
  --binary "$RUST_ORT_VALIDATION" --output "$REPLAY_VALIDATION"
```

三端分别维护 hidden，按原生 InitializeZero／ResetRootHidden／Retire 与原次序消费。
该入口报告实际出现的角色和操作，不把段外退役计作覆盖；它不替代实际 BUY／撤单执行链验收。


撤单完整响应在公共V4链形成独立非经济完成事实，零目标Market响应也完成真实请求。
逐目标失败、明确撤成及结果未知分别保留；登记减完成推导在途，多请求互不代替完成。
已明确撤成的原引用独立保留，后到失败/查询不反转关闭，迟到成交仍按唯一经济事实应用。
SIM的有限显式暂失败场景经原输入日志配置，在订单仍有余量且未产生效果时命中；命中记录同原输入组，
重放仍执行原输入与场景条件。没有故障概率、固定冷却、最短挂单时间或自动补撤。

当前输入合同为 `native_inputs_v5`，具体布局与SHA来源为 `native-inputs/feature_schema.json`。
`export_metadata.json` 与连续验收来源信息同时记录该版本和 schema SHA。412—414新增
`ActionBeforeReadV2::pending_noe_record_count()` 的原三态计数编码：已领取且因依赖未满足而尚待
Ledger应用的适用原记录数。零不证明未来事实已齐；它不是金额、缺序号数量或禁单条件。
实到NOE引用先进入共享观察，经济应用仍保留原引用和真实前驱条件；正常经济等待不会隐去合法模型组。
已知现金只描述本地已提交经济前缀与原身份预留，不重复扣待应用差额。
新增账户415—432为全局请求块；撤单184含两个68槽订单模板、24槽Market及18槽候选请求块。
四态适用性、可撤性、最新逐目标结果与T6独立编码；原账户0—414及30/60参数布局保持；v5市场布局见下文。
当前可BUY未结算市场的修复缺口按唯一成交事实中已公开FinalSuccess实际份额求差，再扣原非终态承诺；
164—175与Actor复用同一冻结投影。122—163的成功经济池、本金、配对和NAV仍要求经济应用完成。
新版短验证和完整C/P使用独立433/184图；旧412或415图结果不作为新版验收通过证据。

`model_train.components` 保留特征编码器、标准裸 GRU、残差 GRU 和 Actor 分数组件。
`model_train.graphs` 将它们组合为阶段 #6 的状态图与参数图；数学结构来自
[#5 技术 §8](https://github.com/slowchillchill/btc-updown-marketmaker/issues/5#issuecomment-5565139746)，
张量与两遍撤单协议来自
[§14.2.1](https://github.com/slowchillchill/btc-updown-marketmaker/issues/5#issuecomment-5565141870)。
最终特征索引见 [#6](https://github.com/slowchillchill/btc-updown-marketmaker/issues/6) 正文及[正式续篇3](https://github.com/slowchillchill/btc-updown-marketmaker/issues/6#issuecomment-5747531067)。

状态图输入为 `fast[B,596]`、`slow[B,433]`、`book_levels[B,6,N,9]`、`book_mask[B,6,N]`、`h_prev[L,B,H]`，返回 `z_t` 与全部层 `h_next`。
参数图输入规则30维、前缀60维、对象184维、布尔掩码、全局归约及 `price_fee_features[B,9999]`，返回六个固定头和逐对象表示/分数。
参数图无 GRU，不改变隐藏状态；价格轴9999，数量分支2，变体4，post_only 2，有效期3，动作族6。

四配置及初始化种子固定为 `BASELINE-1x256:741001`、`BASELINE-2x256:741002`、
`MAIN-1x256:741003`、`MAIN-30x1024:741004`。v5构造顺序为扩宽后的原递推核心、PM盘口编码器、Binance盘口编码器、规则、前缀、对象、汇总编码器、Actor；
各支路独立权重，两遍对象计算共享一个编码器。首次运行保存一份 `initial_weights.pt`，后续加载它，
不根据验证结果更换种子。模型文件与环境仅保留在被忽略的目录，不提交 Git。

## v5 全量盘口（#166）

六侧顺序为 PM A bid/ask、PM B bid/ask、Binance bid/ask；买价降序、卖价升序。
原公共观测所有可见正数量价位都进入同帧状态图，不设 top-N，不按 tick 重采样。
旧六侧前五档240槽移出 fast，剩余596槽按原顺序保持，规则仍占最后30槽。
逐档九维是原八维 `[price, L(quantity), present, T5]` 加本侧最佳价距离。
距离先在精确有理数域计算：买侧 `best−price`，卖侧 `price−best`，再作 `log1p(distance/unit)`；
PM单位为1 pUSD/份，Binance为1 USDT/BTC。这样能区分100000.00／100000.01价格投影碰撞时的数量互换，
不宣称消除任意浮点碰撞或可从固定宽度表示无损重建盘口。

原生每行保存六个变长数组；Python与Rust只在独立根合批时派生补齐。
`N` 是本批最大侧深度，全空物理 `N=1`、掩码全假。真实档位存在槽为1；补齐不参与归约，
已知空侧和未知侧由原 fast 的 known/Q6 区分。PM T5只取该价位所选数量事实的直接锚点；
Binance无逐档时间时五槽继续为零，不能复制整本时间。

PM四侧共享一套、Binance两侧另共享一套 `9→32→32` 编码器，中间SiLU；仿射按原输入坐标使用既有固定float32加法树。
每侧拼接sum/mean/max共96维，六侧576维。sum沿动态N反复相邻配对相加、奇数尾项保留；
训练路径可微，导出边界把标记展开成标准ONNX Loop，生产仍只有原两图且不需要自定义运行内核。
计数为整数，mean用float32除以计数；max忽略补齐，保留负值，并列梯度只向最早项传播。
全空三组都为有限零。最终标准图再做数值比较、记录和哈希；导出标记本身不算可运行图验收。

MAIN使用 `concat(fast596,book576)` 的1172维原市场编码器，账户433和原门控、逐层残差GRU保持。
BASELINE将 `concat(fast596,book576,slow433)` 的1605维经原LayerNorm/Linear256/SiLU接裸GRU，
不添加门控或残差。每个ContextOnly或DecisionRequired状态帧只推进一次全部层hidden；参数图继续复用冻结z_t。
独立根才合批，同根按原顺序消费；ResetRootHidden、InitializeZero、Retire和原预热保持。

本版本重新按原四种子初始化并保存新权重。旧v4特征缺少深档，须按原cutoff从原公共材料重放；
旧权重、hidden与策略概率不重标为v5，hidden按原初始化/预热重建。本次未增加迁移器或新运行时拒启门。
27个正式架构按原24种MAIN组合及3种BASELINE做构造/布局检查，实际图和三端只覆盖四个结构代表；
这不构成训练、目标CPU/GPU或实盘容量验收。历史验证报告保留原版本归属。

新增边界测试见 `tests/test_book.py`、`native-inputs/src/book_tests.rs`，原生布局对照由原提交
`9361a4ee10e94a7cc11d225f0eef1750cecbe855` 的实际入口独立生成基线。
有限新增前端成本可运行 `python -m model_train.measure_books --output <输出.json>`，
测量仅说明对应本机深度与混批，未设业务上限或硬性延迟门。人工输入样本与本次验证摘要见 `validation/native-inputs-v5.json`。

## 输入与数量语义修订（#162）

`native_inputs_v4` 保持836/433/30/60/184宽度。规则14—15及fast对应槽投影同一冻结快照的真实
平台最低份数mos；它供模型感知并由SIM执行，不屏蔽低于mos的模型数量。正整数n仍表示q=0.01n，
保留精确编码、双现金和预留、配对等量与修复单腿cap，FOK/FAK每腿原本金仍至少1 pUSD。
低量LIMIT原样提交后可以被拒绝；宿主不抬量或换动作。数量完成性撤销固定5与500/499划分。
账户未知覆写完整写入[0,0,1]，不留下旧已知值。资产A/B保持既有固定outcome index；有原Gamma
clobTokenIds证据时标签/价格按其源序关联后投影，缺少原序证据的旧行保留既有读取语义，不能凭标签猜测纠正。

新GTD规则证据版本为 `POLYMARKET_GTD_EFFECTIVE_EXPIRY_V1`：声明截止E保留原编码，实际
截止为D=(E-60)×1000毫秒。准入继续按floor(E_ms/1000)与ceil(P_ms/1000)比较，声明至少
提前120+60秒、等号接受；队列、生命周期来源时刻和开放本金释放使用D。原规则快照仍按原240秒
准入与E截止读取，旧格式不迁移，也不据此新增当前运行的事故恢复能力。

在 #162 交付时图形状和初始化规则未改变；其四配置按原种子导出证据。v3旧轨迹的支持集及条件概率属于旧实现，
不能直接用v4重算冒充原策略。原始采集证据保持不变，可由含源token顺序的原材料重新生成规范化观测；
旧规范化结果不静默改写。本次不签发训练或部署，也不增加运行时迁移/拒启门。

## 两遍撤单宿主

`encode_all` 按宿主既有稳定顺序遍历全部对象；逐对象 float64 累积 sum，精确整数计数后求 mean，
max 用 `torch.max(dim=0)` 保留并列首项梯度。空集合不求 mean/max，汇总网络之后继续归零。
`score_all` 接收所选族的掩码，返回每行全部真实对象分数，供一次全局 `Categorical` 或严格最大值选择。
`active_rows` 只调用有待评分的行，并保留行映射；没有活动行不调用。

按[已批准的故障修复](https://github.com/slowchillchill/btc-updown-marketmaker/issues/6#issuecomment-5725045538)，
上述纯图宿主帮助函数的两遍非空逻辑块均以零特征、假掩码补至256槽，返回后按逻辑长度修剪。`empty` 来自宿主已知完整对象数量，
不是对设备上的掩码求布尔值。完整集合为空仍传 K=1 全假。逻辑分块不改变对象总数、顺序、身份或全局选择；
它也不改变图的动态 K=1—256 接口。比如这些帮助函数的 N257 的256+1与128+129均执行两个256槽物理块。
真实业务调用继续按当时所选族使用合法动态 K，包含 K3/K4，不要求统一补到256。
训练归约和第二遍评分都保留到同一对象编码器的梯度；补齐不保证训练总内存与对象数无关。

## 固定技术切片

`graph_fixtures.py` 只生成有限已知截面的张量和逐槽说明，不是公共经济投影、账本、撮合或完整支持集实现。
它不读取原始数据、不推进经济事实、不调用 `has_completion`，不用费用上界冒充精确费用。
没有对业务值随机抽样；四配置使用同一份固定切片。

| 输入 | 切片及掩码依据 |
| --- | --- |
| 市场596、六侧盘口/规则30 | 市场已开盘60秒；A/B盘口买0.4卖0.6、每侧一档100份，直接BBA同值；Binance买59999卖60001；正式锚点/Chainlink现货60000；未观测成交/TWAP/判定不填造数值，组内仅一个现货事实；参考历史只一个样本。tick均0.01、该夹具平台mos=5份（仅规则输入及执行门）、市场BUY最低支出1、延迟1秒启用、费率0.07指数1且仅Taker。完整规则30维同时逐值进入fast末30维。 |
| 账户433 | 初始2000 pUSD、零持仓/成交、空旧市场集合，全部订单均尚未成交。已知结构性零保留exists/applicable；空订单集合的最早/最近年龄不适用。只有post-only GTC本金占用/承诺/开放订单数变化；尚未发生费用或返佣，两种NAV仍2000。 |
| 撤单184 | 每原pair为A/B各5份，原价0.2/0.3，post_only=true；在截点前1秒接纳。Single保留真实腿；Paired按原ordinal；Market精确汇总所有真实可撤成员。GTC的spend/GTD到期及非本族模板不适用，真实零费有存在位。 |
| 前缀60 | Family未选、三个撤单族已选，以及q=5双腿GTC的逐腿Variant/PostOnly/Price评分前切片。没有伪造完整数量支持或最后意图。 |

P3真实空订单市场保留唯一Market，完整N1；原pair取消A后 Single(B)+Market=N2；85pair生成N256；86pair生成峰值N259，
取消A后N257；另有B=2的真实业务(1,257)，并单独保留图级N0及(0,257)零掩码探针。不存在将单个Single称为完整集合的场景。
`fixture_trace.json` 记录每个槽的数值、原值/掩码说明及合同来源；它是纯图输入证据，不是生产模式冻结件。
重复固定张量的12步独立递推仅检查数值误差与全部层状态，不能称为真实公共业务帧连续验收。

## 复现

使用项目指定 CPython 3.13.15、uv 0.12.9；CPU依赖及传递哈希见 `pyproject.toml` 和 `uv.lock`。
在本目录运行，`uv` 指向该精确版本，可放在本目录 `.tools/`；不更改用户全局环境。

```bash
uv sync --locked
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 .venv/bin/python -m unittest discover -s tests -v
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 .venv/bin/python -m model_train.validate_graphs --config MAIN-1x256 --threads 1 --export --output artifacts/run
OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 .venv/bin/python -m model_train.validate_graphs --config MAIN-1x256 --threads 4 --output artifacts/run
```

对四配置分别执行上述两个验证命令。保留已有图/权重时使用 `--source artifacts/original --output artifacts/new-run`；
来源目录只读，新证据写入新目录。若同时传 `--export`，从来源权重导出到新目录。

对象编码在同一float32 Linear前将B/K前导轴展平，之后还原形状，使导出使用Gemm；
参数、数学和梯度路径不变。原失败导出及相同权重对照材料保留。
撤单头末标量层继承原 `nn.Linear` 参数注册与初始化，乘积的末轴归约使用相邻偶／奇切片的固定加法树，
奇数尾项保留；只展开静态特征宽度，B/K保持动态。LayerNorm沿用原参数、eps和中心化方差定义，
两次均值复用该归约，标准化采用 `centered * torch.rsqrt(variance + eps)`；未改全对象float64宿主汇总。
规则与前缀编码的两层仿射、Actor decoder及撤单头的首层仿射沿用原参数，
逐项乘积后使用同一加法树，按16个输出通道分块；其他仿射、GRU和激活保持原实现。
两端直接导出标准opset18基础算子，不再另行改写导出图，也不强制转换dtype；原双精度独立数学和梯度检查仍适用。
没有依对象身份、相等输入或分差选择计算路径。历史严格选择反例及局部负结果均保留，
当前表达已从原初始化通过 MAIN1 原前史至 `C1-1-1-0/176061000` 的三端比较和真实意图执行；
四配置、1/4线程的固定两图与三端检查、真实 P3—P7 及全部8组完整 C 均通过。
各 C 组完成18轮、121113个正式观察帧和80730次实际决定（含NoTrade），另有128帧独立预热；
全部结束记录的三端hidden、native z与观察根清零。MAIN30两组最大绝对误差均为 `9.5367431640625e-6`，
原逐元素数值门与完整决定严格一致门均通过。结果及证据入口见
[PR #125](https://github.com/slowchillchill/btc-updown-marketmaker/pull/125)。
最终汇总为 `native-inputs-v3/aligned-affine/final-observation/final-c-resources.json`，保留原启动源码、图和二进制绑定，
不将后续修复或文档提交重标为新性能实测。MAIN30三端验证进程结束RSS约4464—4469MiB，
allocator已用块在C2 Orphan首轮有约4MiB台阶，同c／scene三次重复未呈持续增长；
这不是生产单端内存或任意长期无泄漏的证明。本地结果不替代目标容量；按[用户延期决定](https://github.com/slowchillchill/btc-updown-marketmaker/issues/6#issuecomment-5754843215)，
目标CPU实测移出本次任务1.5、PR #125及阶段1的完成条件，继续整体独立审查、合并与适用阶段收尾。计算对齐依据见[#6实施记录](https://github.com/slowchillchill/btc-updown-marketmaker/issues/6#issuecomment-5748954118)。
导出使用 `dynamo=True`、opset18、float32、`optimize/verify/report=True` 及外部参数文件。
正式对比使用两端独立递推，显式 CPU、顺序执行、ORT全优化、interop1、关空转和memory pattern、保留arena。
动态B覆盖1/2/3，图K覆盖1/2/128/129/255/256；宿主分块包含补齐、尾块及混合空行。
相对容差以PyTorch为参考，`atol=1e-5, rtol=1e-4`，非有限值直接失败；选择仍要求相同严格argmax。
失败JSON保留首差异上下文和两端值；撤单选择差异同时记录完整对象索引和场景身份。

本机补齐成本可以单独描述，不形成性能通过门：

```bash
OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 .venv/bin/python -m model_train.measure_padding --config MAIN-30x1024 --source artifacts/run --threads 4 --output artifacts/padding-cost.json
```

计时保持同一模型/会话，预热一次后测10次，记录两遍加宿主归约的中位数/最大值和原样本；旧逻辑只作成本对照。
CPU结果不代表目标CPU容量或GPU模式验收。

上述历史纯图交付切片当时不包含真实经济特征接线、完整精确支持集、三端最终意图一致性、70分钟C场景、长期内存、
目标CPU/GPU测量、训练、部署或资金动作；该切片本身不关闭任务1.5或阶段#6，后续本地完整验收及当前延期范围见上文和末节。市价估费与实际收费按 D-0232 分离；本轮 Actor 接线见下节。


## Rust ORT 三端技术验证

按 [任务1.5分配](https://github.com/slowchillchill/btc-updown-marketmaker/issues/6#issuecomment-5725576224)，
`rust-ort-validation` 是独立局部 Cargo 工程，锁定 `ort/ort-sys 2.0.0-rc.13`、`api-27`。
它用 `init_from` 绝对路径加载锁定 Python ORT 1.28.0 wheel 内的原生库，随后提交环境再创建会话；
不从系统自动选择另一 ORT。Python扩展和独立原生库是 wheel 中两个不同文件，分别记录哈希。
会话显式 CPU、arena开启、顺序执行、全优化、intra1或4、inter1、关闭空转及memory pattern。

从本目录运行；`GRAPHS` 指向已有对应配置的归档目录（必须同时含两图、外部参数与原 `initial_weights.pt`），
不同运行使用不同 `OUTPUT`。该命令只加载原产物，不导出或保存新权重。

```bash
cargo build --manifest-path rust-ort-validation/Cargo.toml --locked --offline
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 .venv/bin/python -m model_train.validate_three_runtimes --config MAIN-1x256 --threads 1 --graphs "$GRAPHS" --output "$OUTPUT"
cargo fmt --manifest-path rust-ort-validation/Cargo.toml --check
cargo clippy --manifest-path rust-ort-validation/Cargo.toml --locked --offline -- -D warnings
```

4线程时同时将 `OMP_NUM_THREADS`、`MKL_NUM_THREADS` 和 `--threads` 改为4。驱动在夹具与模型计算前一次性设置
PyTorch算子内N/算子间1线程，结果记录实际线程值、环境变量与构建/并行计算后端。

首次准备 Cargo 缓存需按局部 `Cargo.lock` 下载依赖，之后可离线执行。四配置分别运行1/4线程。
Python驱动把原合法技术切片写为 `inputs.json`，其中没有隐藏状态、z或对象归约；
Rust自行从零递推全部层状态，并在两遍对象调用之间按全局稳定顺序执行float64 sum/mean/max。
最大值只在严格更大时替换。Python ORT和PyTorch也独立递推并归约。
Rust的 `rust.jsonl` 保留实际各步输出，`rust.stderr` 保留加载/执行错误；`result.json` 只在完整比较通过后写入。
三端不向账本或模拟器提交动作。

当前每配置每线程25个场景保留原20个：B1/2/3各12步、K1/2/128/129/255/256、六种完整对象集合及混合空行、
合法GTC前缀和活动行0/2及空活动集合。N257的256+1与128+129在每端均比较逐对象分数、归约和严格首项argmax；
空集合仍K1全假，非空两遍每逻辑块补到256再修剪。原分块结果不能只以归约相近替代选择一致。
另外5个全盘口场景覆盖N=0/7/8/257/1001、混合有效深度及各3步独立hidden递推。
所有数值比较以PyTorch为rtol参考、拒绝非有限值；索引与行映射要求精确相等。
当前证据见 `validation/native-inputs-v5.json`，旧版摘要保留于 `validation/rust-ort-report.json`，原始大输出保存在本次任务产物目录，不提交Git。
这些结果仅证明固定纯图技术切片三端一致，不代表三端经济最终意图、目标CPU/GPU容量或阶段完成。

## 原生特征与状态、参数组件输入

`native-inputs/` 是独立 Cargo 工程，消费原公共组、`SharedExecutionV2` 与同组
`ActionBeforeReadV2`，实现 #6 正式续篇的 433 槽账户输入和 184 槽撤单对象。
它从根起点、真实市场窗口与公共观测确定预热、当前和旧市场范围；旧集合取完整
Ledger 市场并集，因此仅有本地预留的旧市场也参与汇总。根操作由 Rust 产生，Python
只执行 `ResetRootHidden`、`InitializeZero`、`Retire` 的顺序。

所有组内行借用同一个动作前读取。金额、数量、计数与旧集合先在精确域计算，再用
有理数区间确定自然对数的 float32 最近偶数舍入；不先经 f64 或截断 Decimal。
撤单对象直接保留原集合顺序、真实腿、原 ordinal 和逐身份资金，不另算资格。
可寻址且尚未证实终态的订单保留为候选；缺原形成/接纳、腿或数量的字段保留Unknown，
不以关联尚未齐备丢弃合法模型组。Single须有真实本账户订单证据；普通失败尝试提及ID不足以证明存在。
Market范围借用同组冻结的当前c及完整账户经济旧集合O，空Market仍可寻址。

`NativeInputGroup` 从同一公共帧编码596个市场坐标与六侧全量变长盘口、433 账户输入和 184 撤单对象。
30 槽规则只生成一次，逐值复用于 `fast[566..596]` 和参数图。动态 tick 来自真实 A/B
资产字段；公共冷规则保留精确来源，参数价格用同帧 tick 乘原整数，不用神经投影反解。
`PrefixState` 持有原 BigUint 数量区间/叶子及真实腿、变体、post_only、寿命、价格整数，
编码 60 槽；`NativeActor` 在同一 Rust 精确状态上生成完整支持并推进选择路径。Python 只消费支持与模型分数。
实际布局、来源、单位、变换与缺失规则见 `native-inputs/feature_schema.json`；全量盘口增量合同为 #166，其余继续引用 #6。

市场编码只读公共参考/TWAP、盘口、直接 BBA、完整最近成交及本组全部原成交。
全侧统计包含第五档之外的数量；组内统计使用固定累积量，不保存整组成交副本。
时间来自字段关联的直接锚点，整数求差后投影，保留缺来源时间及负差；不读 dependency
或 lineage 补时间。当前 Binance 公共输出只有整本锚点，因此其合法价量和相邻帧净变保留，
逐档 T5 未知、侧 E9 为空；整本时间仅用于事件统计，不冒充档位来源或中价历史身份。

每个原 hidden key 仅增加合同规定的六条最多 16 样本历史及前帧侧统计。按源事实身份变化
追加，同值新事实可追加，无关组和参数调用不追加；缺失不追加历史，而前帧侧未知会阻断
盘口净差。历史随原 Retire 释放，开市沿用预热。RMS 先精确求增量平方和，复用原有理区间
投影，以 sqrt/log 两端最终同舍入确定 f32，不增加预算、精度拒绝或业务门。

`NativeComponentConsumer` 机械执行根操作，用真实 fast/slow/book_levels/book_mask 对每帧调用 Bare/Residual
状态图一次，保存全部层隐藏值。整组完成后，参数组件使用冻结 z_t、rule、原生 prefix 与
完整对象归约，调用参数图不会再次推进 GRU。对象截点可单独通过 `objects` 消费；缺少前序
hidden 的对象测试不伪装成完整连续帧。Rust ORT 原验证程序增加 `--native` 入口，读取同一
原生组、机械执行根操作并独立递推状态；Python ORT 也用自己的 h_next 独立递推，同
PyTorch 及 Rust ORT 使用同一真实序列和原容差。三端各用自身状态图的 z_t、真实冻结规则与撤单人口，
独立计算对象嵌入及全局 sum/mean/max，再比较固定前缀当前字段的适用参数头；
同时核对参数调用后的 hidden 不变。每个配置包含 12 个状态帧、9 次对象归约和 45 次参数输出对照，
不代表完整 Actor 选择链。复用原归档图与权重，不重新导出或训练。

上述旧组件验收记录只覆盖其当时范围。当前第一 PR 的 Actor 与有限真实三端闭环见下节；
本次连续增量的 70 分钟 C/P 与长期观察见末节；阶段 #6 完成仍须完整观察结果与审查。

在仓库根目录运行独立原生验证（工作区命令不会包含此工程）：

```bash
cargo test --manifest-path model-train/native-inputs/Cargo.toml --locked --offline
cargo clippy --manifest-path model-train/native-inputs/Cargo.toml --all-targets --locked --offline -- -D warnings
cargo fmt --manifest-path model-train/native-inputs/Cargo.toml -- --check
```

在 `model-train/` 运行组件验证：

```bash
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 .venv/bin/python -m unittest discover -s tests -p test_native_inputs.py -v
```

`NATIVE_BASELINE_GRAPH` 和 `NATIVE_MAIN_GRAPH` 分别指向已归档 BASELINE-2x256 与
MAIN-1x256 的两图/权重目录，可同时执行真实原生序列的 PyTorch/Python ORT/Rust ORT 对照（未提供时明确跳过）。
`NATIVE_COMPONENT_WEIGHTS` 可指向原已验证的 MAIN-1x256 `initial_weights.pt`，避免重新
导出或训练。测试从 Rust 正式公共入口生成临时组件输入；不手造 Ledger 投影或模型帧。
覆盖冷启动、重叠预热及切换、同组 100/后组 80、仅本地预留旧市场、平均成本与精确返佣、
PendingApplied/成功/补偿、QuoteBudget 三态、真实撤单人口 N=1/2/256/257/259（空订单市场仍保留 Market） 和不同分块。
缺 formed 的年龄测试沿既有 registry 入口重放真实 consumed 引用，明确属于缺源组件读取；
原46k关联已随有限PAPER补闭合及时可见；未知成员改在真实24172切点验证。
首成交24173尚未到时，形成24171已到但Accepted24250未到，原成员与承诺仍保留。

新增字段验收覆盖远档数量、前帧未知、值缺但直接时间存在、负差与多锚点、原成交缺量/方向、
同值新确认/无关组/16 样本、Binance 只更新一档时不冒造其他档时间、两腿独立 tick、前缀
未选/false/首类别/0.5/B腿/数量叶子，以及真实裸 GRU 两层和残差 GRU 的逐帧递推。
固定前缀只用于实际参数组件验收；参数图没有完整合法选择宿主，不能视为模型动作验收。

## 给定撤单族后的执行接线

`NativeInputGroup::execute_selected` 接收调用方已经选定的撤单族或 `NoTrade`。
它从本组 `action_before.cancel_objects_in(current, old)` 读取完整资格与稳定顺序，沿 184 行索引消费
`NativeComponentConsumer.cancel_scores` 返回的参数图原分数。各 `CancelScoreBlock` 按完整人口顺序
覆盖全部实际行，`valid_len` 排除块内 padding；跨块比较严格更大的分数，并列保留原首对象。
单笔恢复原平台订单 ID，配对保持原 ordinal 0/1，市场对象保留原 condition_id，分别形成既有
`CancelOrder`、`CancelOrders`、`CancelMarketOrders` 后直接交 `SharedExecutionV2::execute`。
同组后续调用仍使用原冻结对象，竞争结果由原反馈确认；不复制对象资格或执行状态。

`NoTrade` 传原 `None`，不调用参数图、不生成请求或撤销旧挂单。状态帧先由既有消费者推进一次；
后续撤单参数调用仅复用该帧 z、规则与完整对象归约，不推进 GRU。这里不计算六族支持集、
训练采样或联合概率，也不交付完整 Actor 或阶段验收。

新增原生测试覆盖真实 86 对的 259 对象前史、一腿取消后的 257 对象、256+1 与 128+129 分块、
尾块市场对象、同族跨 128 边界并列、padding 排除、顺序市场切换后的指定市场取消，以及
NoTrade 后原挂单的真实未来成交。多对象与切换案例是明确截点验证，不冒称完整连续两图验收。
普通独立 crate 测试之外，真实归档图到原意图、发送和回执的测试需显式执行：

```bash
NATIVE_CANCEL_GRAPH=/path/to/original/MAIN-1x256 \
  cargo test --manifest-path model-train/native-inputs/Cargo.toml --locked --offline \
  action_tests::actual_graph_pair_intent_execution_and_frozen_cancel_race -- --ignored
```

该测试使用项目局部 `.venv`、原初始权重和真实公共组，经 Python 实际参数图输出回到 Rust
原生宿主执行；检查每帧状态调用一次、参数调用零次 GRU。归档路径未给出时，普通测试明确
标记该项为 ignored，不能计作已完成真实图验证。

## 已选精确 BUY 参数的执行接线

同一个 `execute_selected` 也接收 `SelectedAction::Buy(&PrefixState)`，复用原共享执行、冻结意图、
Bridge、SDK 请求构造和回执。调用方先完成原合法选择；此处只装配，不重查现金、费用或数量支持。
未完成的前缀返回 `IncompleteBuyPrefix`，不转成 NoTrade，也不调用参数图或再推进 GRU。

数量 `n` 保持 BigUint 并转换为 `q=n/100`，每腿价格是该帧精确 `k*tick`。
按 F.4 已保证的编码域，以系数 `n`、`m=10000p`、`nm` 和 scale 2、4、6 构造 Decimal 后 normalize。
LIMIT 保留原 q、p、post_only；FOK/FAK 保留 `spend=q*p`、`worst=p`。
GTD 到期秒数是 `ceil(decision_logical_time_ms/1000)+60+lifetime`，Bridge 沿原路径转换毫秒；
提交延迟不重启锚点，实际处理时仍可能由原平台规则拒绝。到期值不裁到市场结束。
同帧动作上下文仅增加按 outcome index 0/1 取得的 token 身份，配对始终为 [A,B]，单 B 使用 B 的 tick。

原生测试通过真实公共前史取得帧，验证相邻 `n>2^53` 的四变体精确意图、反序 assets、独立 tick、
冻结后更新、三种 GTD 寿命与混合双腿，再以开户 2000 的原链检验四变体请求、ordinal 和回执。
大数装配测试不是巨额合法业务执行；SDK LIMIT 编码边界继续由已有
`sim-system::buy_cost::tests::limit_cost_keeps_encoding_boundary_and_unaffordable_fee_exact` 验证。
本增量不提供完整 BUY 条件选择/采样，不证明一般非零 MARKET 最大费用，也不宣称阶段通过。


## 完整 Actor 与同进程宿主

`NativeInputRoot::new` 接收创建执行器所用的同一个 `Arc<SealedProcessRuleSnapshotV3>`。
`NativeActor` 借用冻结组，在 Rust 内保留现金根、BigUint 区间、真实修复侧及全部精确选择；
完成性才与既有编码域相交。六族、数量、全部腿属性、全部腿价格按原顺序推进，撤单沿原对象索引。
同组撤单特征、族支持与最终意图共用一次形成的有序人口；UnknownSingle、空市场和配对原 ordinal 保持。
`support()` 提供完整评分分布，`choose()` 只计算当前选中分支；公开状态改变后重新按当次状态计算，不跨调用缓存支持集。
账户433项中431个公共项在组内计算一次，各行再填93/94角色位；不同组或时刻不复用旧特征。
LIMIT 复用公共最大费；FOK/FAK 只按原本金支持和预留，参考费不作为现金硬门。
`execute` 复用原意图装配、稳定请求及 ordinal，先批预留、逐腿登记 SubmittedUnknown，再调用原执行。
SIM 的 FOK/FAK 最低门是每腿原请求本金 1 pUSD；实际 FAK 部分成交金额可以更小，LIMIT 仍有最低份额。

`python` feature 提供 PyO3 `PythonGroup`/`PythonActor`，由 Rust 宿主传入实际组；Python 不构造财务状态。
`features()` 从现有字段直接创建 Python 容器，不先构造 serde Value 中间树；枚举标签、None、整数、浮点及省略字段保持。`consume_actor` 调用标准 Categorical，
单分支不消耗随机数；重算沿 Rust 原路径，返回的条件熵和不声称是精确联合熵。
`ort` feature 的 `OrtActorScorer` 使用同一 NativeActor，严格最大值选择且同分保留原首项。

当前 FOK/FAK Price 步骤的费用轴由同帧公共估费逐候选投影 `log1p(fee/1 pUSD)`，
其余步骤、LIMIT、原规则非法坐标均为零。图内无偏置 `fee_weight` 产生有符号系数；
首PR的费用层注册在原模块之后，当时保留既有参数值。此前 `native_inputs_v3` 将慢支路改为433维、对象支路改为184维；四个种子保持，但初始化消耗改变，因此重新冻结全部权重与两图，不要求跨版本逐值一致。旧图及验证材料保留。

在本目录用精确版本 uv/Python 恢复环境并构建扩展：

```bash
uv sync --locked
VIRTUAL_ENV="$PWD/.venv" PYO3_PYTHON="$PWD/.venv/bin/python" .venv/bin/maturin develop
.venv/bin/python -c 'import model_native_inputs'
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 .venv/bin/python -m model_train.actor_validation --graphs "$NEW_GRAPHS" --old-graphs "$OLD_GRAPHS" --output "$OUTPUT"
```

验证器对四配置逐参数核对433/184维构造器的同种子初始化权重，并嵌入同一 Python wheel 的 ORT 原库。有限真实帧逐端独立递推，
逐帧比较 `z_t` 与全部 `h_next`；实际选择路径比较支持集、精确前缀、适用分数和最终原意图。
实际图意图进入原共享执行并消费后继反馈；NoTrade 验证不造请求，预设 FOK/FAK 路径独立标为定向探针。
首个场景的4个后继组和撤单人口场景的2个后继组继续使用三端原会话与各自隐藏状态，
逐帧比较状态及适用下一决定；后继决定仅比较，不追加执行。状态、分数及重算比较均拒绝任一端 NaN／Inf。
同帧真实估费覆盖不适用及两腿不同 tick 的混合批次；参数调用不推进 GRU。
真实259对象人口核对三撤单族全局支持、完整分数（含尾块）与原身份；原259→257案例仍验证单腿关闭。
真实 Rust 前缀的采样及原路径重算比较联合 `log_prob` 和梯度，族内单对象路径不消耗随机数。

单独 cargo 默认忽略需要外部图产物的三端测试；上述命令显式启用 `python,ort` 并串行运行两项。
普通独立 crate 回归不需要图产物。撤单有限场景沿原夹具观察至56000，核对实际动作后继结果；
首PR扩展到该窗口后曾在396000之后的下一组遇到 `InvalidBucket`，该失败材料保留。
下节连续增量修正真实夹具尾部顺序并单独记录 C/P 与长期观察结果，首PR有限证据不替代它们。
目标主机尚未准备，本地结果不作为目标容量结论；不启动训练、GPU 或部署。


## 连续 C/P 本地验收宿主

`continuous-validation` feature 连接原真实 DPT 录制、有限公开消费、共享执行与结算入口。
C1 为单根，C2 为两个独立账户根；每个根分别运行交易／补偿与配对赎回、孤腿赢家／输家、
返佣日成熟三个固定变体。前两变体为 23:50 至次日 01:00、首市场 23:55；返佣变体为
23:45 至次日 00:55、首市场 23:50，均保持完整 70 分钟和后续五分钟市场接续。
经济前史执行明确指定的原意图，三端隐藏历史全程连续；其后的自主阶段执行各端实际图决定。各根在原前史真实N=2决定帧比较一次三端实际参数路径，
明确记为不提交意图的交接探针；该探针不推进额外GRU，也不计为自主执行决定。
独立经济期望覆盖预留、未终局量、成交失败补偿、部分成本、判定应收和付款、原身份现金修正、
返佣成熟与低额日，不从待测 NAV 反算期望。

输入深度沿 `random.Random(742000+j)` 在闭区间 `[100,200]` 抽取，顺序为三个变体再根索引；
重复、配置与线程共享同份业务录制和数值，账户根独立。该随机量确实进入公开盘口，固定经济量价不随机化。
状态图使用真实活动批次，C2／P7 形成 B=2；每端保存自己的全部层隐藏状态并逐实际帧比较。
参数步按未完成原行压缩和回填，使用同帧 z 与原精确 Actor 前缀，不推进 GRU。
第一块计算全部活动行，续块只计算仍有对象的撤单头，并保持原行位置和全局对象顺序；
同次前缀与费用特征在块循环外形成。非撤单头只返回首块，自定义分块仅用于完整对象归约和撤单候选轴。
完整人口的编码与 sum/mean/max/exists 归约仍供所有头消费；组件结果仅持有特征与归约，
不额外拼接保存整张对象编码。图的对象编码输出仍服务实际图比较和梯度验证。
`graph_fixtures.market_slice` 是手填历史的抽象数值探针，真实市场输入另由原生公共组验证。
P3—P7 覆盖真实完整人口 1／2／256／257、259 峰值、双根 `(1,257)`；纯图零对象探针另保留，并以同对象、同权重核对
`256+1` 与 `128+129` 分块、全局归约与分数、完成行、空集、稳定并列及条件概率梯度。
原首轮 P 已运行但缺 Rust 第二布局及独立资金核对；审查返工后四项 P 已重新通过。
Rust 两布局均实际执行首遍嵌入与次遍分数，逐根比较 sum/mean/max、每个对象分数和全局选择；
P7 保留空订单市场根的原索引与 B2；该根仅调用有支持的 Market 族，N257 根仍比较三族。36000 准备峰和56000取消后分别从原公共读取核对各族计数、
两腿活动份额、本金、零即时费、已应用2000现金及actual/maker可用与预留，不从张量反算。

每配置／线程使用一个独立根预热恰好 128 个真实帧，再在同进程长寿命会话与 allocator 上
各做三次完整观察。会话显式使用 CPU、ALL 优化、顺序执行、1／4 intra 线程、1 inter 线程、
关闭 spinning 和 memory pattern；CPU arena 保持启用。加载、预热、观察分别计时。
每跨过 128 个实际帧、市场退役、返佣成熟／付款及开始／结束时记录动态 RSS、allocator 可用统计、
持有引用、隐藏状态和对象数。若一组跨过采样边界，记录实际采样位置及所跨边界，不补造模型帧。
每组比较后释放临时张量；观察结束排空已接纳材料，再释放该观察根的模型引用，保留会话。
截止后不推进时钟获取未来事实，结束时仍持有的全部订单身份及其真实未知／未闭合状态、承诺剩余量随结果记录。

原长人口夹具的尾部失败来自截止 320000 的夹具仍交付 396000 探针，随后倒退交付终态。
探针现在受原观察截止约束，真实终态及后继组按因果顺序交付。另修复有限 PAPER 驱动的空档闭合：
已知下一真实组时刻 t 且所有 `<t` 义务处理完后，通过原 `CloseTime/NextGlobalEventAt(t)`
将前沿闭合到 t−1，再应用原组。t 本身仍开放；同毫秒不重复关闭，在线 PAPER 和隐藏路径不变。
这使原已接收但落在最后调度点与 t 之间的 Accepted 在首个 t 组可领取。真实反证核对 `<t`、`=t`、
`>t` 接收边界、同毫秒组间登记请求、到期请求晚于全部同毫秒行情，以及原背压／写故障传播。
新增 CloseTime 数量随排空日志计数，其开销包含在公开组测量内。

完整撤单排序在整集合具有原 SIM 公开材料时，使用 Accepted 的逐项有效处理时间、首次请求回执的
input_sequence 和原 ordinal；这与生产者真实形成源序保序，不使用整批响应时间或反馈到达顺序。
原形成反馈尚未到时，形成年龄与时间存在位仍保留缺失；不填造原源序或时间。
已有形成依据可由同一种完整材料覆盖时，使用该先后链；否则保留处理时间／回执链P与形成源序链S，
按两条链的先后约束作稳定拓扑排序，不直接混比两个数值域。无形成依据的对象按真实身份稳定保留，
不因此删除候选或将整集合改为Unavailable；Paired返回的两腿仍保持原 ordinal。
Taker Contribution 是独立经济事实，接纳不再额外等待该成交的 FinalSuccess 用户反馈。
贡献沿原原子应用、位置幂等和贡献去重通路进入 accrued；不强置成交成功、不提前配对或成熟 NAV。
MakerAccrualFinalized、DayFinalized、Payment 及真实账户经济前驱依赖保持。
有界真实回归在原173732000领取贡献并消费完整三端帧，173733000才领取迟到成功；
这两个切点仅比较参数、不提交新意图，用于定位，不替代完整自主 C 观察。

C 宿主逐组对照原录制的 group_position、实际交付时刻与市场窗口独立列举的合法行，
比较角色和 condition，截止内原组必须全部消费；不把期望帧数写成固定常量。
Native 返回 None，或实际合法行的角色／condition与原录制的独立期望不一致时，立即保留材料并失败。
真实经济等待本身不跳过合法帧，也不以settlement返回false作为失败门。
失败快照区分 before-exogenous、after-exogenous 与 after-native-consume，后两者重新领取真实 cutoff。
此前 v4 曾因贡献额外依赖遗漏合法帧，已全部停测，旧局部结果不拼接为完整验收。

验收宿主发生异常时保留该观察目录内实际写出的原 SIM／NOE／Ledger、最后已应用截止与失败阶段，
原始错误写入进程日志；异常产物不宣称已干净排空，停止后不继续读取后继事实。


以下均为显式 ignored 验收入口，普通 cargo 测试不会替代执行：

```bash
# 先以 CONTINUOUS_RECORDINGS 指定目录生成一次九份实际录制，后续只读复用。
cargo test --release --manifest-path model-train/native-inputs/Cargo.toml \
  --features continuous-validation --locked --offline continuous_prepare_recordings -- --ignored
# 每个 MODEL_GRAPH_DIR / MODEL_THREADS 独立进程，设置 CONTINUOUS_OUTPUT 为新证据目录。
cargo test --release --manifest-path model-train/native-inputs/Cargo.toml \
  --features continuous-validation --locked --offline continuous_three_runtime_matrix -- --ignored --nocapture
cargo test --release --manifest-path model-train/native-inputs/Cargo.toml \
  --features continuous-validation --locked --offline continuous_real_population_probes -- --ignored --nocapture
```

运行使用该工作树局部 Python 环境、原初始权重和两图（含实际外部参数文件），映射同一 wheel 的 ORT 库；
来源与二进制、依赖、录制和运行时摘要在初始化时冻结，动态采样不重复哈希大文件。
本地并行观察须附实际并发环境；描述性时间不是独占性能或目标 CPU 容量结果。
目标 m7i.2xlarge／c8a.2xlarge CPU 测量未执行，已按[用户决定](https://github.com/slowchillchill/btc-updown-marketmaker/issues/6#issuecomment-5754843215)延期，
不再阻塞本次任务1.5、PR #125及阶段1审查、合并与关闭；仍须完成整体独立审查、适用收尾及准确合并版本的阶段组合审查。
延期不等于已测或永久取消，不指定后续时间；本地结果不替代目标容量，不新增云主机，不进入训练、部署或下一阶段。
