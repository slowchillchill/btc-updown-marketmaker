# model-train 纯计算组件与两图

`model_train.components` 保留特征编码器、标准裸 GRU、残差 GRU 和 Actor 分数组件。
`model_train.graphs` 将它们组合为阶段 #6 的状态图与参数图；数学结构来自
[#5 技术 §8](https://github.com/slowchillchill/btc-updown-marketmaker/issues/5#issuecomment-5565139746)，
张量与两遍撤单协议来自
[§14.2.1](https://github.com/slowchillchill/btc-updown-marketmaker/issues/5#issuecomment-5565141870)。
最终特征索引见 [#6](https://github.com/slowchillchill/btc-updown-marketmaker/issues/6) 正文及两条正式续篇。

状态图输入为 `fast[B,836]`、`slow[B,412]`、`h_prev[L,B,H]`，返回 `z_t` 与全部层 `h_next`。
参数图输入规则30维、前缀60维、对象129维、布尔掩码及全局归约，返回六个固定头和逐对象表示/分数。
参数图无 GRU，不改变隐藏状态；价格轴9999，数量分支2，变体4，post_only 2，有效期3，动作族6。

四配置及初始化种子固定为 `BASELINE-1x256:741001`、`BASELINE-2x256:741002`、
`MAIN-1x256:741003`、`MAIN-30x1024:741004`。构造顺序为状态、规则、前缀、对象、汇总编码器、Actor；
各支路独立权重，两遍对象计算共享一个编码器。首次运行保存一份 `initial_weights.pt`，后续加载它，
不根据验证结果更换种子。模型文件与环境仅保留在被忽略的目录，不提交 Git。

## 两遍撤单宿主

`encode_all` 按宿主既有稳定顺序遍历全部对象；逐对象 float64 累积 sum，精确整数计数后求 mean，
max 用 `torch.max(dim=0)` 保留并列首项梯度。空集合不求 mean/max，汇总网络之后继续归零。
`score_all` 接收所选族的掩码，返回每行全部真实对象分数，供一次全局 `Categorical` 或严格最大值选择。
`active_rows` 只调用有待评分的行，并保留行映射；没有活动行不调用。

按[已批准的故障修复](https://github.com/slowchillchill/btc-updown-marketmaker/issues/6#issuecomment-5725045538)，
两遍非空逻辑块均以零特征、假掩码补至256槽，返回后按逻辑长度修剪。`empty` 来自宿主已知完整对象数量，
不是对设备上的掩码求布尔值。完整集合为空仍传 K=1 全假。逻辑分块不改变对象总数、顺序、身份或全局选择；
它也不改变图的动态 K=1—256 接口。比如 N257 的256+1与128+129均执行两个256槽物理块。
训练归约和第二遍评分都保留到同一对象编码器的梯度；补齐不保证训练总内存与对象数无关。

## 固定技术切片

`graph_fixtures.py` 只生成有限已知截面的张量和逐槽说明，不是公共经济投影、账本、撮合或完整支持集实现。
它不读取原始数据、不推进经济事实、不调用 `has_completion`，不用费用上界冒充精确费用。
没有对业务值随机抽样；四配置使用同一份固定切片。

| 输入 | 切片及掩码依据 |
| --- | --- |
| 市场836/规则30 | 市场已开盘60秒；A/B盘口买0.4卖0.6、每侧一档100份，直接BBA同值；Binance买59999卖60001；正式锚点/Chainlink现货60000；未观测成交/TWAP/判定不填造数值，组内仅一个现货事实；参考历史只一个样本。tick均0.01、LIMIT最低5份、市场BUY最低支出1、延迟1秒启用、费率0.07指数1且仅Taker。完整规则30维同时逐值进入fast末30维。 |
| 账户412 | 初始2000 pUSD、零持仓/成交、空旧市场集合，全部订单均尚未成交。已知结构性零保留exists/applicable；空订单集合的最早/最近年龄不适用。只有post-only GTC本金占用/承诺/开放订单数变化；尚未发生费用或返佣，两种NAV仍2000。 |
| 撤单129 | 每原pair为A/B各5份，原价0.2/0.3，post_only=true；在截点前1秒接纳。Single保留真实腿；Paired按原ordinal；Market精确汇总所有真实可撤成员。GTC的spend/GTD到期及非本族模板不适用，真实零费有存在位。 |
| 前缀60 | Family未选、三个撤单族已选，以及q=5双腿GTC的逐腿Variant/PostOnly/Price评分前切片。没有伪造完整数量支持或最后意图。 |

P骨架为完整 N0；原pair取消A后 Single(B)+Market=N2；85pair生成N256；86pair生成峰值N259，
取消A后N257；另有B=2的(0,257)。不存在将单个Single称为完整集合的场景。
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

上述纯图交付不包含真实经济特征接线、完整精确支持集、三端最终意图一致性、70分钟C场景、长期内存、
目标CPU/GPU测量、训练、部署或资金动作；不关闭任务1.5或阶段#6。非零FOK/FAK费用证明仍属原剩余依赖。


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

每配置每线程18个场景覆盖B1/2/3各12步、K1/2/128/129/255/256、六种完整对象集合及混合空行、
合法GTC前缀和活动行0/2及空活动集合。N257的256+1与128+129在每端均比较逐对象分数、归约和严格首项argmax；
空集合仍K1全假，非空两遍每逻辑块补到256再修剪。原分块结果不能只以归约相近替代选择一致。
所有数值比较以PyTorch为rtol参考、拒绝非有限值；索引与行映射要求精确相等。
证据摘要见 `validation/rust-ort-report.json`，原始大输出保存在本次任务产物目录，不提交Git。
这些结果仅证明固定纯图技术切片三端一致，不代表三端经济最终意图、目标CPU/GPU容量或阶段完成。

## 原生特征与状态、参数组件输入

`native-inputs/` 是独立 Cargo 工程，消费原公共组、`SharedExecutionV1` 与同组
`ActionBeforeReadV1`，实现 #6 正式续篇的 412 槽账户输入和 129 槽撤单对象。
它从根起点、真实市场窗口与公共观测确定预热、当前和旧市场范围；旧集合取完整
Ledger 市场并集，因此仅有本地预留的旧市场也参与汇总。根操作由 Rust 产生，Python
只执行 `ResetRootHidden`、`InitializeZero`、`Retire` 的顺序。

所有组内行借用同一个动作前读取。金额、数量、计数与旧集合先在精确域计算，再用
有理数区间确定自然对数的 float32 最近偶数舍入；不先经 f64 或截断 Decimal。
撤单对象直接保留原集合顺序、真实腿、原 ordinal 和逐身份资金，不另算资格。
关联未齐或撤单全集仍为 `Unavailable` 时返回 `MissingAssociation`，不输出成功组、
推进 hidden 或替代为 NoTrade。调用方不得把这一结果解释成新的重试或跳组策略。

`NativeInputGroup` 现在从同一公共帧编码完整 836 市场输入、412 账户输入和 129 撤单对象。
30 槽规则只生成一次，逐值复用于 `fast[806..836]` 和参数图。动态 tick 来自真实 A/B
资产字段；公共冷规则保留精确来源，参数价格用同帧 tick 乘原整数，不用神经投影反解。
`PrefixState` 持有原 BigUint 数量区间/叶子及真实腿、变体、post_only、寿命、价格整数，
编码 60 槽；它不生成合法支持集、不采样动作，也不以固定前缀证明完整 BUY 费用支持。
实际布局、来源、单位、变换与缺失规则见 `native-inputs/feature_schema.json`，业务合同仍为 #6。

市场编码只读公共参考/TWAP、盘口、直接 BBA、完整最近成交及本组全部原成交。
全侧统计包含第五档之外的数量；组内统计使用固定累积量，不保存整组成交副本。
时间来自字段关联的直接锚点，整数求差后投影，保留缺来源时间及负差；不读 dependency
或 lineage 补时间。当前 Binance 公共输出只有整本锚点，因此其合法价量和相邻帧净变保留，
逐档 T5 未知、侧 E9 为空；整本时间仅用于事件统计，不冒充档位来源或中价历史身份。

每个原 hidden key 仅增加合同规定的六条最多 16 样本历史及前帧侧统计。按源事实身份变化
追加，同值新事实可追加，无关组和参数调用不追加；缺失不追加历史，而前帧侧未知会阻断
盘口净差。历史随原 Retire 释放，开市沿用预热。RMS 先精确求增量平方和，复用原有理区间
投影，以 sqrt/log 两端最终同舍入确定 f32，不增加预算、精度拒绝或业务门。

`NativeComponentConsumer` 机械执行根操作，用真实 fast/slow 对每帧调用原 Bare/Residual
状态图一次，保存全部层隐藏值。整组完成后，参数组件使用冻结 z_t、rule、原生 prefix 与
完整对象归约，调用参数图不会再次推进 GRU。对象截点可单独通过 `objects` 消费；缺少前序
hidden 的对象测试不伪装成完整连续帧。Rust ORT 原验证程序增加 `--native` 入口，读取同一
原生组、机械执行根操作并独立递推状态；Python ORT 也用自己的 h_next 独立递推，同
PyTorch 及 Rust ORT 使用同一真实序列和原容差。三端各用自身状态图的 z_t、真实冻结规则与撤单人口，
独立计算对象嵌入及全局 sum/mean/max，再比较固定前缀当前字段的适用参数头；
同时核对参数调用后的 hidden 不变。每个配置包含 12 个状态帧、9 次对象归约和 45 次参数输出对照，
不代表完整 Actor 选择链。复用原归档图与权重，不重新导出或训练。

本增量完成真实特征与状态/参数组件输入；完整 Actor 合法选择链、一般非零费用 FOK/FAK
支持、最终意图的两图业务验收及阶段 #6 通过仍未完成。

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
PendingApplied/成功/补偿、QuoteBudget 三态、真实撤单人口 N=0/2/256/257/259 和不同分块。
缺 formed 的年龄测试沿既有 registry 入口重放真实 consumed 引用，明确属于缺源组件读取；
46k 未齐关联也单独验证为组合未完成，两者均不声称连续合法模型组。

新增字段验收覆盖远档数量、前帧未知、值缺但直接时间存在、负差与多锚点、原成交缺量/方向、
同值新确认/无关组/16 样本、Binance 只更新一档时不冒造其他档时间、两腿独立 tick、前缀
未选/false/首类别/0.5/B腿/数量叶子，以及真实裸 GRU 两层和残差 GRU 的逐帧递推。
固定前缀只用于实际参数组件验收；参数图没有完整合法选择宿主，不能视为模型动作验收。

## 给定撤单族后的执行接线

`NativeInputGroup::execute_selected` 接收调用方已经选定的撤单族或 `NoTrade`。
它从本组 `action_before.cancel_objects()` 读取完整资格与稳定顺序，沿原 129 行索引消费
`NativeComponentConsumer.cancel_scores` 返回的参数图原分数。各 `CancelScoreBlock` 按完整人口顺序
覆盖全部实际行，`valid_len` 排除块内 padding；跨块比较严格更大的分数，并列保留原首对象。
单笔恢复原平台订单 ID，配对保持原 ordinal 0/1，市场对象保留原 condition_id，分别形成既有
`CancelOrder`、`CancelOrders`、`CancelMarketOrders` 后直接交 `SharedExecutionV1::execute`。
同组后续调用仍使用原冻结对象，竞争结果由原反馈确认；不复制对象资格或执行状态。

`NoTrade` 传原 `None`，不调用参数图、不生成请求或撤销旧挂单。状态帧先由既有消费者推进一次；
后续撤单参数调用仅复用该帧 z、规则与完整对象归约，不推进 GRU。这里不计算六族支持集、
训练采样或联合概率，也不交付 BUY 装配、完整 Actor 或阶段验收。

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
