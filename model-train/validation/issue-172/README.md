# #172 离线复跑

本目录的工具面向 [#172](https://github.com/slowchillchill/btc-updown-marketmaker/issues/172) 冻结的有限真实样本。生产代码不含这里的快照、反序列化或诊断计数。基线固定为 `e57503821391a9943ce694eabab00ec0fdff1141`；五输入、两图、完整档位和原精确投影保持。

`prepare.py` 复用 #170 已验证的无交易恢复夹具，生成独立源码副本；当前基线及单项差异由本目录覆盖，不能把 #170 的版本名或性能值套用到本轮。`cache`、`projection`、`combined`、`buffer` 是内部副本名，分别对应 `parse_once`、`time_rules`、`miss_dedup`、`scratch_capacity`；`history` 承载最终候选。旧 20 组首次筛选中的 `history` 指容量单项，原始值在结果中单独映射。`candidates.py` 生成可检查的单项补丁，不能直接向生产应用全部补丁。

已运行源码另有不可变快照：[正式宿主差异归档](evidence/source/formal-overlay.tar.gz)、[前史原生宿主差异归档](evidence/source/prehistory-overlay.tar.gz) 和 [内容身份](evidence/source/archives.json)。归档仅包含相对固定基线有差异的文件，路径为仓库相对路径；tar 的所有者与时间归零，源码内容保持原样。正式身份清单覆盖原生宿主和四个恢复依赖，前史清单只覆盖当时保存的原生宿主文件，二者范围不同。要重建测量时的正式源码，在新的空目录 `ARCHIVE_SOURCE` 展开固定基线，再叠加正式归档；要回查旧前史宿主，再叠加前史归档。归档恢复只用于独立离线副本。

```bash
git -C "$SOURCE" archive e57503821391a9943ce694eabab00ec0fdff1141 | tar -x -C "$ARCHIVE_SOURCE"
tar -xzf "$SOURCE/model-train/validation/issue-172/evidence/source/formal-overlay.tar.gz" -C "$ARCHIVE_SOURCE"
```

逐项源码 SHA-256、二进制 SHA-256 分别在 [formal-source-identities.json](evidence/source/formal-source-identities.json) 与 [prehistory-source-identities.json](evidence/source/prehistory-source-identities.json)。当前生成器、旧前史宿主和已测二进制各有独立身份，不能互相替代。

冻结正式副本中，未参与正式 example 构建的 `cfg(test)` 区域有一处诊断插桩误匹配。当前生成器已限制为首次实际调用；如从归档复跑定向测试，另应用 [测试修正补丁](evidence/source/directed-test-fix.patch)（`patch --fuzz=0 -p1`），其测试源码／二进制身份见 [directed-source-identities.json](evidence/source/directed-source-identities.json)。该补丁不追改已运行正式二进制的身份。

在仓库锁定的 Python 环境运行图工具；原生构建使用本地 Rust 工具链。下列路径参数由复跑者提供，不依赖作者工作站路径：

```bash
python3 model-train/validation/issue-172/prepare.py \
  --source "$SOURCE" --output "$LATENCY_OUT"
CARGO_TARGET_DIR="$LATENCY_BUILD" cargo build --release --features ort \
  --manifest-path "$LATENCY_OUT/native-harness/model-train/native-inputs/Cargo.toml" \
  --example benchmark_projection --example frame_latency --example action_fetch --locked --offline
```

公共录制、源配置、原生 468 组选择、1,024 帧原输入及两个代表图来自报告列明的原始产物。`NATIVE_SELECTION` 使用 #170 冻结清单：每分钟首 64 组共 320 组、旧 20 组、两类压力事件各 64 组。`NATIVE_DIAGNOSTIC=1` 只运行独立诊断；`NATIVE_SCREEN=1` 运行各单项的 8 线程两轮反序；不设置二者时运行最终原生矩阵，并单列各候选的 8 线程证据。进程累计 CPU 在完整调用时钟之外读取；完整组和共享状态比较也在计时之外。

仓库保留 [选择清单](evidence/selection/native-selection.json)、[公开组与源输入对应](evidence/selection/native-selection-sources.json) 和 [原选样属性](evidence/selection/native-selection-attributes.json)。属性文件中的历史候选计数原样保留，不用它替换本轮实际消费行数。

```bash
NATIVE_SELECTION="$NATIVE_SELECTION" RAYON_NUM_THREADS=8 \
  "$LATENCY_BUILD/release/examples/benchmark_projection" \
  "$CONFIG" "$RECORDING" "$NATIVE_OUTPUT" decisions 9999999
```

`graphs.py` 直接导入 ORT，不通过包初始化导入 Torch。`profile` 对原 133 帧做独立算子诊断；嵌套 Loop 子图与父节点不能相加。`action` 比较全部输出、仅 family、仅对象编码的真实执行节点集合，并开启 `only_execute_path_to_fetches`。`screen` 只改变归约 Loop 的整数形状控制，保持所有浮点操作次序，按两轮反序计时，计时结束后再核对数值；未保留该候选，因此没有扩大到 24 架构矩阵。

`action_fetch` 对 Rust 宿主实际动态加载的 C 库再做独立输出选择节点诊断，使用 `OutputSelector`，并记录 `ort::info()` 的实际构建信息。该接口未冒称设置 Python 的 `only_execute_path_to_fetches` 字段。其输出目录由 `FETCH_OUTPUT` 指定，原图和库仍由 `FRAME_GRAPHS`、`ORT_DYLIB_PATH` 指定。

```bash
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 python model-train/validation/issue-172/graphs.py \
  --mode profile --graphs "$GRAPHS" --packed "$PACKED" --output "$PROFILE_OUTPUT"
# mode 另可取 action 或 screen；输出目录独立。
```

`frame_latency` 是纯 Rust ORT 宿主。准备运行从原录制顺序恢复全部公共事实、账户和原生历史，对两代表各自递推每个模型行的全部层 hidden。普通段入口为 201582，高更新段入口为 418584。段入口派生状态通过离线 JSON 再构造；原顺序基线与独立还原根在两个 64 组片段逐组核对完整原生输出、历史、缓存和 Ledger 投影。动作仍只接受当前实例 `consume` 构造的组，不能从序列化的旧组制造 Actor。

```bash
FRAME_MODE=prepare FRAME_GRAPHS="$GRAPHS" FRAME_PACKED="$PACKED" \
FRAME_CHECKPOINTS="$CHECKPOINTS" NATIVE_SELECTION="$NATIVE_SELECTION" \
ORT_DYLIB_PATH="$ORT_LIBRARY" RAYON_NUM_THREADS=8 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  "$LATENCY_BUILD/release/examples/frame_latency" \
  "$CONFIG" "$RECORDING" "$PREPARATION_OUTPUT" decisions 9999999
```

正式片段的每个模型、版本和轮次都独立建立模拟执行器与账户，顺序重放公共前史及共享应用。到准确入口时核对账户投影并恢复已验证的无交易派生状态；此后独立执行原生构造、打包、逐行状态图、合法动作链和执行入口。交易中不恢复快照。小模型 ORT 为 4 线程，大模型为 16 线程；原生均为 8 线程。图加载和 16 次原真实帧预热单列，动作图另做空人口预热，预热不推进真实 hidden。正式整链从 `consume` 前开始，到适用动作完成为止；证据序列化、hidden 摘要和写文件在终点之后。

```bash
FRAME_MODE=baseline FRAME_MODEL=MAIN-1x256 FRAME_FIRST=201582 \
FRAME_GRAPHS="$GRAPHS" FRAME_PACKED="$PACKED" FRAME_CHECKPOINTS="$CHECKPOINTS" \
ORT_DYLIB_PATH="$ORT_LIBRARY" RAYON_NUM_THREADS=8 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  "$LATENCY_BUILD/release/examples/frame_latency" \
  "$CONFIG" "$RECORDING" "$CONTINUOUS_OUTPUT" decisions 9999999
```

对 `MAIN-1x256`、`MAIN-30x1024` 与两个入口执行基线／候选各两轮，第二轮反转模型、片段及版本顺序；`FRAME_MODE=candidate` 选择最终候选。所有 CPU 正式计时串行，避免并发构建或性能任务。每 15 分钟查看长运行结果。准备成本、真实到达间隔、完整服务时间和离线积压算例分别报告；短片段不构成在线容量验收，分阶段分位数不得相加。

`run.py --mode continuous` 将上述 16 次独立运行固定为两轮反序，并为每次保存退出状态、UTC 时间及 `/usr/bin/time -v` 资源结果。`--mode native` 驱动正式原生矩阵。它不覆盖已有证据目录，某次失败立即结束串行队列，保留该次原始输出。

```bash
python3 model-train/validation/issue-172/run.py --mode continuous \
  --binary "$LATENCY_BUILD/release/examples/frame_latency" \
  --config "$CONFIG" --recording "$RECORDING" --output "$CONTINUOUS_RUNS" \
  --graphs "$GRAPHS" --packed "$PACKED" --checkpoints "$CHECKPOINTS" \
  --ort-library "$ORT_LIBRARY"
python3 model-train/validation/issue-172/summarize.py --mode continuous \
  --source "$CONTINUOUS_RUNS" --output "$TABLES"
```

`summarize.py` 另有 `native`、`diagnostics`、`reuse` 三种复算入口，分别处理原生逐调用分布、包含关系明确的独立诊断和按精确价格关联的 Binance 九字段复用观测。完整服务复算会核对每个模型、片段的两版本两轮完整原生组、hidden 摘要、动作路径、实际意图和执行回执一致；输出逐调用表保留组号、所有行角色和形状、每步动作成本。到达积压只计算“段入口队列为空、单服务者、同 64 组”的离线 FIFO 算例，不补造段外吞吐。

逐调用 CSV 使用 gzip 保存，`gzip -dc <文件.csv.gz>` 可直接还原原始整数纳秒与身份字段。`--mode details --source "$TABLES" --output "$TABLES"` 只从公开的 `continuous-calls.csv.gz` 复算阶段、每类动作步和到达间隔，无需再次加载大原生组。正式 `continuous` 复算也自动生成这些明细。

生成副本中的 `frame_probe::directed_frame_action_costs` 是纯 Rust ORT 的定向业务对象夹具。通过已有真实录制入口和实际执行器构造 0／1／86 对已提交订单，保留市场级撤单对象，分别触发无交易、多步买单及三种撤单家族；每次使用独立账户。它的输入人口和强制合法选择明确标为夹具，不并入市场自然动作分布。即使没有订单，当前合同仍有一个市场级撤单对象，不能把这个业务状态伪称为图的零对象输入。

下面分别令 `MODEL=MAIN-1x256, THREADS=4` 和 `MODEL=MAIN-30x1024, THREADS=16` 运行，各自使用独立 `DIRECTED_OUTPUT`；每次内部包括两轮反序。公开的空 mask 图探针与实际业务对象成本分列。

```bash
MODEL_GRAPH_DIR="$GRAPHS/$MODEL" MODEL_THREADS="$THREADS" \
DIRECTED_OUTPUT="$DIRECTED_OUTPUT" FRAME_PACKED="$PACKED" \
ORT_DYLIB_PATH="$ORT_LIBRARY" RAYON_NUM_THREADS=8 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
CARGO_TARGET_DIR="$LATENCY_BUILD" cargo test --release --features ort \
  --manifest-path "$LATENCY_OUT/native-harness/model-train/native-inputs/Cargo.toml" \
  --lib directed_frame_action_costs --locked --offline -- --ignored --nocapture
```

生产回归与性能运行分开。Python 测试通过当前生产 Rust 入口生成有效业务夹具；传入两个原图目录后启用真实 PyTorch、Python ORT、Rust ORT 顺序对照。Python 原生扩展如需重建，在 `model-train` 使用仓库锁定环境的 `maturin build --release --features python --locked --offline --interpreter .venv/bin/python`，再以 `uv pip install --offline --no-deps` 安装该本地产物，保留 wheel 身份。

```bash
NATIVE_MAIN_GRAPH="$GRAPHS/MAIN-1x256" \
NATIVE_BASELINE_GRAPH="$GRAPHS/BASELINE-2x256" \
NATIVE_COMPONENT_WEIGHTS="$GRAPHS/MAIN-1x256/initial_weights.pt" \
NATIVE_VALIDATION_OUTPUT="$VALIDATION_OUTPUT" OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  "$PYTHON" -m unittest discover -s tests -v
PYO3_PYTHON="$PYTHON" LD_LIBRARY_PATH="$PYTHON_LIBRARY_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
cargo test --manifest-path model-train/native-inputs/Cargo.toml \
  --features continuous-validation --lib --tests --locked --offline
cargo test --workspace --lib --bins --tests --examples --locked --offline -- \
  --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
```

第一条在 `model-train` 目录运行；后两条在仓库根运行。`PYTHON_LIBRARY_DIR` 从该锁定解释器的 `sysconfig.get_config_var("LIBDIR")` 读取，只加入本次命令环境；启用 Python 嵌入的 Rust 测试需要找到实际 `libpython`。固定工作区命令明确跳过具名 UTC 时钟测试，不能将其记为通过。原生静态检查使用 `--all-targets --features continuous-validation --locked --offline -- -D warnings -A clippy::needless_borrow -A clippy::too_many_arguments`；两类排除沿用基线已有告警，不因此改动无关实现。实际运行与通过数量见报告的回归记录。
