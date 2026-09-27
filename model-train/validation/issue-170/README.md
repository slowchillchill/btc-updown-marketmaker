# #170 离线基准复跑工具

这些补丁仅供 `model_train.prepare_native_benchmark` 在指定输出目录建立基准副本，不能应用到生产工作树。生产实现位于 `native-inputs/src/market.rs` 和 `model_train/book.py`。基准副本保留旧实现、缓存单项、投影单项、两者组合、再加历史复用五种版本。

复跑使用本次实现提交的源码和 `provenance.json` 对应的录制、输入、图及权重。基线固定为其中记录的提交，不能将未来 `main` 的不同实现与本次原始结果混用。

公共属性扫描使用原公共录制和无交易模拟账户，沿真实公共前史恢复观测。选样器按每分钟首 64 个合格目标帧和两类输入变化峰值选取片段；旧 20 帧限定为市场开始前 30 秒的合格上下文。选择依赖原帧属性，不读取耗时。`--group-index` 指向同一录制生成的组索引，用组身份和关闭输入位置建立来源映射，不能仅比较不同录制的局部序号。

```bash
python -m model_train.prepare_native_benchmark --source <源码根> --output <产物目录>
CARGO_TARGET_DIR=<专用构建目录> cargo build --release \
  --manifest-path <产物目录>/native-harness/model-train/native-inputs/Cargo.toml \
  --example benchmark_projection --locked --offline
NATIVE_SELECT=1 RAYON_NUM_THREADS=8 <基准二进制> \
  <配置JSON> <公共录制根> <产物目录>/selection-pass decisions 9999999
python -m model_train.select_native_benchmark <产物目录> --group-index <同录制组索引>
NATIVE_SELECTION=<产物目录>/native-selection.json RAYON_NUM_THREADS=8 <基准二进制> \
  <配置JSON> <公共录制根> <本轮输出目录> decisions 9999999
# 单独验证真实连续段：00:04:30 起上下文及开市后的 32 个决策行。
NATIVE_CONTINUOUS=1 RAYON_NUM_THREADS=8 <基准二进制> \
  <配置JSON> <公共录制根> <连续验证输出目录> decisions 32
```

测量时基线完整顺序恢复原生根状态。选中组前复制原生根、Ledger 内存派生状态、registry、消费游标和 pending；各候选从调用前状态恢复，复制与转换不计入耗时，计时包含完整 `consume` 和基准副本的 Rayon 池分派。市场接纳等 NOE 记录仍会在无交易夹具内到达；恢复后在计时内重新执行真实领取、Ledger 应用和市场登记，并与原始首调用比较领取／登记次数及账户投影。原 Ledger 录制回调保留；这些反复执行的诊断录制含同组重复调用，不能作为新的权威交易录制。消费游标恢复不回滚后台耐久确认。每次调用核对完整输出的序列化位表示、下一帧缓存键与 f32 位、六条历史的值／身份／时间和 hidden／reset；S1 派生缓存另与旧公式重算值逐位比较。快照和恢复的逐调用开销单列，不计入生产推理收益，不相加冒充端到端时延。该方法不代表有交易账户的共享状态恢复，也不提供生产快照接口。

`benchmark_optimization` 用 ORT 推理。正式矩阵与最大值诊断按模块入口启动，会经包初始化导入 PyTorch，但不执行 PyTorch 推理；独立内存诊断须用下列直接脚本入口，不导入 PyTorch，并在入口拒绝已导入的环境。`screen` 使用既有固定 133 帧，`matrix` 使用原 1,024 帧并执行两轮反序。每个进程先构造输入，再加载图、做 16 次覆盖最大形状的预热；计时仅 `session.run`。`memory` 记录加载、预热、推理阶段的 RSS 和生命周期峰值；不能将两个生命周期峰值相减当作激活内存。`outlier --diagnostic-from <正式结果JSON>` 在正式计时结束后，对最大值帧和普通帧分别重复 16 次，将系统计数与正式时延分开。

以下在 `model-train` 目录、使用锁定依赖的 Python 环境运行。`benchmark_state export` 从原图目录读取原配置和同一归档权重，仅用当前实现重新导出；`--config '*'` 同时生成两个 BASELINE 回归代表和 24 个 MAIN 配置。`screen` 和 `matrix` 的驱动会串行创建独立 ORT 子进程。

```bash
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 python -m model_train.benchmark_state export \
  --graphs <原图目录> --config '*' --output <候选图目录>
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 python -m model_train.benchmark_optimization matrix \
  --mode matrix --baseline <原图目录> --candidate <候选图目录> \
  --packed <原1024帧输入目录> --output <矩阵输出目录>
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 python model_train/benchmark_optimization.py run \
  --mode memory --config MAIN-30x1024 --threads 8 --version candidate \
  --graphs <候选图目录> --packed <原1024帧输入目录> --output <内存结果JSON>
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 python -m model_train.benchmark_optimization run \
  --mode outlier --config MAIN-30x1024 --threads 16 --version candidate \
  --graphs <候选图目录> --packed <原1024帧输入目录> \
  --diagnostic-from <同配置线程候选正式结果JSON> --output <异常诊断JSON>
```

内存对照按两个代表模型的基线／候选分别调用 `memory`。最大值诊断保留完整 1,024 帧预构造输入驻留，仅对原正式最大值帧和普通帧各测 16 次；所选正式结果的身份、驻留帧数与预热列表保存在输出中，不回填正式样本。本轮较早只驻留 32 次输入的诊断另存，按完整驻留条件补测后单独报告。

连续验证在段入口仅转换一次调用前原生状态，此后候选独立推进历史和缓存，逐组对照完整输出及下一状态；结果单列，不进入正式计时统计。初始无行的根隐藏重置操作也保留在输出，供原三端消费者执行相同连续输入。

如需复现本次独立原生资源诊断，将选样 JSON 限定为其中带 `old20` 标记的 20 组，并在连续验证命令中同时设置 `NATIVE_RESOURCE=1 NATIVE_SELECTION=<这20组的JSON>`。它只比较基线与最终组合、四个线程数、两轮反序，共 320 次额外调用，连续段仍由独立候选推进到指定决策行数。计时外通过现有锁定版本的 `libc` 读取 `CLOCK_PROCESS_CPUTIME_ID`，保存调用前后累计 CPU 纳秒和增量，包含进程全部线程；基准副本增加该直接依赖，生产依赖不变。它不补写原正式运行缺失的 CPU 值，也不替换其时延。

所有 CPU 计时串行运行；长测按每 15 分钟查看一次结果。输出为有限真实片段／真实帧抽测，不能解释成完整五分钟连续市场推理或在线容量。
