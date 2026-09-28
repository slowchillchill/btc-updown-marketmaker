# #174 完整输入跨帧复用：验证入口

本轮基线固定为 `43765885dedd2593abdbdde868da3d64ed6fb646`（PR #173 合并）。使用真实原普通组 `201582—201645` 与高更新组 `418584—418647`；所有原始录制、图、权重和已验证无交易前史沿用 #172 已冻结身份。新派生缓存不在旧检查点内，入口第一次 `consume` 冷建并计时，随后连续推进。

缓存仅保存神经编码前的当前侧精确解析和完整九维结果。变化识别仍扫描冻结十进制输入与所选锚点；结构变化建立一次临时索引，不能声称普遍 O(变化档位)。数量差值、最佳价差值及历史继续按每次真实调用推进，最佳价改变仍使用原精确距离投影。slow 每组由冻结账户重新编码，h_prev 每次来自实际全部层 hidden。

已发布侧使用不可变共享存储；Python 默认 `features()` 序列化保持，`shared_books=True` 仅改变进程内传输表示。Rust 同步 ORT 使用私有补齐存储与借用输入；PyTorch 任何已发布张量都不原地更新，变化时保留必要复制，覆盖较早前向延后反传与 detach 持有的情形。

所有性能运行串行，每 15 分钟查询。报告准备、构造、打包、状态与动作及真实执行回执成本；有限片段不代表在线容量验收。原生普通窗口的候选全窗口均值仍约 6.03 ms，不能用中位数低于 4 ms 宣称目标达成。

## 驱动与复跑参数

`prepare.py` 从固定基线的 Git archive 建立两份独立源码目录；只复用 #172 中未变化的四个业务依赖的离线恢复补丁。它不会修改源工作树。候选另含独立基线原生模块，用于同一公共事实下的逐位对照，基线正式服务则使用单独基线二进制。`source-identities.json` 记录实际生成源码；保留生成目录及执行文件，不能用后续编辑后的源码替代运行身份。

下面的 `ARTIFACTS` 是本机保留产物根参数。`SOURCE` 指向当前源码，`OUTPUT` 必须选取尚未存在的新一轮目录。配置、录制、图、原始打包输入和两个检查点均为只读输入。

```bash
SOURCE="$PWD"
ARTIFACTS=/path/to/retained/artifacts
OUTPUT="$ARTIFACTS/issue-174-performance/reproduction"
CONFIG="$ARTIFACTS/realmarket-review-20260926/input-trace-r2/configuration.json"
RECORDING="$ARTIFACTS/realmarket-review-20260926/input-trace-r2"
CHECKPOINTS="$ARTIFACTS/issue-172-performance/checkpoints"
GRAPHS="$ARTIFACTS/issue-170-performance/final-graphs"
PACKED="$ARTIFACTS/realmarket-review-20260926/sampled-inputs"

python3 model-train/validation/issue-174/prepare.py --source "$SOURCE" --output "$OUTPUT/harnesses"
python3 model-train/validation/issue-174/build.py \
  --harnesses "$OUTPUT/harnesses" --build "$OUTPUT/build"

python3 model-train/validation/issue-174/run.py --mode native \
  --candidate-binary "$OUTPUT/harnesses/binaries/native_incremental" \
  --config "$CONFIG" --recording "$RECORDING" --checkpoints "$CHECKPOINTS" \
  --output "$OUTPUT/native"
```

完整服务在原生筛选证明正确且拟保留实现后执行。`ORT_LIBRARY` 指向已核对的原 ORT C 动态库；线程参数由驱动固定为原生 8、小模型 ORT 4、大模型 ORT 16。

```bash
python3 model-train/validation/issue-174/run.py --mode continuous \
  --baseline-binary "$OUTPUT/harnesses/binaries/baseline-frame" \
  --candidate-binary "$OUTPUT/harnesses/binaries/candidate-frame" \
  --config "$CONFIG" --recording "$RECORDING" --checkpoints "$CHECKPOINTS" \
  --graphs "$GRAPHS" --packed "$PACKED" --ort-library "$ORT_LIBRARY" \
  --output "$OUTPUT/continuous"
python3 model-train/validation/issue-174/summarize.py --mode native \
  --source "$OUTPUT/native" --output "$OUTPUT/public-native"
python3 model-train/validation/issue-174/summarize.py --mode continuous \
  --source "$OUTPUT/continuous" --output "$OUTPUT/public-continuous"
```

`build.py` 先执行独立基线对照，再依次编译候选与基线；共享构建缓存时先复制候选执行文件，避免后续基线构建覆盖其运行身份。`run.py` 又将实际执行文件复制到本轮运行目录，并记录 SHA-256。计时运行间不并发编译或其他性能任务。

`summarize.py` 复用 #172 的标准库分位数与 CSV 输出函数。原始服务记录包含完整输入、共同业务状态、hidden/z 位摘要和实际动作/回执；公开逐调用表保留一致性摘要、各阶段时长和缓存容量，不复制本机路径。分位数采用排序后线性插值。`entry` 表示首组，`continuation` 表示随后 63 组：候选新增缓存首组为空，基线则恢复其既有投影缓存，两者不能都称作完全冷态。首组始终包含在 `all` 统计内。

## 正确性与依赖身份

锁定 Python 环境准备完成后，下面的检查驱动依次构建并安装当前扩展、验证共享输入延后反传、两代表的 PyTorch／Python ORT／Rust ORT、真实撤单反馈、Python 与原生回归、Clippy、格式和固定工作区命令。动态库必须与 `ORT_LIBRARY` 为同一已冻结构建；检查驱动由环境内 ORT 包定位它。`checks.py` 会设置嵌入 Python 所需的进程内路径，不修改系统环境或全局配置。

```bash
python3 model-train/validation/issue-174/checks.py --source "$SOURCE" \
  --output "$OUTPUT/validation" --build "$OUTPUT/build" --graphs "$GRAPHS"
python3 model-train/validation/issue-174/freeze.py --source "$SOURCE" \
  --harnesses "$OUTPUT/harnesses" --graphs "$GRAPHS" --packed "$PACKED" \
  --recording "$RECORDING" --config "$CONFIG" --checkpoints "$CHECKPOINTS" \
  --ort-library "$ORT_LIBRARY" --output "$OUTPUT/harnesses/provenance.json"
```

图、权重、检查点等大产物保留在原专用目录，不重复提交。`freeze.py` 与 #172 已公开的内容身份逐项核对；其原生基线仍是本轮 `4376588`，没有将 #172 的 `e575038` 驱动当作新基线。检查中的具名 UTC 测试明确跳过，原图专项忽略项和显式执行的结果分别报告。

## 证据归档与复算

本次实测保留目录为 `target/issue-174-performance/prototype-r3`（生成源码与执行文件）、`native-r1`、`continuous-r1` 及 `validation-r1`。早期编译/环境尝试同样保留，但未计入正式性能数据。`publish.py` 将两份实际生成源码相对准确基线的差异存为确定性 tar 归档，并导出验证和运行身份；源码未变的部分通过基线提交重建。执行文件继续保留在运行目录，归档不含本机认证或私有原始日志。

```bash
python3 model-train/validation/issue-174/publish.py --source "$SOURCE" \
  --artifact-root "$ARTIFACTS" --harnesses "$OUTPUT/harnesses" \
  --validation "$OUTPUT/validation" --native "$OUTPUT/native" \
  --continuous "$OUTPUT/continuous" --output "$OUTPUT/public"
```

将 `evidence/source/baseline-overlay.tar.gz` 或 `candidate-overlay.tar.gz` 解压到基线 `4376588` 的独立副本，即得到对应实际测量源码；全部文件摘要见 `source-identities.json`，不是后来按描述重写的一份近似驱动。完整服务的实际源码分别使用原基线打包与候选的私有补齐存储。

公开逐调用 CSV 使用 gzip 压缩，时长字段单位为 ns，复合字段是 JSON 字符串。完整服务按 `run` 分组，原生按 `segment`、`variant`、`round` 分组；`entry_call` 标识入口。下面示例无需私有录制即可复算完整服务全部均值与 P50；其他分位数使用 `summarize.py` 引用的同一线性插值函数。

```python
import csv, gzip, statistics
from collections import defaultdict

runs = defaultdict(list)
with gzip.open("model-train/validation/issue-174/evidence/continuous/continuous-calls.csv.gz", "rt") as stream:
    for row in csv.DictReader(stream):
        runs[row["run"]].append(int(row["service_ns"]) / 1e6)
for name, values in sorted(runs.items()):
    print(name, len(values), statistics.fmean(values), statistics.median(values))
```

完整输入/业务/hidden/回执一致性的摘要列用于关联相同结果，不能代替重新执行独立基线。需要复验语义时使用保留录制、两份冻结源码和本页驱动；需要复算性能分布时公开逐调用表已足够。
