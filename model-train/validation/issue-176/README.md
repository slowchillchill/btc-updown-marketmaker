# #176 复现与比较边界

实际结果、未达目标及验证限制见 [工程验证报告](report.md)，全部跳过项见 [跳过清单](skipped-tests.md)。

主矩阵固定基线 `a39f2cc2bb160c49fd53dba1a106364b0666f7c3`（包含 #174）及其旧策略图。
候选是本轮冻结提交与新策略八图；MAIN-1×256/MAIN-30×1024、普通201582—201645/高更新418584—418647，
各64组，两版本、两轮反序，共16次串行运行；原生8线程，ORT分别4/16线程。两策略各自闭环执行，
动作及后续账户/负载变化另列，不把全部差异归因于内核。复用原录制、前史入口和已归档模型，正式计时不并发编译。

同新策略、同权重、同固定前缀的有限纯计算探针在首次测量前固定为：两种 MAIN 配置，各四个切片
（B1/P99、B1/P999、B1/P11原坐标19—29、B2/P11同区间）。沿固定已知q=5普通GTC价格前缀，
第三/四片只检验给定合法区间的计算，不用人工切片证明支持规则。每片每路径记录首次调用一次、预热8次、正式32次；
参考逐阶段重复准备、计算999个价格再取行，候选共享准备、先取有效行再评分。两路径顺序反转一次仅用于
上述有限探针，不扩展成第二套真实闭环矩阵。两次顺序分别保留，不合并首次/热态结果；首次调用发生在模型已加载、候选共享准备之后，不代表冷进程初始化。

另一个原生 release 进程在真实入口的0.01→0.001两帧各固定q=5普通GTC B腿价格阶段，记录每档首次网格/有效索引准备，随后预热8次、测32次支持集、索引和费用特征；旧 Actor 仍保留99档。该小探针不扩展闭环矩阵。

正确性先检查原生真实链的普通GTC费用、FOK最低本金及反馈、混合类型、低于mos原样拒单、tick变化与原值，
以及16路完整覆盖/编码边界。随后核对PyTorch、Python ORT、Rust ORT相同模型的数值容限和完整业务意图；
共享计算/重复计算对照核对梯度值、`.grad`为None或零的状态及有限一步AdamW结果，保留延后反传和旧帧所有权。
完整联合熵预算使用独立完整动作样本，不以单路径节点熵和代替。

状态图沿v5数学；新图/路径归属 `native_inputs_v6/native_actions_v2`。仅进行工程验证，不启动正式训练、
收益竞赛、GPU、256路候选或部署。具名 UTC 时钟测试在固定工作区回归中明确跳过。

## 复跑入口

从仓库根运行，`SOURCE` 为冻结候选源码、`OUTPUT` 为新的专用目录。`GRAPHS` 下的 `baseline/`
引用 #170 原 MAIN 图归档、`candidate/` 引用本次对应 MAIN 八图归档；不覆盖原产物。
配置、录制、打包输入、ORT库及两个前史检查点沿原只读产物提供。

```bash
python3 model-train/validation/issue-176/prepare.py --source "$SOURCE" --output "$OUTPUT/harnesses"
python3 model-train/validation/issue-176/build.py --harnesses "$OUTPUT/harnesses" --build "$OUTPUT/build"
model-train/.venv/bin/python model-train/validation/issue-176/freeze.py \
  --source "$SOURCE" --harnesses "$OUTPUT/harnesses" --graphs "$GRAPHS" --packed "$PACKED" \
  --recording "$RECORDING" --config "$CONFIG" --checkpoints "$CHECKPOINTS" \
  --ort-library "$ORT_LIBRARY" --output "$OUTPUT/harnesses/provenance.json"
python3 model-train/validation/issue-176/run.py --mode continuous \
  --baseline-binary "$OUTPUT/harnesses/binaries/baseline-frame" \
  --candidate-binary "$OUTPUT/harnesses/binaries/candidate-frame" \
  --config "$CONFIG" --recording "$RECORDING" --checkpoints "$CHECKPOINTS" \
  --graphs "$GRAPHS" --packed "$PACKED" --ort-library "$ORT_LIBRARY" --output "$OUTPUT/continuous"
python3 model-train/validation/issue-176/summarize.py --source "$OUTPUT/continuous" --output "$OUTPUT/public"
```

离线宿主从准确 a39 基线分别展开；沿 #172 已验证的有限公共前史恢复补丁，不改生产恢复接口。
`instrument.py` 只给两份宿主原函数加时钟读取和累加，保留源码摘要与实际运行二进制。
动作 Session 输入构造、`Session::run`、费用特征和支持集分别记账；评分总时间减这些独立部分后的
宿主余项仍含固定前缀、分组、复制、归约结果获取与调度，不把它冒充单一算子耗时。
各步骤与服务时间包含计时开销；不将各阶段分位数相加。

`freeze.py` 逐张量证明两策略状态权重相等，才复用同一无交易前史 hidden；动作权重与路径属于
各自策略。新旧策略完整业务摘要可不同，同一版本两轮的完整输入、hidden、动作和回执仍须一致。
全部16次都保留图加载、预热、公共前史、首组、后63组、CPU和峰值RSS；首组不得移出总体分布。

## 首轮审查补证

锁定项目 Python 环境就绪后，从仓库根执行以下入口。它依次运行真实小域联合概率/熵和单选
AdamW 验证、原生默认回归、规定工作区回归及原生静态/格式检查；不重导出模型或启动性能矩阵。
输出目录必须是本次新的专用目录。命令、源码文件摘要、退出码和私有日志摘要写入该目录。

```bash
python3 model-train/validation/issue-176/review_checks.py \
  --source "$SOURCE" --build "$OUTPUT/build" --output "$OUTPUT/review"
```

小域由真实现金约束形成，直接消费原生 Actor；测试独立枚举全部合法动作，不向生产添加限制。
共享与重复参考使用同一单选跳过规则和256行对象编码布局，并分别核对空状态及已有状态的一步
AdamW。完整结果和首跑参考布局失配的失败记录见报告中的审查补证链接。
