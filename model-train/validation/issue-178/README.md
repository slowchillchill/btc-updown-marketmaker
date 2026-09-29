# #178 复现与比较边界

字段/责任审计见 [audit.md](audit.md) 和 [逐字段表](field-audit.csv)，采用依据与结果见 [性能报告](performance.md)。当前生产基线固定为
`bdb54b4e46adf29a8b607171338a6efce701d301`。本专项入口与产物独立，不覆盖 #172/#174/#176 的历史证据。

## 测量生命周期

1. `prepare.py` 从准确基线展开离线宿主；候选只覆盖当前原生输入、shared-run、ledger 来源。
   随后加入仅离线可用的历史/缓存恢复及归因探针；生产模块不增加逐帧诊断核对。
2. `FRAME_PREPARE=1` 从原录制起点沿真实组建立 native 历史与完整侧缓存；在普通201582、高418584入口保存真实热缓存，
   与旧检查点的精确业务/历史比对。每个 `[T−30,T)` 的实际 native 计算另写 `warmup-measurements.jsonl`。
   高窗口从下一市场热身的第25.414秒开始，因此入口前只能报告已实际发生的热身前缀，不能提前读取其余未来5秒。
3. 正式进程重放公共前史，在入口恢复上述完整热缓存，并恢复已有相同状态图/权重的真实前史 hidden。
   图加载、纯形状预热分别计时；`cache_restore_seconds`合计native暖缓存、模型hidden及打包缓存恢复，未将三者分别计时。
   准备总耗时还包含公共前史重放及检查点读取。模型前史推理没有在本轮再执行，不能称为本轮整模型30秒热身成本。
   打包缓存由实际前一组输入恢复；不能重复推理同一帧代替真实热身。
4. 两模型×两个64组窗口×基线/候选×两轮反序，16次串行。全部使用相同当前 v6 八图、相同权重；
   必须比对完整输入位、历史、账户、hidden、z、动作、回执。每次首组进入总体，另列首组只帮助解释开销。
5. 正式计时不编译 `bench-profile`；归因计时单独报告，嵌套或并行工作时长不能相加成 native 墙钟或CPU占比。
   动作测量沿 #176 的会话计时入口，各路径均包含相同离线计时开销；服务内保持实际两行竞争、动作与执行。
   宿主按原组关闭输入分配非递减因果时间，不模拟实时到达队列，因此不能由该离线结果推断 paper/live 排队为零。
6. `/usr/bin/time -v` 保留进程CPU及峰值RSS；它包含图加载、实际公共前史和证据生成，不能称为新增缓存独占内存。
   native warmup 的逐组CPU、墙钟单列；本轮未重新计算历史模型推理的成本不混入已测native热身。

## 命令

以下均从工作树根执行，变量由操作者绑定实际本机路径。`OUTPUT` 使用本次新的专用目录；
`GRAPHS` 直接含 `MAIN-1x256/` 与 `MAIN-30x1024/`，两实现共同读取，不能给两实现不同权重。
`RECORDING` 指整个录制包根，其中直接包含 `scenario.json`、`record-completion.json` 及 `recording/`；不是后者子目录。

```bash
python3 model-train/validation/issue-178/prepare.py --source "$SOURCE" --output "$OUTPUT/baseline-harness" --variant baseline
python3 model-train/validation/issue-178/prepare.py --source "$SOURCE" --output "$OUTPUT/candidate-harness" --variant candidate
python3 model-train/validation/issue-178/build.py --harnesses "$OUTPUT" --target "$BUILD" --output "$OUTPUT/binaries"
```

分别对 `baseline`、`candidate` 执行真实native前史准备；原生8线程，不并发编译或模型计时。

```bash
RAYON_NUM_THREADS=8 FRAME_MODE="$VARIANT" FRAME_PREPARE=1 FRAME_CHECKPOINTS="$CHECKPOINTS" \
  /usr/bin/time -v -o "$OUTPUT/warm-$VARIANT.resources.txt" \
  "$OUTPUT/binaries/$VARIANT-profile" "$CONFIG" "$RECORDING" "$OUTPUT/warm-$VARIANT" decisions 9999999
```

核对图与前史证据，运行正式矩阵并汇总：

```bash
python3 model-train/validation/issue-178/freeze.py --source "$SOURCE" --graphs "$GRAPHS" \
  --checkpoints "$CHECKPOINTS" --warm-baseline "$OUTPUT/warm-baseline" --warm-candidate "$OUTPUT/warm-candidate" \
  --warm-baseline-binary "$WARM_BASELINE_BINARY" --warm-baseline-harness "$WARM_BASELINE_HARNESS" \
  --warm-candidate-binary "$WARM_CANDIDATE_BINARY" --warm-candidate-harness "$WARM_CANDIDATE_HARNESS" \
  --config "$CONFIG" --recording "$RECORDING" --packed "$PACKED" --ort-library "$ORT_LIBRARY" --output "$OUTPUT/provenance.json"
python3 model-train/validation/issue-178/run.py --baseline-binary "$OUTPUT/binaries/baseline-formal" \
  --candidate-binary "$OUTPUT/binaries/candidate-formal" --config "$CONFIG" --recording "$RECORDING" \
  --checkpoints "$CHECKPOINTS" --warm-baseline "$OUTPUT/warm-baseline" --warm-candidate "$OUTPUT/warm-candidate" \
  --graphs "$GRAPHS" --packed "$PACKED" --ort-library "$ORT_LIBRARY" --output "$OUTPUT/matrix"
python3 model-train/validation/issue-178/summarize.py --source "$OUTPUT/matrix" --output "$OUTPUT/public"
python3 model-train/validation/issue-178/aggregate.py --source "$OUTPUT/public"
python3 model-train/validation/issue-178/tables.py --source "$OUTPUT/public" --output "$OUTPUT/public"
python3 model-train/validation/issue-178/field_observations.py \
  --measurements "$OUTPUT/matrix"/r1-*-candidate/measurements.jsonl --output "$OUTPUT/public/field-observations.csv"
```

同一入口加 `--diagnostic` 并使用两份 `*-profile` 二进制，只运行大模型的两个窗口×两实现共4次，
用 `profile_summary.py` 归纳六项墙钟成本和嵌套工作量；当最终局部候选保存在独立目录时，使用 `--candidate-source` 明确其实际来源，仍和基线逐组核对闭环结果及执行文件。正式矩阵使用 `*-formal`，不把探针结果当最终均值。
`screen_summary.py` 对先行native-only的两个64组窗口核对完整f32位并报告筛选成本；
`warm_summary.py` 汇总真实热身前缀；`field_observations.py` 对正式实际消费过的输入补充取值范围。
本轮[取值附表](field-observations.csv)来自r1两模型×两窗口的候选256组：384市场行、1158208个盘口档记录、256撤单对象及302次前缀向量。
动作前缀在原评分器已有36维输入上复制用于证据，未为观察重新执行精确投影；该离线复制开销在两实现中一致，计入所报告动作阶段。
盘口细分探针分别记录结构/数量识别与有效档扫描、精确总量、锚点与时间、输出构造、缓存位置更新；
这些侧级时长有并行关系。账户对象数在原有registry/funds迭代中计数，事件访问数按原有四遍成员遍历计数，
没有为探针额外建立快照或逐帧业务核验。

长实验状态查询按15分钟间隔；运行命令自行输出阶段完成通知。公开摘要不含本机配置或绝对路径，
原始私有产物保留逐组数据和实际失败记录。相同模型的训练后收益或异常自主应对能力需要独立证据，不能由延迟矩阵证明。
