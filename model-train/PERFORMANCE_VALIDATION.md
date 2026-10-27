# #377 短验证接口

本任务已授权固定短验证和后继完整性能包。根编排器按原任务完成材料、程序、共同工具和12槽绑定后执行，不另需用户批准。本文只描述入口和必填字段；占位符不是可执行配置。A 独立归档保留原字节，后继 B 与共同工具使用新版本绑定。

## 文件绑定和源码

`BoundFile` 为 `{path: string, sha256: string, max_bytes: positive integer}`。读取前检查长度，读取上限与 SHA 一并核对。`max_bytes` 可以冻结为实际字节数。材料文件、方法文件和源码根由原研究/根交付；不通过未读文件的自有摘要证明业务计算正确。

R0 的 `source.files` 绑定原 `model_worker.py`、`learning_engine.py`。A/B 绑定准确候选的这两文件及 `model_protocol.py`、`input_cache.py`。每项值为该同根文件的 SHA-256。`source.root` 指准确源码根。共同 `performance_validation.py` 与 `performance_capture.py` 的 SHA 另列于 `observer`；全部槽用同一最终版本，不在运行途中补 B 工具。

## 有界准备入口

命令为 `model_performance_samples <PREFIX_CONFIG.json>`。`PrefixSampleConfig` 字段：

| 字段 | 内容 |
| --- | --- |
| consumer / learner | 各自的 `PrefixSource` |
| output_directory | 尚不存在的本次准备目录 |
| stop_file | 根监督器指定的原停止文件 |
| max_wall_seconds | 已冻结准备限额，正数且不超过360；不是独立追加预算 |
| f4 | 本方法需要的 `FixtureSampleConfig` |

`PrefixSource`：`consumer_config` 为原consumer配置或含consumer的learner配置；`trajectory` 为准确原轨迹；`labels` 在consumer为null，在learner为原完整窗冻结标签路径；`account`、`group`、`trajectory_version` 为对应冻结身份。不要把日号、ordinal和call混写。

`FixtureSampleConfig`：`rules` 使用原 `FrozenRuleBindingV1`；`finality_policy_path` 为原有限政策；`implementation_identity`、`encoding`、`sampling_seed` 由根按既有批准材料明确绑定。规则、MOS和开仓时间门不为夹具放宽。

输出为 `C65.jsonl`、`L65.jsonl`、`F4.jsonl`、`F4-construction/` 和 `prefix-report.json`。三个 `.jsonl` 文件都要求每个完整对象占一个物理行，以 LF 结束；空白不用于跨行分隔对象。C65/L65 每条为 `{ordinal, expected_call, row, actual_public_group, frame, label?}`，仅 L65 包含原完整 H 标签 `label`；F4 每条为 `{case, frame, construction}`。写出时只删除标签字符串外的 JSON 空白 SP/HT/LF/CR，并用安全 `RawValue::from_string` 重新验证；数字词法、字符串及其转义和标签字段/数组顺序保持，不经过 `Value/f64` 再序列化标签。builder 的首记录读取与 Rust slot 的逐行读取使用这一边界。此格式是本接口合同，不将 `.jsonl` 后缀本身当成 RFC 保证。报告 `complete:true` 后，根将新小材料的实际大小/SHA绑定到全部槽模板。轨迹/label只核所读前缀，报告不声称复验旧大文件全SHA。准备、公共重建、比较和排空均计入准确来源任务批准的共同短验证窗口，当前以 [#377 的准确授权6395558242](https://github.com/slowchillchill/btc-updown-marketmaker/issues/377#issuecomment-6395558242) 为准。

## 槽配置

命令为 `model_performance_slot <SLOT_CONFIG.json>`。`SlotConfig` 必填：

```text
slot: 1..12
manifest: BoundFile
consumer_worker: WorkerConfig
learner_worker: WorkerConfig
consumer_samples: BoundFile -> C65.jsonl
learner_samples: BoundFile -> L65.jsonl
fixtures: BoundFile -> F4.jsonl
learning_open: {phase:"learn_open", config:<原LearningParameters>,
                group:<准确原消费组>, account_weights:<五序列各0.2>}
output_directory: <尚不存在的准确槽目录>
```

`WorkerConfig` 保留 `python/script/model/device/torch_threads/max_batch/encoding`。`script` 统一指最终共同观测 wrapper；consumer model为原seq3导出包。learner_worker.model仍是原配置绑定的bootstrap构造包（SHA以原材料为准），只供建立原Policy；learn_open必须再从本槽checkpoint_directory/checkpoint.pt加载原完整seq2的权重、Adam、RNG和consumed。不能把完整learner checkpoint当作Service所需模型包，也不能用bootstrap/fresh Adam代替seq2。该目录必须是准确本槽隔离完整副本，不采用原目录当前seq4，不向原材料写入。

R0 缺省不序列化 `performance`。A 设置 `mode=storage_v1`，B 设置 `mode=device_v1`；其余字段为 `native_library/rust_threads/host_cache_bytes/gpu_cache_bytes/runtime_reserve_bytes/streaming_block_bytes/queue_bytes/torch_interop_threads`。原启动并发为Rust16、Torch intra2/inter1，原参考4线程差异在manifest单列。

GPU容量参数的单位是字节，不能将GiB与十进制GB混用。GPU缓存预算须同时容纳固定驻留子集及B的两个raw槽；固定页暂存仅两个槽。全驻留槽7需实际host后备为0、GPU主输入非0、无流式H2D；槽8需固定驻留和host后备均非0、真实两个槽及其复用均命中。`streaming_block_bytes=P` 表示单槽容量，raw与pinned各有2P；较大输入按字节/列分片，保持同一TBPTT GRU。根在首槽前冻结全部具体数值，不靠配置名称当覆盖证据。

外层stdout/stderr监督日志应放在槽目录之外的既有父目录；driver自己建立 `output_directory`。外层负责真实native rc、进程组截止和回收，不从JSON猜退出码。

## 共同manifest

```text
version: "b-performance-validation-v1"
slot: <与SlotConfig相同>
variant: "R0" | "A" | "B"
source: {root:<准确同根源码>, files:{<文件名.py>:<SHA>}}
observer: {"performance_validation.py":<SHA>, "performance_capture.py":<SHA>}
method: BoundFile -> 原short-validation-method.md
execution_method: BoundFile -> execution-bindings-v2
capture_method: BoundFile -> 获接收的hidden捕获补充
reference_loss: BoundFile -> 原R0 learning_engine.py
limits: {capture_bytes:<冻结的每worker捕获载荷限额>}
timing: {started_monotonic:<本槽实际锚>, deadline_monotonic:<本槽截止>,
         stage_deadline_monotonic:<原工程截止>, short_deadline_monotonic:<共同短包截止>}
expected_runtime: {torch_intra_threads:<数值>, torch_interop_threads:<数值>,
                   omp_threads:<原字符串或null>, mkl_threads:<原字符串或null>}
f4: {order:["F-N","F-P","F-R","F-S"],
     account_weights:{<原L65账户>:0.2,<四工程账户各>:0.2},
     label_rule:<共同工具MAIN_LABEL_RULE原对象>,
     boundary_rule:<共同工具BOUNDARY_RULE原对象>,
     reference_labels:<槽2..12绑定槽1f4-labels.json>,
     reference_boundary_labels:<槽2..12绑定槽1ppo-boundary-labels.json>}
```

`label_rule/boundary_rule` 使用工具顶部精确对象，不手写另一套公式。F4主要标签只在槽1冻结；边界辅助复用已计图，不再前向。后槽只能引用槽1准确文件。R0未核历史interop/OMP/MKL可按原方法只列已知expected_runtime键，实际值仍在已计worker内记录；候选必须四键齐全。原六个精度/TF32/cudnn元数据继续精确核对，仅四个线程观测路径按批准范围单列。

根监督器只动态填写四个timing值、后槽两份F4 BoundFile，再把最终manifest SHA回填 `SlotConfig.manifest`。材料、模型、源文件、程序、slot语义和所有资源/捕获常量在首槽前固定。不能通过任意hook/override改变运行内容。

## 输出与真实工作量

Rust `driver-result.json` 顶层为 `slot/elapsed_seconds/result/error`。成功的result包含 `consumer`、`learner` 和 `all_workers_waited:true`；learner在冷槽另有hot_finish/cold。错误、监督rc与退出/排空分别保持原事实。

每槽共用 `work.jsonl`。每条为 `kind/amount/counts/identity/deadline/limits/pid/elapsed`，末条counts为跨C65/L65及可选cold的累计值。关键键为 `main_gru_frames`、`actual_adam`、`reference_adam`、`actual_adam_completed`、`reference_adam_completed`。正常槽272主前向和2+2临时Adam；停止槽166主前向、0Adam；12槽合计3052主前向、20+20临时Adam。GRU按实际有效batch×time计，分支/teacher/恢复不另领额度。

每worker落 `identity.json`、`finish.json` 与 `captures/index.jsonl` 及有界数值捕获。B另记 `captures/buffer-events.jsonl` 和 `device-learning.jsonl`。buffer记录直接读取真实base指针、ordinary/pinned状态、slot+generation、原始传输字节、CUDA stream句柄和事件查询结果；只记录“提交”不能冒称“完成”。图输入释放只表示原backward已提交并释放显式持有，不声称GPU已完成。预取但未消费的代次单列，不计成已做模型工作。

## 比较入口

```text
python performance_validation.py compare <REFERENCE_SLOT_DIR> <CANDIDATE_SLOT_DIR> <NEW_REPORT.json>
```

比较只读既有短捕获，不建立Policy、不执行forward或Adam；其Torch tensor读取和比较成本仍计共同短包/准备额度。报告键为 `pass/reference_counts/candidate_counts/reference_count_gates/candidate_count_gates/comparisons/scope_exclusions/coverage`，没有泛化的任意gates配置。

计数门包括主前向、实际/同梯度参考Adam的发起与完成。停止增加production_stop，冷恢复增加production_cold_restore/one_production_restore，非R0增加production_collection_frames。B驻留/流式结构证据由真实finish及原buffer记录核对，不用配置替代。

## 完整成本和文档勘误

生产consumer正确命令为 `model_consumer run <CONFIG.json> <NEW_SUMMARY.json>`；learner为 `model_learner run <CONFIG.json>`。原 `elapsed_seconds` 和learner attempt report保留其事务时点含义。成功CLI返回新增 `elapsed_seconds_through_worker_exit` 与 `worker_exit.shutdown_elapsed_seconds`，保留真实normal/waited。研究仍以外层E2E计完整准备、CLI输入/输出、提交和排空，不把新增字段当整个进程的完整成本。

CUDA事件与存储观察依据PyTorch2.8正式接口：[Event](https://docs.pytorch.org/docs/2.8/generated/torch.cuda.Event.html)、[Stream](https://docs.pytorch.org/docs/2.8/generated/torch.cuda.Stream.html)、[Storage](https://docs.pytorch.org/docs/2.8/storage.html)、[is_inference](https://docs.pytorch.org/docs/2.8/generated/torch.Tensor.is_inference.html)。这些接口语义支持实现设计，不替代本任务槽内的实际生命周期与数值证据。
