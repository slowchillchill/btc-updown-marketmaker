# 真实组件末端投影有限敏感性诊断

本诊断使用公共短 BUY 夹具，增加已有的 BBA 中点 `0.2→0.4→0.3` 序列，形成实际非零 RMS。
每种政策独立从根构造行情、账户、静态规则、盘口投影和历史缓存；在同一时点执行同一个
原生 Actor 形成的 5 股双腿 GTC 意图，保持共同外生账户前史。夹具只提供稳定有限前缀，
诊断在其最后一个公开组截止，不声明源已自然结束。

固定未训练的 `MAIN-1x256` 实际组件连续消费全部组。记录完整 slow/fast/rule/book 特征、
连续隐藏状态和 z、共同精确 Actor 路径的支持、分数、分布及意图。各政策也各自实际采样
原生 Actor 路径，再调用原概率重算入口比较 log_prob 和梯度；自身采样意图不执行，权重不更新。
这不能代替真实训练、长期盈利或 GPU 常驻性能验证，不读取用户训练数据或独立第三留出集。

## 末端输入捕获

`exact.rs` 的诊断入口仅存在于 `cfg(test)`，由专用 ignored 单测试激活。进程级锁保护的状态
覆盖 Rayon 工作线程，记录实际调用的运算种类、规范有理分子分母、原精确 CPU 位模式和调用阶段。
生产构建继续使用原精确算法。平方根区间内部的 log 端点直接调用原算法，不混入末端输入表。

缺失替换表键时允许原精确结果帮助发现后续路径，但**整轮标为 `discovery_only`**。
只有全部实际调用都有 CUDA 替换结果、`missing_replacements=0` 的重新运行才能参与比较。
每次补表后都从新根开始，不能在已有缓存上切政策。

## 分阶段运行

先使用已锁定 Python/PyTorch 和 Rust 环境。`PYO3_PYTHON` 指向该解释器；嵌入 Python 还需
按照该环境的实际 base prefix、lib 和 site-packages 配置 `PYTHONHOME`、`LIBRARY_PATH`、
`LD_LIBRARY_PATH`、`PYTHONPATH`。不下载或安装依赖。本地检查与目标 GPU 环境分别记录。
AutoDL 上的源码、构建、缓存、材料与产物必须全部放在 `/root/autodl-tmp/`。
最终 exact_cpu 基准与候选组件比较须在同一个 Python/PyTorch、CPU 与线程配置下执行。
可将实际 GPU 替换表回传本机，再在同一本机环境运行各政策；不得将跨平台组件差异归因于投影。

1. 在独立单测试进程捕获公共组件，不设置任何替换表：

   ```bash
   PROJECTION_SENSITIVITY_OUTPUT=/root/autodl-tmp/artifacts/issue-181/projection/exact-cpu.json \
   cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --features python \
     --locked --offline \
     action_tests::buy_tests::projection_sensitivity_tests::actual_component_projection_sensitivity \
     -- --exact --ignored --nocapture --test-threads=1
   ```

2. 将捕获报告交给标准 CUDA 诊断。源码先按准确提交经 Git bundle 同步，原22项默认模式继续保留：

   ```bash
   python model-train/scripts/gpu_standard_projection_sample.py \
     --cpu-reference /root/autodl-tmp/artifacts/issue-181/projection/exact-cpu.json \
     --nvcc /usr/local/cuda/bin/nvcc --devices 0 1 --warmups 2 --iterations 20 \
     --output-dir /root/autodl-tmp/artifacts/issue-181/projection/cuda-001
   python model-train/scripts/gpu_projection_sensitivity.py tables \
     --gpu-output-dir /root/autodl-tmp/artifacts/issue-181/projection/cuda-001
   ```

   `tables` 只接受真实公共组件捕获材料，拒绝拿22个独立标量构造组件比较。
   它分别导出每卡 FP64 候选与 FP32 对照表。非有限输出保留原结果，但不能回填为模型有限输入。

3. 每个候选用独立进程与根重放，沿用第一步命令并增加：

   ```bash
   PROJECTION_SENSITIVITY_POLICY=fp64_then_fp32
   PROJECTION_SENSITIVITY_REPLACEMENTS=/root/autodl-tmp/artifacts/issue-181/projection/cuda-001/device-0-fp64_then_fp32-replacements.json
   PROJECTION_SENSITIVITY_OUTPUT=/root/autodl-tmp/artifacts/issue-181/projection/device-0-fp64.json
   ```

   这些变量须 `export` 或作为同一次 cargo 命令的环境前缀。FP32 对照政策为 `fp32_control`。
   若报告为发现轮，将该报告作为第二步的新输入补齐实际 CUDA 表，再从根重跑；不得比较发现轮。

4. 比较完整报告：

   ```bash
   python model-train/scripts/gpu_projection_sensitivity.py compare \
     --reference /root/autodl-tmp/artifacts/issue-181/projection/exact-cpu.json \
     --candidate /root/autodl-tmp/artifacts/issue-181/projection/device-0-fp64.json \
     --output /root/autodl-tmp/artifacts/issue-181/projection/comparison-device-0-fp64.json
   ```

比较器要求同模型权重、相同外生意图、共同路径支持和精确状态不变，以及每个政策自己的概率与梯度
重算一致。数值输出只报告差值，不新增统一容差。各政策自主采样的路径可以不同，报告如实列出，
不把这一技术诊断解释为策略收益改善或恶化。

## 证据与限制

保留准确提交、命令、环境和运行日志。CUDA 报告继续保存编译参数、输入/源码哈希、设备阶段位模式、
传输与显式设备工作集。替换表带源 CUDA 报告与布局哈希；最终比较带两份组件报告哈希。
捕获和回填发生在 CPU 组件装配中，GPU只批量计算末端投影，不能据此声称完整热循环已GPU常驻。

## 批准训练内部的真实五分钟诊断

新增 `prepared_component_projection_sensitivity` ignored 测试，复用公共 backtest 的
`run_prepared_historical_with_observer_v1`。诊断窗口固定为 2026-09-15 04:15—04:20 UTC，
Native 根从 04:14:30 开始。公共来源范围可以更早，以实际准备描述及其 SHA-256 为准；
必须处于已授权训练内部、恢复真实盘口前序，不能把统一来源裁切直接设成 T−30。
观察回调在每个真实公共组后调用 `NativeInputRoot.consume`，不提交意图、不推进时间或消费下一组。
历史公共/hidden 供料和所有到期驱动继续由 backtest 原循环负责。
该唯一宿主沿原 `end` 截止过滤事件后才交付 observer；诊断覆盖原宿主实际交付范围，
不声称逐条消费 prepared 材料中合法可见时刻落在截止点之外的存储尾部。
本窗口按用户裁决从原12:00窗口改为04:15窗口，以减少必要前史。原容量故障证据保持原范围，
不重标为本窗结果。来源仍从04:14:15.316恢复；04:15市场的首条有效锚点在开市后才可见，
此前缺失保持真实，不从后续锚点提前填值。运行身份为`projection-real-20260915-0415`，
NoOp会话种子沿`SHA256(run_id)`生成，模型固定种子保持。

账户固定为 2000 pUSD、零仓位、零挂单。原策略入口只返回 NoOp，模型生成的意图不提交。
各政策分别从根重跑，共享相同规则、账户参数、时序、材料和外生历史。真实平台规则必须由
`load_historical_process_snapshot_v3` 校验文件摘要及历史因果时刻；本诊断不在线查询平台规则。
finality 材料、时序、账户 admission 和资源参数需有明确交接或本次诊断选择，不能用
`synthetic-command.json` 的测试规则替代。SIM 时延仍由原 `load_embedded_proxy_latency_profile`
加载编译内置材料，不新增一个模型诊断时延 profile。

`StreamingSensitivity` 每组只保留当前输入与连续 hidden 数值。默认使用 `no_grad`；前32个
`DecisionRequired` 组把进入前的 hidden 视作 detach 常量，启用当组状态组件和 Actor 的梯度。
每组只做一次状态推进，随后重算自身实际采样路径的 log_prob 和参数梯度。结束该组后再次
detach hidden，保留数值进入下一组。报告分别记录 state/action 已用和非零梯度元素；只有
一个合法动作时，零梯度按真实结果记录。第32组后仍消费所有公共组并捕获实际末端输入。
此处不计算穿过更早组的梯度，不证明正式 TBPTT/PPO、盈利或 GPU 常驻性能。

第一轮 exact_cpu 在这32组冻结自身实际采样的原生路径。候选重评分同一冻结路径，并各自
实际采样与重算；共同路径按组序号、真实 cutoff、row 和 hidden 身份绑定。新路径出现未覆盖的
末端输入时，沿原规则标为 `discovery_only`，补齐 GPU 表后从根重跑。

逐组完整特征、h_prev/h/z 和32组详细结果写入新建的 gzip JSONL，不在内存积累整窗。
流包含 header、连续 group 和 end。尾记录绑定组数与此前内容 SHA-256，完成报告再绑定压缩文件
字节数与 SHA-256。比较器逐组读取，拒绝截断、尾后额外内容、漏组以及发现轮；数值差异按字段
汇总，保留32组动作详情，不引入统一误差阈值。`status=complete` 只表示测量完成；必须检查
比较器的 `all_exact_component_invariants_hold`、梯度覆盖和实际差值，不能仅凭 complete 宣称通过。

### 生成私有配置

先取得真实 prepare 已发布的 `prepared-run-v1.json`、已核规则文件、显式运行参数、
finality 文件以及当前运行的 `ImplementationIdentityV1` JSON。以下命令只生成配置及来源摘要，
不会创建运行目录、读取行情或执行回放。含私有路径的输出放在仓库外或被忽略的产物目录。

```bash
python model-train/scripts/gpu_projection_sensitivity.py config \
  --run-template <已有正式HistoricalRunConfigV1或HistoricalPreparedRunConfigV1_JSON> \
  --prepared <materials/prepared-run-v1.json> \
  --rules <rules/platform-rule-snapshot-v3.json> --rules-sha256 <已核SHA256> \
  --finality <已交接finality_JSON> --implementation-identity <当前源码身份_JSON> \
  --run-output-dir <本政策全新运行目录> --output <私有诊断配置_JSON>
```

生成器复用模板的 timing、admission、容量和 session_seed，只把本次技术身份、2000 pUSD、NoOp
描述与输出定位写成诊断值。每个政策的输出目录不同，账户/会话技术身份保持一致。所有政策须用
相同模型执行环境与业务参数；替换表可以在 GPU 算出后回传同一本机执行。

若本次明确选用 NoOp 零额外延迟机制对照，可用 `--noop-zero-delay` 代替 `--run-template`。
它显式将四项宿主延迟设为零，不声称生产延迟为零；容量复用 #172 实际无交易配置的
67,108,864 bytes / 128 in-flight groups，session_seed 沿已有真实入口取 `SHA256(run_id)`，
账户 admission 沿 `DualCashNoMarketLimit`。生成器把各字段依据写入来源 sidecar。
四项时序仍影响 cutoff、来源年龄或观察身份；零的含义是不对原来源合法可见时刻额外添加
宿主延迟，不删除原 received/admission/依赖时序或 SIM 内置平台及代理时延。各政策使用同一
时序。输出目录不同导致配置文件 SHA 不同；另以 `runtime_semantics_sha256` 绑定去掉纯输出
定位、用材料摘要替代材料路径后的完整运行参数，比较时要求该摘要与参数内容同时一致。
`ImplementationIdentityV1.cargo_lock_sha256` 应绑定实际构建使用的
`model-train/native-inputs/Cargo.lock`；当前源码身份由本次组合提交提供，不能复用旧产物身份。

```bash
PROJECTION_SENSITIVITY_PREPARED_CONFIG=<私有诊断配置_JSON> \
PROJECTION_SENSITIVITY_OUTPUT=<新完成报告_JSON> \
PROJECTION_SENSITIVITY_SOURCE_COMMIT=<准确运行提交> \
cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --features python \
  --locked --offline \
  action_tests::buy_tests::projection_sensitivity_tests::prepared_component_projection_sensitivity \
  -- --exact --ignored --nocapture --test-threads=1
```

候选另设 `PROJECTION_SENSITIVITY_POLICY`、`PROJECTION_SENSITIVITY_REPLACEMENTS`，以及
`PROJECTION_SENSITIVITY_COMMON_PATHS=<完成的exact_cpu报告_JSON>`。CUDA `--cpu-reference`
可以直接读取新的流式完成/发现报告中的 capture；`tables` 和 `compare` 命令保持原样。
执行前需按已核环境配置嵌入 Python，并创建本政策运行目录。进度每10秒显示模型已消费组数，
公共回放沿配置打印原有进度；不估算未知剩余组数。

原13组 ignored 测试可增加 `PROJECTION_SENSITIVITY_FIXTURE_STREAM_OUTPUT=<新.gz文件>`，
用同一真实夹具对比旧完整历史图与流式实现的 features/h/z；仅比较前向数值，不要求两种
不同梯度范围的梯度相同。此兼容检查仍不读取真实训练材料。
