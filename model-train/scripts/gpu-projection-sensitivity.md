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
