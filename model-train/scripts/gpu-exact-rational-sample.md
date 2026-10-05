# GPU 有理归约与 f32 投影有限样板

本入口继续 #181 的精确数值验证。公共有理数算法的 GPU 诊断位于
[exact_rational.cuh](../../contracts/gpu-diagnostics/exact_rational.cuh)，固定案例入口位于
[exact_rational_sample.cu](../native-inputs/gpu-diagnostics/exact_rational_sample.cu)。
它们未接入生产行情、账户、预留或训练热循环，也未解决生产动态整数宽度。

## 本次执行范围

| 项目 | CPU 权威来源与 GPU 比较内容 |
| --- | --- |
| 符号与约分 | 公共 `EconomicRational` 解析五个既有归一化输入，输出正分母、互素分数和 `0/1` |
| 比较与相消 | 比较负数／零、相等分数、正数／零；`-1/2 + 1/2` |
| RMS 四则 | 既有中点 `0.2 → 0.4 → 0.3` 的差、平方、累计及最终除法，逐步比较规范分数 |
| 两项技术构造 | `(-7 × 2^140)/(15 × 2^140)` 的跨 limb 约分，以及 `(-7/15) ÷ (25001/50003)`；与既有业务案例单列 |
| `round_f32` | 原有六项最近偶数舍入案例，包括正负中点、次正规数与大整数，逐个比较 f32 位模式 |

JSON 中的 `deferred_gpu_scope` 保留公共 CPU 的 log/sqrt 参考，本次 GPU 不执行或验收它们。
输入、结果和中间步骤均来自公共 CPU 实现。Python 入口只提取输入、记录整数表示位数，
并比较现有输出；不另算 gcd、四则结果或 f32 期望位模式。

诊断采用 256 位幅值、独立符号和 512 位完整乘积。输入协议包含固定的 26 对分子分母；
当前最大输入为 151 位，跨 limb 约分构造最大为 144 位。CUDA 内部检查所需中间容量，
失败即保存诊断证据，不把此检查变成生产域约束或拒单规则。

## CPU 参考

默认忽略的导出测试仅位于测试模块，不扩大公共生产接口：

```bash
cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --locked --offline \
  exact::projection_tests::export_gpu_exact_numeric_reference_v1 -- --exact --ignored --nocapture
```

提取唯一 `BTC_GPU_EXACT_NUMERIC_REFERENCE_V1=` 行的 JSON，保存为
`gpu-exact-numeric-reference-v1.json`。保留源码版本、日志及文件 SHA-256；已有准确 JSON
可以复用，不必每次 GPU 运行重做导出。对应原有回归为：

```bash
cargo test --manifest-path model-train/native-inputs/Cargo.toml --locked --offline --test rounding
```

CPU 测试直接调用 `EconomicRational`、`round_f32`、`signed_log_f32` 和 `sqrt_log_f32`。
原高精度参考常量仅用于断言，不替代实际函数输出；log/sqrt 的 CPU 成功不代表 GPU 已实现。

## 材料与目标运行

沿用[费用样板的固定 CGBN／GMP 材料及哈希表](gpu-exact-sample.md#目标构建与运行)。
不更改第三方源码，不使用未合并 PR 35，不自动下载或换版本。先将本地已审查提交经
Git bundle 同步到目标数据盘，再确认执行源码版本一致。所有材料与运行产物位于
`/root/autodl-tmp/`；nvcc 使用已核验的系统工具绝对路径。

```bash
python model-train/scripts/gpu_exact_rational_sample.py \
  --nvcc /usr/local/cuda/bin/nvcc \
  --cgbn-root /root/autodl-tmp/dependencies/CGBN-114601b07ae8c3e4d46b250208ea8d9def7a12f0 \
  --cgbn-record /root/autodl-tmp/offline/cgbn/material-record.json \
  --gmp-prefix /root/autodl-tmp/tools/gmp-6.2.1 \
  --cpu-reference /root/autodl-tmp/artifacts/issue-181/environment/gpu-exact-numeric-reference-v1.json \
  --devices 0 1 --warmups 2 --iterations 20 --progress-seconds 10 \
  --output-dir /root/autodl-tmp/artifacts/issue-181/gpu-exact-rational-sample-001
```

输出必须使用新目录。构建显式传入 CGBN 与 GMP 头文件、库目录和 rpath，使用 C++17、
`sm_120` 和指定 nvcc。只在子进程环境设置输出目录下的 TMPDIR／CUDA 缓存及 GMP 库路径，
保留已有 `LD_LIBRARY_PATH`。每张卡独立、顺序运行；中文进度间隔可控，不估计未知剩余时间。

## 结果与计时口径

`input-layout.json` 记录 26 对输入的顺序、来源字段与表示位数。`report.json` 记录源码
提交及相关文件哈希、CPU JSON 哈希、CGBN 材料及实际头文件哈希、GMP 头／库哈希、nvcc
版本、准确编译命令和二进制哈希。逐卡原始结果与比较结论保存在 `device-*.json`，日志为
`compile.log` 和 `device-*.log`。失败仍保留报告及日志。

每轮是一个 kernel，单个完整 TPI8 组按序执行整个有限案例集；32 线程块的其余组不写入。
编译墙钟、GPU 预热、GPU 稳态与进程墙钟分别报告。稳态 CUDA 事件区间包含 kernel 启动间隔，
不能据此宣称单个四则运算延迟、并行环境吞吐或完整训练速度。

全部输入只做一次 H2D；中间步骤在 GPU 内衔接，结束后输出和错误报告各做一次 D2H。
报告中的显式设备分配字节数不包括 CUDA 上下文、模块和事件，不能当作整卡峰值显存。
