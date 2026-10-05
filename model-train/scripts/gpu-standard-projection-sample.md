# GPU 标准数学末端特征有限诊断

本入口比较两种末端特征计算：标准 CUDA FP64 `log1p/sqrt` 后转 FP32，以及单列的
标准 CUDA FP32 `log1pf/sqrtf` 对照。它没有接入生产特征、账户或训练入口。
原有精确 CPU 参考与已有 GPU 有理归约、有限归属样板继续保留各自范围。

## 输入与参考

使用 `gpu_exact_numeric_reference_v1` 中原有的 15 项 `signed_log_f32` 和 4 项
`sqrt_log_f32`。它们的期望位模式直接来自公共 CPU 实现，诊断脚本不重算或覆盖这些位模式。
其中 `sqrt_log_f32(1/40)` 继续引用已有精确 RMS 均值；本入口不重复实现先求差或有理归约。

另外增加三项有限技术探针：

| 输入 | 目的 |
| --- | --- |
| `(7 × 2^1100 + 1) / (10 × 2^1100 + 3)` | 分子分母均超过 1024 位，但比值约为 0.7，检查分别转 FP64 所产生的溢出问题 |
| `sqrt_log_f32(10^-56)` | 平方量直接存为 FP32 会下溢，变换后的参考特征仍可表示 |
| `sqrt_log_f32(10^40)` | 平方量直接存为 FP32 会上溢，变换后的参考特征仍有限 |

新增三项由 Python 标准 `Decimal` 在 128 和 192 位十进制精度分别计算，转为 FP32 后交叉核对。
报告保存两次高精度结果及位模式。这是有限技术参考，不能当作公共精确 CPU 合同或全域正确舍入证明。
样本不是实际训练分布，也不能证明生产位宽足够。训练数据和独立第三留出集不参与本入口。

## 设备计算

Python 只将完整整数编码为小端 `uint32` limb。GPU 从每个整数取最多 53 个有效高位，
先在有限尾数之间相除，再按二进制指数缩放，得到 FP64 比值。因此，不需要把巨大分子分母
分别转成可能溢出的 FP64 整数。低位被截去，这个算法**不保证整体比值正确舍入**。
诊断材料最多 40 个 limb 的布局只服务这 22 项输入，不定义生产金额、位宽或拒绝规则。

FP64 候选执行 `copysign(log1p(abs(x)), x)`，或 `log1p(sqrt(x))`，最后一次转为 FP32。
FP32 对照先将相同设备端 FP64 比值转为 FP32，再使用真实 CUDA `log1pf/sqrtf`。
它不是主机数学库模拟，也不是分子分母分别进行 FP32 转换。
精确零保持正零。编译明确保留次正规数与准确除法、平方根，不启用 `--use_fast_math`。

每项输出原始阶段位模式。Python 分别比较：

- GPU 比值转换与 CPU `float(Fraction(n, d))` 整体转换的 FP64 差异。
- GPU 最终 FP32 与相应 CPU 参考位模式的绝对差、相对差及 ULP 距离。
- FP32 输入、平方根和变换阶段的非有限值或归零情况。

参考为零时相对差记为 `null`；非有限结果保留分类与位模式，差值记为 `null`，不把它们排除成
“对照通过”。汇总中的有限结果最大误差与非有限数量、非零参考归零数量同时报告。
FP64 候选对这批有限参考产生非有限、错误符号或丢失非零值时记录执行缺口。
其余误差只作为测量结果，不新增统一的 1 ULP 或其他验收阈值。FP32 已知上溢和下溢不冒充候选通过。

## 构建与运行

本诊断不使用 CGBN 或 GMP，也不自研高精度 GPU `log/sqrt`。
先从本地已审查提交制作 Git bundle，经本机同步到目标机并核对准确提交。
显式指定现有 nvcc。全部材料、缓存、临时目录和结果位于 AutoDL 数据盘：

```bash
python model-train/scripts/gpu_standard_projection_sample.py \
  --nvcc /usr/local/cuda/bin/nvcc \
  --cpu-reference /root/autodl-tmp/artifacts/issue-181/environment/gpu-exact-numeric-reference-v1.json \
  --devices 0 1 --warmups 2 --iterations 20 --progress-seconds 10 \
  --output-dir /root/autodl-tmp/artifacts/issue-181/gpu-standard-projection-sample-001
```

输出必须是新目录，两张卡依次执行。沿用既有 runner 的定期进度和失败日志保留方式。
`input-layout.json` 保存完整整数、参考来源与各类位模式；`cpu-reference.json` 保留原参考。
`report.json` 保存源码提交、执行文件的 Git 状态与 SHA-256、参考材料哈希、nvcc 版本、
实际编译命令和二进制哈希。逐卡原始结果与差值保存在 `device-*.json`。
`completed_measurement_only` 只表示诊断测量完成，不表示生产数值或盈利验收通过。

## 测量边界

两种方法分别预热、分别用 CUDA 事件测量。每轮只有一个 32 线程块，22 个线程分别处理样本；
记录的时间包含整数转换、特征计算和 kernel 启动间隔，不是单个 `log/sqrt` 延迟、饱和吞吐或训练性能。
比较时须保留这个小工作集的局限，不能据此声称完整训练有同等加速。

元数据和整数 limb 各一次 H2D，两种方法全部完成后各一次结果 D2H。显式设备工作集包含输入元数据、
整数 limb、FP64 输出、FP32 输出四次分配，不含 CUDA 上下文、模块或事件，不能当作峰值显存。
编译墙钟、各方法预热与稳态、分配传输区间及进程墙钟分开记录。
