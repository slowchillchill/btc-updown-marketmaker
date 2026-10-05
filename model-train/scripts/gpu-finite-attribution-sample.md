# GPU 有限定点归属样板

本入口验证 #181 已批准试验的 12 位最近偶数分配及精确补差候选。它调用独立的
[GPU 诊断头](../../contracts/gpu-diagnostics/finite_attribution.cuh)，由
[CUDA 入口](../native-inputs/gpu-diagnostics/finite_attribution_sample.cu) 在设备内执行有限案例，
再与公共 [CPU helper](../../contracts/src/finite_attribution.rs) 的实际结果比较。
它未接入生产账户或训练热循环；旧的费用、有理归约样板继续保留各自范围。

## 表示与规则

金额使用整数系数与十进制尺度 `S`：实际值为 `coefficient × 10^(-S)`。
每组输入取 `S = max(12, 源金额所需精确尺度)`，保留细于 12 位的源金额。
权重使用共同精确尺度的整数；共同因子在比值中相消。Python 只做这种精确表示转换，
不计算舍入、分配结果或递推期望值。

设源总额整数为 `X`，权重为 `part/whole`，`F = 10^(S-12)`。GPU 先计算
`RNE(X × part / (whole × F)) × F`。`RNE` 表示舍入到最近整数，正好一半时选偶数。
分配结果限定在精确的 `[0, X]`，余量始终用 `X − allocated` 得到。
权重为零或全额时直接返回精确端点。端点与余量可以保留细于 12 位的数值，
因此例如 `6e-13` 的源总额不会因舍入成 `1e-12` 而产生负余量。

容量分配候选按输入中的稳定顺序，使用剩余总额和剩余容量逐项二分。
每项约束在 `[max(0, remaining_total − future_capacity), min(capacity, remaining_total)]`；
末项取精确余量，零容量输出零。此处使用小数份额只是技术候选，未把 `1e-12` 宣布为
真实平台份额的合法最小单位。生产归属和份额规则仍以所属合同及接入验收为准。

计算采用 CGBN 256 位整数及 512 位完整乘积，检查本次诊断的进位和宽除法前提。
这些容量只覆盖随附有限材料，不定义生产数值上限或拒绝规则。旧规则的超大分母不装入 GPU；
本样板验证相同输入序列采用新分配规则后的状态更新。

## CPU 参考和覆盖

使用公共 helper 的 `finite_attribution_reference_v1` JSON，保留其导出源码、运行日志和 SHA-256。
输入与全部期望结果分开存放；GPU 不接收期望结果。已有同版本参考可直接复用。

CPU 导出直接调用公共 helper：

```bash
cargo run -p contracts --example finite_attribution_reference --locked --offline \
  > /tmp/finite-attribution-reference-v1.json
```

此命令示例在本机执行；目标机的参考文件按下方数据盘路径交接。
导出源码为 [finite_attribution_reference.rs](../../contracts/examples/finite_attribution_reference.rs)，
标准输出只含 JSON，进度写标准错误。原 helper 的定向测试由其责任方维护。

| 案例 | 核对内容 |
| --- | --- |
| 六项舍入 | 零、正负半值，以及偶数／奇数相邻整数的最近偶数选择 |
| 六项二分 | 三分之一、细源向上舍入、细源余量、零权重、全额权重、零总额 |
| 四项容量候选 | 三项尾差、可行区间下界、零容量、零总额；逐项比较实际分配结果 |
| 3654 轮成本更新 | 初值 `1.5`；每轮加入 `0.004`，调用 `split(total, 1, 501)`，保留剩余成本 |

成本循环的当前结果直接成为下一轮输入，中间不回到 CPU。GPU 输出初值、终值、累计分配、
累计投入，以及第 `1、2、10、100、1000、3654` 轮的成本与累计分配。
每轮检查 `剩余成本 + 累计分配 = 初始成本 + 累计新增成本`。
`max_persisted_denominator_bits` 用设备端 GCD 求成本分数的精确约分分母后统计；
它统计写回成本，不把未约分中间乘积或累计分配的分母混入。

旧的纯有理递推为 `C[n+1]=(C[n]+0.004)×500/501`，其约分分母在第 3654 轮达到 32772 位。
这是由源规则构造的数值反例，不是实际历史成交记录，也不是新规则的 CPU oracle。
新规则的终值、摘要和分母位数只取自公共 CPU helper 的本次实际输出。

## 材料与运行

沿用[费用样板的固定 CGBN／GMP 材料](gpu-exact-sample.md#目标构建与运行)，不修改第三方源码，
不采用 PR 35，不自动下载。官方 `cgbn_load` 的参数是指向非 `const` 数据的指针，
诊断头先复制局部输入再加载，保留输入只读语义；见
[固定提交中的接口](https://github.com/NVlabs/CGBN/blob/114601b07ae8c3e4d46b250208ea8d9def7a12f0/include/cgbn/cgbn.h#L493)。
每个 kernel 使用 32 线程块，只有首个完整的 8 线程整数组参与计算。

先由本地已审查提交生成 Git bundle，同步并核对目标执行版本。全部材料、临时文件、缓存和产物
放在 `/root/autodl-tmp/`，使用显式 nvcc、GMP 头文件／库目录及 rpath：

```bash
python model-train/scripts/gpu_finite_attribution_sample.py \
  --nvcc /usr/local/cuda/bin/nvcc \
  --cgbn-root /root/autodl-tmp/dependencies/CGBN-114601b07ae8c3e4d46b250208ea8d9def7a12f0 \
  --cgbn-record /root/autodl-tmp/offline/cgbn/material-record.json \
  --gmp-prefix /root/autodl-tmp/tools/gmp-6.2.1 \
  --cpu-reference /root/autodl-tmp/artifacts/issue-181/environment/finite-attribution-reference-v1.json \
  --devices 0 1 --warmups 2 --iterations 20 --progress-seconds 10 \
  --output-dir /root/autodl-tmp/artifacts/issue-181/gpu-finite-attribution-sample-001
```

两张卡分别顺序运行。输出须是新目录；失败保留报告和日志。
`input-layout.json` 记录来源字段、尺度与准确整数材料；`report.json` 记录源码提交和文件哈希、
CPU 参考哈希、CGBN 实际头文件哈希、GMP 哈希、nvcc 版本、编译命令及二进制哈希。
逐卡原始输出和逐值比较结论保存在 `device-*.json`。

## 计时口径

一次测量迭代是一个完整 kernel，包含全部案例及 3654 次连续成本更新。
预热和稳态每次都从相同初值重跑；稳态时间包含 kernel 启动间隔，以及逐步守恒检查和
每轮用于分母统计的 GCD。它不是单次分配延迟、完整 100ms 帧耗时或训练吞吐。
编译墙钟、GPU 预热、GPU 稳态、分配／传输区间和进程墙钟分别记录。

全部输入一次 H2D，结束时结果和 CGBN 错误报告各一次 D2H。
显式设备分配字节数不包含 CUDA 上下文、模块和事件，不能当作整卡峰值显存。
Python 比较器只把 CPU 已得结果转成相同精确尺度；不重算业务期望值。
