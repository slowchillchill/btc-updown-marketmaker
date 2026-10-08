# GPU 精确算术有限样板

本页仅保留已有样板的范围、格式和测量边界，供复用历史证据；不再作为当前执行手册或开发前置。
旧运行命令见[固定历史版本](https://github.com/slowchillchill/btc-updown-marketmaker/blob/344fd08e459cd9ff8befc5872e82967483d1093d/model-train/scripts/gpu-exact-sample.md)。
当前实施按 [#181](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181) 和
[D-0300](https://github.com/slowchillchill/btc-updown-marketmaker/issues/62#issuecomment-6008518300)：
直接建设真实 GPU 路径，复用仍适用的结果；必要的新 CPU／GPU 核对也在训练服务器完成，
当前按 #181 选择单张 RTX 5090（32GB 显存），充分使用实际 CPU、内存、GPU 及显存；
条件升级为单张 RTX PRO 6000 Blackwell（96GB）沿当前合同执行，不恢复独立低配置参考路线。
以下旧样板的固定案例、位宽和顺序只说明当时实现，不约束当前工作集或调度。

此入口记录 #181 早期精确经济样板。它不是正式 100ms 训练入口，也没有接入公共账本、
订单预留、NOE、撮合或完整 Actor。源码在训练服务器的任务工作树中编写，按治理规则审查、提交并直接与 GitHub 同步；
运行记录准确提交及执行范围内的额外改动。全部材料和运行输出放在 `/root/autodl-tmp/`。

## 范围

| 项目 | GPU 执行内容 | CPU 比较来源 |
| --- | --- | --- |
| 普通 LIMIT 费用 | 数量 500 百分之一份，费率 0.07，指数 1，tick 0.01；价格 0.19／0.20／0.25 | `buy_tests.rs::export_gpu_exact_buy_reference_v1`，直接调用公共 `limit_buy_cost_v1` |
| 混合双腿价格 | 现金 2 pUSD，5 份 A 普通 GTC／B FOK；GPU 计算完整 999 位 A 价格 mask，再读取该 mask 核对已给定的 A@0.19、B@0.20 并形成意图字段 | 同一 CPU JSON 的完整 mask 和选定双腿意图 |
| 超过 u128 的公共费用 | 既有极端规则反例：数量 `7922816251426433759354395` 百分之一份，费率 `79228162514264337593543950335`，指数 1，tick 0.0001，价格索引 9999 | `buy_cost.rs::tests::export_gpu_wide_buy_reference_v1`，复用原反例夹具与公共入口 |
| 独立整数探针 | 256 位全进位；256×256→512 位乘法；512／256 商余数；超过 u128 的半向上舍入 | Python 精确整数仅计算这些整数原语的期望，不计算费用或可负担性 |

family、数量、两腿类型和两个待选价格坐标是已指定样本输入。本样板不运行神经网络、
16 叉数量选择或遍历其他类型的合法域。数据在进入 GPU 后，费用、价格支持和两腿资金依赖
均在 GPU 内衔接；CPU 不在它们之间读取结果。账户／资产身份在 GPU 中复制，CPU 结束时
仅将整数单位格式化为公共意图字段。

公共费用算法位于 [exact_buy_cost.cuh](../../sim-system/gpu-diagnostics/exact_buy_cost.cuh)。
使用 256 位操作数和 512 位乘积，指数固定为 1。完整乘积高位非零、算术库报错或样本条件
不符时，本诊断失败并保留日志；这些行为不构成新的生产拒单规则。普通样本和既有极端
费用反例均不能证明全部合法 Decimal／BigUint 域可由 256 位覆盖。整数探针与费用反例
分别报告，不将技术构造称为实际平台费率。

## 历史 CPU 参考

历史参考来自两个默认忽略的导出测试。原普通样本已在本地及目标机对照过；
该历史事实不授权本机重新执行。已有同版本 JSON 直接复用，当前必要核对只在训练服务器进行。

分别提取唯一 `BTC_GPU_EXACT_REFERENCE_V1=` 和 `BTC_GPU_WIDE_REFERENCE_V1=` 行的 JSON。
大整数为十进制字符串；宽费用测试沿既有本地脚本化 HTTP 夹具的规则解析与密封过程，
不访问外部平台。保留日志、源码提交、JSON 哈希和规则快照哈希，随材料同步。

## 目标构建与运行

使用未修改的 NVlabs/CGBN
[`114601b07ae8c3e4d46b250208ea8d9def7a12f0`](https://github.com/NVlabs/CGBN/tree/114601b07ae8c3e4d46b250208ea8d9def7a12f0)，
不使用未合并的 PR 35。CGBN 主机头需要 GMP；GMP 开发包与运行库解包到同一数据盘前缀，
`gmp.h` 在 `usr/include/x86_64-linux-gnu`，库在 `usr/lib/x86_64-linux-gnu`。入口显式传入
这些目录和 rpath，不使用带 `/usr` 固定前缀的 pkg-config，也不修改全局 PATH 或环境。

本次可复建材料如下。CGBN 归档来自官方仓库的固定提交；GMP 三包均为 Ubuntu jammy
amd64 的 `2:6.2.1+dfsg-3ubuntu1`，版本与目标原有运行库一致。解包到项目数据盘前缀，
不执行系统安装；同时解包运行库，使开发链接在该前缀内指向实际文件。

| 材料 | 字节数 | SHA-256 |
| --- | ---: | --- |
| [CGBN 固定提交归档](https://codeload.github.com/NVlabs/CGBN/tar.gz/114601b07ae8c3e4d46b250208ea8d9def7a12f0) | 90428 | `1a8774bea1c4f15e11b75bfc877618260454a07db5c02316b9e2b162bdcdf586` |
| [libgmp-dev](https://packages.ubuntu.com/jammy/amd64/libgmp-dev/download) | 336684 | `e4ce547c5c5e4efd98854d06559349b3a03272eb343f1bd8e4ccac7b783229a3` |
| [libgmp10](https://packages.ubuntu.com/jammy/amd64/libgmp10/download) | 250148 | `d199a77e11701d4d620dd4f62e05740e3072dce893856ea3bbaa6ae9fa19fef1` |
| [libgmpxx4ldbl](https://packages.ubuntu.com/jammy/amd64/libgmpxx4ldbl/download) | 9580 | `73e8145633a86c8f01466bb42c5b0734665268f7b2656e82365c1659525f7874` |

Ubuntu 下载页给出官方来源、版本与 SHA；本次 `.deb` 文件来自
`https://archive.ubuntu.com/ubuntu/pool/main/g/gmp/`。核对材料后，以
`dpkg-deb -x <准确材料路径> /root/autodl-tmp/tools/gmp-6.2.1` 分别解包三份材料。
CGBN 保留原归档中的 `LICENSE`，不修改第三方源码。材料来源记录用于运行报告，
不包含账号、SSH 或代理绑定。

旧构建与运行命令保存在页首固定历史版本，本页不再提供执行指令。

输出目录须为新目录。入口只在本次子进程环境中设置 TMPDIR、CUDA 缓存目录和 GMP 库目录；
已有 `LD_LIBRARY_PATH`（含项目 Python 动态库）继续保留。编译使用 C++17、`sm_120` 和
指定 nvcc；不自动换版本、下载依赖或更改第三方源码。旧入口逐卡顺序测量，不作为当前资源安排。

## 证据与计时

`report.json` 记录源码提交与文件哈希、CGBN 材料记录和实际头文件哈希、GMP 头／库哈希、
nvcc 版本与编译命令、二进制哈希、CPU 参考哈希，以及逐卡全部原始结果和比较结论。
`compile.log`、`device-*.log` 和 `device-*.json` 保留实际输出；失败也保留报告和日志。

编译墙钟、GPU 预热、GPU 稳态和进程墙钟分别计时。每轮是四个 kernel：四笔费用、999 位
mask、一个固定双腿选择、整数探针。稳态时间含 kernel 启动间隔，不能取倒数声称单操作
延迟或 RL 环境吞吐。设备选择／上下文初始化主要在进程墙钟中；CUDA 事件用于设备区间。

输入只进行一次显式 H2D 复制；结束时输出与 CGBN 错误报告各进行一次 D2H 复制。错误报告
使用显式设备分配，不使用托管内存的隐式迁移。报告列出调用数、字节数和设备清零字节数。
显式设备分配总量不包含 CUDA 上下文、模块和事件，不能当作整卡峰值显存或训练容量。
