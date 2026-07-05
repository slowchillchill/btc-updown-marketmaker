# Repository Guidelines

## 开发环境与部署拓扑

- 当前本地开发、文档落盘、代码编写与本地构建仓库：`/home/chillchill/btc-updown-marketmaker`
- 各远端服务器上的源码/工作目录统一写作 `~/btc-updown-marketmaker`；在 `ubuntu` 用户服务器上该路径通常解析为 `/home/ubuntu/btc-updown-marketmaker`，但文档和命令不得把它误写成本地开发路径。
- 与 Polymarket 相关开发必须基于官方 Polymarket Rust SDK。若
  Polymarket Rust SDK 的能力、类型、认证、签名、请求构造、响应解析或覆盖范围
  不满足当前需求，必须先明确告知用户具体缺口、影响范围和可选方案，并在获得用户
  同意后才能继续开发；不得在未获同意前自行改用 raw REST、manual signing、自建
  SDK 旁路或其他 shadow implementation。
- 本地系统：`原生 Ubuntu 26.04`
- 本地硬件：`32` 核 `AMD AI MAX+`、`128G` 共享内存、`2TB` 磁盘
- 本地机器是当前的核心开发和监控中枢，通过 SSH 命令编排和监督远端训练、数据采集、模拟盘和实盘任务。

远端服务器（已配置好 `SSH key pair`）：

- 云 5090GPU 训练服务器：`ssh -p 40228 root@connect.bjb2.seetacloud.com`
- 云 3090GPU 训练服务器：`ssh -p 37267 root@connect.nmb2.seetacloud.com`
- AWS 爱尔兰 paper-trade 服务器：`ssh ubuntu@54.155.172.85`
- AWS 爱尔兰 live-trade 服务器：`ssh ubuntu@108.130.74.143`
- 数据采集服务器：`ssh ubuntu@3.255.156.183`

AWS 爱尔兰 live-trade 服务器 `108.130.74.143` 是运行目标，不是构建机。
它只有约 `2 vCPU`、`1.9 GiB RAM`、无 swap，不具备 Rust 编译硬件条件。
禁止在该服务器上运行 `cargo build`、`cargo test`、`cargo check`、`cargo fmt`或任何会触发 Rust crate 编译的命令；也不得为编译目的安装 toolchain 或 build dependencies，除非用户在未来明确改变该硬件策略。live-trade release binary
必须在本地 `/home/chillchill/btc-updown-marketmaker` 编译完成，保留构建命令、commit、sha256
和产物路径，再把编译后的二进制作为部署产物放到服务器运行。服务器不需要保留源码 checkout，不走服务器 `git pull`，也不需要 GitHub deploy key；
交易服务器上的训练模型产物与交易程序二进制文件必须放在 `/home/ubuntu/live-trade`。
这些产物路径解析到 `/home/ubuntu/live-trade` 之外必须 fail closed。

模型文件不得通过GitHub、`git push`、`git pull` 或服务器 `git pull` 同步；不得放入 `wallets/`。
`wallets/` 只保留 gitignored 钱包 secret/env 与钱包公共地址派生配置材料。
部署模型时必须从本地已验证 artifact 路径直接复制到
`/home/ubuntu/live-trade` 下的独立运行产物目录，记录来源路径与 sha256 证据。
