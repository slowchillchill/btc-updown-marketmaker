# 仓库指南

## GitHub 治理与裁决回顾

- 本仓库采用全局 `AGENTS.md` 的 GitHub 任务治理。
- 项目看板：https://github.com/users/slowchillchill/projects/2
- 准备提出新裁决或收到用户新决定时，按[项目裁决索引 #62](https://github.com/slowchillchill/btc-updown-marketmaker/issues/62)
  的维护规则回读相关原始决定、替代关系和当前合同，由 `orchestrator` 同步获批结果与索引。

## 一级模块与职责

以下仅列一级业务模块及目标职责；“规划”不表示已实现，目录存在也不表示模块已验收。

```text
contracts/                # 跨模块公共类型与合同
data-collect/             # 外部行情采集与公共数据交接
data-pipeline/            # 数据准备、公共归约、历史回放与在线输入
sim-system/               # 模拟撮合、执行时序与模拟平台经济事件
intent-bridge/            # 公共交易意图到 SIM/LIVE 后端的提交适配
execution-normalization/  # 执行与经济反馈规范化，形成 NOE
ledger/                   # 账户、持仓、预留及费用返佣等经济事实归约
shared-run/               # 跨环境共用的观察、执行、账户读取与结算协调
baseline-mm/              # 人工盈利策略、参数与业务研究（当前仅文档）
backtest/                 # 公共历史回测入口、运行装配与通用结果汇总（规划）
model-train/              # 模型训练、开发竞赛与模型产物交付
offline-evaluation/       # 冻结模型的独立留出评价与晋级（规划）
paper-trade/              # PAPER 上层运行入口与在线模拟装配（规划）
live-trade/               # LIVE 上层运行入口与真实账户装配（规划）
```

- 回测、PAPER、LIVE 的上层入口组合策略库与公共运行能力；公共核心不反向依赖具体策略。
  `baseline-mm` 首版不另建三环境运行装配，策略规则和必要记忆由策略自身维护。
- 人工策略回测、模型开发比较与独立评价复用 `backtest`；模型开发竞赛仍归 `model-train`，独立留出评价仍归
  `offline-evaluation`。行情、模拟、NOE 和账本继续复用所属模块，不在消费者内复制。
- 并行任务明确公共能力的交付归属，缺口回到所属模块补齐；审查沿实际调用链核对复用。
  当前 PAPER 验收脚本仍在 `shared-run`，不代表上述完整 PAPER 宿主已交付。

## 代码定位与项目上下文

- 使用代码索引时，以当前任务实际开发或审查的工作树及其源码状态为范围；不能以主工作树索引代替
  任务工作树的当前源码。通用查询、覆盖核对和缺口回退遵守全局工程准则。
- 本项目包含根 Cargo workspace、`model-train/native-inputs`、`model-train/rust-ort-validation`
  的独立 Rust 工作区及 `model-train` 的 Python 代码。按任务涉及模块和实际调用链检查相关范围，
  不能以根 workspace 的结果推断其他范围已经覆盖，也不要求为每个 Cargo 包单独建立索引。
- 当前 Issue 与适用合同决定业务语义、批准范围和验收。跨模块或 Rust/Python 边界的关键关系回读
  实际源码；模块存在或图中有关联不代表对应能力已经实现或验收。

## 开发与部署

- 本地开发仓库为 `/home/chillchill/btc-updown-marketmaker`；新建独立开发事项时，从最新 `main`
  创建 `issue-<编号>-<slug>` 分支及隔离工作树。
- 每次合并后，按全局治理的收尾程序同步上述本地主工作树的 `main`；更新任务工作树或仅抓取远端
  引用不能代替该路径的源码更新，同步受阻时保留现场并报告具体缺口。
- 本地环境为原生 Ubuntu 26.04，配备 32 核 AMD AI MAX+、128G 共享内存、2TB 磁盘；
  通过 SSH 编排和监控远端任务。
- 远端源码工作目录统一为 `~/btc-updown-marketmaker`；实盘运行产物使用下述专用目录。
- `live-trade` 二进制文件在本地开发工作树编译，保留构建命令、提交、SHA-256 和产物路径；
  实盘服务器只接收和运行部署产物。实盘程序的 Rust 构建、测试、静态检查、格式化及构建工具链安装均在本地进行，
  实盘机的构建策略调整须经用户明确批准。
- 交易服务器上的模型和程序二进制文件统一放在 `/home/ubuntu/live-trade`，路径解析越界时拒绝执行。
- 模型仅从本地已验证产物路径直接复制到 `/home/ubuntu/live-trade` 下的独立运行产物目录，
  记录来源路径与 SHA-256 证据。
- `wallets/` 仅保存被 Git 忽略的钱包密钥、环境变量文件和钱包公共地址派生配置材料。

## Polymarket SDK

- Polymarket 开发使用官方 Rust SDK。能力、类型、认证、签名、请求构造、响应解析或覆盖范围
  存在缺口时，先向用户说明缺口、影响和可选方案，获得同意后再继续相关开发。
- 已批准的公开规则加载例外仅适用于 `sim-system` 进程冷启动：在创建任何会话和处理任何业务请求前，
  以一个显式 `btc-updown-5m` 参考市场同步一次规则。加载器仅查询当前 Issue 列明的固定官方
  CLOB/Gamma HTTPS `GET` 端点，使用 `reqwest` 和专用 `serde` 强类型响应，无钱包、凭据或签名；
  进程内所有运行与会话共享该不可变快照。`data-collect` 及实盘的认证、签名、订单构建、提交、撤单、
  查询继续使用官方 Rust SDK；具体实施按当前 Issue 的批准范围执行。

## 验证

- 测试验证可执行业务合同；面向人的文档通过审查核对。跨模块文档引用公共合同类型和版本。
- 网络帧、Parquet、JSON、manifest 和跨模块持久产物是不可信边界。每类可达损坏只保留
  一个代表性边界测试；真实平台样本和已发生事故优先于理论排列组合。
- 内部测试通过真实构造或解析入口建立符合平台约束的有效类型状态。
- 全工作区普通开发、提交和阶段完成回归固定使用：

  ```bash
  cargo test --workspace --lib --bins --tests --examples --locked --offline -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
  ```

  该具名测试的夹具依赖实际 UTC 时钟，验证记录必须明确标为“跳过”。
  `--all-targets` 可用于 `cargo check` 和 `cargo clippy` 的静态验证。
