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
- AWS 爱尔兰 数据采集服务器：`ssh ubuntu@3.255.156.183`

AWS 爱尔兰 live-trade 服务器 `108.130.74.143` 是运行目标，不是构建机。
它只有约 `2 vCPU`、`1.9 GiB RAM`、无 swap，不具备 Rust 编译硬件条件。
禁止在该服务器上运行 `cargo build`、`cargo test`、`cargo check`、`cargo fmt`或任何会触发 Rust crate 编译的命令；也不得为编译目的安装 toolchain 或 build dependencies，除非用户在未来明确改变该硬件策略。live-trade release binary
必须在本地 `/home/chillchill/btc-updown-marketmaker` 编译完成，保留构建命令、commit、sha256
和产物路径，再把编译后的二进制作为部署产物放到服务器运行。服务器不需要保留源码 checkout，不走服务器 `git pull`，也不需要 GitHub deploy key；
交易服务器上的训练模型产物与交易程序二进制文件必须放在 `/home/ubuntu/live-trade`。
这些产物路径解析到 `/home/ubuntu/live-trade` 之外必须 fail closed。

模型文件不得通过GitHub、`git push`、`git pull` 或服务器 `git pull` 同步；不得放入 `wallets/`。
`wallets/` 只保留 gitignored 钱包 secret/env 与钱包公共地址派生配置材料。
部署模型时必须从本地已验证 artifact 路径直接复制到服务器
`/home/ubuntu/live-trade` 下的独立运行产物目录，记录来源路径与 sha256 证据。

## data-collect 文档权威路由

- 处理任何 `data-collect` 需求、代码、测试、运行或部署问题前，必须先读
  `data-collect/docs/CURRENT.md`。它是唯一 current 路由入口；只有其列出的 master
  demand/plan/changelog 和 `data-collect/docs/current/` 主题 owner 能提供当前文档
  合同。
- `data-collect/docs/archive-manifest.tsv` 及其指定 Git tag/commit/path/blob 只用于
  回答“当时发生了什么”，不得作为当前执行 authority。读取历史后必须回到
  `CURRENT.md`、当前代码和测试复核。
- 不得为搜索方便把 manifest 中的历史长文恢复进活动工作树，也不得从历史片段中的
  `当前/下一步/PENDING/READY/命令` 推导当前动作。
- manifest `historical` 行对应的历史长文含首屏 banner；按需读取局部片段时，必须
  同时读取同一 Git blob 的首屏生命周期/最终快照/supersession 字段，并核对
  `CURRENT.md` 对应主题 owner。
- manifest `snapshot` 行是活动路径被 current/route/governance 层替换前的冻结
  快照，旧 blob 不保证含 banner；必须以 `kind=snapshot` 判定其当前 authority 为
  `none`，并在读取前核对 manifest 行、活动树同路径文件和 `CURRENT.md` 对应主题
  owner。不得从 snapshot 自身的状态词或命令推导当前状态。
- 两类归档都不得只读搜索命中的中段；缺少所属 kind 要求的核对项时，只能作为未
  采用线索。
- `CURRENT.md`、current 文档、代码、测试或用户最新决策相互冲突时，必须停止并列出
  精确冲突，交由用户决定。
- current 运维文档本身不授权部署、启停服务、替换 binary、删除数据或其他远端状态
  变更；这些动作仍需独立批准的活动三件套。

## 模型框架竞赛研究来源

设计、审查或更新模型框架竞赛时，优先查询下列已核验来源。清单非穷举，也不是 PyTorch 或任何生态的准入白名单。这里固化的是检索入口和证据等级，不代表已批准任何框架、模型、依赖或实现路线；每次引用仍须记录访问日期、版本/tag/commit、许可证、维护状态和与本项目的适用边界。

### 证据优先级

1. 本仓库真实代码、typed raw、manifest、artifact、测试、运行记录、已批准需求与 changelog。
2. Polymarket 官方文档、官方 Rust SDK、官方合约仓库、release、issue 和 PR。
3. 依赖或工具的官方文档、官方仓库、release、issue 和 PR。
4. 同行评审论文、作者发布的代码与可复现实验；论文中的预测指标或模拟收益不得直接当作本项目盈利证据。
5. 官方开发者社区、Quantitative Finance Stack Exchange、Cross Validated 等社区讨论，只用于发现问题、失败模式和待验证假设，不能覆盖前述权威来源。

普通 README 盈利自述、博客、排行榜、营销材料和 Reddit/Discord/Telegram 发言只能作为低等级线索；未经本项目固定数据、固定合同、固定模拟平台和真实 artifact 复核，不得进入需求事实、框架晋级或盈利结论。

### 共用数据、训练、竞赛与部署工具

- `https://docs.pytorch.org/`、`https://github.com/pytorch/pytorch`、`https://discuss.pytorch.org/`：模型实现、数据加载、确定性、导出和官方开发者问题。PyTorch 可作为成熟公共实现底座候选，但不是预定获胜模型框架。
- `https://arrow.apache.org/docs/`：Arrow Dataset、PyArrow、Parquet、bounded scan、projection、filter 和 RecordBatch streaming；用于共享 canonical 数据与有界加载证据。
- `https://xgboost.readthedocs.io/`、`https://github.com/dmlc/xgboost`：可解释树模型与 grouped learning-to-rank 基线。
- `https://onnx.ai/`、`https://onnxruntime.ai/docs/`、`https://github.com/microsoft/onnxruntime`：模型交换、算子覆盖、推理、性能和 parity 证据；导出成功不等于运行时语义或数值 parity 通过。
- `https://optuna.readthedocs.io/`、`https://github.com/optuna/optuna`：预注册超参搜索、持久 study、trial 状态和预算管理候选。并行/分布式搜索不天然可复现，多目标 pruning 等能力须按当前官方文档核验。
- `https://mlflow.org/docs/`、`https://github.com/mlflow/mlflow`：run、参数、指标、dataset、artifact 和 model lineage 的竞赛台账候选。MLflow 只帮助记录，不自动保证数据公平、统计有效或可复现。
- `https://docs.jax.dev/`、`https://flax.readthedocs.io/`、`https://www.tensorflow.org/guide`、`https://keras.io/`、`https://scikit-learn.org/stable/`、`https://lightgbm.readthedocs.io/` 与 `https://catboost.ai/en/docs/`：非 PyTorch 训练后端、树模型和统计基线的一手入口。共享竞赛合同必须保持语言和训练后端中立；能否入场取决于同一数据、动作、复现、资源、导出和 runtime parity 门，而不是库名。
- `https://github.com/advisories` 与 `https://osv.dev/`：依赖安全公告检索入口；实际引入时还必须核对官方 release、包注册表元数据、传递依赖和锁文件。

优先复用成熟工具的明确能力；只有当其不能满足已冻结合同，或引入更高的许可、部署、性能或复杂度风险时，才设计本地最小实现。

### 模型、量化与模拟研究入口

- `https://arxiv.org/`、`https://proceedings.mlr.press/`、`https://openreview.net/`、`https://proceedings.neurips.cc/`、`https://papers.ssrn.com/` 和论文 DOI/出版社正文：检索原始论文、方法前提、统计设计与限制。
- `https://hftbacktest.readthedocs.io/`、`https://github.com/nkaz001/hftbacktest`：latency、queue 和 replay 假设参考；其 market-data replay 不改变市场且无真实 market impact，不能直接充当本项目 simulator 真相。


### 外部方案进入竞赛前的最低筛选

任何外部框架、模型、代码或论文在进入候选清单前，至少核验：

- 是否解决本项目任务，而不是只报告分类准确率、F1、AUC 或通用股票回测；
- 是否严格因果并支持不规则业务事件、T-30、重叠 market、持续状态和 market 隔离；
- 是否能消费同一逻辑 `ObservationContract`，通过薄且可哈希的 adapter 接入，而不重排事件、重建另一套盘口/标签；
- 是否能输出同一 BUY-only `Model Intent`，且每个输出有唯一 consumer；
- 是否能在同一 simulator、NOE、ledger、reward、seed、split 和评估合同下复现；
- 是否满足训练资源预算及 future live 的延迟、内存、导出和 Rust runtime parity 门；
- 许可证是否允许本项目的使用方式，仓库是否仍维护，依赖是否可锁定，失败/限制是否在 issue、PR 或 release 中有证据。
- 外部代码、权重、数据和依赖是否完整记录 provenance、许可证、传递依赖与安全告警；许可证未知或不兼容时只能作为只读研究证据，不得执行或晋级，也不得接触钱包、交易密钥或生产凭据。

如果候选要求新增信息源、改变 `ObservationContract`、动作空间、市场规则、simulator、reward 或评估方法，必须创建新的竞赛轮次/实验合同，不能与原轮候选混在同一榜单。
