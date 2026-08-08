# 仓库指南

## 当前活动事项与首读路由

- 截至 2026-08-08，当前工作树的活动事项为
  `DATA-COLLECT-EXTERNAL-DISK-GUARD-20260807`。该事项的需求为
  `demand/data-collect-external-disk-guard-demand.md`，已批准规范计划为
  `plan/data-collect-external-disk-guard-plan.md`，历史事实记录为
  `changelog/data-collect-external-disk-guard-changelog.md`。需求和计划已于 2026-08-08 获得
  用户批准；阶段 0 的批准后文档同步、独立复审、提交和首次分支推送已经完成。用户随后授权
  阶段 1 本地代码实现、必要测试、静态验证、提交和推送；两个目标的实现、完整验证、独立
  阶段复审、提交和推送已经完成。用户随后授权并完成阶段 2 的发布候选构建和生产只读核查；
  任务 2.3 确认同日清单身份阻断后，用户否决跨 UTC 日候选维护窗，确认 UTC 昨日及更早数据
  已经备份并接受当前日数据缺口，授权任务 2.4 停止旧服务、永久删除且只删除当前 UTC 日目录、
  安装冻结产物并启动候选长期运行。当前已完成停止、删除和安装，但首次候选启动被错误的
  `NRestarts` 验收门主动停止；第二次启动在 180 秒内保持候选进程稳定且没有重启，但启动恢复
  尚未完成，随后按获批验收门停止。取消该硬时限后，同一候选已完成 302 秒启动恢复和就绪后
  182 秒稳定写入验收；当前服务为 `active/running`，清单覆盖为 `active`。生产事实文档同步和
  两路独立审查已经通过。用户已授权把清理 timer 调度更新为每日 UTC `12:00:00` 并首次启用，
  并选择先复审、提交和推送当前 10 份文档与 1 份 timer 源文件，再部署这个 timer 单文件候选。
  服务器在部署前仍为原调度且 `disabled/inactive`；PR、手动运行清理服务、其他产物重装、再次
  手动删除及额外数据删除均未授权。
- 处理该事项前，必须先读取需求和计划首屏、计划中的“当前游标”和“已冻结的用户决定”；然后按
  当前任务读取 `data-collect/docs/CURRENT.md`、真实代码、数据和测试。不得从 changelog 的
  旧状态推导当前状态或动作。
- `data-collect/docs/CURRENT.md` 仅属于 `data-collect`。只有任务实际涉及 `data-collect` 的
  需求、代码、测试、运行、部署或需要核对其上游合同时，才按下文 `data-collect` 专属规则
  读取；不得因为该文件存在而把它作为无关 `data-pipeline` 工作的默认首读文档。
- 用户明确改变当前活动事项后，必须先同步更新本节的事项和首读路由，再按新的当前事项工作。

## 开发环境与部署拓扑

- 当前本地开发、文档落盘、代码编写与本地构建仓库：`/home/chillchill/btc-updown-marketmaker`
- 用户已于 2026-08-04 对事项 `DATA-PIPELINE-TRAINING-20260803` 作出窄范围路径覆盖：
  该事项后续开发、文档落盘、代码编写、本地构建和验收只在当前工作树
  `/home/chillchill/sim-system` 进行，不得切换到 `/home/chillchill/btc-updown-marketmaker`。
  本条不改变其他事项、远端目录或部署拓扑；若要再次改变该事项工作树，必须由用户明确决定。
- 用户已于 2026-08-07 对事项 `DATA-COLLECT-EXTERNAL-DISK-GUARD-20260807` 作出窄范围路径
  覆盖：该事项的文档、后续获批代码、本地构建和验收只在工作树
  `/home/chillchill/data-collect-external-disk-guard` 进行，并以合并 PR #3 后的
  `main@de51c297882016aa9b68e8079d648508973cc2c4` 为创建基线。不得回到旧
  `/home/chillchill/btc-updown-marketmaker` 工作树实施本事项，也不得把旧工作树中的未跟踪
  `data-collect-training-collector` 纳入本事项提交。
- 各远端服务器上的源码/工作目录统一写作 `~/btc-updown-marketmaker`；在 `ubuntu` 用户服务器上该路径通常解析为 `/home/ubuntu/btc-updown-marketmaker`，但文档和命令不得把它误写成本地开发路径。
- 与 Polymarket 相关开发必须基于官方 Polymarket Rust SDK。若
  Polymarket Rust SDK 的能力、类型、认证、签名、请求构造、响应解析或覆盖范围
  不满足当前需求，必须先明确告知用户具体缺口、影响范围和可选方案，并在获得用户
  同意后才能继续开发；不得在未获同意前自行改用原始 REST、手动签名、自建
  SDK 旁路或其他影子实现。
- 用户已于 2026-08-04 批准一项窄例外：只有 `sim-system` 每个新运行在首个业务请求前
  冷启动读取公开平台规则时，可以不经过官方 Rust SDK，改用固定官方 CLOB/Gamma HTTPS
  `GET` 端点、`reqwest` 与专用 `serde` 强类型响应结构。该加载器必须无钱包、无凭据、无
  签名，只能查询需求列明的公开规则端点；不得扩展为通用 Polymarket 客户端、
  `serde_json::Value` 任意探测、订单/账户/用户频道调用、手动签名或影子交易 SDK。现有
  `data-collect` 的 SDK 路径与 live 的认证、签名、订单构建、提交、撤单和查询仍必须只使用
  官方 Rust SDK。本例外只解决技术路线冲突，不自动批准尚处于草案状态的代码实施。
- 本地系统：`原生 Ubuntu 26.04`
- 本地硬件：`32` 核 `AMD AI MAX+`、`128G` 共享内存、`2TB` 磁盘
- 本地机器是当前的核心开发和监控中枢，通过 SSH 命令编排和监督远端训练、数据采集、模拟盘和实盘任务。

远端服务器（已配置好 SSH 密钥对）：

- 云 5090GPU 训练服务器：`ssh -p 40228 root@connect.bjb2.seetacloud.com`
- 云 3090GPU 训练服务器：`ssh -p 37267 root@connect.nmb2.seetacloud.com`
- AWS 爱尔兰模拟盘服务器：`ssh ubuntu@54.155.172.85`
- AWS 爱尔兰实盘服务器：`ssh ubuntu@108.130.74.143`
- AWS 爱尔兰 数据采集服务器：`ssh ubuntu@3.255.156.183`

AWS 爱尔兰实盘服务器 `108.130.74.143` 是运行目标，不是构建机。
它只有约 `2 vCPU`、`1.9 GiB RAM`、无交换空间，不具备 Rust 编译硬件条件。
禁止在该服务器上运行 `cargo build`、`cargo test`、`cargo check`、`cargo fmt`或任何会触发 Rust crate 编译的命令；也不得为编译目的安装工具链或构建依赖，除非用户在未来明确改变该硬件策略。`live-trade` 发布二进制文件
必须在本地 `/home/chillchill/btc-updown-marketmaker` 编译完成，保留构建命令、提交、SHA-256
和产物路径，再把编译后的二进制文件作为部署产物放到服务器运行。服务器不需要保留源码检出目录，不走服务器 `git pull`，也不需要 GitHub 部署密钥；
交易服务器上的训练模型产物与交易程序二进制文件必须放在 `/home/ubuntu/live-trade`。
这些产物路径解析到 `/home/ubuntu/live-trade` 之外必须拒绝继续执行。

模型文件不得通过GitHub、`git push`、`git pull` 或服务器 `git pull` 同步；不得放入 `wallets/`。
`wallets/` 只保留被 Git 忽略的钱包密钥、环境变量文件与钱包公共地址派生配置材料。
部署模型时必须从本地已验证产物路径直接复制到服务器
`/home/ubuntu/live-trade` 下的独立运行产物目录，记录来源路径与 SHA-256 证据。

## 需求、计划、变更记录与测试职责

- 需求文档只从产品经理层面的宏观视角描述业务目标、范围和验收结果，必须简洁、准确；
  不在需求文档中展开技术框架、逐字段实现、测试排列组合或开发过程。
- 计划是依据需求文档形成的业务语义和技术实施的唯一规范来源。计划必须包含技术框架实践
  与成熟工具复用的可行性调研、必要且简洁的技术框架测试、执行顺序和符合需求验收的测试，
  但不得为每个字段或理论异常穷举负例。
- changelog 只记录当时真实发生的事实、验证结果、用户决策和后续取代关系，不承担当前
  业务或技术权威。当前判断不得从 changelog 的旧状态、旧命令或旧路径推导。
- 新决策改变当前语义时，直接覆盖或删除需求与计划中的旧内容，不在当前文档中并排保留
  已失效方案、兼容叙事或层层追加的取代段落；旧事实只留在 changelog 和 Git 历史中。
- 跨模块文档只引用归属计划、公共合同类型和版本，不复制另一模块的字段清单、阶段状态或
  执行叙事。不得创建测试登记表、文档守卫或其他治理系统来机械同步这些文本。
- 禁止 Rust 测试读取、搜索或断言 Markdown 的固定文本、标题、状态词、文件数量或互引
  措辞。面向人的文档通过任务自审和独立审查核对，不进入 Cargo 测试完成门。
- 网络帧、Parquet、JSON、manifest 和跨模块持久产物是不可信边界。每类可达损坏只保留
  一个代表性边界测试；真实平台样本和已发生事故优先于理论排列组合。
- 输入通过边界校验后，内部测试不得绕过真实构造或解析入口，直接篡改类型来制造平台不可
  能状态。
- 普通开发、提交和阶段完成验证不得使用 `cargo test --workspace --all-targets`；全工作区普通
  回归固定使用
  `cargo test --workspace --lib --bins --tests --examples --locked --offline -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second`。
  该具名测试因当前 `data-collect` 测试夹具依赖实际 UTC 时钟而被精确跳过；验证记录必须明确
  写作“跳过”，不得表述为该测试通过。`--all-targets` 可以用于 `cargo check` 和
  `cargo clippy` 的静态验证；`data-collect` 中的 bench 只有在当前计划明确要求或用户明确批准后
  才能单独运行，并必须使用独立产物目录。

## 仅适用于 data-collect 的文档权威路由

- 处理任何 `data-collect` 需求、代码、测试、运行或部署问题前，必须先读
  `data-collect/docs/CURRENT.md`。它是唯一当前路由入口；其中列出的计划是当前业务语义和
  技术实施的唯一规范来源，需求只描述宏观业务目标与验收，changelog 只保存历史事实，
  `data-collect/docs/current/` 只提供由计划派生的当前操作和接口参考。
- `data-collect/docs/archive-manifest.tsv` 及其指定的 Git 标签、提交、路径和 blob 对象只用于
  回答“当时发生了什么”，不得作为当前执行依据。读取历史后必须回到
  `CURRENT.md`、当前代码和测试复核。
- 不得为搜索方便把清单中的历史长文恢复进活动工作树，也不得从历史片段中的
  `当前/下一步/PENDING/READY/命令` 推导当前动作。
- 清单中 `historical` 行对应的历史长文含首屏标记；按需读取局部片段时，必须
  同时读取同一 Git blob 的首屏生命周期、最终快照和替代关系字段，并核对
  `CURRENT.md` 对应主题归属文档。
- 清单中 `snapshot` 行是活动路径被当前层、路由层或治理层替换前的冻结
  快照，旧 blob 不保证含首屏标记；必须以 `kind=snapshot` 判定其当前执行权威为
  `none`，并在读取前核对清单行、活动树同路径文件和 `CURRENT.md` 对应主题
  归属文档。不得从快照自身的状态词或命令推导当前状态。
- 两类归档都不得只读搜索命中的中段；缺少所属类别要求的核对项时，只能作为未
  采用线索。
- 用户最新决策、当前计划与真实代码、数据或运行结果不能同时成立时，必须停止并列出精确
  冲突，交由用户决定。changelog 的旧事实或已经被新决策删除的旧文本不构成当前冲突来源。
- 当前运维文档本身不授权部署、启停服务、替换二进制文件、删除数据或其他远端状态
  变更；这些动作仍需独立批准的活动三件套。

## 模型框架竞赛研究来源

设计、审查或更新模型框架竞赛时，优先查询下列已核验来源。清单非穷举，也不是 PyTorch 或任何生态的准入白名单。这里固化的是检索入口和证据等级，不代表已批准任何框架、模型、依赖或实现路线；每次引用仍须记录访问日期、版本、标签、提交、许可证、维护状态和与本项目的适用边界。

### 证据优先级

1. 本仓库真实代码、类型化原始数据、清单、产物、测试、运行记录、已批准需求与变更记录。
2. Polymarket 官方文档、官方 Rust SDK、官方合约仓库、发布版本、议题和拉取请求。
3. 依赖或工具的官方文档、官方仓库、发布版本、议题和拉取请求。
4. 同行评审论文、作者发布的代码与可复现实验；论文中的预测指标或模拟收益不得直接当作本项目盈利证据。
5. 官方开发者社区、Quantitative Finance Stack Exchange、Cross Validated 等社区讨论，只用于发现问题、失败模式和待验证假设，不能覆盖前述权威来源。

普通 README 盈利自述、博客、排行榜、营销材料和 Reddit、Discord、Telegram 发言只能作为低等级线索；未经本项目固定数据、固定合同、固定模拟平台和真实产物复核，不得进入需求事实、框架晋级或盈利结论。

### 共用数据、训练、竞赛与部署工具

- `https://docs.pytorch.org/`、`https://github.com/pytorch/pytorch`、`https://discuss.pytorch.org/`：模型实现、数据加载、确定性、导出和官方开发者问题。PyTorch 可作为成熟公共实现底座候选，但不是预定获胜模型框架。
- `https://arrow.apache.org/docs/`：Arrow Dataset、PyArrow、Parquet、有界扫描、列投影、过滤和 RecordBatch 流式处理；用于共享规范数据与有界加载证据。
- `https://xgboost.readthedocs.io/`、`https://github.com/dmlc/xgboost`：可解释树模型与分组排序学习基线。
- `https://onnx.ai/`、`https://onnxruntime.ai/docs/`、`https://github.com/microsoft/onnxruntime`：模型交换、算子覆盖、推理、性能和一致性证据；导出成功不等于运行时语义或数值一致性通过。
- `https://optuna.readthedocs.io/`、`https://github.com/optuna/optuna`：预注册超参数搜索、持久研究、试验状态和预算管理候选。并行或分布式搜索不天然可复现，多目标剪枝等能力须按当前官方文档核验。
- `https://mlflow.org/docs/`、`https://github.com/mlflow/mlflow`：运行、参数、指标、数据集、产物和模型血缘的竞赛台账候选。MLflow 只帮助记录，不自动保证数据公平、统计有效或可复现。
- `https://docs.jax.dev/`、`https://flax.readthedocs.io/`、`https://www.tensorflow.org/guide`、`https://keras.io/`、`https://scikit-learn.org/stable/`、`https://lightgbm.readthedocs.io/` 与 `https://catboost.ai/en/docs/`：非 PyTorch 训练后端、树模型和统计基线的一手入口。共享竞赛合同必须保持语言和训练后端中立；能否入场取决于同一数据、动作、复现、资源、导出和运行时一致性门，而不是库名。
- `https://github.com/advisories` 与 `https://osv.dev/`：依赖安全公告检索入口；实际引入时还必须核对官方发布版本、包注册表元数据、传递依赖和锁文件。

优先复用成熟工具的明确能力；只有当其不能满足已冻结合同，或引入更高的许可、部署、性能或复杂度风险时，才设计本地最小实现。

### 模型、量化与模拟研究入口

- `https://arxiv.org/`、`https://proceedings.mlr.press/`、`https://openreview.net/`、`https://proceedings.neurips.cc/`、`https://papers.ssrn.com/` 和论文 DOI/出版社正文：检索原始论文、方法前提、统计设计与限制。
- `https://hftbacktest.readthedocs.io/`、`https://github.com/nkaz001/hftbacktest`：延迟、队列和重放假设参考；其市场数据重放不改变市场且无真实市场冲击，不能直接充当本项目模拟器真相。


### 外部方案进入竞赛前的最低筛选

任何外部框架、模型、代码或论文在进入候选清单前，至少核验：

- 是否解决本项目任务，而不是只报告分类准确率、F1、AUC 或通用股票回测；
- 是否严格因果并支持不规则业务事件、T-30、重叠市场、持续状态和市场隔离；
- 是否能消费同一逻辑 `ObservationContract`，通过薄且可哈希的适配器接入，而不重排事件、重建另一套盘口或标签；
- 是否能输出同一仅买入的 `Model Intent`，且每个输出有唯一消费方；
- 是否能在同一模拟器、NOE、账本、奖励、随机种子、数据切分和评估合同下复现；
- 是否满足训练资源预算及后续实盘的延迟、内存、导出和 Rust 运行时一致性门；
- 许可证是否允许本项目的使用方式，仓库是否仍维护，依赖是否可锁定，失败或限制是否在议题、拉取请求或发布说明中有证据。
- 外部代码、权重、数据和依赖是否完整记录来源、许可证、传递依赖与安全告警；许可证未知或不兼容时只能作为只读研究证据，不得执行或晋级，也不得接触钱包、交易密钥或生产凭据。

如果候选要求新增信息源、改变 `ObservationContract`、动作空间、市场规则、模拟器、奖励或评估方法，必须创建新的竞赛轮次或实验合同，不能与原轮候选混在同一榜单。
