# 有限来源进展入口与十二分钟测量

`PublicObservationCoreV1::push_source_progress` 用于只消费来源时间、质量和同步事实的研究。它与原 `push` 共用输入验证、业务归约、代际恢复、选择和封组；完整组归约后返回 `PublicSourceProgressAppliedV1`，不生成各市场盘口观察，也不复制全来源进展集合。每组只交付有变化的 `SourceStreamProgressV1`；代际变化使另一资产待快照的同步变化同样交付，同值有效新消息保留自己的证据身份。固定市场首次建立时交付原 `MarketIdentityV2`（含窗口）及资产绑定。

紧凑返回类型不能交给完整 writer、SIM、训练或经营回测。原 `push`、观察、writer、完整 recording/replay、输入与组位置、正式稳定前缀保持。紧凑入口的 `applied_prefix` 只表示核心已应用的输入，不是持久提交证明。其每组 `visible_at_utc_ms` 沿原接收／采集准入与依赖事实时间最大值；SIM 原时间函数调用同一个公共纯原语。消费者需要单调可见前沿时维护该前沿，不新增业务时钟。

`data-pipeline-training::finite_source_progress::prepare_finite_source_progress_v1` 接收显式 `PublicFiniteFileV1` 和可选的原 `MetadataInitializationConfigV1`，沿原文件身份／模式／解码边界、两遍代际识别、外排、`HistoricalMergeV1::prepare_input` 连续消费。跨文件和跨UTC日不会重建core；损坏行的原错误继续传播，包含所选文件的窗外坏行。它不生成 captured/public recording、hidden 或稳定目录。文件事实取自已审清单；此入口不签发全日准入。

测量例 `measure_finite_source_progress` 只输出来源标量增量、首次市场绑定、资源及分母，未引入normal/max、三态、健康阈值或策略动作。其私有配置包含 `preparation`（上述入口配置）和 `progress_interval_seconds`；0关闭周期进度，阶段变化仍输出。逐行输出中来源数字索引由 `summary.json.sources` 解释；每条来源增量依次为：来源索引、输入位置、组位置、该组来源可见时间、单调可见前沿、最后有效消息、最后观察消息、连接代次、同步、质量。消息只投影原身份／接收时间／事件时间，未知值仍为null。

```bash
cargo build -p data-pipeline-training --example measure_finite_source_progress --release --locked --offline -j 8
measure_finite_source_progress CONFIG OUTPUT
```

以下实测仅覆盖执行Issue #246批准的UTC 2026-09-09 `[00:00,00:12)` 与原初始化；不代表18日资源闭合。core仍保留原历史市场、去重、证据及参考状态，投影优化没有给这些状态新增期限或淘汰政策。

## 一次运行的输入、结果与对照

本次运行绑定实现提交 `6abafc2605361b580f6f5b57309032b96eea8cdc`；随后文档提交只增加结果，不代表再次运行。二进制17,963,464字节，SHA-256 `8ea10f925ab0d05fdd7529b8eaf4f5862555ba596b236235dcf9a039291333c0`。完整逻辑配置、来源分母、阶段原始计数、对象数量及对照结果见[机器报告](finite-source-progress-12m.json)。私有生效配置SHA-256 `dbfcf4929280c9aec84de7ac7506caf5455e9ad1f42eca241696c997150031dc`；公开配置仅替换路径，不冒称字节摘要相同。

沿原 `issue-214-end-to-end-12m-20261004` 十二分钟材料的生效配置，读取已审2026-09-09日清单全部145文件（2,169,975,828字节、34,766,440行），两遍均完整解码并按原接收窗口筛选。decode1024行／64MiB；外排32768行／64MiB、fan-in8、16打开文件、128临时run、8GiB原参数保持。初始化使用原09-08 metadata的23小时文件与两个原Snapshot身份，通过原选择器选择3条前序，输入位置[0,1,2]及全部来源消息身份与旧证据相同，未补其它前史。

一次运行正常End且exit0：208,684窗内主输入+3初始化+1正常End，共208,688输入、208,638组；5市场、10资产、12个实际有进展scope、388,930条来源进展变更。紧凑输出未构造原806,966份全市场观察，不生成原1,259,221,189字节capture。没有重跑或改写旧十二分钟健康候选统计。

新来源标量流与[旧报告](../../../baseline-mm/docs/research/complementary-pair-market-source-health-20260909-0000-0012.md)及其已归档区间只读对照：以下8个重合scope的有效身份变化分母、窗内首尾身份／接收／事件／可见前沿／输入位置一致；全部181,933条标量区间逐条相等。第9个TWAP30继续无进展，其整窗未知区间也与旧1条区间相同。两个目标市场的窗口与Up/Down绑定、原初始化身份和原输入／组分母相同。原实际报告只保存市场／资产数量，未将本轮详细对象计数冒称与旧实际报告逐项相等。

| 重合scope | 有效身份变化 | 相等的标量区间 |
|---|---:|---:|
| 4352318-Up / Down | 63907 / 63791 | 41561 / 41541 |
| 4352362-Up / Down | 74550 / 74333 | 44889 / 44789 |
| Binance成交 / 深度 | 4405 / 7175 | 2087 / 5977 |
| Chainlink spot / TWAP60 | 659 / 661 | 544 / 545 |

## 实测资源与工程验证

GNUtime总墙钟239.30秒、user235.51秒、system1.31秒、CPU98%，峰值RSS421,388KiB（431,501,312字节）。阶段端点来自`/proc/self`，CLK_TCK=100，CPU为累计tick差；以下RSS是端点，不是各阶段独立峰值。第二遍供料和初始外排交错，未虚构两者独立成本。

| 阶段 | 墙钟s | user / system s | 结束RSS KiB | 结束临时逻辑字节 |
|---|---:|---:|---:|---:|
| 原初始化选择 | 0.015 | 0.00 / 0.00 | 18924 | 0 |
| 第一遍代际扫描 | 102.789 | 101.86 / 0.55 | 307860 | 0 |
| 第二遍供料／初始外排 | 113.040 | 112.07 / 0.52 | 318648 | 33290243 |
| 外排finish／merge建立 | 1.410 | 0.62 / 0.06 | 318680 | 41632600 |
| 公共归约／标量输出 | 21.973 | 20.94 / 0.15 | 318904 | 41632600 |

每秒监控的临时逻辑空间峰值44,138,247字节，结束41,632,600字节；来源标量、市场绑定与summary合计156,103,723字节。GNUtime文件系统I/O原计数inputs4,521,536／outputs463,919；它们不是文件长度。进程阶段I/O及页缓存影响的原计数在JSON保留，不能据第二遍物理读取较小宣称没有读取。

最终core仍有223,441条原保留状态、195,594个seen-message、26,687个Binance深度保留条目；PM数量／level证据各804，absence344、selection1148、公共level anchor1148；pending terminal0。上述条目数不等于分配字节或RSS。十二分钟测量没有证明18日历史状态增长可承受。此次总耗时包含两遍原日文件准备，旧健康运行只重放已录材料，两者不是相同工作量，不给出加速比。

代表性合法构造对照在同一输入上核对完整与紧凑路径的来源事实、组身份／位置、独立原可见时间公式、市场绑定及所有core对象计数：资产隔离、同值有效新确认、真实重复、无效／无关来源、持续旧事件、PM代际重置另一资产与真实快照恢复、Binance质量缺口与恢复保持。有限入口另验证跨午夜连续core以及窗外坏行仍传播；原完整writer/replay回归保持。首个新增夹具使用了不匹配当前深度状态的坐标而真实失败，修正为原合法101→104路径后通过，未修改生产接纳语义。

固定workspace回归106个结果目标、1361通过、0失败、23ignored；具名依赖实际UTC的 `four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second` 明确跳过。workspace及最终相关模块`clippy --all-targets --locked --offline`通过，既有警告保留；fmt、diff检查通过。构建、回归、实际测量串行，实际输入只消费一次。

必要配置、二进制、原始进度／GNUtime日志、来源标量输出、对照脚本与结果独立保存在已授权归档逻辑位置 `issue-246/compact-source-progress-20261005/`；本机路径仅在私有交接。公开报告不采用正式健康数值，不交付三态策略，也不启动18日、经营回测、PAPER/LIVE、训练、部署或资金。
