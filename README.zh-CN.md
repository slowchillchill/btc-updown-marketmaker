[English](README.md) | **简体中文**

![金融市场数据、模型计算与本地推理](assets/readme/cover.png)

# btc-updown-marketmaker

**面向预测市场的金融 AI 与交易系统**

以 Polymarket 的 BTC 五分钟涨跌市场为研究场景，建设多源行情、人工策略、历史回测、GPU 强化学习与本地模型推理能力。项目关注模型如何理解市场、作出交易决策，以及如何用真实数据和账户结果检验这些决策。

[![点击前往 YouTube 观看项目介绍](assets/readme/video-thumbnail.png)](https://youtu.be/a9qzkJ_NjKA)

[▶ 前往 YouTube 观看项目介绍](https://youtu.be/a9qzkJ_NjKA)

视频展示早期两小时演示，与下方七小时成绩属于不同回测。

[策略研究](baseline-mm/) · [模型训练](model-train/) · [交流与合作](https://github.com/slowchillchill)

## 项目目标

将金融策略研究推进到可复现的工程实践：保留市场数据的时间顺序，在模拟执行与账户约束下比较策略，记录训练过程，并验证模型交付后的行为一致性。

端侧 AI 在本项目中指**交易节点上的本地模型推理**。B 模型路线复用人工策略已使用的公共数据、回测、执行和账户能力，已按 [#181](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181) 实现真实模型消费者与整组学习接口。[模型接口及证据边界](model-train/README.md)

## 工程成果

| 能力 | 已有证据 |
| --- | --- |
| 金融数据工程 | 完成 **18 天、约 7.62 亿行原始数据**的来源健康研究，覆盖 2,864 个输入文件；通过按文件汇总控制中间数据规模。[研究记录](baseline-mm/docs/research/complementary-pair-source-health-18d-20260909-20260926.md) |

B 路线已实现 GRU Actor/Critic 消费者、整组 PPO/TBPTT 学习、有限时域经济目标及学习检查点事务恢复。[原工程交付](model-train/LEARNING.md)记录有限合成验证。后继研究分别获准并完成了[最小真实数据链](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181#issuecomment-6305408177)和[有限两臂机制研究](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181#issuecomment-6339458665)。这些结果不证明完整 PPO 正确性、独立留出表现、全日容量或盈利；模型及专项奖励机制尚未采用。

## 人工策略回测

以同一互补配对策略的 **7 组候选配置**比较连续历史回放表现。行情范围为 **2026-09-12 UTC 00:00—07:00**，每组独立初始资金 **2,000 pUSD**；本次结果于 2026-10-14 提取。

| 候选配置 | 期末净值 NAV（pUSD） | 净值变化率 | 确认净损益（pUSD） | 最大净值回撤 |
| --- | ---: | ---: | ---: | ---: |
| 参考配置 `center` | 1,950.54 | -2.47% | -36.01 | 15.15% |
| 关闭创新项 `no-innovation` | 2,321.12 | +16.06% | +323.12 | 5.66% |
| 小订单 `small-size` | 2,060.89 | +3.04% | +61.61 | 2.98% |
| 关闭库存偏移 `no-inventory` | 1,688.54 | -15.57% | -292.22 | 23.84% |
| 关闭主动补腿 `passive-repair` | 1,848.86 | -7.56% | -130.90 | 15.74% |
| 严格健康阈值 `health2` | 1,972.36 | -1.38% | -3.27 | 11.95% |
| 宽松健康阈值 `health8` | 2,201.74 | +10.09% | +226.11 | 13.66% |

净值变化率为期末 NAV 相对初始资金的变化。NAV 采用截止时点的保守持仓估值，因此净值变化与确认净损益不同；未成熟返佣未计入。回撤为起点至七小时截止的累计最大值。

这些成绩来自 [#301 公共资金政策修复](https://github.com/slowchillchill/btc-updown-marketmaker/issues/301)之前的历史版本，不用于评价修复后的资金政策。本表取自各组共同完成的前七小时快照；原定十二小时作业在第八小时因资金预留失败中断，截止时仍有未决持仓和开放订单。模拟延迟受请求标识影响，组间差异不能全部归因于参数。以上为单次历史模拟结果，不代表实盘收益或稳定盈利。[原始数值摘录与口径](baseline-mm/docs/research/complementary-pair-seven-hour-results-20261014.json)

## 模块关系与决策流

下图展示人工策略的历史回测闭环。`backtest` 组织整个运行，`shared-run` 协调观察、决策、提交与账户更新。B 模型消费者和学习器复用这条公共链；具体接口另见 [model-train 说明](model-train/README.md)。

![模块关系与行情、决策、执行反馈的循环](assets/readme/architecture-flow.gif)

[查看静态图](assets/readme/architecture-static.png) · [交互版 HTML（下载后本地打开）](assets/readme/architecture.html)

行情经采集和数据准备形成公开观察；策略结合账户状态生成交易意图，经资金预留后提交模拟执行。成交与经济反馈经过规范化和账本更新，再进入下一轮决策。隐藏撮合材料仅交给模拟器，结果浏览器只读展示报告。动画高亮表示流程顺序，不表示实际耗时或实时运行状态。

<details>
<summary>查看模块职责</summary>

| 功能区 | 模块 | 职责 |
| --- | --- | --- |
| 市场数据 | `data-collect`、`data-pipeline` | 多源采集、数据准备与历史回放 |
| 策略与模型 | `baseline-mm`、`model-train` | 人工策略研究、模型开发与训练 |
| 仿真与回测 | `sim-system`、`backtest` | 模拟撮合、历史验证与结果汇总 |
| 执行与账户 | `intent-bridge`、`execution-normalization`、`ledger`、`shared-run` | 交易提交适配、执行反馈、资金与结算 |
| 研究可视化 | `visualization` | 浏览市场、账户与运行证据 |

`contracts` 定义跨模块公共类型与接口。完整的 PAPER、LIVE 上层入口及独立留出评价仍属于规划范围。

</details>

## 技术栈

| 方向 | 技术 | 用途 |
| --- | --- | --- |
| 系统与执行 | Rust · Tokio · Polymarket Rust SDK | 异步行情、交易接口与公共运行能力 |
| 数据工程 | Arrow · Parquet · Rayon · DuckDB | 列式存储、并行处理与研究查询 |
| 模型训练与推理 | Rust · Python · PyTorch · GRU · PPO/TBPTT | 模型消费者、整组学习及检查点接口；[当前实现](model-train/README.md) |
| 研究可视化 | Rust · React · TypeScript · ECharts | 读取与浏览研究证据 |

## 当前阶段与合作

项目处于策略研究阶段。B 路线已在公共实现上接入模型推理、整组学习及显式规则加载与恢复接口，并完成上文所列的有限真实数据研究。正式训练、独立留出评价、全日容量验证及完整实盘部署仍为后续事项，须分别取得相应授权和预算；各项成果以链接中的版本、样本和验证范围为准，已完成运行不授权新作业。[规则加载与恢复接口](model-train/RULES.md)

关注金融场景 AI、GPU 训练流水线、Rust 交易基础设施与本地模型推理。欢迎相关技术岗位与项目合作交流：[GitHub · slowchillchill](https://github.com/slowchillchill)。
