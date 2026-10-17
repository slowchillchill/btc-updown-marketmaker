# candidate_v2 同窗主动补腿 OFF 对照包（2026-10-17）

本包落实 [#315的OFF准备授权](https://github.com/slowchillchill/btc-updown-marketmaker/issues/315#issuecomment-6232603784)，只准备和审查同窗单因素对照。**真实OFF回放与本轮预算尚未签发。** [ON首包](complementary-pair-candidate-v2-first-20261017.md)已经完成一次完整小时，[审定结果](https://github.com/slowchillchill/btc-updown-marketmaker/issues/315#issuecomment-6232563454)保留其ON身份，不重跑ON，也不继承其已结束预算。

## 唯一实验因素与完整条件

可执行配置为 [candidate-v2-active-off-20261017.json](../../../backtest/research/candidate-v2-active-off-20261017.json)。它直接取ON已审41字段配置，仅将 `repair.enable_active_repair` 从布尔值 `true` 改为 `false`，其余40字段逐值相同。候选 `strategy` 与 `run.strategy.effective_parameters` 两处完整参数一致。全部变化的原值、新值和字段路径保留在包内 `evidence/configuration-diff.json`；没有顶层strategy覆盖，启动脚本也不传策略覆盖参数。

运行、会话、账户、请求、桥接、公开/隐藏流、消费序列、输出及unit使用独立身份。这些标识隔离材料，不是第二个策略实验因素。策略版本仍为 `complementary_pair / candidate_v2`，没有将其另造为新算法版本。

| 条件 | ON与OFF共同绑定 |
| --- | --- |
| 窗口 | UTC2026-09-24 `[00:00,01:00)`；毫秒 `[1790208000000,1790211600000)`，单一原fold4 center |
| 账户与参数 | 每次独立2000 pUSD、50份、gate0；全部概率、信号、健康、报价、资金/库存风险和路径参数沿ON原值，不新增预热或前日账户继承 |
| 运行源码/程序 | `c3e500e6252481db24c12a882301a2b0540e6017`；复用ON实际stage-timing程序，34069848字节，SHA-256 `025c2397a4664f56701d8103a0052b00fa85c9de60b357656c176ec72d0b9b03`，没有重建 |
| 源码材料/锁 | 原 `git archive --format=tar` SHA-256 `c6f5eee7c8412a44dbe4471ae80a39ec177c14078f739e2e70a786ecba158786`；Cargo.lock `684470a91e49d4a3ec0066ae1a33666cb635b2f90d1902fc0fa62ecf3a7be4ed`，完整源码仍保存在原ON准备归档 |
| 公共政策 | A1，HistoricalBacktest、`PUBLIC_TRADE_CONSERVATIVE_FIFO_V1`、Gamma `qualified_public_consumption`、`ActualCashNoMarketLimit`、原SIM自动结算→NOE→Ledger |
| 宿主时序 | public delivery / calculation start / calculation duration / dispatch为固定 `0/0/15/0ms`，SIM原经验时延不变 |
| 规则/最终性 | 原文件分别为 `70e592f369d3e58f0ab4bc7cdfb8d1e2ae48ceae54321c66f908bdf63c500f85` / `b98102f86cbd2e0d635bdc5074dcb1d7c1fb1a84de61d419d07a475c2e8533ef`；因果起点1790207400076；政策假设及空failure overrides沿ON披露 |
| 随机条件 | 同种子 `6e92d4823607cfd85fae1596d28aac4bdfb58b84e4c94a772753ec7faf1fc66d` 与 `xoshiro256plusplus_jump_multiply_high_empirical_rank_v2`；不从OFF名字派生种子 |
| 输入/初始化 | 09/24原manifest，09/23 23时原元数据Snapshot与既有3条前序选择；两份manifest、初始化文件及选择条件同ON，不增加前史用途 |
| 资源/汇总 | 单日期单候选、原8192领先目标/128交接及其余资源配置；同运输诊断关闭、stage timing开启、60秒进度、原summarize_research.py |

源码 `baseline-mm/src/strategies/complementary_pair/repair.rs` 中，只有开关为true时才计算active候选；false时active保持None，FAK选择分支不成立。随后原Maker修复候选继续和等待、保留孤腿比较。OFF不会一并关闭Maker修复、新配对、撤单或其他原策略路径；也不新增固定过补份额/金额门。正常业务拒绝、无成交、部分成交、费用变化和本地整批资金规则仍按原合同处理。

当前OFF配置SHA-256为 `6e1513bc95d9d29f078f6f85c02b835f3213df51ffba06eb8905034c6532fe08`。准备包的Git提交由本包所属PR定位，运行源码仍是上述c3e500e6，避免把未存在的包提交填成构建提交。

## 输入、路径和停止

本轮只补核09/23与09/24原manifest摘要及必要元数据文件摘要，均与ON已审材料一致；不重新扫描全池，也不读新日期经营结果。manifest分别为 `fd2958a10e8a36947d3377c5fbd9ff9d5d26d11c221853179cb25aec0e247465` 和 `4a7bf6e3fc0255ca583719f197ade087819a0f0718d84e0b7655217befca7caf`。原初始化入口的3条前序与最晚准入1790207700060证据复用；其选择配置逐值不变。D-0361的数据用途、开发身份和业务完整性边界沿原首包。

实际OFF根目录通过私有交接绑定，独立于ON包和待清理工作树。JSON路径以作业当前目录为基准：

```text
OFF_ROOT/
  bin/backtest-cli                       # 原ON程序的独立文件副本
  bin/summarize_research.py
  configs/research-days.json
  inputs/platform-rule-snapshot-v3.json
  inputs/trade-finality-policy-v2.json
  datasets/                              # 指向原数据根，不复制行情
  run_candidate_v2_active_off.sh
  evidence/
  outputs/2026-09-24/preparation-active-off/  # 运行后才出现
  outputs/2026-09-24/center-active-off/       # SIM、NOE、Ledger、run及result
```

[准确启动脚本](../../../backtest/research/run_candidate_v2_active_off.sh)只替换ON脚本的OFF身份和输出目录。真实范围及投入获准后，唯一启动命令为：

```bash
bash "$OFF_ROOT/run_candidate_v2_active_off.sh" "$OFF_ROOT"
```

**推荐一次整体30分钟，第25分钟请求协作预算停止，5分钟供返回及汇总，当前仍是未批准建议。** 原外层Type=exec service使用RuntimeMaxSec=1800、KillMode=control-group、KillSignal=SIGKILL；CLI使用 `--wall-budget-seconds 1800 --summary-reserve-seconds 300`，包含读取配置、规则、初始化与运行；CLI、落盘和原Python汇总都在同一service期限内。固定新unit是 `issue315-candidate-v2-active-off-20261017.service`，与ON unit不同。

复用ON已验证的2秒合成整组停止证据，没有新建停止机制。初始化或长操作可能延迟感知第25分钟的预算标记；没有外部优雅停止接口，第30分钟外层开始强杀整组，不能保证完整报告、排空、time输出或精确瞬间退出。内核不可中断等待等限制保持。手工终止只操作新unit：

```bash
systemctl --user status issue315-candidate-v2-active-off-20261017.service
systemctl --user stop issue315-candidate-v2-active-off-20261017.service
```

期限到达不重跑、加时或续跑；原日入口没有检查点恢复能力。ON实际328.007秒是已有观测，不保证OFF更快或在同样时间结束。运行成本仍未知。获准运行后仍由主会话每15分钟查询原进度；未知总量/剩余时间不编造百分比。

## 比较问题与解读

本轮要比较的是“保留主动补腿”与“关闭主动补腿、保留原其他路径”两套策略在同一开发小时的整体经营结果。不是从ON记录中扣掉Taker费用或删除FAK成交来构造OFF；OFF库存、Maker修复、订单和未来可用现金会沿实际反馈自然变化。

| 比较量 | 原证据及含义 |
| --- | --- |
| 净资产与损益 | 分别报告保守NAV相对初始差额、已确认净损益、现金/预留及当前资产。两者不能混为最终清算收益 |
| 成交与费用 | 分别列Maker/Taker实际份额、本金、费用、已付与未到账返佣，以及实际发送/接纳/无效果结果；trade最终性阶段不重复计数 |
| 库存与资本 | 原孤腿、持仓成本、开放订单/义务、未知状态，平均/最大资本占用、回撤及时间覆盖；Redeemed市场的历史repair_gap不当作期末库存 |
| 尾部与完整性 | RangeEnd、recording_drained、最后观察右界−1、原完整窗判定；待完成计算、未闭合订单和未到账款项保留，不强平或补未来 |
| 成本 | 准备、CLI、原Python汇总、后续有限读取分别计量；原stage timing重叠/嵌套部分不相加当墙钟；强停后不可得项写未知 |

只有两个账户均完整覆盖同一窗口时，才列完整窗的OFF−ON差额，并保留各项原值和方向。ON已完整；OFF若StrategyExit，则按合法退出时点和原因报告；若BudgetStop、错误或硬停，则报告实际前缀、未知与尾部，不能直接对完整ON窗口做收益排名。完整窗仍只有一个开发小时的有限证据，不构成普遍盈利、参数晋级或完整日预算保证。

同seed固定原随机条件，但策略请求序列可能分叉，不能承诺ON/OFF每一笔延迟随机数严格配对。运行ID变化仅隔离材料；不要求两次依赖身份的摘要相同。

现有记录不能还原逐次策略ProfitAllocationV2的分配/保留/释放；规范化RejectedNoEffect也没有具体SIM拒绝原因。这两项缺源沿ON已披露边界，不据余额倒推利润支持，不把无效果结果擅自标为余额不足或无盘口。当前没有新增诊断、影子账或反事实经营归约。

## 定向检查与准备成本

本轮已完成：两处41字段完整配置一致，唯一策略差异为布尔true→false；其余条件与身份/输出差异逐项列出；实际程序/汇总/规则/finality副本与ON完全一致；正式CLI成功解析OFF完整配置后，在故意缺失规则文件处停止，未进入输入准备或回放。解析到运行描述重建的路径按当前源码核对；同源码已通过的 `actual_baseline_command_applies_explicit_values_and_false_then_records_effective_config` 证据复用，证明false曾被记录为生效参数。新OFF运行的run.json尚未生成，不能声称已经读到它的生效报告。

脚本无策略覆盖，shell语法与差异检查通过，新unit核对时不存在且无活动回放；停止能力复用原证据。未重建、未重跑测试套件、未运行OFF或重跑ON。只查任务工作树实际源码，没有使用代码图或创建索引。

精确复制/配置耗时、正式CLI探测耗时及准备观察边界保存在 `evidence/preparation-evidence.json`、`parser-probe-result.json` 和 `preparation-clock.json`。主程序构建成本为本轮0，后续真实OFF运行与汇总成本未知；不将准备耗时冒作策略吞吐。

实际OFF包、必要检查和候选文件独立归档在当前仓库绑定下 `issue-315/candidate-v2-active-off-20261017/c3e500e6-package`。不复制ON outputs或原始行情；ON基准仍使用已审ON根及原归档，原summary/report摘要在准备证据中记录。准确根路径和复制核对收据由私有交接提供。
