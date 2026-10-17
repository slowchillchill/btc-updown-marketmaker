# candidate_v2 09/25 同小时 ON/OFF 双候选准备包（2026-10-17）

本包落实 [#315的第二窗口准备授权](https://github.com/slowchillchill/btc-updown-marketmaker/issues/315#issuecomment-6232914879)，准备UTC2026-09-25 `[00:00,01:00)` 的主动补腿ON/OFF对照。**本次只获准准备、检查和审查；真实回放及新预算尚未签发。** 09/24的ON与OFF实际运行仍保留各自身份，不重跑，不继承其预算，也不把结果改称09/25证据。

这是另一个已批准开发日期的同小时窗口。它没有按09/25经营结果挑选；准备时未查看该日策略收益。两个相邻开发日的小时不自动成为代表窗口，不证明日级盈利、普遍有效或晋级。评价数值门槛、完整日投入和正式采用仍需后续证据与裁决。

## 完整条件与唯一策略因素

可执行配置为 [candidate-v2-sep25-on-off-20261017.json](../../../backtest/research/candidate-v2-sep25-on-off-20261017.json)。一个JSON只有一个日期、两个候选，`parallel_dates=1`。ON完整41字段逐值复制09/24已审V2 ON，OFF仅把 `repair.enable_active_repair` 从布尔 `true` 改成 `false`，其余40字段不变。两个候选的 `strategy` 与各自 `run.strategy.effective_parameters` 完全一致；正式CLI以候选 `strategy` 构造策略并重建运行描述，无顶层参数覆盖，启动脚本不传策略覆盖参数。

41字段仍为health16、probability6、quote8、repair9、risk2。原50份、gate0、sigma `7.33489262319592`、innovation `0.5`、孤腿限额100/400等不变。没有恢复已删除的独立5份过补门，也没有替换为5 pUSD、隐含零过补或其他新限制。OFF只关闭active候选及FAK修复选择；原Maker修复、新配对、等待、撤单等路径继续生效。窄定位见 `baseline-mm/src/strategies/complementary_pair/repair.rs:174` 和MakerRepair选择处。

| 条件 | 本包共同绑定 |
| --- | --- |
| 窗口/候选 | UTC2026-09-25 `[00:00,01:00)`；毫秒 `[1790294400000,1790298000000)`；原fold4 center的ON、OFF各一个 |
| 账户 | 两个独立SIM会话、各2000 pUSD；不共享账户、持仓、预留、策略记忆或前日账户状态 |
| 实际运行源码 | `c3e500e6252481db24c12a882301a2b0540e6017`；原源码归档SHA-256 `c6f5eee7c8412a44dbe4471ae80a39ec177c14078f739e2e70a786ecba158786`，Cargo.lock `684470a91e49d4a3ec0066ae1a33666cb635b2f90d1902fc0fa62ecf3a7be4ed` |
| 实际程序 | 复用已审release/stage-timing二进制，34069848字节，SHA-256 `025c2397a4664f56701d8103a0052b00fa85c9de60b357656c176ec72d0b9b03`；本轮构建0次 |
| 规则与最终性 | 原规则SHA-256 `70e592f369d3e58f0ab4bc7cdfb8d1e2ae48ceae54321c66f908bdf63c500f85`；finality `b98102f86cbd2e0d635bdc5074dcb1d7c1fb1a84de61d419d07a475c2e8533ef`；MATCHED/MINED/CONFIRMED为0/10/20，economic ordinal1，空failure overrides；新因果起点1790293800070 |
| 公共政策 | A1、HistoricalBacktest、保守Maker `PUBLIC_TRADE_CONSERVATIVE_FIFO_V1`、Gamma `qualified_public_consumption`、`ActualCashNoMarketLimit`、原SIM自动结算→NOE→Ledger；冻结历史假设不代表重新在线核验BTC市场 |
| 宿主时序 | public delivery / calculation start / calculation duration / dispatch固定 `0/0/15/0ms`，原SIM经验时延不变 |
| 随机条件 | 两候选同seed `6e92d4823607cfd85fae1596d28aac4bdfb58b84e4c94a772753ec7faf1fc66d`，原 `xoshiro256plusplus_jump_multiply_high_empirical_rank_v2`；不按候选名称重选种子 |
| 共享资源 | 原单日producer，领先目标8192；原canonical/recording资源不变，每候选有限交接128；运输诊断关闭、stage timing开启、60秒进度 |
| 汇总 | 原 `summarize_research.py`，SHA-256 `8c1c34d169202d9d208c29297a9ee2aea31b567933fe81e05e8c1c6d973c4340`，按run_id汇总两个候选 |

运行、会话、账户、宿主、请求、桥接、激活、消费序列、输出和unit均使用新身份，与旧包隔离。身份变化用于隔离材料，不是额外策略因素。同seed固定随机政策，但ON/OFF请求序列会分叉，不保证逐笔延迟随机数配对，也不能把差额解释为每笔主动补腿的因果收益。

当前包配置SHA-256为 `e0f19388e9089c745cc64e05f06239d713e78a4e2eb1b7d6bf1c3d7b53fb768c`。准备包提交由所属PR定位，实际程序始终绑定上述c3e500e6；不将尚不存在的包提交填成运行源码。

## 共享生产、独立账户与故障边界

复用 `backtest/src/day.rs:308` 开始的既有装配：当天原始数据只由公共producer读取和归约，各候选在独立线程创建自己的策略、SIM和运行上下文，读取同一公开/隐藏日流。ON/OFF没有各自再扫一遍输入，也没有新增调度器或合并账户。`parallel_dates=1` 限制日期并发，不把两个候选改为串行。

程序故障会影响同包另一候选：`day_state.rs` 的 `finish` 在候选Err时登记共享failure并通知等待者，producer与其他候选据此停止；producer失败也通知全部候选。原入口保留根故障来源及可取得的部分报告，区分 `OriginalFailure`、`CollateralStop`、`NoReport`。正常平台拒绝、无成交、部分成交和资金不足是业务反馈，不能直接称为程序故障；正常StrategyExit按既有退出口径报告。出现程序故障不自动重试、改参或另起另一候选，保留现场交主会话处理。

## 输入与合法初始化

复用已有 `complementary-pair-input-audit-20260909-20260926.json` 和input-readiness结论，只补本窗口身份和选择事实。实际数据根由私有运行材料绑定，`datasets/`指向原只读数据根。没有全池重扫，也没有查看留出收益。09/25仍属开发用途，不声明独立资格；manifest闭合不等于窗口业务完整性已经验收。

| 材料 | 当前事实 |
| --- | --- |
| 09/25输入manifest | `datasets/2026-09-25/typed_raw/utc_day=2026-09-25/manifest.json`；SHA-256 `6676470736c0b2e0616ff4f69722740fb2f1c7a04f0bd47b5cbff44a12500bd9`，与旧审计一致 |
| 初始化manifest | 09/24 manifest SHA-256 `4a7bf6e3fc0255ca583719f197ade087819a0f0718d84e0b7655217befca7caf`，与旧审计一致 |
| 必要元数据 | 09/24 `source_kind=polymarket_market_metadata/polymarket_market_metadata_23.parquet`；100131字节、791行；SHA-256 `72ff6bc4d7565305129daf568625e3e801d9ca4b4e580b9364c5e407e11ba0e4` |
| 市场1 | condition `0x28ebbeb19cd419de8e91bd79a497dd521fc3a4766ce3823ac09a5a6b1bb5314e`；Snapshot source_message_id `19b4d2b501315a1c38454a3d37e7baee81485046307276783a19659cbcd2badc` |
| 市场2 | condition `0x437a4a04e76e120eb99232f38906ab037d8ee717b33ffa9a38f659ade8a6e66c`；Snapshot source_message_id `c79fba8da1a8418ce03f770925018499a8f33f0e46789ad0a055562491b7406f` |

仅从旧 `first-round-20261016.json` 第二日复用初始化文件与市场选择事实，没有取其V1参数、排名或收益。已调用同c3源码的原生 `MetadataInitializationSelectionV1` 入口，实际选入3条：两个指定Snapshot及中间一条起点前Existing元数据。接收时刻依次为1790293800070、1790294100022、1790294100091；最晚准入1790294100099，全部在新窗口开始前。因果起点按实际最早接收设为1790293800070，没有照搬09/24的1790207400076。

选择器输入、原生请求、三条输出与原stdout/stderr在 `evidence/initialization-*`。前史只提供入口所需市场身份/规则/快照，不增加策略预热、价格/盘口前史、参数拟合或前日账户状态继承。09/25实际窗口业务完整性须在获准运行后结合原健康/覆盖报告判断；本次小范围核对不作提前验收。

## 路径、准确命令与推荐预算

作业根 `JOB_ROOT` 由私有交接给出，独立于待清理工作树和09/24包。完整JSON路径以作业根为基准：

```text
JOB_ROOT/
  bin/backtest-cli
  bin/summarize_research.py
  bin/probability_boundary               # 本轮只读初始化核对所用旧产物
  configs/research-days.json
  inputs/platform-rule-snapshot-v3.json
  inputs/trade-finality-policy-v2.json
  datasets/                             # 原数据根符号链接，不复制行情
  run_candidate_v2_sep25_on_off.sh
  evidence/
  outputs/2026-09-25/preparation/        # 真正运行后才创建
  outputs/2026-09-25/on/                 # 独立SIM、NOE、Ledger、run/result
  outputs/2026-09-25/off/
```

[启动脚本](../../../backtest/research/run_candidate_v2_sep25_on_off.sh)在输出根已存在时拒绝覆盖。准确范围与预算获准后，唯一执行命令为：

```bash
bash "$JOB_ROOT/run_candidate_v2_sep25_on_off.sh" "$JOB_ROOT"
```

**推荐双候选一次整体30分钟，第25分钟请求协作停止，预留5分钟返回与汇总；尚未获准。** 这30分钟共同覆盖两候选、共享初始化与producer、CLI输出、原Python汇总和输出大小计量，不是每候选各30分钟。原构建已复用，本轮不另收构建时间。既有09/24单候选ON/OFF service耗时328.007/326.142秒，仅作为观测依据；共享producer和双线程的实际成本仍未知，不承诺本包耗时、吞吐或更快完成。

外层使用Type=exec、RuntimeMaxSec=1800、KillMode=control-group、KillSignal=SIGKILL，固定新unit `issue315-candidate-v2-sep25-on-off-20261017.service`。CLI的 `--wall-budget-seconds 1800 --summary-reserve-seconds 300` 从初始化计时，在生产循环感知1500秒预算标记并由合法事件边界返回BudgetStop。初始化、长操作和输出可能延迟感知；25分钟不是必然立即返回，5分钟也不是完整报告保证。无外部优雅停止接口；30分钟外层强杀整组，time/汇总/排空报告可能缺失，内核不可中断等待等限制保持。原2秒合成整组停止证据直接复用，没有新建停止功能或重做停止测试。

手工终止仅操作本包固定unit：

```bash
systemctl --user status issue315-candidate-v2-sep25-on-off-20261017.service
systemctl --user stop issue315-candidate-v2-sep25-on-off-20261017.service
```

预算到达或异常后保留实际前缀，不加时、重跑或续跑；当前日入口没有检查点恢复能力。真实长作业获准后，主会话每15分钟查询原可控进度；未知剩余时间如实说明。启动前再次核对冻结文件、数据位置/身份、磁盘、unit和输出未使用。

## 业务问题、完成判据与证据限制

本包比较两套策略在第二个同小时开发窗口的完整运行。保留实际反馈对后续Maker修复、库存和资金的影响，不能从ON中删去FAK成交或Taker费用来构造OFF。两账户均满足原Complete、RangeEnd、recording_drained=true、最后观察为1790297999999时，才列完整窗OFF−ON差额；否则报告各自实际覆盖、退出原因、未知与尾部，不把前缀和完整窗直接排名。

复用请求、反馈、SIM、NOE、Ledger和现有报告，回答实际发送/接纳FAK、正常无效果/拒绝、零成交/部分成交、实际Maker/Taker份额与费用、预留变化、库存/孤腿及成本、开放义务、已确认净损益、保守NAV、平均/峰值资本占用、回撤和实际成本。已确认损益与NAV相对初始差额分别列出；尾部未解决资产、挂单预留和未到账款项保持原口径，不强平、不补未来。有限的每市场AccountEconomics可帮助区分已兑付与末端未解决事实，不进行反事实归因或影子账归约。

策略按当时公开盘口、持仓和承诺估计，SIM按订单到达时行情、账户与撮合规则产生实际结果。决策可行不保证成功；正常业务失败与程序故障分开。本地已知资金不足仍整批零提交；仅释放确定未发送义务，尝试过或未知的继续保留；公共层不自动缩量、改价或重试。市场变化导致实际费用不同，不表示改动冻结费率。

现有证据不能唯一还原逐请求ProfitAllocationV2的冻结、保留、占用变化与释放，也没有规范化RejectedNoEffect的具体SIM拒绝原因；保持此前已批准的缺源边界，不新增诊断。不按余额猜利润分配或拒绝原因。Taker成交为0不证明主动补腿没有被评估；OFF开关的意义由源码和实际完整生效配置核对。即使两个新候选均完整，也不证明该开关在其他时段有效、收益可日化、尾部风险下降或正式参数可采用。

## 已完成的定向检查与成本记录

已核两处41字段完整一致、ON与原V2 ON逐值一致、ON/OFF仅一个策略字段不同；其余日期、初始化、因果起点及身份/输出变化逐项记录在 `evidence/configuration-diff.json`。正式CLI读取完整双候选配置后，在故意缺失的规则文件处按预期exit1，未进入历史输入准备；这证明正式配置可解析，不声称已执行初始化/回放或生成新的run.json。false实际生效重建复用同源码已通过的 `actual_baseline_command_applies_explicit_values_and_false_then_records_effective_config` 记录，并窄核当前装配源码。

实际程序、选择器、汇总和规则副本身份已核；原生初始化选择完成；脚本bash语法通过；新unit不存在，输出未创建，无活动本任务回放。复用原停止测试与工程验证，没有重建或重跑测试套件。原stage-timing套件的旧reuse静默stderr断言失败及默认特性适用性说明随原证据保留，不宣称原整套全绿。只查当前任务工作树源码，未使用代码图，不以缓存代替源码。

`evidence/preparation-evidence.json`、`initialization-selector-result.json`、`parser-probe-result.json` 与 `preparation-clock.json` 保留准备观察边界和必要操作计量。原生选择约0.010秒，复制加选择约0.017秒，正式解析与本轮定向检查分别记录；本轮构建和真实回放均为0。获准运行后CLI与汇总分别由 `/usr/bin/time -v` 计量且共同受外层预算；systemd CPU/Memory/timestamp和原进度作为硬停时可取得的证据，缺失项写未知。后续有限读取分析耗时与回放分开，重叠stage timing不相加当墙钟。

本包及必要证据独立归档在当前绑定根下 `issue-315/candidate-v2-sep25-on-off-20261017/c3e500e6-package`。原ON/OFF包、结果及准备档保留，不复制其outputs或原始行情。实际路径、精确候选摘要和复制核对回执由私有交接提供。
