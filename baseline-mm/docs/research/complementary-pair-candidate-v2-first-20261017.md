# candidate_v2 首个机制实验包（2026-10-17）

本包落实 [#315本次恢复授权](https://github.com/slowchillchill/btc-updown-marketmaker/issues/315#issuecomment-6232105771)，属于 [#185任务3—4](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185)的实验准备。配置、实际程序与停止方式已准备，历史经营回放尚未启动。下述30分钟整体预算和任何主动补腿开关对照仍需用户签发。#314、#318、#320适用证据复用；本包不重做全面审查、不恢复旧检查点或旧预算。

## 实验条件与身份

唯一候选取自 [旧首轮配置](../../../backtest/research/first-round-20261016.json) `days[0].candidates[0]`，即原fold4 center。新配置是 [candidate-v2-first-20261017.json](../../../backtest/research/candidate-v2-first-20261017.json)，完整显式41字段。UTC2026-09-24 `[00:00,01:00)`，毫秒 `[1790208000000,1790211600000)`；一个独立2000 pUSD账户、同窗连续、无额外预热或尾段。日期已参与开发；本包不是独立验证、正式参数或V2盈利结论。

| 条件 | 准确绑定 |
| --- | --- |
| 运行源码 | `c3e500e6252481db24c12a882301a2b0540e6017`；本轮只修改包、启动脚本及研究说明，不改Rust行为 |
| 源码身份算法 | `git archive --format=tar c3e500e6252481db24c12a882301a2b0540e6017`；SHA-256 `c6f5eee7c8412a44dbe4471ae80a39ec177c14078f739e2e70a786ecba158786` |
| Cargo.lock | SHA-256 `684470a91e49d4a3ec0066ae1a33666cb635b2f90d1902fc0fa62ecf3a7be4ed`；构建 `--release --locked --offline -p backtest-cli --features backtest/stage-timing` |
| 实际二进制 | `bin/backtest-cli`，34069848字节，SHA-256 `025c2397a4664f56701d8103a0052b00fa85c9de60b357656c176ec72d0b9b03`；构建命令/环境/原日志已独立保留，尚未用它运行历史策略 |
| 策略身份 | `complementary_pair / candidate_v2`；运行配置中 `implementation_identity` 和 `noe_implementation_identity`绑定上述源码；包提交由本包所属Git提交记录，避免把未存在的自身提交填作构建提交 |
| 公共语义 | A1；HistoricalBacktest，`PUBLIC_TRADE_CONSERVATIVE_FIFO_V1`，Gamma `qualified_public_consumption`，`ActualCashNoMarketLimit`，原SIM自动结算经NOE/Ledger归约 |
| 宿主时序 | public delivery / calculation start / calculation duration / dispatch 固定 `0/0/15/0 ms`；策略报价及repair的path字段保持原值，SIM原经验时延不变 |
| 随机条件 | 种子 `6e92d4823607cfd85fae1596d28aac4bdfb58b84e4c94a772753ec7faf1fc66d`；原算法 `xoshiro256plusplus_jump_multiply_high_empirical_rank_v2`。同种子不保证策略分叉后逐请求抽样相同 |
| 规则文件 | `inputs/platform-rule-snapshot-v3.json`，SHA-256 `70e592f369d3e58f0ab4bc7cdfb8d1e2ae48ceae54321c66f908bdf63c500f85`；因果起点 `1790207400076`，覆盖最早必要Snapshot接收时刻 |
| 最终性文件 | `inputs/trade-finality-policy-v2.json`，SHA-256 `b98102f86cbd2e0d635bdc5074dcb1d7c1fb1a84de61d419d07a475c2e8533ef`；MATCHED@0、MINED@10、CONFIRMED@20ms，economic ordinal=1，failure overrides为空 |
| 资源配置 | 一个日期、一个候选，`parallel_dates=1`；原 `producer_lead_groups=8192`、交接128、decode/sort/记录容量全部沿模板；运输诊断关闭，阶段计时开启、进度60秒。并行库按本机可用资源运行，无单线程限制 |

规则原件直接复用旧首轮已采用文件。早期十八日输入审计的 `4397d7…` 是来源快照，不能混成当前规则身份；当前文件的L2证据关联#289及该来源摘要，历史Maker已采用保守公开成交模型。冻结规则保留最小订单5份、fee rate0.07 / exponent1 / taker_only、rebate_rate0.2及原文档taker delay250ms；进程共享参考市场模板与正常最终性都是项目假设，不能据此声称9月逐市场或当前在线BTC规则已核验。行情和执行价变化会改变实际费用金额，不代表修改封存费率。

## 配置迁移与业务边界

CLI首先读取候选 `strategy`，通过 `ComplementaryPairConfigV2`解析后重建 `run.strategy.effective_parameters`，不能只改宿主标签。本包两处完整参数逐项一致，41字段按16健康＋6概率＋8报价＋9修复＋2风险组成；相对旧43字段只删除 `repair.maximum_overfill_shares="5"` 和 `repair.maximum_overfill_pusd="5"`。没有把它们替换成金额门、零过补门或其他隐藏限制。以下41字段均保持原候选值，逐字段新旧对照另存原始证据 `parameter-migration.json`。

| 字段 | 旧值＝本包值 |
| --- | --- |
| `health.polymarket_silence_normal_ms` | `100` |
| `health.polymarket_silence_ms` | `400` |
| `health.polymarket_event_delay_normal_ms` | `100` |
| `health.polymarket_event_delay_ms` | `400` |
| `health.chainlink_silence_normal_ms` | `1100` |
| `health.chainlink_silence_ms` | `4400` |
| `health.chainlink_event_delay_normal_ms` | `2600` |
| `health.chainlink_event_delay_ms` | `10400` |
| `health.binance_trade_silence_normal_ms` | `1000` |
| `health.binance_trade_silence_ms` | `4000` |
| `health.binance_trade_event_delay_normal_ms` | `1100` |
| `health.binance_trade_event_delay_ms` | `4400` |
| `health.binance_depth_silence_normal_ms` | `100` |
| `health.binance_depth_silence_ms` | `400` |
| `health.binance_depth_event_delay_normal_ms` | `200` |
| `health.binance_depth_event_delay_ms` | `800` |
| `probability.volatility_window_ms` | `300000` |
| `probability.initial_sigma_usd_per_sqrt_second` | `7.33489262319592` |
| `probability.estimation_error_usd` | `0.0` |
| `probability.binance_innovation_weight` | `0.5` |
| `probability.binance_pressure_weight_usd` | `0.0` |
| `probability.binance_trade_pressure_weight_usd` | `0.0` |
| `quote.base_pair_shares` | `50` |
| `quote.base_half_spread` | `0.0050` |
| `quote.inventory_shift_per_pusd` | `0.00005` |
| `quote.uncertainty_spread_weight` | `0.02` |
| `quote.signal_spread_weight` | `0` |
| `quote.joint_fill_scenario_weights` | `[0.25, 0.25, 0.25, 0.25]` |
| `quote.minimum_business_result_per_share` | `0` |
| `quote.execution_path_ms` | `0` |
| `repair.base_loss_allowance_per_share` | `0.0050` |
| `repair.time_loss_allowance_per_share` | `0.0050` |
| `repair.inventory_loss_allowance_per_share` | `0.0050` |
| `repair.maker_fill_weight` | `0.5` |
| `repair.wait_price_improvement_per_share` | `0` |
| `repair.residual_risk_weight` | `1` |
| `repair.maker_path_ms` | `0` |
| `repair.active_path_ms` | `0` |
| `repair.enable_active_repair` | `true` |
| `risk.per_market_orphan_limit_pusd` | `100` |
| `risk.account_orphan_limit_pusd` | `400` |

数量50份、gate0、主动补腿开启保持原值，signal、sigma、innovation和100/400孤腿额度没有联动调参。100/400仍是研究假设，尚非正式风险采用。run/session/account/host/opening/bridge/activation/request/consumption/public/hidden身份使用新的 `issue315-v2-first-…`，输出在独立JOB_ROOT内，绝不覆盖原V1运行。

策略只按当时可见盘口、持仓、未决承诺估计；SIM按到达时真实模拟条件撮合。允许正常拒绝、失败、无成交、部分成交及余额不足。决策时可行不承诺实际成功。本地已确认资金不足仍整批零提交；只释放确定未发送义务，已尝试或未知义务保留。公共层不缩量、改价或重试。原#318定向工程证据复用，不以新短窗重新证明所有极端路径。

## 输入、初始化与已有使用

只复用 [原十八日审计](complementary-pair-input-readiness.md)及其JSON，补核09/23、09/24清单身份和实际必要元数据。09/23 manifest SHA-256 `fd2958a10e8a36947d3377c5fbd9ff9d5d26d11c221853179cb25aec0e247465`；09/24 manifest SHA-256 `4a7bf6e3fc0255ca583719f197ade087819a0f0718d84e0b7655217befca7caf`，与原审计一致。09/24原审计146文件、3025521344字节、50907435行，属于整日规模，不能冒作本小时解码工作量或业务验收。当前仅复用原文件内容准入证据、检查文件存在与长度；获准运行时由原公共入口执行其既有准入，未新增一轮全池扫描。

初始化取09/23 `source_kind=polymarket_market_metadata/polymarket_market_metadata_23.parquet`：98101字节、792行，Snapshot物理行664/730（0起算）。两condition与snapshot source id见完整配置。使用本源码构建的既有 `probability_boundary` 的 `metadata_clocks` 操作，通过原 `MetadataInitializationSelectionV1`实际选得3条前序，最晚准入 `1790207700060`；接收及准入均早于午夜。只核窄元数据文件，不运行策略、不查看新经营结果；不带前日账户、订单、价格/盘口、AnchorRule或策略预热。

09/24已有原概率/健康开发比较及旧V1首轮和四小时开发用途，不能再称未见；本轮只读取配置、规则、初始化和使用记录，不重读旧收益来排名，也不看10/10—10/16留出经营结果。manifest闭合、空gaps、Snapshot存在或旧准入通过均不证明所选小时的全部业务依赖可得；运行中真实缺失按原公共及策略健康语义报告。

当前日期用途已由 [D-0361](https://github.com/slowchillchill/btc-updown-marketmaker/issues/315#issuecomment-6231818529)批准：09/08部分日与必要初始化；09/09—09/26已有开发；09/27—10/02扩展开发；10/03—10/09固定候选验证；10/10—10/16独立留出预留，均含首尾。10/09五条gaps是去重错误原因，不能自动删日；两日采集细节与独立资格在实际使用时再按事实核对。日期批准不等于14日计算授权。

## 目录、命令和停止

实际JOB_ROOT、数据根及归档根通过私有交接绑定。所有JSON路径基于进程当前目录。新包位于工作树外；归档盘保存停止写入后的材料，正式SIM/NOE写入使用本地工作盘。布局如下：

```text
JOB_ROOT/
  bin/backtest-cli
  bin/summarize_research.py
  run_candidate_v2_first.sh
  configs/research-days.json
  inputs/platform-rule-snapshot-v3.json
  inputs/trade-finality-policy-v2.json
  datasets/                          # 本地绑定到已批准数据根，不复制行情
  evidence/                          # 构建、迁移、原生初始化、检查、成本
  outputs/2026-09-24/preparation/     # 运行后才出现
  outputs/2026-09-24/center/          # sim.jsonl、noe/、ledger/、run.json、result.json
  research-output.json / summary.json / progress.log
```

已执行的构建与安装形状如下，准确命令及工具版本在证据内。`SOURCE`为上述源码，`TARGET`是本轮专用构建输出，构建和工程检查另计、不计入待批的真实实验30分钟：

```bash
/usr/bin/time -v -o build.time env CARGO_BUILD_JOBS=32 CARGO_TARGET_DIR="$TARGET" \
  cargo build --manifest-path "$SOURCE/Cargo.toml" --release --locked --offline \
  -p backtest-cli --features backtest/stage-timing
install -m 755 "$TARGET/release/backtest-cli" "$JOB_ROOT/bin/backtest-cli"
```

[准确启动脚本](../../../backtest/research/run_candidate_v2_first.sh)已复制到包根，当前未执行。用户签发后唯一启动命令为：

```bash
bash "$JOB_ROOT/run_candidate_v2_first.sh" "$JOB_ROOT"
```

脚本用一次 `systemd-run --user --wait` 的 `Type=exec` service 包住CLI及原Python汇总，`RuntimeMaxSec=1800`、`KillMode=control-group`、`KillSignal=SIGKILL`。CLI为 `--mode day --config configs/research-days.json --progress-interval-seconds 60 --research-summary --wall-budget-seconds 1800 --summary-reserve-seconds 300`。不使用写死24小时的旧run_research.sh或旧日期/候选模板。新JOB_ROOT的outputs只能创建一次，避免重复覆盖；失败/硬停后不自动重试。

**推荐投入仍为整体30分钟、25分钟请求预算停止、5分钟返回预留。** CLI的started在读取配置、规则和初始化前建立，在其第1500秒后通过生产循环请求BudgetStop，候选在合法事件边界返回；初始化或长操作可能延迟感知。1800秒时CLI本身可exit124；外层从service active起1800秒开始强杀全部计算及汇总子进程。没有外部优雅stop接口，也没有续跑能力；5分钟是预留，不保证完整报告、排空或完整time输出。外层active起点与CLI started有小幅启动差，不把两者说成同一精确时刻。内核不可中断等待也可能使实际退出晚于期限。

手工停止固定unit（按相同SIGKILL策略，可能只剩部分材料）：

```bash
systemctl --user status issue315-candidate-v2-first-20261017.service
systemctl --user stop issue315-candidate-v2-first-20261017.service
```

工程上已经用2秒期限、仅shell与sleep子进程验证整组终止，Result=timeout，shell及子进程均退出；未触及任何行情或策略。依据是本机systemd259与[官方v259服务期限](https://github.com/systemd/systemd/blob/v259/man/systemd.service.xml)、[官方v259信号及范围](https://github.com/systemd/systemd/blob/v259/man/systemd.kill.xml)：RuntimeMaxSec只适用于active期限，oneshot不适用，故使用Type=exec。

运行后保留 `service-result.log`、`service-exit-code.txt`、`service-properties.txt`。失败unit保留到读出Result/时间/CPU/Memory等现有属性；正常结束时unit可能已卸载，部分属性将不可得。CLI及Python分别time -v，原JSON汇总后同service内以du -sb记录outputs、原JSON及summary字节；缺失项保留错误。硬停可能连time一起终止，此时使用仍存在的systemd属性与进度，无法取得的成本标未知。日志不是业务完成证据。按现有监督方式每15分钟在主会话读取progress和unit状态；阶段有processed inputs、候选活动、已耗时，未知总量/剩余时间如实写unknown，不编造百分比。

`RangeEnd`且 `recording_drained=true`、最后观察为窗口右界−1、唯一候选齐备才可称完整窗。`StrategyExit`是合法提前退出；`BudgetStop`是预算前缀；CLI错误、JSON损坏和I/O故障分开报告。实际平台拒绝/无成交/余额不足不自动变成程序故障。强停后的缺失不补零、未来兑付或假设排空，不清仓、不拉长结算尾段、不追加无界追溯作业。后续若需新运行，必须新账户从头开始并另获授权。

## 业务问题、现有证据和未证明项

| 问题 | 既有材料与判读 | 本包完成条件及局限 |
| --- | --- | --- |
| 主动补腿是否实际发生 | SIM发单请求与反馈中定位FAK，按request/condition/leg连接NOE与Ledger实际成交；区分已发、拒绝、零成交、部分成交 | 给出请求及执行事实或确认未观察到已发；Taker成交为0不能证明从未评估或未被策略选择。候选内部逐次未选原因未持久化 |
| 预留怎样变、实际费用怎样变 | 请求预算及Ledger预留/释放、NOE实际fee补丁、原账户现金/承诺读取 | 对实际发生的请求连接预留与真实费用，允许差异、未知与业务失败；无触发即证据不足，不为制造样本改参数 |
| 正常拒绝怎样返回 | SIM结果→Bridge/共享反馈→NOE/Ledger请求终态或保留义务；原report.failed_batch及原始错误 | 区分策略的本地整批零提交、平台正常拒绝与程序I/O错误；正常未触发不宣称该短窗验证通过全部拒绝路径 |
| 库存和经营结果怎样变 | research.hours/final_snapshot、AccountEconomics、day_economics、精确NOE/Ledger现金与持仓 | 报现金、预留、保守NAV、已确认损益、费用/返佣、孤腿、未闭合项、实际覆盖与回撤；不把未到账返佣计现金，不日化或跨日复利 |
| 利润支持能解释到哪里 | 公共已确认利润、请求义务与账户经济读取可解释支持来源及占用背景；代码中的ProfitAllocationV2/diagnostic可说明现行算法 | 不能唯一还原每次策略冻结分配、保留和释放；公共利润不重命名为完整策略利润分配轨迹。FAK和MakerRepair都可占用同一利润池 |
| 真实成本是多少 | 独立构建/检查计量；CLI和汇总time；原stage_timing、进度；可得的systemd属性 | 逐段报告墙钟、CPU、RSS/内存峰值及输出字节，区分共享/候选、重叠阶段、初始化/执行/汇总；硬停不足则标未知，不由旧V1吞吐保证本包耗时 |

**具体证据缺口与最小后续方案。** 当前策略的 `ProfitAllocationV2`包含冻结估计本金/费用/份额、original_orphan_shares及本金单价、estimate_cutoff和allocated_profit；这些值及 `reconcile_profit`每次所得当前占用没有完整落盘。仅凭公共最终余额或FAK请求无法反推；MakerRepair同样消费profit_pending。若用户要求逐请求完整利润支持生命周期，建议另立准确实施事项：仅保存已选修复请求（FAK＋Maker）的现成冻结allocation和qualified/available_profit，dispatched后是否保留，在原reconcile占用改变/闭合时输出其既有计算值和公共读取截止；区分Some(0)未决零占用与None释放。只由上层研究宿主接窄观察，不重算影子账，不改NOE/Ledger经济事件、不记录未选分支或泛化全诊断。本次不实施；成本随已选修复及变化次数增长，未测不能承诺常量或硬停完整落盘。

推荐先用本包现有链回答可观察机制与经营/费用/库存，并明确利润支持的局限；若完整策略分配轨迹是启动前必需条件，先裁决上述最小工程方案再运行。单窗主动补腿没有发生或证据不够时，报告未触发/缺证据，再设计少量可解释对照和代表窗口；不直接追加开关回放。

本包交付验收是完整可执行配置、实际构建身份、可核输入和初始化、预算/停止可落实、定向检查及独立内容审查。它不证明历史窗口盈利、正式参数、全天成本、最终晋级、七天统计充分性、PAPER/LIVE接线或真实市场适用。评价方法、盈利/晋级数值未确定时保持未定。

## 工程检查与成本证据

已完成：41字段逐项迁移核对及两个删除项；正式CLI完整解析本包后在故意缺失规则文件处返回，确保未进入历史数据准备；当前源码原生metadata选择器选中3条合法前序；实际stage-timing构建；shell语法；2秒systemd整组终止；现有stage-timing日预算前缀/从头新运行用例通过。

`cargo test --release --locked --offline -p backtest-cli --features backtest/stage-timing --test command`原8项中7通过、1失败。失败原文为reuse测试 `assertion failed: result.stderr.is_empty()`：原 `StageTimings::drop` 在计时构建向stderr输出，测试的静默假设仅适用于默认构建；它不是首包day执行失败。根据源码和[Cargo官方feature规则](https://doc.rust-lang.org/cargo/reference/features.html)核对后，仅该项按默认特性作一次定向核验，1通过；未修改测试或生产代码。原stage-timing的7通过包含完整day参数生效、旧版本/删除字段拒绝、失败JSON与非零状态、既有汇总行为。原始失败与后续适用性核验均保留，不宣称全套计时构建全绿。

当前源码和规则不变，未跑全workspace；具名UTC时钟夹具也未运行。没有新增业务测试或诊断系统。构建、必要检查和窄输入核查的实际time日志在包evidence中，准备人工/协调的起点仅有分钟级记录，不把它冒作精确CPU或完整计时。实测主构建墙钟1:05.79，CPU user/system=959.37/37.07秒，峰值RSS 1810376 KiB；原CLI检查（含编译）墙钟0:25.20。原生初始化辅助构建墙钟1:00.59；默认特性单项核验0:21.92；stage-timing预算单项（含编译）0:11.52。这些调用部分并行或等待Cargo锁，不能相加当作准备总墙钟。窄输入核查、元数据选择及停止检查分别保留原输出与耗时，真实历史CLI/经营汇总未运行，成本未知。

核心证据及执行材料独立归档到当前仓库绑定下 `issue-315/candidate-v2-first-20261017/c3e500e6-package`。行情数据仍由原数据根提供，不复制入Git或归档。包提交、精确PR头和独立审查结论由#315/PR记录，本文不复制治理进度或设置新的批准状态文件。
