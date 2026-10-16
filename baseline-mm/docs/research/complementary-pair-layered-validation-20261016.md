# 新基线上的分层策略验证与首轮作业包

本说明承接 [#315 当前合同与本轮范围](https://github.com/slowchillchill/btc-updown-marketmaker/issues/315#issuecomment-6209636360)。当前只准备材料、完整配置和启动建议，尚未运行策略。正式业务语义继续引用 [#185](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185)及[原研究方案](complementary-pair-research-plan.md)，公共采用范围引用 [#314 审查矩阵](https://github.com/slowchillchill/btc-updown-marketmaker/issues/314#issuecomment-6209143039)。本说明不设第二套业务合同，不修改策略、撮合、预算或数据接受条件。

**首轮回答什么。** 在可信公共基线上，固定日期与其他条件，观察报价规模 50/18 份如何改变订单、实际成交、孤腿、现金占用、修复和退出。它建立新的行为参考，并记录一次真实作业的准备、执行及输出成本。三段日初窗口仅为固定开发覆盖，尚未证明包含趋势、震荡或极端波动；不是按旧策略利润挑选，也不代表全天收益。

**完整实验条件。** 可直接交给现有 `backtest-cli --mode day` 的配置为 [first-round-20261016.json](../../../backtest/research/first-round-20261016.json)。全部文件路径相对于作业进程当前目录，配置文件所在目录不是路径基准。配置含三个日期、每日期两个独立账户，共六个账户运行、三个共享公共小时；不是六个独立公共数据准备任务。

| 条件 | 本包绑定及含义 |
| --- | --- |
| 公共源码 | `3585ee5ad6b1a00f2e949ca5c7f11a01d5d868c8`，#314 的合并版本；本包不声称已生成正式策略运行二进制 |
| 源码材料 | `git archive --format=tar <上述完整提交>` 的 SHA-256 为 `f9e9c228200145c67885edf8553812ff860ccbeb6d0388be58c7d29b45c5280f`；此处是明确的源码身份算法，不是业务计算正确性证明 |
| 依赖锁 | `Cargo.lock` SHA-256 `684470a91e49d4a3ec0066ae1a33666cb635b2f90d1902fc0fa62ecf3a7be4ed` |
| 完整策略 | [business-candidates-20261008.json](../../configs/strategies/complementary_pair/business-candidates-20261008.json) 的 fold4 `center`、`small-size`；拟合截止为 UTC 9月24日00:00，原开发窗为9月24—26日 |
| 唯一策略差异 | `quote.base_pair_shares` 为 `50` / `18`；其余42个策略字段逐值相同，保留各自完整43字段配置，宿主描述中的有效参数相同 |
| 账户与记忆 | 每候选每日2000 pUSD，新账户；同一窗口内账户、策略记忆及高水位连续，跨日不继承；原100/400 pUSD孤腿数值仍是研究假设，不作正式风险额度采用 |
| 公共政策 | 当前 A1；`HistoricalBacktest` 与冻结规则中的 `PUBLIC_TRADE_CONSERVATIVE_FIFO_V1`；`QualifiedPublicConsumption` Gamma；`ActualCashNoMarketLimit`；SIM原自动结算，经NOE和Ledger读取原精确现金与估值 |
| 规则材料 | D-0357准确归档中的原规则，SHA-256 `70e592f369d3e58f0ab4bc7cdfb8d1e2ae48ceae54321c66f908bdf63c500f85`；规则观测时刻早于所选初始化，因果起点绑定最早初始化接收时刻 `1790207400076` |
| 最终性材料 | 同一归档与[十八日输入审计](complementary-pair-input-audit-20260909-20260926.json)的原材料一致，SHA-256 `b98102f86cbd2e0d635bdc5074dcb1d7c1fb1a84de61d419d07a475c2e8533ef`；不换成测试规则 |
| 宿主时序 | 公开交付、计算启动、计算耗时、派发延迟分别为固定 `0/0/15/0 ms`；15ms是当前研究假设，不用本机墙钟改变业务时间，SIM原经验时延保持 |
| 固定随机条件 | 六账户均显式使用原D-0357种子 `6e92d4823607cfd85fae1596d28aac4bdfb58b84e4c94a772753ec7faf1fc66d`；算法为 `xoshiro256plusplus_jump_multiply_high_empirical_rank_v2`。不从候选名、目录或窗口长度派生；不同策略后续请求和事件可以分叉，不承诺逐请求延迟相同 |
| 并行与记录 | `parallel_dates=3` 是待批准启动建议；同日两个候选共享输入，各自执行。生产领先目标8192、记录交接容量128及原资源参数沿精确模板；运输诊断关闭，保留阶段计时和60秒真实进度，无利用率门槛 |

规则和完整宿主配置的原始入口为 [D-0357](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6199944025)。本包只替换本轮日期、初始化选择、fold4策略、独立身份、明确种子、路径和源码身份，不继承旧经营胜负排序。正式构建在获准启动时绑定上述源码、构建命令和二进制SHA-256；准备包版本与运行程序版本分别记录。

**三个午夜的真实初始化。** 各窗为 UTC `[00:00,01:00)`：9月24日 `[1790208000000,1790211600000)`，9月25日 `[1790294400000,1790298000000)`，9月26日 `[1790380800000,1790384400000)`。初始化分别取前日23时元数据文件中的前一23:55市场和00:00市场，不把模板中的旧日期或旧市场身份套过来。

| 研究日 | 前日23时文件物理行数/字节 | 两个Snapshot物理行（从0开始） | 原选择器实际选入 | 最晚初始化准入毫秒 |
| --- | --- | --- | --- | --- |
| 09-24 | 792 / 98101 | 664、730 | 3条必要前序 | `1790207700060` |
| 09-25 | 791 / 100131 | 663、729 | 3条必要前序 | `1790294100099` |
| 09-26 | 792 / 98145 | 663、730 | 3条必要前序 | `1790380500067` |

三个文件路径均为前日清单中的 `source_kind=polymarket_market_metadata/polymarket_market_metadata_23.parquet`。总物理规模296377字节、2375行。先窄读必要身份及时间列，随后通过当前源码构建的既有 `probability_boundary` 的 `metadata_clocks` 入口和 `MetadataInitializationSelectionV1` 核验原完整行。六个完整 `condition_id` 和 `snapshot_source_message_id` 保存在配置的 `metadata_initialization.markets`，原生计算身份、物理行及选择输出独立归档。全部选入记录的接收和准入时刻早于各自午夜。9月23—26日清单摘要与既有十八日审计一致，未因此重新扫描整日行情。

这只证明本包的必要元数据前序可被原入口接受，不证明一小时行情已经回放或全天业务完整。前日账户、订单、盘口、价格历史和AnchorRule不进入初始化；窗内来源仍按真实接收和资格形成可见事实。

**首轮投入建议，尚未批准启动。** 建议三个日期并行，两个候选各运行一小时，整体运行期限建议为60分钟，到期请求强制终止。CLI使用 `--wall-budget-seconds 3600 --summary-reserve-seconds 900`。CLI从配置/规则读取及输入准备前计时，在45分钟后请求预算停止，候选在原合法事件边界返回已有成绩；这不是保证45分钟立刻全部停下。最后15分钟供已有结果返回、记录结束和汇总，正常提前完成即结束，不自动重跑或追加预算。

CLI内部硬截止只覆盖CLI自身。为使CLI和后续 `summarize_research.py` 处于同一个整体期限，建议沿既有方式由一次临时 `systemd-run` 作业包住二者。使用临时 service 的 `Type=exec`，不是 scope。`RuntimeMaxSec=3600` 在服务激活约60分钟后触发停止，`KillMode=control-group` 将同组shell、CLI、记录子进程和汇总进程纳入终止范围；`KillSignal=SIGKILL` 在停止开始时直接发出不可捕获的信号，不额外预留默认停止宽限期。60分钟是触发强制终止的期限，包含CLI和汇总，不是保证全部进程在某个精确墙钟瞬间已退出；内核不可中断等待仍可能延后退出。它是本次待批准启动方式，不是新公共运行政策，也不提供恢复或完整输出保证。[systemd服务期限](https://github.com/systemd/systemd/blob/main/man/systemd.service.xml)、[终止范围与信号](https://github.com/systemd/systemd/blob/main/man/systemd.kill.xml)

现有 `prepare_six_hour.py` 写死9月12日、fold0、七候选、初始化身份及按名称生成种子；`run_research.sh` 写死24小时预算，均不能原样启动本包。当前CLI已经足够，本轮没有新增或修改执行脚本。`summarize_research.py` 继续直接读取现有报告，不扫描NOE、不重演策略、不补未来事实。

**准确目录与启动命令。** 作业根应在本地支持原禁止覆盖发布语义的文件系统上，独立于待清理任务工作树。归档盘只用于停止写入后的证据复制，不直接作为SIM/NOE写入目标。相对布局如下，已准备的私有复核包含同一配置和核实的原规则材料；`datasets` 绑定用户现有数据根，未复制行情数据。

```text
JOB_ROOT/
  configs/research-days.json
  inputs/platform-rule-snapshot-v3.json
  inputs/trade-finality-policy-v2.json
  datasets/                              # 指向已有数据根的本地绑定
  outputs/2026-09-{24,25,26}/{center,small-size}/
```

下列命令仅供审查，当前未执行。`SOURCE`、`TARGET`、`JOB_ROOT`、`BINARY`、`SUMMARY_SCRIPT` 均在正式交接中绑定实际路径；`SOURCE`为上述固定源码，`SUMMARY_SCRIPT`为本包核对过的既有汇总脚本。构建在正式策略作业计时前完成，保留构建及产物身份，不启动旧二进制或旧检查点。

```bash
CARGO_BUILD_JOBS=32 CARGO_TARGET_DIR="$TARGET" \
  cargo build --manifest-path "$SOURCE/Cargo.toml" --release --locked --offline \
  -p backtest-cli --features backtest/stage-timing
install -m 755 "$TARGET/release/backtest-cli" "$BINARY"
sha256sum "$BINARY"

# 仅在用户批准本轮具体投入后执行；同一临时作业覆盖CLI及后续汇总。
systemd-run --user --wait --collect --expand-environment=no \
  --unit=issue315-first-round \
  --property=Type=exec --property=RuntimeMaxSec=3600 \
  --property=KillMode=control-group --property=KillSignal=SIGKILL \
  --working-directory="$JOB_ROOT" /usr/bin/bash -c '
set -uo pipefail
mkdir -p outputs/2026-09-{24,25,26}/{center,small-size} || exit $?
env -u RAYON_NUM_THREADS BACKTEST_TRANSPORT_DIAGNOSTICS=0 \
  /usr/bin/time -v -o cli-resource-usage.txt \
  "$1" --mode day --config configs/research-days.json \
  --progress-interval-seconds 60 --research-summary \
  --wall-budget-seconds 3600 --summary-reserve-seconds 900 \
  > research-output.json 2> progress.log
cli_status=$?
python3 "$2" configs/research-days.json research-output.json summary.json
summary_status=$?
if (( cli_status != 0 )); then exit "$cli_status"; fi
exit "$summary_status"
' bash "$BINARY" "$SUMMARY_SCRIPT"
```

本机只读核对为systemd 259，user manager当前为running；这些参数可用于该临时service。`--expand-environment=no` 保留传给shell的字面变量；`--wait` 等待实际作业结果，而不是只报告提交成功；`--collect` 在结束后卸载临时unit。正常进程退出时返回作业状态，强制终止为非零服务结果，不能只凭该数字区分CLI硬截止与外层终止，应同时保留systemd返回记录及日志。

上述正常路径中的CLI错误仍保留原非零退出，同时只尝试汇总已经返回的原JSON。截断或不存在的JSON不能被当作完整报告；汇总失败保留原日志。外层强制终止可以发生在输入准备、记录或JSON输出中，不保证有完整汇总、记录排空或全部候选报告。硬截止后的未知继续未知；重新运行是新账户从头计算，不是续跑。

**怎样解读首轮返回。** `CandidateOutcomeV1::Complete` 只表示候选函数返回，不等于跑满一小时。既有汇总的 `full_window_range_end` 还检查原 `RangeEnd`、`recording_drained` 和最后观察时刻；`all_candidates_complete_window` 要求六个账户结果齐备。脚本退出0本身不是该结论。

| 返回情况 | 本轮解释与下一步 |
| --- | --- |
| `RangeEnd` 且完整范围条件成立 | 一个已完成的有限窗口；检查原经济分项及未闭合义务，不日化 |
| `StrategyExit` | 合法策略退出，按实际触发时点和风险状态分析；不补退出后零结果，也不称完整窗口 |
| `BudgetStop` | 已返回的预算前缀，读取原cutoff、pending及记录状态；没有跑完的部分不比较完整窗收益 |
| `Failure`、候选/生产者错误或CLI非零 | 保留真实原因、已返回前缀和其他候选结果；停止依赖该失败的结论，按原模块查明，不自动重试 |
| CLI硬截止或外层强制终止 | 区分退出码、服务结果和最后可读材料；没有成功返回的内容未知，不能推定已排空或继续运行 |

同毫秒已经完成的Exit保留原原因；预算停止不执行RangeEnd尾部推进，不清仓、不发起未来结算。窗口内的原自动结算照常；窗口末尚未到账的返佣、未决持仓和未知估值保持原义。

首轮按日期/候选列出结束原因、实际覆盖、现金/预留、NAV与确认净结果、费用/返佣、孤腿/未闭合状态、资金周转、累计回撤、原订单/成交及决定计数、来源健康和冻结待发项。原报告未提供唯一归属或完整历史的量仍写未知，不从价格触及推断自身成交，不用累计回撤相减伪造小时内最大回撤。

成本同时保留配置/输入准备、初始化、原共享输入与候选活动、装配/计算、等待、记录和输出的原阶段计时、墙钟、CPU及峰值RSS。重叠线程的耗时不能求和当墙钟，公共inputs不是账户完成比例。记录各候选退出时间和实际活动长度，再用这些事实形成下一层预算。PR #313 的4726秒十二小时结果只有small-size到RangeEnd，其余六候选提前Exit；不能外推七候选全部跑满一天或十五天，也不能除以七估计单候选成本。一小时七候选跑满的旧结果同样不是本轮fold4两候选成本保证。

**后续每一步及其依赖。** 以下是完整推进方案，尚未启动后续层次；准确候选、日期成员和预算在相应投入前固定。工程通过、短窗经营结果和正式策略采用分别判断。

| 顺序 | 工作量与问题 | 升级依据或停止判断 |
| --- | --- | --- |
| 1. 首轮日初机制参考 | 9月24—26日各1小时，center/small-size；检验规模变化是否改变实际行为、孤腿与现金压力，并取得真实成本 | 先解释失败、退出与覆盖。只按短窗利润选赢家没有依据；机制未触发表示证据不足。新增业务含义或风险选择交用户裁决 |
| 2. 扩展连续窗口 | 同三日期各 `[00:00,04:00)`，最多保留这两个有区别的候选；必要时下一轮只改一个有依据因素 | 检查更多市场切换、累计库存、修复、资金周转与晚于首小时的行为。它需要新运行，不能接一小时检查点，也不默认首小时精确相同。预算依据首轮真实阶段成本和实际存续候选另定 |
| 3. 两个完整独立账户日 | 建议先选开发池9月24日与9月26日，固定少数候选，日期在启动前确认；日内连续、每日独立2000 | 检查全天风险、高水位、尾部义务及资金循环。两天不是盈利充分证明；按完整经济与风险证据形成少数候选，而不是把不同日期账户拼成复利净值。完整日预算需另定 |
| 4. 固定候选验证 | 拟用10月3—8日六个完整账户日；先核实输入、合法初始化及历史用途，再固定候选与可比条件 | 报告跨日离散、费用后结果、最差日、风险和未决项。若据此改参，如实归入开发，再冻结下一候选；不继续称其为独立评价 |
| 5. 封存评价后裁决正式采用 | 评价前冻结拟采用的参数版本、风险口径、经营成功条件及投入，再对该版本使用拟留出的10月10—15日完成封存评价 | 先证实这些日期没有参与相关选优；已使用则报告并另定留出。正式阈值不能从旧利润或配置100/400臆定。保留评价中的失败、风险与证据局限；评价后由用户结合结果裁决正式采用及PAPER接续。封存结果若引发调整，承认其已被使用；六天只有有限证据 |
| 6. PAPER | 按独立准确事项衔接公共宿主、策略接线和在线验证 | 先处理原PAPER模型、迟到成交/撤单/资金及对齐边界；历史基线可用不等于整个PAPER宿主已交付 |
| 7. LIVE | 先交付所缺公共能力与非资金组合，再按另行授权处理真实资金验证 | 不从策略研究批准推导部署、凭据或下单授权；本轮不执行这些工作 |

层间保留可解释的基准，每轮改变少量相关参数或一个机制假设。无独立证据时不按一小时PnL自动淘汰、排名或添加风险限制。需要新的诊断时先看现有聚合能否回答；只有实际缺口才回所属模块，不新建逐决策永久账本或实验框架。来源延迟、失同步、部分成交等场景按输入事实选取，不能按策略赚钱与否挑窗口。

**数据用途与污染未知。** 36个完整采集区间分为已有开发18日（9月9—26日）、扩展开发6日（9月27—10月2日）、拟冻结验证6日（10月3—8日）和拟留出6日（10月10—15日）。9月8日为部分采集日，10月9日缺目录。清单区间或目录存在不等于业务验收；缺口记录不直接变成筛日门。10月10日须检查真实初始化，不能因前日缺目录自动排除，也不能补造前史。

原fold4拟合只读其较早拟合范围；9月24—26日已经是开发材料，不再称未见。新日期仓库内没有找到使用记录，不证明从未被其他研究或B选优使用。冻结验证和留出前沿已有任务、产物与使用记录核对；本轮不扫描留出价格/标签来选择场景，不扩展B数据授权。数据池用于覆盖和研究，不要求每次改参回放十五天。

**本包准备验证。** 已核对当前原生初始化入口、规则/finality原文件及摘要、fold4完整配置单因素差异、六个独立身份与共同种子、三个接收范围、输出相对路径，以及现有CLI预算和汇总语义。只为元数据原生身份与选择构建既有辅助例子，没有运行正式策略或基准，也没有全工作区回归。本包完整差异、私有配置绑定、原生输入/输出和验证日志按本轮证据归档保存；准确启动投入仍待用户裁决。
