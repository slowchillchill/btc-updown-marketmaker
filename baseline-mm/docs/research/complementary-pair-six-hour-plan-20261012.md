# 七候选连续六小时研究启动方案

本轮代码交付完成最小汇总、预算保存和启动材料；最初代码交付时，正式研究尚未启动。
依据[#185当前授权D-0335](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6111218554)
及[最小实施记录](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6111290180)。
此代码PR不提供未运行的经营结果，也不关闭#185剩余研究任务。

研究固定UTC2026-09-12 `[00:00,06:00)`、fold0。候选按以下顺序列出；七候选并行，共享公共供料：
center、no-innovation、small-size、no-inventory、passive-repair、health2、health8。每组完整策略参数直接复制
[`business-candidates-20261008.json`](../../configs/strategies/complementary_pair/business-candidates-20261008.json)。
每组独立2000pUSD，六小时连续账户、策略记忆和高水位。保留原单市场1000pUSD、高水位30%、
SIM auto redeem、原规则/finality及0/0/15/0ms宿主时序。不加历史价格预热。
七候选线程共享一个公共producer，parallel_dates=1、producer_lead=8192、checkpoint=300s。
RAYON_NUM_THREADS=16对应本轮主机16物理/32逻辑CPU，进度900s。不设利用率门槛。
旧两候选一小时41m50.59s、平均2.29核不保证本轮42候选小时七小时内完成。

输入准备开始计入总七小时预算；6h45m请求最后完整Running检查点，留15分钟保存和汇总。
正常提前完成立即结束。策略提前Exit按实际终止时点结束，不填后续零收益。
预算保存仅在原协调发布成功后返回，保留pending、原源游标及未闭合义务。
不做RangeEnd尾部处理、强制清仓或补发。若最后保存失败或硬截止，只承认最后成功发布代。
未输出阶段计时保持未知；嵌套/并行count、total、max不相加当总墙钟。

## 准备与启动

请求根、配置、二进制和证据必须位于任务工作树外的独立目录。旧665159d二进制与检查点不能用于本次。
获审提交后，将实际实现身份（三个原 `ImplementationIdentityV1` 字段）写入独立JSON，重新生成正式配置。
代码审查期间的未提交配置只供核对，不代表已获审或可以启动。规则/finality复用原不可变材料；
午夜初始化保留原metadata选择，范围结束和metadata初始化结束同时改为06:00。

配置生成入口创建各候选 `run.json`、SIM日志和结果共用的产物父目录；NOE/Ledger子目录仍由原owner创建。
首次正式启动因这些父目录缺失，在写 `run.json` 时返回ENOENT，尚未进入开户。修补用全新临时输出根执行
真实helper命令，核对七个父目录及元数据文件可创建；不重跑Rust、研究或重新构建原二进制。
修复后继续同一研究仍沿首次启动的绝对预算截止，不重新计七小时。

以下变量由正式启动交接提供，公共文件不包含私有绝对路径。`SOURCE`是获审源码，`REQUEST_ROOT`是本轮独立产物根，
`TARGET`是保留的共享Cargo缓存。`TEMPLATE`是旧已核定日配置，`BUSINESS`是上述七候选参数文件。

```bash
CARGO_BUILD_JOBS=32 CARGO_TARGET_DIR="$TARGET" cargo build --release --locked --offline -p backtest-cli --features backtest/stage-timing
python3 "$SOURCE/backtest/research/prepare_six_hour.py" \
  --template "$TEMPLATE" --business-candidates "$BUSINESS" \
  --dataset-root "$DATASET_ROOT" --output-root "$REQUEST_ROOT" \
  --implementation-json "$IMPLEMENTATION" --rules "$RULES" --finality "$FINALITY"
cp "$TARGET/release/backtest-cli" "$REQUEST_ROOT/backtest-cli"
```

随后在独立根创建一个顺序脚本（变量用实际值绑定，两个辅助文件也复制到该根），
其正文为：

```bash
#!/bin/bash
set -euo pipefail
export RAYON_NUM_THREADS=16
"$REQUEST_ROOT/backtest-cli" --mode day --config "$REQUEST_ROOT/configs/research-days.json" \
  --progress-interval-seconds 900 --research-summary --wall-budget-seconds 25200 \
  --checkpoint-reserve-seconds 900 > "$REQUEST_ROOT/run-output.json" 2> "$REQUEST_ROOT/run.log"
python3 "$REQUEST_ROOT/summarize_six_hour.py" "$REQUEST_ROOT/run-output.json" "$REQUEST_ROOT/summary.json"
```

用单个外层期限覆盖CLI与后续机器汇总，正式启动时才执行：

```bash
systemd-run --user --unit=issue185-six-hour --property=RuntimeMaxSec=25200 \
  --property=KillMode=control-group --property=KillSignal=SIGKILL --working-directory="$REQUEST_ROOT" "$REQUEST_ROOT/run-six-hour.sh"
```

外层到期对服务组发送SIGKILL，覆盖CLI及顺序汇总，不追加宽限期。该设置仅落实已批准的硬期限；
依据[systemd服务运行期限](https://github.com/systemd/systemd/blob/main/man/systemd.service.xml)和
[停止信号与控制组](https://github.com/systemd/systemd/blob/main/man/systemd.kill.xml)。
不在CLI结束后无限追加汇总。如果CLI输出未完成或系统到期，
不得把截断JSON当报告；保留原日志和成功发布引用。查询频率15分钟，报告当前阶段、输入数、
活动候选及已耗时，总输入或剩余时间未知则如实保留未知。不得靠进程存活宣称有业务进展。

## 返回事实与解释边界

每候选按小时和整段给出现金/预留、NAV/WCM、确认净结果、各类未到账返佣、孤腿/资金占用、
订单/在途、三类决定和健康累计。变化量读取同口径精确差额；累计回撤单列，不冒称小时内最大回撤。
小时快照不包括边界下一小时事件，保留真实cutoff。无事件小时延续已有状态，不造行情或决策。
成功决定分类独立于后来发送/失效；旧检查点没有分类时保持未知。
Maker/Taker经济金额与应用/成功笔数来自原Ledger事实。有SIM回执的提交请求、其声明订单腿、
撤单请求/响应来自原registry，字段采用receipted名称；回执不证明平台接受或成交。
无回执注册单列，是否已提交保持未知；未发送冻结pending独立保留，不造实际发送事实。
保留每候选实际终止原因及最后成功发布代，不把未提交末尾或部分报告放入completed。

有限研究不证明真实Maker FIFO、真实盈利或线上尾延迟，不更改撮合/结算/策略时序。
本代码准备阶段不启动真实研究；后续执行已批准的七候选六小时研究，不另行扩展窗口、训练或运行B。
也不把阶段计时解释为性能优化成果。

## 工程验证记录

验证基线为 `122902683743caeb6838580713a233c3205bdaff`，在本轮未提交候选上验证。
共享target保留；以下命令均在本机A执行，使用 `CARGO_BUILD_JOBS=32 CARGO_TARGET_DIR="$TARGET"`。
这些小夹具只有有限公开输入，逻辑跨小时不代表运行真实一小时。构建墙钟不是策略耗时。

| 命令 | 实际结果 |
|---|---|
| `cargo test --release -p backtest --lib --tests --features stage-timing --locked --offline` | 46 passed / 0 failed / 1 ignored；墙钟67.36s；13+3+22+8个通过 |
| `cargo test --release -p backtest --test historical research_ --features stage-timing --locked --offline` | 最后计数口径补验：4 passed / 0 failed；夹具0.06s、含构建墙钟71.12s |
| `cargo test --release -p backtest --test historical research_zero_duration_exit --features stage-timing --locked --offline` | 退出边界最后补验：1 passed / 0 failed，夹具0.01s；保存/恢复终止报告相同 |
| `cargo test --release -p backtest-cli --test source_health research_counts --features backtest/stage-timing --locked --offline` | 1 passed / 0 failed，夹具0.01s；同毫秒失效不撤销成功决定分类 |
| `cargo test --release -p shared-run --test continuous v4_real_partial_cancel_and_zero_target_completion_preserve_economics --locked --offline` | 1 passed / 0 failed，最后口径补验墙钟26.80s；重复身份去重、有SIM回执请求/声明腿分列、5次撤单响应累计 |
| `cargo test --release -p backtest-cli --bin backtest-cli --features backtest/stage-timing --locked --offline` | 2 passed / 0 failed，墙钟0.68s |
| `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s backtest/research -p 'test_six_hour.py'` | 2 passed / 0 failed；完整参数复制、账户独立、范围及部分结果不补零 |
| `cargo clippy --release -p backtest -p backtest-cli -p shared-run --all-targets --features backtest/stage-timing --locked --offline` | 最后补验exit 0，墙钟2.51s（此前61.28s）；非阻塞lint建议保留，未清理无关模块 |
| `cargo build --release -p backtest-cli --features backtest/stage-timing --locked --offline` | 最后构建exit 0，墙钟72.49s；独立候选二进制保留在工作树外 |
| `cargo fmt --all -- --check`、`git diff --check` | exit 0 |

46项回归在最后Exit预算边界补验前完成。最后仅修改新预算分支中已到期Exit的终止处理，
补一个可推翻错误BudgetStop分类的夹具；未到期Exit仍保存，不提前完成。其余已通过证据按影响复用，
最后另将计数名称明确为有SIM回执请求，单列无回执注册，并补验四个研究场景和原撤单累计夹具。
最后源码经过静态检查及release构建。原 `source_health_finite_overhead_measurement` 保持ignored；
原CLI计时夹具 `real_strategy_finite_health_callback_measurement` 和
`same_millisecond_day_stage_measurement` 本轮没有重跑。没有运行全workspace、真实小时预演、完整研究、扩样或训练。

首次两个新夹具失败分别源于将包含原模块字节的Running CBOR整份读成JSON，以及误要求不同账户cutoff中
各自run_id相等。修正为局部类型读取、核对真实Running/pending和保存分类/小时、分别比较经济累计并确认身份独立。
原CBOR格式及有效业务断言保持，修正后通过。后续编译中的命名空间、返回类型和夹具借用错误已定位修正；
没有原因不明失败。完整原日志、失败日志及候选差异在工作树外保留，正式交接提供证据入口。
公共生产者的inputs不是账户完成比例；各候选真实前沿以其原cutoff/检查点为准。
