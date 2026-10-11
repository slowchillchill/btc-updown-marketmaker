# 当前版本分阶段有限计时与早期 Maker 追溯

本报告对应 #185 当前分配的两项有限证据。授权与最小安排见 [批准执行](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6109607016)和[准确方案](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6109653416)。没有重跑旧小时，也没有扩大样本或改变 15ms 模拟参数。

## 计时方法

基线为 `c8c3c8807e04186de6433105d8bf6940cf2e378b`，测量时本轮辅助尚未提交；实际测量使用基线加本次辅助差异。`baseline-mm/tests/host_timing.rs` 只取首折 `center`，三个原状态各预热一次、计时32次，共96次。原866市场、301个点/300秒成熟窗口、闭市原持仓与挂单责任均保留。三状态是当前市场空仓、挂单、孤腿，整个账户都非空。原 sigma 和成交价格/数量断言继续执行。

每次先测 `strategy_observation_input_with_trade_details`，再独立测纯 `StrategyV1::decide`。按真实 `requires_trade_economics_details()`（false）读取；需要完整逐交易事实的前后验证在计时外。测量包装器额外输出、结果复制和返回对象析构在计时外；装配内部必要复制仍计入装配时间，decide内部必要工作仍计入decide时间。不分派测量决定。每case排序第16/31/32项为 p50/p95/max。两项p95不能相加；同次样本配对求和仅表示这两个边界，不是完整宿主时延。

另以 `source_health.rs` 的实际 Maker 请求/成交/健康夹具和 A1 同毫秒失效夹具进入 `HistoricalDayConfigV1/run_historical_days_v1`。`Trace` 显式透传原策略的逐交易读取需求与所有健康回调，只在原委托调用外记录计时。包装器记录/诊断复制不进入纯 `decide` 时间。

`backtest/stage-timing` 为显式构建开关，默认关闭。开启后 day/run 只记录次数/总纳秒/max，并在每个运行末输出一次。没有逐事件计时I/O、新运行配置、保护或性能门槛。SIM边界包含NOE；`apply_closed` 包含NOE领取和Ledger。`advance_before` 等是包含等待的墙钟区间，不是撮合纯CPU成本。下一健康时点包含依赖查找，复核包含当前账户读取与完整资格，发送包含预留/完整资格/Bridge/SIM。启动装配、供料、发布、供料等待、记录等待、最终drain和日完成点分别保留。

生产者和消费者并行，若干边界嵌套，累计值不能相加成总墙钟。两个 day 场景样本很少，仅报告次数/总量/max，不生成分位数。它们是有限合成工程负载，不能表示旧小时调用分布、在线尾延迟、线上积压或完整Maker真实性。866市场负载与小day场景分别报告。

## 复现

输入规则和finality复用旧测量相同身份，路径由环境提供，不包含本机绑定。共享缓存可通过 `CARGO_TARGET_DIR=<shared-target>` 复用，禁止因本次测量清理共享缓存。

```bash
CARGO_BUILD_JOBS=32 cargo test --release -p baseline-mm --test host_timing -p backtest-cli --test source_health --features backtest/stage-timing --locked --offline --no-run
ISSUE185_RULES=<sealed-rule-file> ISSUE185_FINALITY=<finality-file> ISSUE185_TIMING_OUTPUT=<new-result-json> ISSUE185_SOURCE_HEAD=c8c3c8807e04186de6433105d8bf6940cf2e378b ISSUE185_SOURCE_SHA256=<host_timing.rs-sha256> <release-host-test-binary> --ignored --exact representative_host_calculation_timing --nocapture
STRATEGY_HEALTH_MEASUREMENT_ROOT=<new-fixture-root> <release-source-health-test-binary> --ignored --exact real_strategy_finite_health_callback_measurement --nocapture
STRATEGY_HEALTH_MEASUREMENT_ROOT=<another-new-fixture-root> <release-source-health-test-binary> --ignored --exact same_millisecond_day_stage_measurement --nocapture
```

默认关闭核对使用相同源码、相同两个夹具和相同调用环境路径重新构建，无 `--features`。前次独立保存目录后，在原路径创建同样的新夹具；比较原报告与原SIM字节，不重新测量host。样本、阶段原始汇总和必要验证结果见配套JSON；原完整日志与私有材料保留在仓库外。

## 有限实测结果

配套[原始有限样本与记录投影](complementary-pair-stage-timing-maker-trace-20261011.json)保留96对纳秒样本、两份day原阶段汇总和三笔原记录。它不包含既有大块账户debug输出、私有绝对路径或本机绑定。原完整输出在仓库外保留。

下表单位为ms，每格是p50 / p95 / max。三个共同case分别比较；96个新样本不与旧672个混合样本直接比较整体分位数。

| center状态 | 宿主输入装配 | 纯decide | 同次两段配对和 |
| --- | --- | --- | --- |
| empty | 7.463631 / 8.452743 / 8.652099 | 4.007975 / 4.359296 / 5.439100 | 11.474171 / 12.812039 / 13.317232 |
| resting | 7.628872 / 9.634819 / 10.459080 | 4.772765 / 6.251038 / 6.490379 | 12.416014 / 14.820150 / 16.710118 |
| single_leg | 7.548031 / 8.227359 / 8.247447 | 4.042460 / 4.266432 / 4.769659 | 11.592354 / 12.219284 / 12.997018 |

配对和由同次原样本求和后再取分位数。它不含完整宿主工作，不能作为模拟15ms参数的合格/失败判据。旧共同case的混合边界p50/p95/max依次为empty 13.731658/14.538096/14.574585、resting 14.539139/15.630342/17.067618、single_leg 13.765061/14.413442/15.015996ms。旧方法包含完整逐交易读取和calculate的cutoff复制，新方法按策略实际false装配并单测纯decide；这些数值只能描述各自边界，不能单独归因为某项优化收益。

下面只列小day夹具的代表边界，单位ms，形式为次数 / 总量 / max；全部边界见JSON。回调与外围宿主桶嵌套，不相加。

| 边界 | Maker/健康夹具 | 同毫秒失效夹具 |
| --- | --- | --- |
| 包装器纯decide | 14 / 0.558100 / 0.097865 | 6 / 0.119876 / 0.030037 |
| 包装器公共资格 | 1 / 0.001443 / 0.001443 | 1 / 0.001843 / 0.001843 |
| 包装器完整资格 | 11 / 0.023204 / 0.008015 | 2 / 0.003016 / 0.002124 |
| 包装器下一健康时点 | 49 / 0.034127 / 0.003747 | 19 / 0.011440 / 0.002394 |

Maker夹具整个day调用墙钟120.806621ms；同毫秒夹具58.409302ms。Maker消费者的sim_advance_before墙钟桶42次、总73.225783ms、max72.904310ms，包含NOE及可能等待，不能直接称作撮合CPU瓶颈。记录停止与drain为22.263313ms；生产者等待累计103.730653ms。有限供料队列等待不能证明线上积压，drain也不是在线业务时延。

两个夹具的开关关闭/开启完整报告JSON相等，原SIM文件字节也相等；关闭日志没有stage_timing输出。Maker场景保留一市场真实模拟成交与14次决定。同毫秒场景只失效一次：原计算开始+1301ms，在+1701ms公共组检测失效，原请求不发送；恢复后的最早决定+1701ms，原200ms计算加200ms分派后+2101ms发送。两个代表入口都是合成工程负载，不能混入866市场host分布或推广到旧小时。

## 最多三笔早期Maker追溯

旧运行实现头为`665159d2c3efcfe7f943ef5937526299125d44f4`，早于本轮计时基线及A1修复。此处只读取已有材料，不生成旧小时新收益。旧原运行保留15ms计算参数。市场为`0x094426c14d2742346e7e2b3f5f3c4620e5e140a8c2a32dd22f76a5e3d471d784`。

Ledger只读首段4条和次段前128条；V4 NOE只读选中的12条成交生命周期/经济事实和3条接单事实。SIM文件虽命名sim.jsonl，实际是SIMDAY01二进制，使用所属原解码器输出7447组的解码前缀，最后完整组逻辑字节偏移305930，止于首个超过5.000秒的组（5.001秒）。这不是精确磁盘物理读取量，缓冲读取可能超出逻辑前缀。PM用原v3 Arrow解码器只读首小时Parquet前4096行，选出该市场1211行，再保留六个价档前后候选行及15条LastTradePrice。4096是解码/输出行前缀，Parquet row group读取可能有读放大，也不是精确物理读取量。没有解码器复制、账户恢复、小时归约或新模拟器。

| trade / order | UTC 2026-09-12成交时点 | 数量 / 价格 / 金额 | 原Ledger物理记录序号 |
| --- | --- | --- | --- |
| sim-1 / sim-6（Down） | 00:00:03.032 | 50 / 0.48 / 24 | 51–54 |
| sim-2 / sim-5（Up） | 00:00:03.716 | 50 / 0.49 / 24.5 | 55–58 |
| sim-3 / sim-7（Up，部分成交） | 00:00:04.789 | 19 / 0.50 / 9.5 | 69–72 |

三笔原角色均为MakerInferred，立即taker费用为0。前两笔的原post_only GTC请求为`:4:0`和`:4:1`，各50份，决定2.769秒、发送2.784秒、接受2.792秒、回复收到2.802秒。第三笔请求`:5:0`，原50份，决定3.954秒、发送3.969秒、接受3.978秒、回复收到4.053秒。上述短标识均使用配套JSON的同一旧run命名空间；JSON保留完整原请求、cutoff和NOE身份。

原NOE Matched、Mined、经济应用、Confirmed与Ledger引用逐条一致；经济应用在Mined路径发生，随后Confirmed确认已应用。三笔经济有效时点为3.042/3.726/4.799秒；反馈收到时点与经济有效时点分列在原记录中。V4 durable_available_at是原产物提交墙钟，不是2026-09-12模拟成交时钟。订单→NOE→Ledger的身份、金额和时间链一致，不能把这些账务事实直接当作撮合真实性证明。

同源时点有三组L2 BUY价档下降候选：Down 0.48从110降至30（行629→656），Up 0.49从253降至105（901→938），Up 0.50从65降至0（1181→1186）。它们发生于3.032/3.716/4.789秒，收到/接纳为3.071/3.727/4.804秒。原SIM对应隐藏桶分别引用输入位置1443/1443/1683及隐藏索引范围[1190,1198)/[1760,1762)/[2260,2268)。这些仅是可能触发行和桶位置，尚未绑定驱动该笔成交的具体步骤。

已有有限前缀还有15条LastTradePrice，原记录逐条投影在配套JSON；下表时间为2026-09-12 00:00后的秒数，全部原side为BUY、fee_rate_bps为0。它们与三笔SIM成交按token和价格分别对照，不把互补腿或邻近时点当作同一笔。

| 原PM行号 | 源时点 | token对应腿 | 价格 | 数量 |
| --- | --- | --- | --- | --- |
| 50 | 0.224 | Up | 0.51 | 60 |
| 105 | 0.428 | Up | 0.51 | 18.52 |
| 158 | 0.728 | Down | 0.5 | 6 |
| 196 | 0.912 | Up | 0.51 | 9.803922 |
| 327 | 1.318 | Down | 0.5 | 41 |
| 400 | 1.732 | Up | 0.51 | 1.960785 |
| 627 | 2.984 | Down | 0.5 | 10.4 |
| 652 | 3.029 | Down | 0.5 | 10.4 |
| 677 | 3.046 | Down | 0.5 | 10.4 |
| 723 | 3.069 | Down | 0.5 | 10 |
| 891 | 3.524 | Down | 0.5 | 10 |
| 1209 | 4.807 | Down | 0.5 | 10.4 |
| 1231 | 4.826 | Down | 0.51 | 10.196079 |
| 1241 | 4.837 | Down | 0.5 | 10.4 |
| 1258 | 4.876 | Down | 0.5 | 65 |

sim-1的Down 0.48、sim-2的Up 0.49、sim-3的Up 0.50均没有同token同价格的LastTradePrice。第三笔附近4.807秒的公开记录是Down 0.50、10.4份；其余记录也仅为Up 0.51或Down 0.50/0.51。本轮仅完成同token、同价格的逐条对照，尚未核验互补撮合关联及公开事件字段语义。不同token不自动排除关联；现有时点、数量和身份信息也不足以确认这些公开成交对应所选SIM成交。

官方[CTF Exchange MINT场景](https://github.com/Polymarket/ctf-exchange/blob/main/docs/Overview.md#scenario-2---mint)允许互补token的两笔BUY通过MINT匹配。这只说明平台具备该能力，不说明本样本发生了MINT。该前缀公开成交尚不能直接支持三笔；有限读取没有覆盖全部真实成交，也不能证明真实成交不存在或模型错误。

本前缀没有取得每笔成交前后的真实队列段、外部排队量变化及其逐步骤映射。因此撮合依据不足，不能声称三笔已验证符合模型、真实FIFO或我方必成交；也不能反向断言发现错误。第三笔只证明此前缀内19/50部分成交，不能认定最终孤腿或剩余31最终未成交。三笔不能估计全局错误率。此处按有限合同停止，不继续读小时或补造追踪平台。


若用户继续授权，必要对照提案只选sim-3 / sim-7的Up 19@0.50一笔，原请求仍为50份。固定旧源码`665159d2c3efcfe7f943ef5937526299125d44f4`、原规则/finality、参数（含15ms）、请求命名空间与`:5:0`请求及原时序。只使用足以建立3.978秒接单前队列起点并推进至4.789秒成交的最短必要旧输入前缀；起点按实际队列恢复需要确定，不扩大到完整小时。先核对外部排队量、我方订单位置及此前消耗/补充，再逐步核对消耗归属，绑定具体触发步骤、SIM隐藏索引、原输入位置与源消息，再核对适用撮合类型及公开事件字段语义，最后与已有盘口和上述公开成交对照；无法建立映射时保留未知。该对照只能解释旧模型为何成交，不能证明真实FIFO或真实必成交。此提案未执行；新模型、完整小时和扩样另行裁决，不设新统一门槛、业务保护或追踪平台。

复现窄读时，以下输入和输出路径由调用者提供。输出必须是新文件；NOE引用数组从原Ledger记录的transaction.source.NoeConsumed.result提取，选序号49/50/51–58/67/69–72，不枚举全部NOE。配套公开引用路径已转换为旧account目录相对路径，读取时使用原材料中的原引用。

```bash
cargo run -p backtest --example limited_maker_ledger_trace --locked --offline -- <old-account>/ledger/ledger-parquet-00000000000000000000.parquet <new-prefix-0-json>
cargo run -p backtest --example limited_maker_ledger_trace --locked --offline -- <old-account>/ledger/ledger-parquet-00000000000000000001.parquet <new-prefix-1-json>
cargo run -p backtest --example limited_maker_noe_trace --locked --offline -- <selected-original-references-json> <new-noe-output-json>
cargo test -p sim-system --lib --features shared-execution --locked --offline --no-run
ISSUE185_OLD_SIM=<old-account>/sim.jsonl ISSUE185_TRACE_END_MS=1789171205000 ISSUE185_SIM_TRACE_OUTPUT=<new-sim-prefix-json> <sim-test-binary> --ignored --exact event_log::tests::issue185_readonly_early_day_requests --nocapture
cargo run -p backtest --example limited_maker_pm_trace --locked --offline -- <original-PM-first-hour-parquet> <new-pm-prefix-json>
```

## 必要验证与保留

使用共享target、32构建作业；测量期间无无关并发研究。开启release编译112秒，关闭release编译99秒；host执行28.51秒。cargo fmt --all -- --check及git diff --check通过；受影响静态检查通过。

- 两个代表day夹具开关开启各1 passed/0 failed；关闭各1 passed/0 failed，报告JSON和SIM字节一致。
- 默认关闭source_health回归：11 passed/0 failed/2 ignored，0.07秒。ignored为real_strategy_finite_health_callback_measurement与same_millisecond_day_stage_measurement，两者已分别显式执行。
- `cargo test -p backtest --lib --bins --tests --examples --locked --offline`：42 passed/0 failed/1 ignored，11.05秒。ignored为source_health_finite_overhead_measurement，属于旧独立计时入口，本轮不重复测量。
- 原SIM日日志回归historical_day_frames_preserve_groups_hits_and_resume_at_physical_checkpoint及窄只读辅助各1 passed/0 failed；三类只读示例实际运行成功。
- `cargo clippy -p backtest -p backtest-cli -p baseline-mm --tests --examples --features backtest/stage-timing --locked --offline`通过，25.53秒；保留原已有警告，不为本轮修改无关模块。

初次check曾把仅支持Serialize的HistoricalRunReport误当成可Deserialize；改为读取序列化Value后通过。PM窄读示例初次编译遇到通配导入的Result别名与main双泛型返回冲突，限定std::result::Result后通过。两次均为确定编译原因，原失败日志保留；未改变业务测试期望或通过重试跳过验证。没有重跑A1全矩阵或根workspace、旧一小时、训练和性能门槛。

计时边界差异与最终完整候选差异分别保留在仓库外；计时实际使用基线加本轮host/Trace/day/run辅助，随后加入只读追溯辅助和报告。SIM公开open仅提取原from_file解码入口供只读测试使用，格式与业务不变。私有旧材料、原96样本完整debug输出、有限窄读原件和原日志保留。共享构建缓存不清理；没有本轮专用构建目录或新归档。未使用任务索引，全部引用按当前任务树源码核验。
