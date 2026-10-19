# 公共历史宿主的100ms顺序市场帧

[D-0369](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181#issuecomment-6270139404)采用持续SIM时间及逐帧顺序消费。
本能力接在原 `run_context_with_hook`、日供料和公共执行链内；没有第二宿主或模型实现。
默认 `timing.decision_mode` 省略或为 `event_driven`，保持原A事件/健康模式。
显式 `fixed100ms_fifo` 启用以下行为，角色、Maker、Gamma和有限生命周期仍由原配置分别选择。

## 生成、等待和处理

网格锚定原宿主经营起点 `max(context.start, operating_start)`，每100ms推进，严格处于原范围 `[S,E)`。
本毫秒先按原序消费全部合法公共组、原隐藏桶及可见反馈，再生成市场帧。
没有消息时也读取原公共统计在该合法观察时刻的窗口结果，不制造行情消息，不按网格重采样共同历史。
尚无合法receipt或所需市场观察时不造帧，报告 `grid_without_market_frame`，不发出NoTrade。
市场观察内的未知来源仍为未知；出现元数据不表示盘口或其他来源已经初始化。

每个已生成帧进入同一账户FIFO。帧最早可处理时间为：

```text
市场帧cutoff时间 + calculation_start_delay_ms[该合法帧序号]
```

该参数是显式模拟开始延迟，不是实测帧生成耗时，也没有校准真实模型或输入工作量。
`Fixed` 与 `Sequence` 沿用原时序类型；序列耗尽仍返回原错误，不循环、不补零。
开始延迟按合法帧生成序号取值；计算和发送延迟继续按实际成功计算序号取值。
公共交付延迟仍按原公共组序号选择，并沿原 `max(last, visible+delay)` 交付。

同账户继续使用原单个 `Pending` 计算/发送槽。前帧的计算及发送等待结束后，队首到达最早处理时间才开始；
因此后帧不能利用前帧的发送等待重叠计算，本段没有引入并发动作政策。
实际开始时刻由原事件循环选出，先沿原SIM推进、同毫秒消费、观察及 `SharedExecution::apply_closed`，再调用策略。
这不是在旧SIM时刻只给 `decide` 传一个未来时间。一次实际调用才更新策略自己的必要记忆。
积压不会停止公共组、旧单、成交或经济义务的时间流动；没有默认丢格、跳格、停止或记忆重置阈值。
策略自己的 `next_evaluation_at_utc_ms` 不在此模式额外插入调用，实际消费由固定网格FIFO决定；原事件模式仍保留该机制。

## 两个截止和输入冻结

`StrategyInputV1::market_frame_timing` 为新模式显式提供：

- `market_cutoff`：整体原市场帧的可信截止，含合法receipt/公共前缀，不依赖可空统计反推时间。
- `earliest_start_at_utc_ms`：该帧的显式最早处理时刻。

`action_before.cutoff()` 单独记录实际开始时刻的合法账户/反馈截止；`decide` 参数及
`StrategyCalculationV1::calculation_started_at_utc_ms` 是同一SIM开始时间。
模型可由开始减市场时间取得帧年龄，由开始减最早处理时间取得已有排队等待。
这些是当时已知时间，不提供尚未发生的最终推理耗时、隐藏成交或未来反馈。
`StrategyCalculationV1` 与原 `CalculationTimingRecordV1` 保留市场时间，原input cutoff、完成和发送字段继续保留账户/执行时间。
没有市场帧的旧计算序列化省略新增可选字段。

`strategy_input_for_market_frame` 先用当前合法公开观察给当前Ledger/Registry冻结 `ActionBeforeReadV2`、订单、在途义务及费用反馈，
包括按当前合法公开标记计算的 `account_economics_v3` 估值，然后组合旧市场向量。
它不拿旧市场帧给新Ledger冒充当前估值，不推进SIM，也不回填旧帧的市场值。
此拥有式入口直接移动旧帧，不经 `BorrowedStrategyInputV1::to_owned_input` 重取当前市场。
后续行情、账户和反馈推进不改已选择完整动作；发送沿原Bridge/SIM/NOE/Ledger，以真实到达时刻和资金/行情约束执行。
宿主不隐含撤换或改动作。只有策略显式返回空意图才是空动作；忙碌、未调用和未完成均不是NoTrade。
模型撤单禁令仍待真实模型动作消费者执行，公共Cancel及A原动作不变。

## 共同统计的实际装配

`HistoricalRunConfigV1` / `HistoricalPreparedRunConfigV1` 的可选 `public_statistics` 直接使用
`PublicStatisticsConfigV1`，传入完整窗口参数，没有B私有公式或隐含参数。
单运行与原Parquet日运行都在供料前把原 `PublicStatisticsStreamV1` 交给 `enable_public_statistics`。
成功SIM receipt按实际组应用时钟更新原共同历史，网格仅查询和冻结结果。
本段核实并补齐了原普通backtest没有调用该入口的接线缺口；仅持有可选Arc不能证明已启用统计。

原日运行中，只有同输入/初始化来源、流头/规则版本、完整统计参数和公共交付时序才共享同一统计流。
计算/发送速度和账户不同不分裂该公共流；公共交付时序或参数不同则各自绑定。
启用共同统计的日候选同时声明完整Binance深度数量需求，F6不被策略的精简投影请求删去；未启用者继续原需求协商。
缓存仍归原prepared组/统计历史，无全局隐式缓存。各账户持有自身成功receipt的合法前缀，慢账户不读生产者未来前沿。
统计结果、规则和共同来源按原Arc共享；FIFO只保留未消费的必要市场快照及其引用，消费即释放，不留下全程完整观察副本。
若模型长期慢于100ms，未消费帧会随积压增长；本段没有擅自增加容量拒绝、丢帧或自动停止政策。

## 有限结束与输出

仍按D-0367处理EOF：不越宿主截止完成队列、未来发送或未来经济义务，不制造RunSeal或终局。
`fifo` 报告网格次数、无帧次数、已生成/已开始/未开始数量及待处理峰值。
`queued_frame_timings` 只输出未消费帧的时间身份，不输出其完整观察；原pending timing另报告已调用但未完成发送的工作。
原未闭承诺、未消费NOE和后续必要驱动仍如实返回。这不是环境/模型协调恢复或可续跑承诺。

## 合成验证与使用

原 `finite_environment_fixture` 的 `timing_override` 可完整指定原 `HistoricalTimingV1`；指定后覆盖其旧固定交付参数。
`public_statistics` 原样传入实际日配置。完整可调用配置见[时序合成配置](examples/runtime-timing-fixture.json)。

```bash
cargo build -p backtest --example finite_environment_fixture --release --locked --offline -j20
<显式程序路径> <完整配置.json> <新输出目录>
```

固定请求仅为公共经济接线验证，不是模型、学习、B资金或订单子域默认。
本材料的合成窗口不证明真实5分钟或完整UTC日覆盖。
合法材料只有PM及元数据，欠缺Binance/RTDS时F1—F8相关量如实未知；F7按当前原深度缺失规则处理。
新宿主夹具要求统计非空并核对F0市场、F1—F8缺失/PM数量及F9原来源/时间；
复用原公共统计六项手算、边界及完整冻结夹具验证其他来源公式，不在宿主复制算法。

手算代表场景：元数据1ms合法接纳，再延迟100ms，首个合法网格为200ms。
零计算/发送延迟时，范围0—3000ms有30个网格机会、28帧/28次调用，无帧2次。
突发场景第13帧1400ms计算900ms，再等发送100ms，占原槽至2400ms；1500ms帧到2400ms才处理，年龄900ms。
此时旧市场UP卖价仍0.6，2301ms已交付的新卖价0.5不覆盖它；原成交/现金义务已按2400ms合法前缀进入账户。
原冻结DOWN动作仍为0.45，不按后来0.65盘口改价；队列峰值10，短暂积压后原序追赶。
另一场景首帧最早250ms，计算5000ms，范围仍止于3000ms：只实际调用一次，剩余27帧；未完成动作不派发、不变成NoTrade。
共享账户夹具另核对同公共输入及不同处理速度复用同一统计流，而现金和积压独立。

验证及实际成本绑定本PR完整源码和最终产物，详见本PR验证记录。本段没有真实模型或GPU消费者，
不执行真实数据扫描、账户回放、训练/评价、PAPER/LIVE、部署或资金作业。
合成时序只证明接线，真实帧生成成本、工作量/突发密度校准、模型推理和学习恢复均为后继工作。
EOF存留、短暂追赶或小样本吞吐不能证明100ms服务容量达标。

CLI脚本回归另暴露训练主机缺少原 `/usr/bin/time` 的工具路径依赖。脚本现支持显式 `GNU_TIME_BIN`，缺省路径和原退出/汇总合同保持；验证使用精确Ubuntu22.04 `time=1.9-0.1build2` 制品并校验发行版包SHA-256。原选项及退出透传可查[发行版GNU time手册](https://manpages.debian.org/bookworm/time/time.1.en.html)。该包的程序版本横幅实际显示 `UNKNOWN`，不把横幅改称源码版本；制品身份绑定包版本及哈希。本修补没有替换计时算法或修改业务断言。

## 本候选的验证与成本

基线为 `1b2ed1f93c1f60927e0c795261b280042765d69d` 加本PR完整差异。
最终Rust/Cargo源码集合SHA-256为 `bd76ecd5dbd8167105d64958c44f5fe01b62c9d8db05914c0c9fc74ddfb3d0b1`，
按541个文件的有序相对路径、NUL、文件内容、NUL计算。
程序SHA-256为 `9c875f1a86bf4c2ce0f99a91831863b1e6e6c180387875c1a6238ad82c3f39df`。
测量时尚未提交；合并前另核对提交与冻结候选的一致性，不改写原配置的base/dirty身份。
完整配置可在仓库外修改已支持参数，无需修改源码或重编译。

验证命令：

```bash
cargo check --workspace --all-targets --locked --offline -j20
cargo test -p shared-run --test strategy_boundary --locked --offline -j20 -- --nocapture
cargo test -p backtest -p backtest-cli --lib --bins --tests --examples --locked --offline -j20 -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
# CLI脚本环境缺少缺省工具时，先显式提供真实GNU time产物，再重验CLI批：
GNU_TIME_BIN=<显式GNU-time路径> cargo test -p backtest-cli --tests --locked --offline -j20 -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
```

共享策略边界10项通过；backtest受影响批93项通过、3项原ignored，含四项FIFO、A连续出口和原公共统计六项；
CLI修正工具路径后完整批35项通过、3项原ignored。首次CLI批的缺工具失败原样保留，不能写作第一次全部通过。
六项ignored为原日队列/空等待诊断、有限健康开销及CLI的三项显式成本测量；未执行这些额外作业。
没有全工作区测试通过主张。具名UTC时钟相关生产吞吐测试未运行。
全workspace全目标静态检查覆盖实际公共类型消费者，格式及shell语法/差异核对通过。

实际训练服务器配额25vCPU、90GiB。Rust/Cargo1.95.0，锁定离线依赖，release默认opt-level=3，
无额外RUSTFLAGS/LTO，20个编译作业。首次release构建墙钟91.1365秒；最终只纠正未进入该程序的CLI测试一行缩进，
缓存构建确认程序字节相同。首测原配置和结果保留，最终配置绑定纠正后的完整源码集合，以下为最终配置各一次调用：

| 独立运行次数 | 公共原输入 | 网格机会 | 生成/实际调用 | 每账户待处理峰值 | 尾部剩余 | 整进程墙钟 | 整进程有效吞吐 | 峰值RSS | 输出目录含summary |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 9 | 120 | 118/118 | 9 | 0 | 0.147641秒 | 6.7732次/秒 | 28,236KiB | 296,410字节 |
| 10 | 90 | 1200 | 1180/1180 | 9 | 0 | 0.815225秒 | 12.2665次/秒 | 39,416KiB | 2,911,703字节 |

分母为外部整进程墙钟，包含合成Parquet/manifest、规则派生、原准入/流式供料、公共统计、SIM/NOE/Ledger、
FIFO/实际策略调用、原经济记录、配置/程序自哈希和summary/标准输出。
内部循环分别0.083607/0.761946秒，不能充当整进程分母。
summary前目录分别273,889/2,716,208字节；表外stdout日志另为22,558/195,867字节，未隐去输出增长。
每次两个合成市场，开始延迟10ms、通常计算20ms且第13帧900ms、发送30ms；
此显式成本场景与上方1400+900+100的手算功能场景不同，故峰值分别为9和10，不混用。
没有GPU消费者，CPU/GPU传输为0；不通过无关GPU作业制造利用率。
这些结果只证明该实际入口与配置，不证明真实数据、全UTC日、GPU模型或100ms服务能力。
EOF仍有队列的功能反例单独通过，不能用本成本场景尾部为0冒称所有作业均能排空。
