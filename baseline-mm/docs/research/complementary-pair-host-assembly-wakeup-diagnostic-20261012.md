# A侧有限装配诊断与空输入等待观察

本段按[#185准确分配](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6118643558)执行。源码基线为`1e8eeda3059ca0203898afb4b55b50571006124d`，实际测量使用该基线加本次测试辅助差异；没有公共生产优化、B工作、服务器访问、旧小时重放或新经营研究。[20261011原报告](complementary-pair-stage-timing-maker-trace-20261011.md)的实现身份、96样本与结论保持。本段原样本及精确纳秒见[配套JSON](complementary-pair-host-assembly-wakeup-diagnostic-20261012.json)，不混用两轮样本。

## 方法与前提

复用`baseline-mm/tests/host_timing.rs::representative_host_calculation_timing`的center三个当前市场状态：空仓、挂单、孤腿。账户仍含已关窗持仓与订单责任，866市场、301来源点及300秒成熟窗口不变；原封存规则、finality和候选参数不变，全部读取需求为false。该原工程夹具用PAPER身份装配共享V2账户，不是新历史Maker模型经营复核。每状态预热1次、保留32次，共96次；每格排序第16／31／32项为p50／p95／max。

先测完整`strategy_observation_input_with_trade_details`，再测纯`StrategyV1::decide`。另外分别调用既有公开的`freeze_action_before`、`funds_read_v1().projection()`、`economic_read()`和`AccountEconomicsReadV2::from_ledger_and_observations`；观察复制只测已有`FrozenMarketReadV1`向量的clone，不冒充私有装配函数的精确子阶段。`calculate`用原checkpoint／restore接口构造同一调用前状态的独立策略，保留截止复制；恢复、输出比较和对象析构在各计时区间之外。

完整装配仍执行原`apply_closed`／`observe_group`。各独立诊断与完整输入核对同一截止、Ledger投影及账户经济读取；首末样本核对全部观察值和规则Arc，calculate与decide逐次核对完整序列化输出。原闭市责任、5份@0.55成交、300对方差断言及前后经济／成交读取比较保留；不分派计时决定。独立诊断有重复读取，顺序和缓存可能影响单次成本，不能相加成完整路径分解，不能用各p95相加作端到端p95。

## 有限结果

下表单位ms，格式为p50／p95／max。

| 边界 | 空仓 | 挂单 | 孤腿 |
| --- | --- | --- | --- |
| 完整装配 | 8.865762 / 10.049079 / 10.136975 | 9.145789 / 9.515315 / 9.551653 | 9.252229 / 10.277349 / 10.899059 |
| 纯decide | 4.196351 / 4.442745 / 4.907619 | 5.042574 / 5.342718 / 5.433690 | 4.206750 / 4.992059 / 5.003379 |
| freeze_action_before | 5.635350 / 5.873388 / 5.979237 | 5.770634 / 6.021076 / 6.512040 | 5.791424 / 6.046133 / 6.313887 |
| funds.projection | 0.012894 / 0.016231 / 0.016511 | 0.016131 / 0.018264 / 0.019697 | 0.015129 / 0.018535 / 0.019607 |
| economic_read（含内部投影） | 0.005219 / 0.006833 / 0.007043 | 0.005620 / 0.007054 / 0.007614 | 0.006282 / 0.006893 / 0.007914 |
| 原经济读取→账户经济组合 | 4.956301 / 5.295660 / 5.399666 | 5.129678 / 5.435674 / 5.458848 | 5.080555 / 5.582320 / 5.607016 |
| 已冻结观察向量clone | 1.952827 / 2.209460 / 2.252471 | 1.998042 / 2.244716 / 2.341428 | 1.923562 / 2.315589 / 2.506920 |
| 独立calculate（同一调用前状态） | 4.745635 / 5.113467 / 5.640930 | 5.641131 / 6.019924 / 6.326291 | 4.575605 / 5.573994 / 5.941506 |

三个固定状态各32次均有动作（Actions），NoTrade为0；本段未测NoTrade成本，不补造其分布。账户经济组合约5ms、观察向量复制约2ms，在此固定构造中有毫秒级成本。重复投影虽确实存在，独立projection和economic_read只有微秒级，不能据此将它判为主要热点或高收益优化。上述独立成本不能转换成七候选真实增长路径的占比或优化倍数。

旧七候选阶段CSV仅读取19,061B小文件的三行：center装配484,680次、11,517.069620991秒、均值23.762213ms；含截止复制及decide的计算117.045924482秒、均值0.241491ms。这是旧预算停止前缀的阶段统计，不是共同两小时决定数，也不是本次固定状态测量。并发、状态分布、账户历史和边界不同；不能把旧总体均值与本段分位数当优化前后对照。

## 两个空Hub消费者

`backtest/src/day_state.rs`只在已有`cfg(test)`范围加入线程局部计数；两条next中的计数调用也分别受`cfg(test)`控制，非测试编译没有新增字段、状态或可观察行为。两个消费者共用原空Hub，分别进入原public／hidden的next与Condvar.wait。同步起步后主线程只等待200ms，再用原`ProductionV1::Ended`及notify_all结束；没有发布数据、生产者计时通知、业务超时拒绝或固定唤醒次数门槛。原等待／通知逻辑未改，末尾只输出一次。

本次调用墙钟0.203751秒；内部窗口及汇合墙钟200.671936ms。public与hidden各68,102次wait返回，其wait墙钟（含重新取得锁）分别为169.770994／169.671538ms；最后以真实Ended事实正常返回EOF。反复返回时没有业务输入，源码中重复写waiting=true再notify_all提供持续互唤路径。依据[Rust1.95同版本Condvar源码](https://github.com/rust-lang/rust/blob/1.95.0/library/std/src/sync/poison/condvar.rs)，wait返回包含锁重取且可能虚假唤醒；本段未给每次返回标注通知发送者，不能逐次断言全是对方通知。

旧producer_backpressure_wait是wait_timeout调用1,047,422,624次、累计15,294.903828833秒、均值14.602微秒，不是事件或订单数，也不是CPU耗时。两消费者的等待区间重叠，不能求和为墙钟；小并发结果没有量化七候选CPU消耗、可节省等待时间或经营收益。

## 时序与下一步边界

Rust输入装配及策略函数同步执行，不能因它们当时耗了多少真实墙钟就改写模拟时钟。原`CalculationTimingRecordV1`分别记录观察、计算开始、模拟完成、计划发送及实际发送；当前配置仍取计算时长15ms。`backtest/src/run.rs`的calculate返回后按显式延迟生成完成与发送时刻，将计算保存在pending中；其间继续处理公共组及健康失效，dirty合并新的重算需求。既有A1同毫秒失效通过证据复用，不重跑矩阵；99%NoTrade不授权跳过计算。本段不证明实时端到端15ms达标。

后续选项分别回到真实责任边界，不是本段已实施优化：

- 公共日回放等待路径：`day_state.rs`两个next的空分支重复通知，`day.rs`的producer回压也等待同一Condvar。可提出只在waiting实际变化时通知的有限修复；必须保留材料到达、游标推进、checkpoint、结束和失败的有效唤醒，不能全局换成notify_one。本段没有修改它们。
- 公共经济组合：`shared-run/src/group.rs::freeze_action_before`调用`shared-run/src/economics.rs::AccountEconomicsReadV2::from_ledger_and_observations`，再经原逐市场经济组合及未决孤腿best bid重估。本段固定866市场约5ms。需核对实际所需市场、估值变化及所有公共消费者，不能只以余额／时间相同裁剪，也不能在策略内另建账本。
- 公共观察复制：`shared-run/src/strategy.rs::input_from_frames`克隆每市场observation／rule_context；本段测已有冻结向量clone约2ms，两者边界相近但不等同。公共共享／借用方案需保持不同前缀、持有期和未消费旧组，不能以最新Core替换旧观察。

day_state是公共日回放实现：当前main已确认的生产消费者是A宿主，未发现B调用，B未提交候选未核；不能由Rust可见性或本段使用方推断A独占。主线native-inputs确实消费freeze_action_before的公共经济组合，但借用PublicObservationFrameV1编码，未调用input_from_frames或FrozenMarketRead向量clone。观察所有权／公共合同改造可能影响B，与实际复制消费者不同，需按具体改动判断并交用户裁决。本段没有改公共生产逻辑或B，也不声称B当前候选兼容。通知修复可作为当前A消费场景下的公共日实现候选提出，仍须定向验证，不把本次观察当作已获性能收益。

## 验证、首次失败与保留

首轮release构建exit0，42.331858秒。首轮host在empty和resting各完成32样本及等值核对之后，共完成64次；随后在single_leg供料的既有advance／next_group出现RecordingBackpressured，exit101、24.914982秒；没有完成96，原样本不进入本表。准确基线线上源码确认该值表示唯一待交接组的合法回压，真实后台失败另返回Log。原helper未启用有限等待；只在新driver后调用已有`enable_finite_recording_wait()`一次，让未成功commit先交接pending，避免重试复合驱动已成功步骤。容量128、业务输入、计时区间和全部期望不变，生产历史宿主已使用同一入口。

修正后release增量构建exit0、3.907144秒；host具名1 passed／0 failed，33.438265秒，完整96；随后wait具名1 passed／0 failed。两次执行的源均与各自测量补丁一致。首轮失败和辅助编辑脚本的范围匹配错误均保留，不写成首次全部通过。

受影响clippy通过（25.307715秒），修改文件rustfmt及diff检查通过；day_state的原type_complexity／collapsible_if提示在未改的恢复／发布代码，未为本段清理。未运行全工作区、公共撮合回归或研究。适用原发布、结束、失败与A1证据复用；本段测试只核有限诊断辅助，未新增生产接受／拒绝条件。

```bash
CARGO_TARGET_DIR=<shared-target> cargo test --release -p baseline-mm --test host_timing -p backtest --lib --locked --offline --no-run -j32 --message-format=json
ISSUE185_RULES=<sealed-rule> ISSUE185_FINALITY=<normal-finality> ISSUE185_SOURCE_HEAD=1e8eeda3059ca0203898afb4b55b50571006124d ISSUE185_SOURCE_SHA256=<host_timing.rs-sha256> ISSUE185_TIMING_OUTPUT=<new-raw-json> <release-host-test-binary> --ignored --exact representative_host_calculation_timing --nocapture
ISSUE185_WAIT_OUTPUT=<new-wait-json> <release-backtest-lib-test-binary> --ignored --exact day_state::tests::two_empty_day_consumers_observe_existing_wakeups --nocapture
CARGO_TARGET_DIR=<shared-target> cargo clippy -p baseline-mm --test host_timing -p backtest --lib --tests --locked --offline -j32
rustfmt --edition 2024 --check baseline-mm/tests/host_timing.rs backtest/src/day_state.rs
git diff --check
```

本机32核、约107GiB可用内存，复用共享target（构建前188GiB）及约1.060TB可用磁盘；没有专用缓存或清理共享产物。实际任务树没有索引项目，源码及字面量直接回读，不以主树索引替代。原规则、历史材料及此次原日志留在仓库外；提交前冻结完整Git差异，待独立内容审查。
