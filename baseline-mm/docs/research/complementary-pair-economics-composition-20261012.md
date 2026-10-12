# 公共经济组合的精确零运算优化

按[#185当前分配](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6119289486)执行。基线为`1267df31cdc75bcebc7c18cbecb942f204f1a315`，两轮均使用该基线加本次已记录差异；前轮只有测量辅助，后轮加入精确零运算与必要测试。没有新经营研究、B工作或服务器访问。

## 测量与真实规模

沿[前次有限装配诊断](complementary-pair-host-assembly-wakeup-diagnostic-20261012.md)的center，当前目标市场为空仓、挂单、单腿三状态。每格预热1次、保留32次，每轮96。各状态仍保留旧已关闭市场的真实成交孤腿与挂单；“当前市场空仓”不是整个账户没有持仓。866观察市场、301来源点、原规则/finality/seed及输入不变，计时装配按真实策略需求使用requires_trade_economics_details=false。

保留原完整装配、V2及六项独立测量，只新增read.market_totals().collect与AccountEconomicsReadV1::from_ledger两项。各项是独立重复调用，不能相加或相减当精确装配分解。实际Ledger经济市场每格也是866，这是原读取所得，不从观察数推定。未结算正孤腿数及市场数分别为1/1、1/1、2/2；全部市场字段由V1/V2和原完整装配读取在计时外逐值核对。

两轮helper完全相同，SHA256为`455abb2748fc89c4f14ba126501c519656cefe1aed8c8cd9aa2421534034676a`。两轮均核对完整账户经济、全部市场字段、当前/旧市场诊断、意图、成交读取和输出类别，逐值一致。原5份@0.55、闭市责任、成熟概率与观察核对保留，每格32次均Actions；NoTrade成本未测。计时仍用排序第16/31/32项作p50/p95/max。

前测显示market_totals约2.6–2.9ms，V1/V2组合约4.7–5.1ms，不能以独立读的差直接计算内部占比。进一步仅对empty的V1原输入做一次有限独立疑点探针：5类Decimal金额×866=4330、两类confirmed_unpaid分数×866=1732，共6062项，在该状态全部为零。此数不表示所有字段都为零，也不外推其他账户。探针用原EconomicRational公开构造/加法、实际读取操作数和black_box，每轮预热1+32；它不是另一账本或生产组合重建。转换/加法p50为1.427688→0.249139ms，不能直接解释原约5ms组合的比例。

## 最小生产修改

仅修改contracts/src/economics.rs的两个方法。ratio仍先断言分母非零，零分子直接返回原规范0/1。add遇零操作数直接克隆另一已经规范化的内部值，避免重新计算整数积与最大公约数。内部字段私有，构造、反序列化及运算均沿原规范化入口；没有新缓存、状态或公共API。

非零算法不变，from_decimal/sub/mul/divided_by方法代码不变；它们调用ratio时得到同一精确数值及编码。没有浮点近似、12位截断、新舍入、分母限制或新拒绝条件。shared-run/ledger生产代码、观察所有权、字段、前缀、估值可用性、结算、现金和收益归属不改。

## 有限前后结果

单位ms，每格为“前p50 / p95 → 后p50 / p95”。两轮分别一次顺序执行，不与编译或其他本轮诊断重叠；调度和缓存仍可能影响结果。

| 边界 | 当前市场空仓 | 当前市场挂单 | 当前市场单腿 |
| --- | --- | --- | --- |
| 完整装配 | 8.866984 / 8.975498 → 7.416724 / 7.963573 | 8.908592 / 9.475740 → 7.653520 / 8.274358 | 9.599172 / 9.982033 → 7.875006 / 9.090604 |
| market_totals().collect | 2.858672 / 2.921400 → 2.626995 / 2.779594 | 2.594595 / 2.893418 → 2.478938 / 2.965032 | 2.863280 / 3.235322 → 2.704361 / 3.101439 |
| V1账户组合 | 4.857605 / 5.009371 → 3.482666 / 3.667926 | 4.694348 / 4.896308 → 3.343135 / 3.754979 | 4.908240 / 5.289549 → 3.595790 / 4.465107 |
| V2账户组合 | 4.926806 / 5.012998 → 3.484149 / 3.756392 | 4.843579 / 4.976660 → 3.497575 / 3.859436 | 5.083961 / 5.377844 → 3.613984 / 4.267064 |

完整装配和目标组合在此固定构造下都有减少；market_totals减少较小，不能把全部成本归给零运算。没有采全程CPU，也没有同小时经营复核；不据小样本声明整场倍数、B兼容或实时15ms达标。原A1/15ms、SIM自动结算、预算和前缀语义保持。

## 正确性与验证

新增一项零恒等式测试，核对2/3、−7/3、负分母规范化、零编码，以及0/0与除零仍拒绝。原整数比持久化测试继续通过。两个rational测试、部分Maker非终止分数、双市场返佣精确贡献、判定先到/成功晚到三个共享用例及完整auto_redeem文件三项，共8 passed/0 failed；这些独立金额与真实事实断言不以自产摘要代替。自动模式仍无主动redeem，晚到前驱与恢复不重复现金均沿原测试。

前/后96样本测量各1具名passed/0 failed，执行墙钟35.206047/34.168655秒；不把192子样本计成192个测试。release前/后编译38.686581/102.967946秒另计。两轮调用墙钟包括各自夹具准备与成熟输入，不等于纯装配耗时。没有重跑无变化day-wait、A1、撮合矩阵、全workspace或B验证；没有改变估值规则，因此原缺/陈旧报价不可用、新鲜空bid已知零和无暴露市场语义复用。

```bash
CARGO_TARGET_DIR=<shared-target> cargo test --release -p baseline-mm --test host_timing --locked --offline --no-run -j32 --message-format=json
ISSUE185_RULES=<sealed-rule> ISSUE185_FINALITY=<normal-finality> ISSUE185_SOURCE_HEAD=1267df31cdc75bcebc7c18cbecb942f204f1a315 ISSUE185_SOURCE_SHA256=455abb2748fc89c4f14ba126501c519656cefe1aed8c8cd9aa2421534034676a ISSUE185_TIMING_OUTPUT=<per-run-raw-json> <release-host-test-binary> --ignored --exact representative_host_calculation_timing --nocapture
CARGO_TARGET_DIR=<shared-target> cargo test --release -p baseline-mm --test host_timing -p shared-run --test economic_settlement -p backtest --test auto_redeem -p ledger --lib --locked --offline --no-run -j32 --message-format=json
<release-ledger-test-binary> economics::rational_tests:: --nocapture
<release-economic-settlement-binary> --exact real_partial_makers_fix_average_cost_and_nonterminating_fraction --nocapture
<release-economic-settlement-binary> --exact actual_two_market_maker_day_allocates_exact_contribution_ratios --nocapture
<release-economic-settlement-binary> --exact resolution_before_late_success_fixes_fee_bucket_and_updates_pi_without_frames --nocapture
<release-auto-redeem-test-binary> --nocapture
```

相关五包clippy通过（25.225631秒），另补Ledger单元测试静态目标覆盖通过（8.273057秒）；修改文件rustfmt与diff检查通过，没有修改行新增clippy提示。

准确两轮差异、原始样本、有限探针及命令日志保存在本次仓外证据，不覆盖旧诊断。B实际消费公共经济组合，本段只交准确算术差异与本机证据；B当前候选及其消费者验证归所属负责人，不以旧PR或main测试宣称已覆盖。源码冻结后按完整Git差异交独立内容审查，未审前不提交。
