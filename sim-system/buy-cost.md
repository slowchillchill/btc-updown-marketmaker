# SIM 买入成本与公开场景估费

`SealedProcessRuleSnapshotV3::market_buy_scenario_fee_estimate_v1(fills)` 接受同一场景的 `(微份数量, 价格)` 片段。它不读取盘口、账户、隐藏行情或运行时，也不执行撮合。策略负责选择公开卖档，实际执行仍由 SIM 决定。

实现直接复用 [matching.rs](src/matching.rs) 的 `allocate_taker_fee_micro_units_by_key`：每段按封存费表计算 `q*r*[p*(1-p)]^e`，先累加精确贡献，再按原五位金额规则对总额舍入一次；各段费用沿原余数分配核归属。返回 `MarketScenarioFeeEstimateV1` 的 `fee_1e5_units` 与 `segment_fee_1e5_units`，单位均为 `10^-5 pUSD`。逐档成本线使用各段归属，不能用便宜档的均价掩盖已知贵档。

例如4份在0.3、6份在0.4、费率0.07和指数1时，总费用为0.1596。两个各143微份、价格0.5的片段总费用为0.000005005，舍入一次得到0.00001；分别舍入会错误得到0。此纯接口沿原撮合费用核的数量／金额表示和错误，不增加交易资格。

场景估费不是最大费用、实际成交或资金保证。90 pUSD、目标100份、决策价格0.9的估费为0.63；到达价格0.1时可以实际成交900份、费用5.67。SIM仍以订单接纳本金预算加本次实际匹配产生的即时费用检查现金，FAK未使用本金随后释放。预留不足、平台拒绝、部分成交和后续FAILED沿原公共链处理。

`market_buy_fee_estimate_v1` 保留单价情景用途，`limit_buy_cost_v1` 保留原 LIMIT 最大即时成本及 post_only 语义。实际费用、匹配、取整与失败补偿不因增加纯场景入口而改变。

依 [D-0360](https://github.com/slowchillchill/btc-updown-marketmaker/issues/315#issuecomment-6217387315) 和 [#318](https://github.com/slowchillchill/btc-updown-marketmaker/issues/318)，当前代码删除 `SimMarketBuyBoundsV1`／`sim_market_buy_bounds_v1` 及仅服务全域界的证明测试。旧全域界与 `candidate_v1` 结果保留在原提交和历史材料中，不是本次性能优化引入的回归。新策略不使用当前最低卖价、tick或SDK份额构造去保证未来成交量。
