# 公共统计驱动的 C0/C1 研究候选

本入口实现 [#350](https://github.com/slowchillchill/btc-updown-marketmaker/issues/350) 签发的受控研究候选。正式版本为 `candidate_v4_public_c0_c1`。它们不代表已采纳经营规则，也不证明盈利或真实历史可用性。旧 `candidate_v3`、A1/R3 继续沿原策略构造和原采样路径。

## 配置和启动

完整参数见 [public-c0.json](../../configs/strategies/complementary_pair/public-c0.json) 与 [public-c1.json](../../configs/strategies/complementary_pair/public-c1.json)。两份文件的 `parameters` 完全相同，包含签发的 42 个原参数。唯一差别是 `blb_decrease_veto`。共同公共统计窗口见 [public-statistics.json](../../configs/strategies/complementary_pair/public-statistics.json)，必须绑定到每个宿主 `run.public_statistics`，不能只传策略参数。

复用 `backtest-cli --mode day --config <完整日配置> --progress-interval-seconds <秒>`。每个 `days[].candidates[]` 的 `strategy` 放上述完整对象，`run.strategy.strategy_rule_version` 放新版本；`run.public_statistics` 放共同窗口，其他输入、规则、会话、账户、产物和身份继续使用 [backtest 正式合同](../../../backtest/README.md)。两个候选具有各自的账户和请求命名空间，并复用同一次公共日生产、公共统计流及冻结前缀。不同账户游标不得读取生产者当前可变状态。

签发时序为 `event_driven`，交付／计算开始／计算耗时／派发延迟为 `0/0/15/0 ms`。配置必须显式记录这些值。统计时间取冻结行情截止；账户和计算时钟晚 15ms 不会把该时段新行情加入本帧。CLI 的原字段覆盖语法为 `--strategy.quote.base_pair_shares <值>` 等；对新版本写入 `parameters` 内。`--blb-decrease-veto true|false` 只覆盖新开关。版本声明与参数类型不符会报错，不把旧版本悄悄转为新版本。

`ComplementaryPairV1::new_public(ComplementaryPairConfigV4, rules, namespace, StrategyContinuityV2::new_public_account())` 是库入口。`StrategyMarketReadV1::statistics()` 借用同一冻结统计；owned 与 borrowed 读取都进入同一 `PairMarketRead`。

## 公共字段映射

| 策略输入 | 实际公共字段与缺失行为 |
| --- | --- |
| 当前价格 | F0 的合格 Chainlink spot 优先；缺失时取结算规则对应 TWAP。锚和临时参考沿正式 F0 |
| 波动 | F4 `spot_variance` 中精确匹配 `volatility_window_ms=300000` 的 sigma；缺失沿原 `initial_sigma`，真零保留 |
| 结算积分 | 最后结算窗口内，F5 `spot_integrals` 精确匹配结算窗口，且左支撑覆盖窗口开始、读到冻结截止。缺失为 `MissingWindowHistory`。本次只签发 60000ms，不补造 30000ms 窗口 |
| 深度项 | F6 `depth.imbalance × binance_pressure_weight_usd` |
| 最近成交项 | F7 `latest_trade_pressure × binance_trade_pressure_weight_usd` |
| 基差项 | F8 与实际选择的 spot／TWAP30／TWAP60 相应通道的 `innovation_usd_per_btc_numeric_proxy × binance_innovation_weight` |

缺失融合分量沿原策略不贡献；`public_inputs` 保留 `None`，不把公共未知写成事实零。新候选不维护私有采样历史或基差基线，不声称数值等价于旧 R3。

## 新初始配对条件

C0 要求两腿真正簿买卖价均已物化，价格和对应量为正，且每腿 `bid < ask`；直接 BBA 不能替代簿。F3 200ms 的 `change_usdt_per_btc` 必须已知且精确为零。

C1 先满足 C0，然后只比较 `t-200ms` 与 `t` 两端。两端必须属于同市场、公共输入时钟、定义和参数；两端 Binance F1 买卖价和量合格且 `bid <= ask`（锁价可比）；同正式 K、锚值及 `official_anchor.direct_anchors` 事实身份，同已知 M、同非 `at_anchor` 的 bid/ask 侧，且 Q 均已物化。只有 `Q(t)-Q(t-200ms)<0` 才否决。不可比回退 C0，保留具体原因，不把缺失、未物化或未知当零。途中越过 K 又恢复不会另加否决。

判断只放在原 `new_pair` 最后创建新初始配对请求的位置。未到该分支时 `public_entry` 缺省，表示未评估。否决后仍执行原无效旧单撤单尾部；维护、撤换、修复、FAK 及既有经济义务不受此新门控制。

## F2 历史与紧凑诊断

公共配置显式增加 `blb_lookback_windows_ms:[200]`。空集合沿旧行为，不物化端点。端点在真实公共 `consume` 后建立，保存必要 F2 标量、同端 F1、正式锚事实、市场／配置／输入身份、组和应用时间。记录状态、锚和 F1 的变化，即使 Q 未变；不把每个组号当成新锚。只保留回看范围及左端最后 asof 支撑。持久容器和 Arc 保护已冻结前缀；当前流裁剪不破坏慢消费者。市场结束后不再为本开仓消费者展开端点，结束外查询缺失不能解释为零。

同毫秒的新前缀可以替换当前末端；较早冻结仍保留原末端。迟到 K 不回填旧前缀。纯查询使用该前缀可见的 asof 点，不制造新行情或来源时间。这里没有 `read(past)` 全历史查询能力。

现有 `run.research_evidence_path` 写出真实决定记录。新增 `public_inputs` 只保存所选公共融合标量；`pair_entry.public_entry` 只保存 C0 输入、C1 两端标量／引用、比较原因、差量和实际新请求 id。没有逐决定复制完整 F0—F9、盘口、BLB 档位或全部公共来源数组。配置与输入身份在运行材料绑定；输出开关和决定频率不能驱动公共历史。

## 有限验证与运行限制

`cargo test -p baseline-mm --test complementary_pair public_v4 --locked --offline` 覆盖真实 Core 归约到共享公共统计、冻结输入、候选请求及诊断，包含 16 个规则例和额外不可比边界、同毫秒／迟到锚／左支撑／静默查询／慢游标、F4—F8 映射、owned/borrowed 同帧、否决仍撤旧单。

`cargo run -p backtest --example public_candidate_fixture --locked --offline -- <新目录> <有限点数>` 只生成合成 Parquet 与完整双候选 `day.json`。随后用正式 CLI 自然结束，可核对准备读取、公共归约／统计／200ms 历史、两个独立游标、必要输出和结束处理。生成器内市场和时间只属于有限工程夹具，不是研究默认。正式成本、构建 SHA、线程、输入与产物量以交回包实测记录为准，不外推真实八小时成本。

日入口没有本次新增的检查点、自动续跑或信号排空协议。进度沿正式 CLI 可控输出。人工终止进程只保留已落盘部分；完整 JSON 行也不保证报告、请求批次、账户经济尾部或自然结束完整，不能据此判断可继续研究或复用账户。中断后应另目录从头运行。真正历史、未来标签、效应和阈值分析仍需研究组按准确包另行签发。
