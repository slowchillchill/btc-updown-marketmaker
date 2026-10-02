# 人工做市策略库

`baseline-mm` 是 A 路线的人工盈利策略业务线，维护具体交易规则、参数、必要算法记忆和策略业务分析。回测、PAPER、LIVE 的上层入口组合策略库与公共运行能力；公共运行核心不依赖具体策略。

首个设计为 BTC 五分钟 Up/Down 的 BUY-only 互补配对做市。其[候选计算规格与研究方案](docs/strategies/complementary-pair-v1.md)说明费用归属、修复利润占用、报价与数量、FAK、风险和生命周期调用需求。当前 workspace 成员提供可调用 Rust 候选库，已交付配置解析、构造与公共策略适配；未提供已校准参数、盈利证据或三环境宿主。候选计算方法不等于正式运行参数。

任务范围、批准状态和推进位置维护在 [策略合同 #185](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185)，公共历史调用边界与运行装配维护在 [公共回测 #186](https://github.com/slowchillchill/btc-updown-marketmaker/issues/186)。完整业务合同以 #185 为准，本目录不保存本地任务镜像。A 内部按[总体合同 #1](https://github.com/slowchillchill/btc-updown-marketmaker/issues/1)的串行任务交接推进。

策略按具体业务名组织，导出策略构造、参数定义及必要适配；不预建三环境消费循环或另一层 CLI 配置覆盖。参数选择由所属上层入口按“显式 CLI > 配置文件 > 策略默认值”合并，布尔开关区分未指定与显式关闭。运行元数据记录策略身份、规则版本、代码版本和完整生效参数。不同策略和对照运行使用各自实例与独立账户状态，同账户多策略隔离不属于首版能力。

公共行情、模拟时序与撮合、订单提交、NOE、Ledger、NAV 和结算由原模块维护。策略只派生其决策与诊断，不复制现金、持仓或订单账本。公共缺口由所属模块补齐，公共回测负责历史装配与通用结果，模型开发竞赛和独立留出评价仍由其所属模块负责。

入口为 `baseline_mm::ComplementaryPairV1`，业务身份 `complementary_pair`、规则版本 `candidate_v1`。宿主提供完整 `ComplementaryPairConfigV1`、封存公共规则、独立请求命名空间与 `StrategyContinuityV1`；新账户用 `new_account()`，原账户接续用原高水位／终止相关窗口／请求序号／未决利润占用，不能当新账户重置。配置解析 `parse_toml` 只读完整策略文件，不实现另一层 CLI 合并；当前没有正式风险默认值。

```rust,ignore
let config = ComplementaryPairConfigV1::parse_toml(&configuration_text)?;
let mut strategy = ComplementaryPairV1::new(config, sealed_rules, namespace, continuity)?;
let input = shared.strategy_input(&mut driver)?.expect("原公共可决策组");
let calculation = input.calculate(&mut strategy, measured_start)?;
let completed = calculation.complete(measured_finish);
let dispatch = shared.dispatch_strategy(&mut decision, &mut strategy, completed).await?;
```

宿主依获批时序推进到发送点；`dispatch_strategy` 把实际整批结果通知原策略实例，策略仅对真实已发或结果未知请求保留利润占用。低层 `execute_batch_reserved` 的直接消费者需自行调用同一 `StrategyV1::dispatched` 通知，不能把形成候选当已发请求。泛型诊断随冻结计算保存在内存，后来的账户／行情归约不能改变旧诊断；本库不落盘 JSON 或数据库。

[合成配置](configs/strategies/complementary_pair/synthetic-engineering.toml) 全部参数显式且无校准资格。规则、公式、公共成本界与限制见规格第 11 节；集成测试经原 DPT／SIM／NOE／Ledger 构造费用、平均成本池、预留和撤单反馈，不建策略影子账本。
