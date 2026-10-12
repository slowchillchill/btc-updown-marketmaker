# 有限历史消费

`contracts::SimCallerRoleV1::HistoricalBacktest` 使用原隐藏撮合路径，与训练和独立评价共享行情归约、请求和经济链。Maker模型由冻结规则及实际调用方选择。公开路径仅用于因果观测。其市场登记可以随着连续历史材料继续增加；原训练和评价的登记冻结规则保持原有职责。

`PublicSimExecutionV1::create_new_historical` 使用 `RunDurabilityRoleV2::FiniteHistorical`。该角色表示实际有限记录前缀，不表示全部市场或经济义务已经闭合，不能签发 `CleanRun`。普通公共消费构造仍沿原持久角色运行。

针对该准确历史身份，真实隐藏或公开源 EOF 不要求每个市场具有完整终态，也不自动推进未来义务。完整隐藏桶引用仍由 DPT 提供。已知下一桶时间或已封口来源证明的空档允许原前缀驱动在宿主明确时点闭合；同毫秒未处理的隐藏桶不会被提前关闭。`FinitePublicConsumptionV1::finish_input_at` 在给定范围末毫秒领取范围内反馈并结束公开输入，不生成市场判定。宿主负责把该时点限制在其显式范围内。

`PublicGroupDecisionV1::cutoff` 保留原冻结观察。`sent_at_utc_ms` 返回请求实际使用的发送时刻，供共享分派结果读取。延迟发送不会把原观察时刻误报为发送时间，也不会在执行时重新选择策略价格。

正常历史范围结束可以调用原 `stop_and_drain` 排空已接纳记录；它不推进业务时间，不领取未来反馈，不产生 `RunSeal`。Exit 直接返回，不调用稳定、排空或完整完成。相关公共宿主与产物含义见 [backtest](../backtest/README.md)。

## 历史 Maker 模型与过渡边界

新冻结配置的 `historical_backtest_model = "PUBLIC_TRADE_CONSERVATIVE_FIFO_V1"` 只切换
`HistoricalBacktest`。PAPER、ModelTrain及OfflineEvaluation继续使用
`L2_BOOK_DELTA_FIFO_ESTIMATE_V1`；后两者的CPU/GPU采用由所属事项协调。快照缺少新增字段时，
历史回测也保留原模型。`maker_model_version_for(caller_role)` 返回实际选择，backtest运行元数据保存该身份。
旧检查点与日志绑定原规则SHA；不能为续跑修改原文件或更换规则SHA。

历史新模型仅为存在本账户挂单的买价位保留Maker排队段。盘口净减只更新实际可执行深度，
不生成Maker成交，也不把未知撤单当作前方订单离开。新外部量排在既有账户单后。
SELL成交消费同资产的买队列；BUY成交映射到互补资产的买队列。当前正式SIM订单入口仅支持BUY。
一条规范成交事实按价格优先和FIFO消费一份有限数量，同一账户多单及互补映射不复制预算。
穿价成交也须扣除更好价位的可见外部量，不使用“穿价就全部成交”。

Maker排位与可执行盘口是不同事实。Required质量边界沿原逻辑清理实际深度，保留既有Maker排位；
重建量不会把账户提升到队头。只有账户单存续的价位保留排位；连续外部尾段合并，最后账户单移除后释放。
成交后的费用、返佣、最终性、私有反馈及SIM自动结算仍走原经济链，成交仍标为MakerInferred。

这些规则不证明真实FIFO、真实本账户成交或市场影响。未挂本账户单的更好价位使用当时可见深度；
成交先到、盘口后到时，该深度可能偏旧，导致保守地少分配量。不同规范记录缺少平台唯一身份时，
不能凭同量、邻接时间或互补价格去重。PAPER暂保留旧模型及原已知局限；其迟到成交、已确认撤单与资金
释放问题归后续#108/#109，本项不宣称解决在线因果。

旧研究材料需要显式生成新规则文件，才能开展新模型的历史复核。入口保留旧平台观测、观测时间、费率和
其他冻结规则，只替换本项Maker项目假设及当前实现身份；项目假设证据日期为本次决定日期，
不伪装成旧采集时已采用。新文件使用原不覆盖封存和历史装载入口，不访问网络。

```bash
cargo run -p sim-system --example derive_historical_maker_rules --locked --offline --   <旧规则JSON> <旧规则SHA256> <历史范围开始UTC毫秒> <新规则JSON> <当前实现身份JSON>
```

实现身份JSON沿原 `ImplementationIdentityV1`，含准确 `git_commit`、`source_identity_sha256`、
`cargo_lock_sha256`。命令输出新规则SHA及历史/PAPER各自模型身份。将新路径和SHA交给原backtest入口；
旧规则文件、旧结果和旧检查点原样保留。该命令不重置或授权研究预算。
