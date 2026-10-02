# 有限历史消费

`contracts::SimCallerRoleV1::HistoricalBacktest` 使用原隐藏撮合路径，与训练和独立评价共享行情归约、请求、费用及成交逻辑。公开路径仅用于因果观测。其市场登记可以随着连续历史材料继续增加；原训练和评价的登记冻结规则保持原有职责。

`PublicSimExecutionV1::create_new_historical` 使用 `RunDurabilityRoleV2::FiniteHistorical`。该角色表示实际有限记录前缀，不表示全部市场或经济义务已经闭合，不能签发 `CleanRun`。普通公共消费构造仍沿原持久角色运行。

针对该准确历史身份，真实隐藏或公开源 EOF 不要求每个市场具有完整终态，也不自动推进未来义务。完整隐藏桶引用仍由 DPT 提供。已知下一桶时间或已封口来源证明的空档允许原前缀驱动在宿主明确时点闭合；同毫秒未处理的隐藏桶不会被提前关闭。`FinitePublicConsumptionV1::finish_input_at` 在给定范围末毫秒领取范围内反馈并结束公开输入，不生成市场判定。宿主负责把该时点限制在其显式范围内。

`PublicGroupDecisionV1::cutoff` 保留原冻结观察。`sent_at_utc_ms` 返回请求实际使用的发送时刻，供共享分派结果读取。延迟发送不会把原观察时刻误报为发送时间，也不会在执行时重新选择策略价格。

正常历史范围结束可以调用原 `stop_and_drain` 排空已接纳记录；它不推进业务时间，不领取未来反馈，不产生 `RunSeal`。Exit 直接返回，不调用稳定、排空或完整完成。相关公共宿主与产物含义见 [backtest](../backtest/README.md)。
