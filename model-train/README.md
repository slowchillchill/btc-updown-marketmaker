# 模型训练（规划）

新 B 路线的规划入口为 [#181](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181)。
当前目录仅保留职责说明，尚无新模型实现或训练入口。

新 B 从最新 `main` 的 A/公共能力重新设计。模型开发比较复用 `backtest`，
行情、模拟、执行反馈和账本能力复用所属公共模块。
独立留出评价仍归规划中的 `offline-evaluation`。

旧模型代码、独立 Rust 工作区、Python 依赖锁、测试和验证历史已从当前源码树移除。
新设计不默认继承旧架构、检查点、模型维度、奖励与风险数值、预算或验收门槛。
后续实现、数据处理和训练按 #181 明确的任务与授权开展。

历史内容可从 Git 历史追溯；清理前版本为
[`c1830bd`](https://github.com/slowchillchill/btc-updown-marketmaker/tree/c1830bd064b3c735b36139e6a3e0fd4feff43620/model-train)。
