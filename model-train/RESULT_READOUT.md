# 既有两臂结果只读读出

`model_result_readout` 读取 #361 指定原包的 `future-execution`。它不构造模型、SIM 会话或账户写入器。使用已编译入口：

```bash
model_result_readout <future-execution目录> <新的输出目录>
```

输出目录必须独立，避免覆盖旧结果。完整原包读取失败时保留部分输出和错误，不自动重跑。原行情、模型及验证/留出数据不属于此入口的输入。

`accounts.json` 含四个冻结账户的精确 K/J、原账户记忆、费用/返佣与义务分项，以及四个训练账户的原 `unit-objectives.json` J 对照。K/J 调用 `learning::confirmed_k/finite_j`，沿用原错误。K 成功不证明必要负成本已知。J 含未决本金代理，不能称已实现损益。Unavailable、None、空集合及缺字段分别保留。

四份 `readout-*.json` / `learn-*.json` 按原包、阶段、组、账户、run 与 context 隔离关联请求。请求来源是原 `report.learning_result.repair_requests`；新输出将其投影到 `special.repair_requests`。保留排程回执和完整 request；原 decisions 保留每项 intent 和 ordinal。SIM 请求使用 owner 的 `File::open`、原日志解码器和顺序读取，输出原请求、输入序号及发送/截止坐标。该接口只投影日志中的 Request/PublicRequest，其他日志种类仅计入覆盖统计，不解释其业务含义；它不授予执行或消费能力。SIM 完整帧、未完成尾部、封口及完整字节偏移按原读取器报告。

NOE 使用 `visit_durable_noe_records_v2`，按已发布批前缀顺序读取；同段逻辑批复用一个 `SequentialReader` 和当前 Arrow 页。统计中的 `segment_opens/pages_read` 来自实际读取器。逻辑引用仍指向原 manifest 所代表的提交容器，不用物理段路径冒充。关联只使用正式 request、订单、成交身份，不用字符串包含关系；同一请求的多腿/订单/成交保持多条证据。待关联订单/成交仅在单个 context 的本次读取内暂存，不建立持久或跨 context 索引。

匹配到的成交通过已有 `ledger::visit_trade_confirmation_changes_v1` 读取已提交 Ledger 变化。没有匹配成交时，该接口不扫描 Ledger。它不打开恢复写入器，不清理未封存尾部。已提交变化保留原 NoeConsumed 引用、`recorded_visibility`、Ledger entry 序号与相对原报告序号的关系。未封存尾部覆盖不足照实报告。

原账户报告早于 `stop_and_drain`，耐久前缀可晚于账户报告。输出将持久事实、原截止报告中的 directed confirmation 和已提交 Ledger 应用分开保存。持久 FinalSuccess 不证明原截止前宿主已观察或 Ledger 已应用。原空确认映射不能证明拒绝或未成交。缺记录保留具体 Unknown，不借全局拒绝数归因，不用新读出补造历史。

`coverage.json` 汇总成功和失败的 context；发生错误时必须同时检查错误与已写出的文件，不能把部分输出称为完整结果。运行证据须另存源码身份、程序 SHA-256、命令、输入规模、实际 CPU/墙钟/RSS/I/O 口径。物理读取量受文件缓存影响，不等于输入逻辑字节。

本入口不实现 #360 的完整 ActionBefore 义务投影，不改变学习公式、专项信用或原结果身份。工程通过不代表专项有效、完整学习正确或模型晋级。
