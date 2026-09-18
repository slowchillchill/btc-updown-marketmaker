# 共享执行组合

本库连接既有公开组决策入口、Intent Bridge、NOE 实际领取队列与唯一 Ledger。合同见任务 #104、共享规格 #101，以及模型生产合同 #5 和 Intent Bridge 合同 #6；不创建模型宿主、账户调度或新的执行策略。

调用方先创建原 `PublicSimExecutionV1` 和 V3 observation，在新 Ledger 首次应用前调用 `bind_ledger_recording_v2`，再调用 `SharedExecutionV1::from_opening`。开户来自原 `EconomicFact`，投影可见时间为 `None`。每组由 `action_before` 排空已放行的原记录并应用全部外生事实，然后重新取得 `decision.frames`；返回值冻结该组必要资金标量与原记录/意图引用。缺经济直接前驱时保留原引用、返回未完成，不产出带洞模型视图。

`execute` 接收宿主已经决定的原 `ModelIntentV1`，同一请求只冻结一次。`None` 表示 NoTrade。五类动作经原 sealed Bridge 构造后调用一次 `decision.request`，实际回执只登记提交，不充当平台接受或经济事实。订单基点、形成序、最新来源生命周期及实际交付顺序各取其原证据；初始 Taker 不重复增加 Accepted 的 matched，后续 Maker 按 trade 身份计一次。GTC/GTD 承诺包含 active 和未终局成交；FOK/FAK 在完整初始成交集合全部终局前保留原 spend/worst 目标。

目标数量沿 #5 的 `q=n/100、P=q*p` 有效生产域从原意图现算，使用整数系数和 scale 保持精确，不缓存 q、不改变 `ExactDecimal`、不增加运行时拒绝或舍入门。旧 Bridge 的宽字段映射不因此被收窄；完整 SDK 数值域证明仍属于其原任务。

`NoeConsumedRecordRefV1` 在正式接线中由真实来源适配器交付。业务领取位置包含 context、source watermark 与原 ordinal；同一反馈的不同归一化记录共享 delivery。普通 `NoeDurableRecordViewV1` 和序列化引用不能直接传入应用或登记入口。公开生产者接口属于受信进程内宿主协作，宿主负责真实规范化批及驱动时点；手工重建批并自填时间仍可间接取得消费引用，这是 #101 已明确接受的风险，私有字段和直接 compile-fail 不证明来源不可伪造。正式耐久读取与 Ledger 恢复不重建输入以重新应用。开户经真实归约及原 NOE 规范化产生即为 Initialization，不等待后台交接、耐久、激活或公共组。热链不编码身份、不扫描磁盘、不等耐久。

Ledger 继续使用同一应用核心，V2 材料包含 Genesis、Transaction、ReservationDecision，交给原 SIM 日志后台 worker。来源交易需要真实 NOE 稳定映射时，后台先同步原 SIM 日志，再沿原 NOE stabilize 发布，按原顺序物化 Ledger；本地事件不虚构 NOE 依赖，也不越过未物化前项。没有第二线程、编号器或满队列策略。完成时原 NOE 稳定、Ledger 段封口后才允许原 SIM seal；实际 I/O 错误原样使本轮未完成。V1 同步记录和恢复保持兼容，V2 恢复还原原应用效果、visibility 和幂等位置，不重放业务领取。

宿主计划停止时，先结束已开始的同步调用并停止新组、请求及 Ledger 追加，然后消耗 `PublicSimExecutionV1` 或 `FinitePublicConsumptionV1` 调用 `stop_and_drain`。它只沿同一后台移交 pending 组、同步 SIM、稳定 NOE、接收最后一批 Ledger 材料并封口 Ledger 段，等待原后台退出；不调用下一组、CloseTime、EndOfInput 或 `complete`，不撤单、清仓或改变已有订单/持仓事实。成功不生成 `RunSeal`，不返回完整冷产物或可恢复账户。原 `complete` 仍要求真实业务闭合。来源摘要 `market_event_source_identity_sha256` 对离线材料保留真实值，在线创建时传 `None`；新读取器兼容旧字符串字段，其他身份摘要保持原合同。

逐请求资金取原预留状态，逐订单资金归属取实际 Ledger 分录及来源关联。迟到 trade 关联只补原分录归属，不重复经济入账；未足够归属的行返回 `Unavailable`，不按腿均分。原 Ledger 生命周期投影的计算语义保持不变：实际接收顺序若为 FinalSuccess 后 PendingApplied，其原 pending 计数仍随输入更新；共享订单/承诺则按源序保留终局。二者不能互相替代。

连续组合测试见 `tests/continuous.rs`，同一源码的真实 typed_raw 准备、DPT 记录、模拟器入口生成材料，覆盖成交、终态、撤单、承诺、资金归属、组冻结和后台失败。`ledger/tests/memory_v2.rs` 覆盖原返佣经济事实下的粘滞拒绝及恢复；`sim-system/src/shared_ledger_tests.rs` 覆盖实际原队列、同 delivery 多 ordinal 和原 worker 耐久。编译反例仅验证只读耐久材料不能直接传参。验证不声称长运行资源、在线平台全域或模型宿主已经交付。
