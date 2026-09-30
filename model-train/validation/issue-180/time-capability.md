# 步骤9：观察时间与发送时间

本次增量沿原公共消费、规范 SIM 输入日志及请求归约实现时间能力，不提供生产 100ms 调度器。

- 有限宿主使用 `next_group_at` 比较下一真实公共输入，与 `next_required_logical_drive`、已安排的观察和计算完成事件共同选择下一时点。`advance_before` 拒绝跨过严格更早的未处理公共输入；`observe_at` 只关闭严格更早的毫秒，不关闭观察所在毫秒，也不新增公共组或改写来源身份。
- 静默时 `frames_at_observation` 复用最后一个原 `PreparedPublicGroupV1`，携带最新实际领取反馈截止。旧 `frames` 保留原组快照，旧 `decision` 保留原逐帧请求时序。没有原材料时没有观察行。
- 模型保存不可变输入和原观察 cutoff。完成后 `send_decision_at` 取得有限发送能力，保留原观察 cutoff；实际请求到达使用独立发送时间加原采样 outbound 延迟。预留、匹配及反馈规则不变。
- PAPER 的 `send_decision` 只取得发送边界，实际 `request` 才用原 UTC 锚和单调时钟取发送时间。幂等请求不新增发送或观察记录。

规范日志沿现有格式增加 `PUBLIC_OBSERVE_TIME` 输入；`PUBLIC_REQUEST` 增加可选 `send_time`。旧包装不写该字段，旧记录缺字段时继续按原 cutoff 时间归约；新记录读取及物化保留两种时间。含新观察输入的日志要求支持该输入的读取器，不声称旧二进制支持新日志。没有第二份日志或影子时间账本。

验证通过真实 DPT 录制和 SIM 公共消费：观察 22000、静默观察 22020、保留原 cutoff 在 22050 发送；到达等于 22050 加原延迟，关闭前缀为 22049。PAPER 取发送能力后推进受控单调时钟，真实请求使用新时刻；在 BUY 到期毫秒观察不执行 BUY，下一毫秒才执行。含旧缺省时间及新独立发送时间的规范日志由原录制读取器恢复公共材料，逐输入调用生产归约与投影，发送/到达结果与原运行相同。

公共消费回归首轮发现旧 `frames` 自动刷新反馈截止的兼容回归，已恢复旧快照语义；32 条受影响公共消费测试通过。固定工作区命令通过，具名 UTC 测试 `four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second` 明确跳过。`sim-system` 静态检查通过，既有告警保留。步骤10的真实模型与宿主并行、100ms 时钟消费者验证另行交付。
