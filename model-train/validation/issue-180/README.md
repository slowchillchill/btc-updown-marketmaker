# #180 公共基线的在线容量诊断

本目录服务于 #180 步骤 3 和步骤 6 / #127，并在同次模型推进中观察 #164 F6。它不改变生产 API、业务状态、timeout 或输入预算，也不实现 100ms 方案。

`prepare.py` 从指定提交的全仓 `git archive` 建立诊断副本，在该副本复用 #174/#176/#178 的真实 NativeInputRoot、连续 hidden、ORT Actor 和测量插桩。全部工具及辅助脚本均从同一 archive 读取。`diagnostic.patch`、源码摘要和锁文件保留副本差异；生产树不加入诊断接口。

`online.rs` 用原 PublicRecordingReaderV1 的 TypedRaw 输入，在独立线程按原接收时间的单调高水位供料，经过原 25000 项 bounded_event_queue 和真实 Session/Message/Received 交接。主线程持有 PublicOnlineSessionV1 和 pending，PreparedPublicGroupV1::from_online 的实际组进入原 FinitePublicConsumptionV1。每条输入的所有派生组完成 native.consume、模型 hidden 推进及 Actor.execute 后，才执行 pending.complete。供料、交接和原 DPT/SIM 记录后台各自运行。

准备前缀沿真实在线链处理。正式输入的供料时钟等待模型图、连续 hidden、暖缓存恢复完成以及全部先前输入 Received；这个门冻结空的初始入口队列。模型/经济沿原有限回放逻辑时间推进，墙钟排队单独计量。源接收时刻、供料计划、真实入队/出队、DPT 应用/记录容量等待、模型和 Actor 开始结束、Received 均通过 input/group 位置关联。正式测量不输出每帧完整特征、账本或哈希。

正式串行顺序固定为：

1. MAIN-1x256 / ORT4 / Rayon8，组 201582 起 60 个源秒。
2. MAIN-1x256 / ORT4 / Rayon8，组 418584 到原录制自然结束。
3. MAIN-30x1024 / ORT16 / Rayon8，同一后段窗口。

后段为 1789517395414..1789517401000（约 5.586 秒），仅 T-30 后段双市场重叠。DPT recording_in_flight_inputs=128 是本次诊断条件；SIM finite_in_flight_groups=128 来自原配置；入口维持 25000。三者分别计量，不能写成共同生产默认。任何边界未饱和均如实报告，不缩容量制造背压。

`run.py` 首先扫描原录制 Input，冻结合法输入尾部和完整末端组，不用组数量推测无组输入的边界。两个窗口均在全部 DPT 输入、派生组及 Actor 完成后显式结束，并调用 SIM session.stop_and_drain；后段保留 DPT 的真实 EndOfInput 控制，不将录制结束映射为 SIM 所有未来市场的完整业务终局。窗口外不供料，窗口内尚未入队的尾部继续等待入队和排空。窗口开放后至全部 DPT、SIM 排空最多观察 20 分钟；外部诊断监视器到限保留进度并终止本次进程，不将未完成写成成功，不自动重跑。准备耗时另列。

小段验证使用真实原录制控制运行导出入口附近的少量 group/reference 和最终逻辑账本；在线准备不双跑整个历史，仅在小段对照双方实际结果。协议验证的供料不作源节奏性能测量，并在消费者中引入明确的 20ms 延迟以核验真实排队和延迟确认；其产物与正式三次分开。初始装载未完成等失败必须保留并明确标注。

F6 使用已有 registry_snapshot_ns、feedback_apply_ns、ledger_projection_ns、扫描循环计数与 measurement_resources。有限窗口的数据只说明该窗口中的规模、扫描和反馈成本，不证明长期无泄漏。TypedRaw 之后供料不覆盖真实 SDK 原始消息、字节预算或超时路径，不能将这些边界写成已验收。

工具先提交，再从精确工具提交建立最终诊断副本并构建。`run.py` 不加 `--execute` 时只冻结；冻结材料进入 PR 后，用相同参数加 `--execute` 执行预声明窗口；`--case` 仅选择仍需测量的条件，不覆盖既有证据。非观察限时的非零退出立即停止串行调度。原始输出放在本轮专用产物子目录，公开报告仅使用仓库相对路径与摘要。
