# 步骤9—10：独立观察、发送时间与真实模型短验证

本轮沿原规范 SIM 日志、公共消费与执行链增加受信观察时间及独立发送时间；短消费者证明模型计算与宿主事实推进可以分开。生产能力提交为 `7cdb5e971571817239e95af50b5e97bb4def53e9`，最终诊断工具为 `4bebac10972b904e9a547fddafc030a33ffe0b8e`。结果待独立审查，尚未形成新的 main 基线或分叉两方案。

## 实现与兼容

`PUBLIC_OBSERVE_TIME` 保存原公共来源与组位置，观察只关闭严格更早的毫秒；当前毫秒义务仍按原公共、到期与 BUY 顺序处理。静默观察复用已知公共材料并读取最新已应用账户。旧 `frames` 保持组缓存截止，新 `frames_at_observation` 才读取观察截止；旧逐帧 `decision` 的时间语义不变。

模型线程只拥有不可变输入及串行三运行时/hidden；宿主领取公共事实、推进 SIM、应用唯一 Ledger。完成后保留原观察 cutoff，再取得发送能力，执行原整动作预留、提交记录、Registry、Bridge 和本地回执。没有重新选择、改价、缩量或新增 BUY 禁令。请求实际到达为独立 send 加原 outbound 延迟。

同一规范 `PUBLIC_REQUEST` 增加可选 `send_time`；旧包装不写字段，旧记录缺字段继续按 cutoff 归约。代表测试用原录制读取器还原公共材料，对完整规范输入调用生产归约和投影，验证旧/新发送与到达相同；这不声称通用 `recover_paper` 新增公共材料恢复能力。含新观察输入的日志需要新版读取器。PAPER 复用原 UTC 锚和单调时钟，在实际请求点取时；受控本地测试覆盖取得发送能力后时钟变化及请求幂等，无网络长期运行。

## 冻结与真实连续模型

完整仓库按精确工具提交 archive，诊断补丁不改业务规则。冻结与紧凑摘要见 [time-results.json](evidence/time-results.json)。原始材料根为 `target/issue-180-public-baseline-validation/time-driver-20261001/`，最终路径是 `final-controls-freeze.json`、`final-controls-diagnostic.patch`、`final-controls-build.log` 及 `final-controls-evidence/{command.json,run.json,trace.jsonl,time-sim.jsonl,consumed-feedback.jsonl}`。摘要保存源码、archive、补丁、二进制、Python 扩展、两个锁文件、38 个原图文件、ORT 库及工具摘要。

MAIN1x256、原权重/规则、seed、账户现金 2000；`continuous-validation` 测试调试构建，ORT 1 线程、Rayon 8。真实 TypedRaw 经原 DPT、SIM、Shared、Native 编码，Torch、Python ORT、Rust ORT 连续推进；原三端阈值 `1e-5 + 1e-4 * abs(reference)` 未放宽。7 次观察形成 8 行状态；两次初始化、一次根 reset、一次退役；T−30 仅热身，开市承接已有 hidden。Actor 使用已计算结果，没有再运行 GRU。

周期窗口只接纳相对首次开市的 0/100/200ms 三个 tick，到 200ms 停止新增观察，后续仅排空已接纳计算到 600ms。T−30、下一市场 T−30、开市和开市后 1000ms 是另外的有限身份观察点，间隔不代表遗漏仍开放的周期 tick。经济计算延迟事先声明 50/250ms；实际墙钟单独留存，不将经济延迟说成实际 CPU 过载。

| 原观察（ms） | 计算开始 | 冻结经济完成 | 排队等待 | 实际计算墙钟（ns） | 自主结果 |
| --- | --- | --- | --- | --- | --- |
| 0 | 0 | 50 | 0 | 10084179 | NoTrade |
| 100 | 100 | 350 | 0 | 10239000 | NoTrade |
| 200 | 350 | 600 | 150 | 9388771 | NoTrade |

0ms 输入冻结后，20ms 的真实公共组在模型仍未完成时已领取并应用，冻结输入保持不变；100/200ms 没有新行情，原来源身份/时间不变而 hidden 仍按顺序推进。场景直接断言完成通道尚为空及真实 20ms receipt，未靠运行速度重试获得结果。

## 执行、反馈与停止边界

六个可交易自主决策中四次 NoTrade，两次在下一市场开市及开市后 1000ms 选择原 `SubmitPairedBuy`（两腿数量 2.18、价格 0.01/0.007）。另从真实支持集受控选择一次双腿 GTC BUY 和一次撤单，明确属于执行覆盖补充。

- 受控 BUY 保留 0ms cutoff，50ms send，59ms arrival；原 Bridge 调度两腿。平台提交结果两腿均为 `Accepted`，初始 matched=0、open=5，59ms 响应、68ms 领取。发送后、平台响应前已能看到两项有效承诺。
- 受控撤单保留 100ms cutoff，350ms send、361ms arrival，原 Bridge 调度一项；实际 `CancellationRequestCompleted` 为 canceled_count=1，370ms 领取。`completed` 是撤单完成字段，不能用 Submission 的 None 推断平台结果。
- 开市自主请求 `time-identity-5` 为 300000ms send、300081ms arrival；已实际领取两项 `RejectedNoEffect`，300099ms 领取。记录只有该规范枚举，拒绝原因未捕获，不按数量推断。
- 末次自主请求 `time-identity-6` 为 301000ms send、301011ms arrival，停止时尚未到期。保持平台结果未知，本地 after_send_unknown_count=2、principal=0.03706、before_send_count=0；不推进 301011 的未来事件来让状态清零。
- 650ms 现金事实经原宿主合法推进并在 700ms 发送前由唯一 Ledger 应用，实际与 maker 可用均为 0.045、预留均为 0.005，平台前缀 4。200ms 冻结的原双腿意图整体预留返回两项 `InsufficientGlobalAvailableFunds`，输入/意图未改；规范日志没有 `time-cash-rejected` 请求，也没有 Bridge 提交副作用。

实际已领取 Submission 的原 source_reference 与枚举仅在诊断副本的原领取边界捕获，保存在 `consumed-feedback.jsonl`。停止排空完成已接受的工作和记录，没有自动撤单、伪造业务终局或 RunSeal；下一必需逻辑点仍为 `PublicVisibility` 86401011。最终测试 exit=0，进程墙钟 2368191485ns；不把停止成功等同于所有未来订单/反馈闭合。

## 最终生产头的旧逐帧对照与检查

最终生产代码仍为 7cdb。针对步骤9曾影响旧 `frames/decision` 的风险，使用精确工具 `3e7ab2d238ddd20421b00270a123bde83386460d` 的原 `consume`/`execute` 短消费者，对已保存精确基线 `178713022eba217ce77ca1782bdaa8e302690b66` 的轨迹比较。`legacy-final-comparison.json` 通过：72 个组、79 行状态、36 次 Actor/选择、37 次执行、1 次受控动作、190 条规范输入；196421 个浮点值和 13005 个精确值一致，最大绝对差 0。原发送、关闭、反馈与完整输入/支持/全层 hidden 均沿原阈值比较。后续提交只增诊断字段/报告，不需再次运行旧路径。

检查原始日志保留在上述材料根 `checks/`：

- 固定 `cargo test --workspace --lib --bins --tests --examples --locked --offline -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second` 通过；具名 UTC 夹具测试明确**跳过**。不汇总含子进程重复计数。
- 受影响公共消费测试 32 项通过；真实独立时间/旧新完整规范归约与 PAPER 取时测试通过。首轮旧 frames 自动刷新截止的失败保留，恢复旧快照后复验通过。
- 独立 native 串行测试 47 项通过、2 项既有 ignored；rounding 3 项通过。
- workspace/native 格式检查、`sim-system` 和独立 native 的 all-targets clippy、诊断编译及 Python 语法检查通过；既有告警保留。最终诊断编译 31.27s，执行 exit=0。

开发阶段的 20ms 夹具遗漏及现金 NOE 未先合法应用的首败保留为诊断夹具错误；修正真实入口后验证，不归为生产缺陷。无新增业务决策缺口。此次没有重跑容量、长前史或旧矩阵；没有交付正式 100ms 特征、训练、生产宿主或网络 PAPER 验收。步骤11组合审查、新 main 基线及两个方案工作树仍由后续编排完成。
