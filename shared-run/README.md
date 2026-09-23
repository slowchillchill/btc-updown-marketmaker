# 共享执行组合

本库连接既有公开组决策入口、Intent Bridge、NOE 实际领取队列与唯一 Ledger。合同见任务 #104、共享规格 #101，以及模型生产合同 #5 和 Intent Bridge 合同 #6；不创建模型宿主、账户调度或新的执行策略。

调用方先创建原 `PublicSimExecutionV1` 和 V3 observation，在新 Ledger 首次应用前调用 `bind_ledger_recording_v2`，再调用 `SharedExecutionV1::from_opening`。开户来自原 `EconomicFact`，投影可见时间为 `None`。每组由 `action_before` 排空已放行的原记录并应用全部外生事实，然后重新取得 `decision.frames`；返回值冻结该组必要资金标量与原记录/意图引用。缺经济直接前驱时保留原引用等待，合法公共帧仍继续消费，冻结读取携带尚未应用的记录数。PAPER 共用应用事实、重新取得 decision 和观察市场的准备步骤，但不构造无人读取的完整账户快照。

`execute` 接收宿主已经决定的原 `ModelIntentV1`，同一请求只冻结一次。`None` 表示 NoTrade。五类动作经原 sealed Bridge 构造后调用一次 `decision.request`，实际回执只登记提交，不充当平台接受或经济事实。订单基点、形成序、最新来源生命周期及实际交付顺序各取其原证据；初始 Taker 不重复增加 Accepted 的 matched，后续 Maker 按 trade 身份计一次。GTC/GTD 承诺包含 active 和未终局成交；FOK/FAK 在完整初始成交集合全部终局前保留原 spend/worst 目标。

目标数量沿 #5 的 `q=n/100、P=q*p` 有效生产域从原意图现算，使用整数系数和 scale 保持精确，不缓存 q、不改变 `ExactDecimal`、不增加运行时拒绝或舍入门。旧 Bridge 的宽字段映射不因此被收窄；完整 SDK 数值域证明仍属于其原任务。

`NoeConsumedRecordRefV1` 在正式接线中由真实来源适配器交付。业务领取位置包含 context、source watermark 与原 ordinal；同一反馈的不同归一化记录共享 delivery。普通 `NoeDurableRecordViewV1` 和序列化引用不能直接传入应用或登记入口。公开生产者接口属于受信进程内宿主协作，宿主负责真实规范化批及驱动时点；手工重建批并自填时间仍可间接取得消费引用，这是 #101 已明确接受的风险，私有字段和直接 compile-fail 不证明来源不可伪造。正式耐久读取与 Ledger 恢复不重建输入以重新应用。开户经真实归约及原 NOE 规范化产生即为 Initialization，不等待后台交接、耐久、激活或公共组。普通内存业务处理不编码身份、不扫描磁盘、不等耐久；明确的市场稳定边界沿下文统一确认。

Ledger 继续使用同一应用核心，V2 材料包含 Genesis、Transaction、ReservationDecision，交给原 SIM 日志后台 worker。来源交易需要真实 NOE 稳定映射时，后台先同步原 SIM 日志，再沿原 NOE stabilize 发布，按原顺序物化 Ledger；本地事件不虚构 NOE 依赖，也不越过未物化前项。没有第二线程、编号器或满队列策略。完成时原 NOE 稳定、Ledger 段封口后才允许原 SIM seal；实际 I/O 错误原样使本轮未完成。V1 同步记录和 V2 后台记录共用当前应用效果表示；V2 恢复还原原应用效果、visibility 和幂等位置，不重放业务领取。恢复读取按原 NOE context 的业务版本分别解析 V3／V4 来源，不新增持久字段，也不把耐久记录转换为可应用领取引用。一次 Ledger 打开仅保留一个批次的位置材料，以容器位置为键；同键复用 boundary 和记录数，每个 ordinal 仍检查范围。换键时，Parquet 从清单和 footer 取得版本、边界和数量，旧 JSON 沿原解析入口读取；不保留批次正文或全历史。后续同键命中不重读文件，独立历史读取按实际用途解码正文；这些入口不再重算 NOE 正文摘要。

宿主计划停止时，先结束已开始的同步调用并停止新组、请求及 Ledger 追加，然后消耗 `PublicSimExecutionV1`、`FinitePublicConsumptionV1` 或 `ContinuousPublicConsumptionV1` 调用 `stop_and_drain`。它只沿同一后台移交 pending 组、同步 SIM、稳定 NOE、接收最后一批 Ledger 材料并封口 Ledger 段，等待原后台退出；不调用下一组、CloseTime、EndOfInput 或 `complete`，不撤单、清仓或改变已有订单/持仓事实。成功不生成 `RunSeal`，不返回完整冷产物或可恢复账户。原 `complete` 仍要求真实业务闭合。来源摘要 `market_event_source_identity_sha256` 对离线材料保留真实值，在线创建时传 `None`；新读取器兼容旧字符串字段，其他身份摘要保持原合同。

新内存运行的物理格式为 NOE `noe_arrow_parquet_v1` 和 Ledger `ledger_arrow_parquet_v1`，与原 NOE 业务版本、`ledger_transaction_segment_v2` 分开。Arrow 的原序号、变体与二进制正文三列逐记录编码；正文明确沿 `serde_json_v1` 保存原变体成员。NOE 一个逻辑批对应一个 Parquet 容器，空批也保留来源水位；Ledger 保留 Genesis、Transaction、ReservationDecision 的原 `durable_record_seq`。批/段边界为单份元数据，未将整个 JSON 文件塞入单元格，也不双写暂存 JSON 或活动 JSONL。SIM 原始 JSONL 和 NOE context 保持原责任，账户投影继续由 Ledger 查询接口按需生成。

唯一后台写出完整 Parquet footer 后同步文件、禁止覆盖地原子发布、同步目录，再发布并同步不可变清单；只有清单稳定后 NOE 才提供原耐久引用，Ledger 封存成功后才确认稳定。清单逐批/段追加，不在后台常驻或重写整次运行的历史清单。NOE 容器引用只携带实际位置，结果引用再携带 ordinal；Ledger 已应用引用按位置和 ordinal 去重，来源水位仍用于业务推进。NOE 暂存写入后只保存未稳定批次的水位和记录数，稳定时完成实际发布并取得当时时刻，不重读正文或常驻整批记录。位置恢复从 Parquet 清单与 footer 读取真实边界和行数，V3/V4 各自分派；独立历史正文读取仍解析完整记录。读取器兼容旧 JSON／JSONL；旧根合法续写仍用旧格式，不迁移或混写。新 NOE context 的物理编码使旧读取器明确拒绝；旧 Ledger 读取器无法取得新格式 Genesis，不能作为空账户恢复成功。实际读取时无法解析的已提交文件报错，未提交尾部不构成已稳定事实。

每个已登记市场的 `end_at` 首次被原角色驱动的已关闭时间前缀覆盖时，自动沿同一 Stabilize 稳定此前已接纳的 SIM／NOE 和已产生的 Ledger。训练/离线取隐藏前缀，PAPER 取公开前缀；同毫秒公共组和请求处理后才真正关闭，单个公共组不是封口。零订单市场也触发，迟登记的过去市场在接纳后的首个合法关闭前缀处理；成功后释放待稳定截止。该边界不提前交付 NOE、不制造经济前驱、不结束账户或结算，迟到事实进入后续原记录流。`real_closed_market_prefix_stabilizes_zero_activity_before_finish` 用真实双市场及原关闭驱动验证 Finish 前自动稳定，后续公共帧才产生的 Ledger 不倒灌。

逐请求资金取原预留状态，逐订单资金归属取实际 Ledger 分录及来源关联。迟到 trade 关联只补原分录归属，不重复经济入账；未足够归属的行返回 `Unavailable`，不按腿均分。原 Ledger 生命周期投影的计算语义保持不变：实际接收顺序若为 FinalSuccess 后 PendingApplied，其原 pending 计数仍随输入更新；共享订单/承诺则按源序保留终局。二者不能互相替代。

连续组合测试见 `tests/continuous.rs`，同一源码的真实 typed_raw 准备、DPT 记录、模拟器入口生成材料，覆盖成交、终态、撤单、承诺、资金归属、组冻结和后台失败。`ledger/tests/memory_v2.rs` 覆盖原返佣经济事实下的粘滞拒绝及恢复；`sim-system/src/shared_ledger_tests.rs` 覆盖实际原队列、同 delivery 多 ordinal 和原 worker 耐久。编译反例仅验证只读耐久材料不能直接传参。验证不声称长运行资源、在线平台全域或模型宿主已经交付。

空初始市场场景沿原场景格式创建账户，不需要占位市场。真实开户仍立即产生 Initialization；没有市场观测的参考组按原序推进消费，`action_before` 返回 `None`。真实市场身份到达后，宿主在其首组前调用两种驱动共用的 `enroll_market`，它沿原执行器登记，并根据返回的真实登记身份及原规则快照补入已有规则映射；重复请求不会使用不同候选身份改写映射。登记模式限制、失败和训练/离线业务开始后的冻结不变。空迭代器不能替代正常输入封口；计划停止只排空原前缀。旧二进制可能拒绝新空场景，本组件接线不代表完整持续 PAPER 宿主已经交付。

任务 #109 的持续入口 `ContinuousPublicConsumptionV1` 与有限入口共用 `PublicConsumptionStateV1`，包含原执行器、规则映射、组回执和决策状态。`PublicGroupDecisionV1` 不再携带有限迭代器类型；Shared 与 Bridge 借用相同组决策，消费、NOE 放行、Ledger/registry 应用、重新获取 cutoff、冻结读取及请求顺序保持不变。

持续 PAPER 入口创建时只取一次 UTC 锚与单调起点，`apply_group(Arc<PreparedPublicGroupV1>)` 在实际串行应用点分配执行毫秒，不接收外部每组时间。来源和接收时间原样保留；处理或同步记录容量等待可以把同来源毫秒的组分到不同执行毫秒，这是在线边界，不证明训练更保守。应用前先处理更早的原义务，容量恢复或前序处理后重新取时钟。相同执行毫秒的 BUY/原 pending mutation 等待时钟严格越过后关闭；GTD、终局和返佣仍经原归约在同刻行情之前处理。

宿主可按 `next_wake_after()` 返回的相对时长唤醒并调用 `advance_due()`，无原义务时返回 `None`。空闲推进只执行已到期原义务，不生成行情 frame、不推进 NOE 的公开可见时间；后续实际组仍按原领取条件放行，不保证首个后继组反馈齐备。供料和空闲调用串行，容量不足沿原同步等待，不另建线程、队列或等待窗口。`stop_and_drain` 不推进剩余义务。

SIM 公共关闭日志新增 `ExecutionClockAdvancedTo` 依据，未扩展 DPT 的 replay-index 关闭合同。原有限 `NextGlobalEventAt` / `EndOfInput` JSON 形状不变，新读取器读取旧日志；旧二进制读取新关闭理由没有兼容保证。PAPER 进程宿主见下节；线上长时资源验证仍未交付。

按 #109 修正方案 A′，迟登记带来的旧返佣不回开已关闭时间。持续驱动从原下一义务 `d`、已关闭前沿 `F` 和最后真实组时间 `L` 选择 `c=max(d,F+1,L)`（缺省项忽略），timer 严格越过 `c` 才关闭，唤醒为 `c+1`；`now==c` 的真实组仍可先进入原归约。最后组时间始终参与，不依赖其是否尚待关闭。正常义务按原最早时序推进，不跨过中间 BUY。

仅 PAPER 公共组与 `ExecutionClockAdvancedTo(now)` 关闭在本次逻辑时刻前补入旧返佣，调用原精确 due 算法，保留金额、归属和经济事件时间。仅这些补入反馈的接收时间取本次实际处理时刻加原 sequence 延迟：组使用其 execution_time，timer 使用关闭理由中的 now。处理时刻只随本次私有投影草稿传递；普通 timer 虽实际晚处理，只要逻辑关闭仍在 due，反馈仍为 due 加原延迟。真实 NOE 的接收关闭/公开可见双条件保持；仅“不调用 observe”不足以证明迟入反馈不会提前可见。

共用 PAPER 公共组不区分有限/持续标记，因此上述组分支适用于两者。隐藏 ModelTrain/OfflineEvaluation、有限 `NextGlobalEventAt` / `EndOfInput` 关闭保持原行为；旧有限 next_group 的旧 due 调度/EOF 缺陷不在本次修复范围，不能据此宣称全路径已修好。新增公共驱动测试验证合法五分钟市场的零返佣时序、真实反馈领取及中间 BUY；非零返佣/日支付的经济算法验证复用原 matching 测试。

在线组合通过 `PreparedPublicGroupV1::from_online(&mut pending)` 消费 DPT 同次材料，复用有限来源的组引用、规范位置与 PM 上下文组装。组的 `Arc` 与 DPT 记录器、SIM 后台及驱动当前组共享；不复制完整组或重新准备 PM。宿主按原序在首个相关组前登记市场，然后调用持续驱动、共享读取与 Bridge，再完成 DPT 的记录确认。`tests/continuous.rs` 的原 socket 组合验证半帧期间保留同一 future 并调用真实 `advance_due`；DPT 自身测试覆盖异步记录容量等待。同步 SIM 容量等待仍不承诺并行 timer，恢复顺序沿原持续驱动。该组件组合不提供通用脚本平台；下节给出本次最短 PAPER 运行入口。


Ledger 经济更新按 #107 F1b 使用 `EconomicsPatch.day_updates`：追加新贡献、设置日结果或设置已支付额，不在每次更新中保存完整历史贡献。`RebateDay.contributions` 仍参与实际日汇总和前驱关联。配对的份额与成本仍保存，已删除无业务消费者的 `Pool.contributors`、`allocations` 及 `paired_allocations` 历史明细出口。所有可失败计算仍在耐久 append 前完成，append 成功后才应用内存更新。

该变化影响同步 JSONL 和后台 LedgerV2 交易中的经济补丁字段。同一冷读取入口接受旧 `EconomicsPatch.days` 完整日快照，恢复时直接移动替换，而非重复追加历史贡献；新写入只生成增量更新，不维护另一份完整日副本。旧程序不能读取新 `day_updates` 补丁；NOE V3／V4 的来源接线保持原版本。

## PAPER 运行入口（#109 任务 7）

`cargo run -p shared-run --bin paper --locked --offline -- <显式配置.json>` 启动独立消费者。二进制仅调用原固定官方 HTTPS 冷启动规则加载器，随后调用 `paper::run_paper_v1`，不启动或管理采集器。此命令描述接口，本轮未执行真实在线业务。受控测试通过原历史加载器得到合法封存规则后调用同一个装配/运行函数，不存在生产历史测试模式、外部每组时钟或任意 HTTP 端点。

配置类型为 `PaperConfigV1`，字段均必填：

- `socket_path`、`dpt_root`、`sim_log`、`noe_root`、`ledger_root` 分别指定原 Unix 交接及唯一产物位置；各原写入器保留创建和错误语义。
- `public_online` 直接使用 `PublicOnlineConfigV1`，包括原输入头、记录额度、在途输入数、稳定触发和分页参数。
- `reference_market`、`rule_snapshot_path`、`implementation_identity` 组成原 `CurrentProcessSnapshotRequestV1`；`finality_policy_path` 指向原最终性策略。延迟固定使用内嵌经验分布，没有替换文件入口。
- `session` 提供原运行/会话/账户/调用方/开户/Bridge 身份、`initial_account_state` 和 `session_seed`，装配固定原 `Paper` 角色。`activation_id`、`consumption_sequence_ref`、`noe_implementation_identity` 明确提供原接线标识。
- `admission_policy` 直接使用 `AccountAdmissionPolicyV1`；`max_group_bytes`、`finite_in_flight_groups` 明确提供原 SIM 记录容量。
- `script` 提供目标 `condition_id`、`token_id`、原 GTC `limit_price_pusd_per_share`、`size_shares`、`post_only` 及独立 `submit_context`、`cancel_context`。本入口不选择生产参数、容量或策略，不增加父策略风险检查。

冷启动后创建空场景与真实 NOE 开户，绑定原唯一后台 Ledger 材料，再构造共享读取、激活和连接 socket。开户不等待市场。每次原输入闭组使用 `from_online` 的同次材料；首次相关组前按原 PM 上下文登记市场，仅查询驱动已有规则表。组经持续 SIM、完整外生事实/新截止共享读取与原 Bridge 后，才完成 DPT Record→confirm。

最短脚本仅保存当前步骤，意图/请求仍归原 registry：首个可用共享组 NoTrade 零请求；后续目标组只提交一次 GTC；按原请求身份找到真实形成、实际可撤订单才取原平台 ID 提交一次取消。只有本次取消请求关联的原 `Cancellation(Canceled)` 才是取消成功，其他结果保持原语义，不造 ID、不重试。若市场双侧初始化或后续反馈尚未到齐，正常输入结束也可以报告步骤未覆盖。

同一个 `next_applied` future 在等待半帧或原异步容量期间保持，按 `next_wake_after` 服务原 `advance_due`。返回输入后 Record→confirm、真实 End 完成段及 Stopped 排空段均不插 timer。SIM 同步容量等待仍串行；恢复后沿原时钟规则。SIGINT/SIGTERM 请求消费者计划停止，不替采集器生成 End。

正常 End 或 Stopped 后只调用 SIM `stop_and_drain`，不推进未来业务、不生成 RunSeal、不撤单清仓。报告分别保留 DPT 结果/确认/应用/稳定前缀、SIM/NOE/Ledger 记录排空、Ledger 实际已消费投影和脚本步骤；`collector_exit: null` 表示没有外部退出证据。基础设施失败以错误和非零退出报告，既有应用/确认/稳定进度保留，不能以成功排空掩盖失败。记录排空、脚本覆盖、账户完成和采集器退出是不同结论。

`tests/paper.rs` 直接覆盖同一入口的双侧盘口与真实 GTC/取消、关联反馈、半帧期间原 timer、单边盘口 Unknown 未覆盖、正常 End、Stopped/Aborted/断连、原后台 I/O 失败及参数错误。测试只使用本地 Unix 对端。原 DPT reserve/取消和 SIM 同毫秒/返佣/背压机制反例继续复用组件测试。本次不证明真实线上稳定性、长运行资源、收益或整个 PAPER 阶段完成。

NOE 有限报告按借用参数直接编码，Parquet 记录变体按实际结构编码/解码，不经过中间 JSON Value 树。新 NOE context、commit、Parquet 清单及共享引用不再写 `boundary_sha256`；旧额外字段被读取忽略，旧 JSON 和 Parquet 的实际重开/重复入账由有限场景覆盖。旧读取器要求该字段，不能读取缺字段的新产物。记录字段与 variant 语义保持，但直接结构编码的字段顺序和物理字节不承诺与旧 Value 编码相同。

去掉 NOE 正文摘要后，仍可解析的正文改写不再单因字节变化被拒绝；成功 stage 后另行破坏文件也不会在 stabilize 被自读发现，后续实际历史解析承担其读取错误。位置恢复不扫描正文，不负责发现未消费正文的损坏。真实 write/close/sync、禁止覆盖发布和目录同步错误仍返回，未稳定发布不触发回调；不将返回错误等同于所有物理文件均已回滚。实际 context 身份派生及账户/目录关联继续保留。Ledger 的持久读取变化见下文。

Ledger JSONL 恢复保留前一成功消费的完整原始行（含换行），跨封存段与活动段比较重复尾；同序号、同结构但原字节不同仍拒绝，不再逐行计算 SHA。只在活动段保留首个唯一记录序号用于封口命名；新段名为 `ledger-sealed-v1--{first}.jsonl`，旧五字段名字只取首序号排序，不重命名旧文件或复核废弃字段。

Ledger Parquet 清单现在只写实际位置，旧清单中的摘要和四项范围字段被忽略；正文四种变体直接结构编解码，实际序号、金额、请求粘滞结果和幂等恢复不变。旧清单经过真实重开、追加再重开验证。新短段名及缺字段清单不能供要求旧格式的读取器使用，直接结构编码也可能改变正文 JSON 字段顺序。封口/打开不再全文计算或复核分片 SHA，空但可解析的段、连续可解析的内容变化、没有后继缺口的完整后缀删除不再因旧摘要/范围门被拒绝；真实解码失败、封存段半条尾、序号缺口及业务恢复错误仍报错。真实关闭、同步、禁止覆盖、清单提交和未提交尾清理保持。

未接线的磁盘投影发布/读取接口及其专用证明测试已退役，不删除已有投影文件，也不提供替代磁盘格式；`projection()` / `projection_v2()` 及账户投影业务类型继续使用。生命周期分录仍计算每市场本金、费用和账户残差，只移除专为全局/市场平台差额自比维护的两项累计。
