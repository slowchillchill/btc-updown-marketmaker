# #180 步骤 6：真实在线容量与有限资源观察

本报告覆盖 #127 的 TypedRaw 后在线容量诊断及同次 #164 F6 观察。所有结果保留实际版本和退出事实。原小模型普通窗口已经完整消费并排空；原小模型高窗口完成所有输入／模型组，但诊断结束接线错误导致 exit1，不能记成完整成功。依赖快照修复后的大模型高窗口结果见下表。本报告不表示 #180 组合交付、100ms 方案、PAPER 或实盘验收完成。

## 实际版本和负载

原前两场使用全仓源码／工具 `5816e51b5333c44796fb61035196d2c6f66a439b`，二进制 `0dc5f294d600bdb33e62461db46619ad82b0ab715ad3e037bb72e60fb21508ff`，实际构建功能 `ort,bench-profile,sim-system/measurement`，冻结见 [freeze.json](evidence/freeze.json)。新大模型场使用全仓源码／工具 `ecb4b22042432ba9cf9559dd5df5749b85592f00`，二进制 `c025f366e107ae1c4137912c757aa1cff53d916db5d4db2fce911ab17ee20308`，实际构建 `continuous-validation,bench-profile`（包含 python、ort、sim-system/measurement，入口不调用 Python），冻结见 [freeze-snapshot-fixed.json](evidence/freeze-snapshot-fixed.json)。该头包含步骤 4、5 和已审生产快照修复 `66a459beed004692057f28a2bf1ffaa31c44243f`；仅适配诊断文本锚点，没有改变 #178 历史工具或计数位置。

新场没有首错捕获补丁，没有 FRAME_STOP_AFTER_GROUP／FRAME_SUPPORT_FAILURE，连续运行完整窗口。每个冻结保留实际源码、诊断补丁、锁文件、Rust/Cargo、ORT库、图／权重、输入配置／完成、规则、最终性、入口检查点和暖缓存摘要。新场沿相同大模型高窗口条件，前两场未在修复后源码重测；不同二进制／功能组合的结果不构成纯模型规模消融。

本机 AMD RYZEN AI MAX+395、32 逻辑核。入口队列 25000；DPT recording_in_flight_inputs=128 为本次诊断条件；SIM finite_in_flight_groups=128 沿原配置，三者分别计量。Rayon8，小模型 ORT4，大模型 ORT16。每场窗口＋恢复观察上限 1200 秒墙钟，准备另列；窗口外不供料，窗口内未接纳尾部继续等待。高窗口 `1789517395414..1789517401000`，5.586 源秒、T-30 双市场重叠，组418584..421620，共3037组；3041条 TypedRaw Message＋1条 End控制，共3042输入。

## 在线协议与计量

独立线程按原接收时刻的单调高水位供料，经原 bounded_event_queue、真实 Session/Message/Received、PublicOnlineSession.next_applied、PreparedPublicGroup.from_online 和 Finite 消费入口，再到 NativeInputRoot.consume、真实连续 Runtime/hidden/ORT Actor/execute。准备前缀沿原在线公共链处理；模型图／hidden／暖缓存就绪、所有准备输入确认后才开放负载，初始入口队列为0。复用真实旧 hidden 与暖缓存，不宣称重新运行完整模型历史。

每条输入的全部派生组及 Actor 完成后才调用 pending.complete；Received 在该完成调用中发出，因此可能早于 complete_end（方法返回）墙钟。摘要核验模型结束≤complete_begin≤Received。DPT真实 End保持 End控制，不发 Message Received；高段最终 applied421715、confirmed421714 的差一不是漏确认。所有窗口输入与派生组处理完后显式 SIM.stop_and_drain，录制结束不冒充未来市场业务终局。

小段19组原路径与在线路径的 reference／payload、同毫秒多组及最终逻辑 Ledger 对照一致；20ms真实慢消费者证明队列等待与 Actor 后确认。原输入0..2的代表截断覆盖零派生尾输入及合法排空，见 [protocol.json](evidence/protocol.json)。正式测量不附带全历史双路比较或每帧完整特征、账本、哈希转储。

表中准备秒来自首条 measurements 的 preparation_seconds：宿主在创建 Runtime 前启动 preparation 标尺，到首个窗口组返回且入口账本断言完成时取值。window_anchor_ns 则使用在线控制的另一 Instant 标尺，记录供料门开放时刻；起点和取点都不同，不能混作同一测点或直接相减。大模型两读数分别为806.317766858秒与806.313884220秒。窗口内墙钟只用同一在线控制标尺作差，不需为这几毫秒差补测。

service_ns 是 native.consume 到 Runtime／Actor 原 execute 完成，排除此前 DPT应用、交接及随后证据输出。DPT完成墙钟取最后输入 complete_end−window_anchor；SIM排空、整进程时间和准备分别列。attempt_to_dequeue 含供料入队等待，不能写成纯驻留；enqueue完成到出队另计，微小负数可能来自接收者先于 Sender返回。source_delivery_lag_ns 在 acknowledge_if_empty 之后采样，是本组消费／确认后采样相对源逻辑接收时刻的墙钟落后；采样点不严格等同于模型结束，不等于每个 SDK 行情价位网络年龄。原逻辑发送时刻和真实 execute耗时保留，本步没有实现步骤9发送时钟语义。

## 结果

三个实际条件均显著落后于源节奏。小模型60源秒窗口耗638.115秒完成DPT，入口25000真实饱和，窗口内尾部到342.992秒才全部接纳；短高段入口未饱和，但原小模型与修复后大模型分别积压至173.854／288.427秒才完成DPT。修复后大模型在1200秒观察上限内恢复并排空，不能据此推导持续高负载容量或两模型纯规模差值。

| 实际条件 | 准备秒 | 组／模型行／Actor数 | 输入／Message Received数 | DPT完成秒（窗起） | SIM排空秒 | 入口峰值 | 整进程秒／退出 |
| --- | ---: | --- | --- | ---: | ---: | ---: | --- |
| 原小模型60源秒 | 261.929 | 49524／49524／49524 | 49530／49530 | 638.114528476 | 0.023698081 | 25000 | 900.282／0 |
| 原小模型5.586源秒 | 789.251 | 3037／5546／3037 | 3042／3041 | 173.853682809 | 无证据 | 2777 | 963.310／1（结束接线错误） |
| 修复后大模型5.586源秒 | 806.318 | 3037／5546／3037 | 3042／3041 | 288.427144023 | 0.046959783 | 2941 | 1095.363／0 |

新大模型连续组位置418584..421620完整，原失败组419346已正常处理；全部3042输入已应用及完成，3041条消息确认，End控制单独应用。尾部到5.577495588秒全部接纳；完成时supplied/enqueued/dequeued/applied=421715、confirmed=421714，无pending输入／组，DPT真实正常End稳定，SIM显式排空和completion成功，未到观察上限。原两场也逐输入核验全部派生组结束后才调用complete及确认，原第二场的SIM退出失败事实仍保留。

| 实际条件 | service合计秒／P50／P95毫秒 | Native P95毫秒 | 源逻辑→消费／确认后采样落后P95／最大秒 | 入队尝试→出队P95秒 | 窗口DPT在途观测峰／含准备session峰 |
| --- | --- | ---: | --- | ---: | --- |
| 原小模型普通 | 399.638／7.845／9.865 | 5.953 | 551.005／578.146 | 307.789 | 3／32 |
| 原小模型高段 | 39.817／13.084／17.186 | 9.223 | 165.892／168.246 | 165.860 | 2／38 |
| 修复后大模型高段 | 165.261／45.686／90.299 | 53.207 | 260.196／282.817 | 260.016 | 2／36 |

service合计不代替DPT端到端墙钟。新大模型入队等待最大25.849微秒、DPT记录reserve等待最大6.463微秒，Actor execute所测SIM记录等待最大13.235微秒；这些精确边界没有显示本窗相应等待饱和，未测SIM在途占用保持未知。原普通段入队等待最大8.185秒，真实背压存在；其峰25000不能把排队解释成事后算例。原高段及新高段总量本来只有3042输入，未将容量人为压小制造背压。

| 实际条件 | 整进程User＋System CPU秒 | 最大RSS MiB（含准备） | 已计量组／输入证据输出秒 |
| --- | ---: | ---: | --- |
| 原小模型普通 | 1210.26 | 420.223 | 5.741／0.291 |
| 原小模型高段 | 1292.37 | 539.008 | 无completion合计证据 |
| 修复后大模型高段 | 2081.28 | 2851.234 | 0.893／0.021 |

这些CPU/RSS是含准备的进程读数，RSS不包括全部系统页缓存。模型区间CPU合计及原始分位数、输出范围见 [results.json](evidence/results.json)，已计量证据输出不等于全部DPT/SIM持久化工作。

新大模型窗口 registry_snapshot_ns P50为1.253毫秒、P95为41.937毫秒、最大199.607毫秒；feedback_apply_ns P95为0.123毫秒，ledger_projection_ns P95为0.019毫秒。该窗口观察到快照成本明显，未凭此定位全部成本来源或实施优化。筛选后订单输出／订单资金投影每组最多99条，请求资金投影最多227条；这些是访问量，不是完整历史容器大小。末组SIM实测business为11324条／534531字节、pending_projection为0，public_pm_evidence为5市场／10资产／980层／338缺失证据，含既有前史状态；不冒称全系统长期资源边界。


## F6与覆盖限制

沿原 registry_snapshot_ns、feedback_apply_ns、ledger_projection_ns、ledger_economic_read_ns、account_economics_projection_ns 和 measurement_resources 观察。registry_order_entries 位于 unfinished_orders 筛选后的 inspect，仅计输出条目访问；不计 unfinished 谓词考察后剔除的闭合依赖，不代表 action_snapshot 对完整 registry 的扫描，也不能当成容器大小。request_funds_entries／order_funds_entries 位于过滤后的资金投影构造，仅表示入选条目访问，非全历史扫描。候选规模来自真实模型行的盘口层数及 SIM public_pm_evidence。

DPT在途和入口深度有直接计量；SIM仅有实际记录等待、业务记录/字节与行情证据规模，没有每时刻真实在途占用，不能因等待短就断言所有SIM记录路径未饱和。记录 session peak 含准备前缀，窗口逐输入观测峰值另列。进程CPU和最大RSS含准备，模型区间CPU另列；短窗不能证明长期有界、无泄漏或未观察历史扫描的规模，订单／请求撤单／资金历史尚未量化部分保持未量化。

TypedRaw后供料不覆盖真实 SDK 原始消息／字节预算／超时路径。容量负结论不通过缩容量、丢输入、改变业务超时或新增资源策略掩盖。

## 修复与验证

旧第三场被自动开始后在准备中断，没有正式窗口；结束接线修正版本第三场处理762组后在419346的Family支持阶段 exit101；一次有界支持捕获重现同组／输入419435／时间1789517396043，均不是容量超限且不计完成场。证据分别保留于 formal-20261001、end-corrected-20261001、support-failure-20261001，冻结引用原版本，不覆盖。

根因是 action_snapshot 删除已闭合sim35订单，却保留仍有开放另一腿的双腿买入原请求与outcomes，导致完整registry Known0 在冻结投影变为 Unavailable。原源取消已在 cutoff前；之后直接响应没有用于解释此前状态，事故.tmp仅未发布材料。修复只保存已保留请求outcomes直接需要的订单证据，同时订单／资金观察仍用原未闭合集合，不把闭合依赖计入数量、年龄、撤单候选，也不填未知0或改Actor规则。[精确头正式审查](https://github.com/slowchillchill/btc-updown-marketmaker/pull/182#pullrequestreview-5371700940) 已批准该修复。

真实 Shared/SIM/Native 红绿场景覆盖 Down FinalSuccess5→双腿买入两腿 Accepted、初始 matched0→Up CallerCanceled先于直接响应且Down仍未闭合；修复后完整／冻结承诺同义、支持计算正常，数量／年龄／资金／取消集合保持，另一腿闭合后当前观察清空、旧冻结读不变。

固定 workspace 命令 `cargo test --workspace --lib --bins --tests --examples --locked --offline -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second` 通过，具名UTC夹具明确**跳过**。独立 native47单测通过、2既有忽略，另rounding3项通过。Shared/native适用Clippy、两工作区fmt、diff检查通过，既有告警保留。新工具适配py_compile与release构建通过（38.89秒），未重复已通过同源码回归或旧ORT矩阵。

原始产物根 `target/issue-180-public-baseline-validation/`。本次新场在 snapshot-fixed-20261001；此前协议在development-20261001，生产修复回归日志在snapshot-dependencies-20261001。结果摘要保留相对路径及直接证据SHA-256，便于核查。
