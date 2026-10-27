# B 性能候选的显式入口

本实现服务 [#377](https://github.com/slowchillchill/btc-updown-marketmaker/issues/377)。候选 A 为 `storage_v1`：整帧传输、同份 Rust 精确域、权重无关输入缓存和 worker 内 epoch。候选 B 为 `device_v1`，在 A 上实现设备学习、逐帧 GRU 主干和输入预取。候选是否通过固定输入数值验证、完整包性能和基线对齐，分别以该 Issue 的准确记录为准。代码存在和无模型检查不证明模型数值或完整性能已通过。

## 生产配置与传输

旧 `WorkerConfig` 缺省不增加 `performance` 键，保持原 JSON 行协议和原配置绑定。显式启用 `performance` 时必须提供准确的 `mode`（A为 `storage_v1`，B为 `device_v1`）和 `native_library`。原有 `LearningParameters`、消费身份和 Adam 恢复保护继续生效；新字段只属于 worker 执行元数据，不递归忽略其他配置差异。

Rust 构建同一包的 `rlib` 和 `cdylib`。显式配置使用相同源码构建的 `libmodel_train.so`。worker 经 hello 协商 `b-frame-v1`，随后发送紧凑帧：`BMF1`、小端元数据长度和载荷长度、JSON 元数据、token 列。每个 token 保留 u32/u32/f32/u8 的原位值，共 13 字节。没有跨更新复用 embedding、hidden、logits 或计算图。stdout 仅承载请求响应，进度写 stderr，间隔 900 秒。

原生域只接收冻结规则绑定和当次资金/条件域请求。它直接调用与 Rust 消费者共用的 Decimal/BigUint、逐腿成本、post 支持集和价格上界函数，不保存另一份 Ledger 或 SIM。规则恢复只读已封存快照，不触发公开规则 GET。消费者仍核对返回域并沿原意图、预留和执行链提交。

`rust_threads` 作用于进程共用 Rayon 池，缺省 16。Python/Torch intra threads 使用原 `torch_threads`，interop 使用显式 `torch_interop_threads`，缺省 1。新模式设置子进程 OMP/MKL 线程；不改变 TF32 标志或依赖版本。实际环境与调用成本仍需运行报告证明。

## 缓存、内存与退出

正常采集在一次 `learn_open` 后记录实际前向的紧凑 tokens、账户/调用身份、原动作与条件 RPC。冻结标签先沿原文件持久化，再与完整原顺序一次上传。每 epoch 在同一 worker 内顺序重建可微状态，沿原 FP64 Actor、FP32 Critic、TBPTT 和两次联合 Adam 执行；A 保留逐帧计算数学。

`gpu_cache_bytes` 控制固定 GPU 输入驻留子集，`runtime_reserve_bytes` 保留运行空间。非驻留部分保留紧凑 CPU 后备；A 每次上传使用独立普通 Tensor，反向结束前由计算图持有。输入池在 inference mode 外创建，两轮只读。`host_cache_bytes` 是紧凑后备的内存规划目标，并非通过拒绝、裁剪或磁盘溢出来改变合法输入的硬限。整组输入在学习完成前不能释放，若达到该目标便等待会死锁。输入计量不等于进程 RSS、完整显存或物理 PCIe 计量。

`queue_bytes` 对公共日队列和 worker 请求队列施加字节背压。公共队列复用原 PM 输入字节提示，并计已知容器、共享快照、book/depth 数组容量，不重做 JSON 编码或展开惰性观察。每个队列条目计一次所引用的已知载荷；跨条目共享部分可能重复计数。分配器、嵌套字符串/证据、statistics 缓存及 opaque 载荷中未被提示覆盖的部分为 Unknown。这是已知载荷的软计量目标，不是全部实际内存的硬限，也不等于 RSS。最终 stderr 报目标、峰值、生产等待时间和计量耗时。单个合法大帧超过目标时，在队列清空后独占发送，保留完整输入。背压等待释放后继续，不等待凑满批。公共生产者等待时仍沿原公共推进能力运行，不改变业务时序。

取消使用独立继承 FD，不排在长 RPC 后。学习在 TBPTT 安全点、Adam 前后与提交边界检查取消。显式 `shutdown` 关闭输入、join 请求线程并 wait 子进程；正常退出状态必须实际回收。原子 checkpoint rename 前不提前修改内存 consumed/sequence。取消不能证明任意 native/CUDA 算子可立即中断，外层预算监督仍须保留。

## 冷恢复

新模式成功采集的既有 `completed-collection` 摘要增加 `collection_sampling_state`，版本 `b-collection-sampling-v1`。它保存原 group、实际业务采样账户、calls 和 CPU Generator 状态。冷入口逐项核版本、组、账户集合和计数，调用生产 `learn_restore_collection_sampling_state`，再从同一封存轨迹、公共查询与原标签一次重建输入缓存。它不重新采样、不从 seed 重新推进 RNG、不复用采集 hidden；学习 hidden 沿原合法起点置零。模型、Adam 和全局 RNG 仍走原完整 checkpoint 恢复。

旧采集摘要缺少该状态时，不支持声称精确的新模式冷恢复；入口明确报缺口。工程 F4 teacher 序列不属于业务采样账户集合。该恢复仅覆盖原组学习，不代表可从采集终态继续同一日经营。

## 有界工程验证入口

`model_performance_samples <冻结配置.json>` 只生成 #377 批准的 C65/L65/F4。它对原轨迹逐条读取，首条必须为规则绑定；至多 131 生产记录、每条 1MiB、实际读取总量 16MiB，取得连续 call 1..65 即停。标签只读取指定首账户的前 65 完整对象，总读取 1MiB、单对象 64KiB，不读完余数组或重算 GAE。读取器只按下一字节需求推进，不在 65 条边界后预取用户态余量；普通操作系统的物理预读未测，记为 Unknown。报告仅证明已读取前缀的摘要，不声称验证未读全文件的 JSON 或 SHA。

公共输入通过原 `visit_day_frames_controlled`、Recovery 规则、原日 preparation/S/E/H 和原查询截止重建；每帧核原可信 group，65 帧成功后 prefix stop。F4 从 typed raw、公共账户/执行/Receipt 冻结入口构造，不手填 options。F-S 仅覆盖原模型数量单值域，不证明低于 MOS 的订单可被平台接受。材料生成、公共重建、比较、输出和排空均计入对应 Issue 签发的共享时窗；早期包的 360 秒非槽限额仅保留为历史记录，不是这些入口的通用预算。开发静态编译不授权实际读取旧大流。

`model_performance_slot <冻结槽配置.json>` 使用真实 `ModelWorker` 启动纯观测 wrapper，继续由绑定原源码 `Service.run` 分派。R0 使用原 begin/条件头；A 使用整帧二进制路径。每次材料读取核准确路径、SHA 和事前字节限额。槽位、来源源码、观察工具、方法、时钟和调用上限由根编排器冻结，driver 不自行选择或重试。

停止槽在首个 32 步 TBPTT 块完成反向后的既有安全点，通过 READY FD 通知 Rust；Rust 随后写生产取消 FD。这是首块的一次反向，不是第 32 次反向。冷槽实际关闭热 worker，再创建新进程，调用正式采样恢复与一次缓存重建。两个入口不会主动选择运行配置、槽位或追加验证。真实模型装载、前向、teacher 和临时 Adam 的用途、次数及预算由对应 Issue 的准确运行签发冻结，入口不会因历史槽位尚未使用而自动取得执行额度。

## 历史与连续发送边界

#377 同分支修正原 #108/#366 的选用范围。`PublicGroupDecisionV1` 在正式构造入口设置私有发送边界模式：State 的 `decision` 为 HostManaged；Finite 原 `send_decision_at` 与 Continuous 的 `send_decision` 为 ActualSendBoundary。Finite 新具名 `send_decision_at_host_managed` 显式选用 HostManaged。该模式不序列化、不是配置字段，调用方只能读取。

B 的原日装配显式调用 `run_historical_days_host_managed_with_options_v1`，通过内部运行 context 选用具名 finite 入口；学习采集复用同一装配。原日入口及独立历史入口保持 ActualSendBoundary，不修改策略 trait 或增加用户配置。共用批次函数在 HostManaged 保留原资格→Bridge 准备→提交顺序，不在批次内提前领取反馈或重新观察；在 ActualSendBoundary 保留逐项准备→原反馈领取/观察→资格→提交。旧 PAPER 脚本使用 State 决定时按原宿主边界运行，正式在线 PAPER 使用连续决定时保持实际逐项边界。`uses_source_health`、caller 字符串及 `paper_clock` 是否空都不能代替正式选用关系。原新增回执字段保留，各分支记录适用的原截止。

该修正的类型接线和定向无模型回归只构成工程证据。固定输入短槽与原完整包的适用性，仍由研究按准确基线门核对。

具体 `PrefixSampleConfig`、`SlotConfig`、共同 manifest 和输出字段见[短验证接口](PERFORMANCE_VALIDATION.md)。真实运行按对应 Issue 的当前签发及其前置条件执行，源码入口和历史授权不代替新阶段授权。旧 A 归档中的 consumer 调用省略 summary 实参及当时的授权标题，沿 Issue 的后继记录解释；旧归档与失败记录保持原字节。

## B 的设备学习与传输

`device_v1` 按原 schedule 取连续同账户片段，最长止于该账户下一个 TBPTT32 边界。片段内仍按原顺序逐帧调用共同主干，每帧执行一次 `[1,1,W]` GRU，并把该帧 hidden 传给下一帧；不使用 padding，也不把同账户时步当独立账户。早期整段 `[1,T,W]` 主干方案的记录保留在 Issue，不能作为当前实现说明。token 仍按原序列先分别 mean，再将同帧各市场等权 mean。原 CPU Generator 采样和精确条件域不变；学习的概率、old logp、有效优势、ratio 和 Actor 在 CUDA FP64 上计算。Critic 沿原 FP32 value 与 Python 标量的原表达式顺序，不将 target 改为 FP64 Tensor。

原 L65 的32、32两段各自反向/截断；尾1及四个独立 F4 尾图按原顺序全部形成后，再在epoch末依账户顺序反向，最后一次 Adam。设备标签只上传一次；必要逐样本报告按段批量导出，不逐条件头做生产D2H。显式逻辑传输计数与框架内部/物理总线流量分开解释。

两个 pinned CPU base 和两个 GPU raw base 各自独立。raw 仅作搬运暂存；四列包含 numeric 都复制/转换为图独占的普通 Tensor 后才进入可求导算子。GPU raw 可在 owned-copy 事件依赖之后复用，独占列一直保留到该账户原 backward 返回。CPU 再写 pinned 前实际等待原 H2D 完成；`wait_event` 本身只提交设备依赖，不证明CPU可复写。

当前帧 GRU 提交后、条件头和报告同步之前，copy stream 仅用原两个槽预取原 schedule 中下一帧所需的原始输入。下一帧消费其准确代次并创建 owned 列；没有第三槽、额外 forward 或提前 backward。全驻留时不创建传输 stream 或搬运槽。取消可能留下已上传但未消费的预取代次，按实际记录，不把它算成完成前向。诊断捕获可能引入同步；结构预取证据不等于物理重叠比例。

设请求GPU份额G、传输块P、实际可用free和运行保留reserve，则实际G为 `min(G,max(0,free-reserve))`；单槽S为 `min(floor4(P),floor4(G/2))`，驻留份额为 `G-2S`。GPU份额包含两个raw槽。R为精确驻留前缀时，应另计两槽、序列4字节对齐与必要池块容量；不能仅把13B/token总量当全部G。host目标、公共byte提示和这些缓存计量都不等于进程RSS。
