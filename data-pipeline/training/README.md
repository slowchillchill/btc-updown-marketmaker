# 在线公共消费

`dpt-public-online <socket> <config.json> <新记录目录>` 接入原 `handoff_v2` 单消费者 Unix socket。生产端使用已有 `data-collect-paper-hot` 或 `data-collect-live-hot`；其四个物理连接、七类来源及原背压规则保持不变。这里没有模型、账户、订单或时钟关闭调度。

接收顺序独立分配公共 `input_position`，原 transport sequence 仅用于交接确认。来源准备和 `PublicObservationCoreV1::push` 复用既有唯一实现；当前回调直接读取完整组、观测及引用。`Received` 仅表示交接确认，不能用作应用或稳定凭据。

核心与当前回调返回后，同一材料转移给唯一后台 `PublicRecordingWriterV1`。编码、写入、同步和稳定凭据均由后台负责。记录容量暂满时，在接纳下一条输入前等待容量恢复；不重新应用、不改变源时间、不丢消息。原采集器的背压失败仍可能发生。容量用输入对象个数计量，不是字节或整链内存保证。

临时参考候选由一个明确稳定前缀的分页读取，加同一记录所有者的未稳定 TWAP 尾部，按原序交给既有唯一选择函数。本条 TWAP 也可参与选择。后台实际稳定后释放对应尾部；不扫描正在写入的目录恢复状态，不保留全历史候选。后台写入时不持候选锁；发布前缀时短暂持锁。准备方读取已公布前缀及尾部，不等待后台稳定，因此不会与容量等待形成相互等待。

正常完成要求生产者真实 `End`、所有已交付输入处理完成、核心原 `EndOfInput` 检查通过以及后台返回实际稳定凭据。`Aborted`、断连、半帧、宿主取消及后台 I/O 错误均返回未完成，并保留已应用/已消费计量及当时已知稳定前缀；不合成正常 End。后台故障可唤醒等待静默来源的接收端。二进制的 Ctrl-C 走同一取消入口。完成只证明本次 DPT 有限前缀，不证明账户或 CleanRun。

库入口 `consume_public_online_with_stop_v1` 将原取消与计划停止分开：取消仍返回未完成错误，计划停止只在当前输入完成应用、记录移交和 `Received` 后生效，在下一次接纳前优先观察停止信号。返回 `PublicOnlineOutcomeV1::Stopped` 时，`measurements.stable_prefix` 是后台稳定的既有前缀，没有任何输入时为 `None`；不向核心发送 End，不处理后续输入，也不掩盖已发生的回调或后台错误。正常生产者 End 则返回 `End`，保留原完整检查。该入口不证明采集器已正常结束，不扩展采集协议；连接和生产者的协作停止由宿主负责。原消费入口与二进制 Ctrl-C 行为不变。

配置必须显式提供 `header`、原 `GateAOutputConfig` 的 `recording`、`recording_in_flight_inputs`、`stabilize_every_inputs`、`read_page_rows` 和 `max_read_batch_bytes`。这些字段没有新生产默认值。记录目录须为新目录；恢复和事故续接不在本入口范围。标准输出仅包含最终完成计量，失败时标准错误包含实际错误和已有进度；唯一事实记录仍在记录目录。

本地 socket 测试覆盖两个角色、七类来源、多成员完整组、同值与新代际输入、跨稳定边界分页参考、暂满恢复、真实 I/O 故障与静默对端、取消/损坏/断连，以及未配对终态的原错误。官方来源有限验证的参数、投入和结果按当前 Issue 冻结；构造测试不冒充官方来源证据。

## 同次材料交接

`PublicOnlineSessionV1::start` 保留原接收器、核心及唯一记录后台。`PublicOnlineProgressV1` 保存原计量，放在宿主作用域，显式取消整个运行后仍可报告真实前缀。`next_applied(self, stop)` 移动会话所有权，在原异步容量或帧读取期间等待；宿主把这一个 future 固定保留，在 timer 唤醒后继续轮询它，不能取消后重建半帧读取。

返回 `Applied(pending)` 后，读取 `applied()` 及同次 `materials()`：闭组为共享 `Arc`，PM 对象从原准备结果移动，规范位置起点直接来自核心。宿主先完成市场登记和同步业务消费，再完整等待 `pending.complete()` 的原 Record→confirm；实际 End 同段完成原稳定操作。此段不选择 timer 或计划停止。只有 `Continue(session)` 才能接纳下一输入；丢弃 pending 即放弃未完成会话，不能漏记后继续。消费失败仍未完成，不声称回滚。

停止信号只在原等待边界返回 `Stopped(session)`，不在 `next_applied` 内等待后台。宿主退出 timer 选择后调用 `session.stop_and_drain()`，沿原既有前缀排空。旧显式 cancel 仍可取消整个运行并返回未完成错误。正常 DPT End 只证明来源输入完成，不代表 SIM 业务完成。旧消费函数包装同一实现，记录格式及稳定参考分页语义不变；本接口没有增加来源协议或通用宿主。

### 外排与捕获写流

Arrow 排序只构造实际排序键列，按返回的置换移动已有行；真正 Parquet 输出仍包含键和载荷。外排保留实际重复键冲突与输入读取错误，取消对同次 Arrow 置换及自产 run 行守恒的重复证明。正式产物所需逻辑摘要在最后一次排序写流中形成，沿用原域、字段长度前缀、行顺序和末尾行数；不再在关闭后为计算摘要全读一次。只消费临时路径和资源计量的内部排序不计算该摘要。

离线 sequential capture 在成功写入批次后累计逻辑摘要和输出行数；后台 public recording 沿现有 `defer_batch_sync`/`close_segment` 流程，不维护无消费者的离线摘要。实际写入、关闭和稳定边界同步错误仍传播；`fadvise` 及仅为该建议打开文件的失败不再拒绝产出。该建议不构成缓存释放或内存上界保证。本轮不改持久格式，也不以取消自产重读代替真实读取入口的解析职责。

TypedRaw Parquet 仍先以原打开的 `File` 完成长度与 SHA 校验，然后在 Arrow 供数侧为该文件共用一个 64KiB 字节窗口。`get_read` 保留独立逻辑位置及原默认 8KiB `BufReader`；`get_bytes` 返回自持有的字节，超窗请求直接读同一文件。短预取不要求填满窗口，真实请求不足沿原 EOF/错误传播。文件 reader 销毁后释放窗口，不跨文件或两遍共享，不改变 schema/footer、物理批次、逐行解码与范围筛选。

切换文件时可能短暂同时存在两个窗口，128KiB 仅指窗口增量；各返回 Read 的 8KiB 缓冲、原 SHA 缓冲、Arrow 页、返回 Bytes 与批次分别占用内存，不构成进程 RSS 上界。仅测试内计量实际文件读取次数/返回字节，生产不增加逐页诊断或新的准入门。

所属[十二分钟材料验收](docs/typed-raw-byte-window-12m.md)仅运行一次原材料试制：完整trial业务比较与两遍物理工作量保持，合并475.669秒对历史495.617秒；读请求量／次数明显降低但整体仅约4%时点差异，不外推稳定加速或全研究预算。

历史范围筛选对六类 Existing 和 Metadata V6 Existing 复用 core 的原来源 parts 提取与同一个语义校验，只返回原接收时间，省去筛选中不用的最终消息身份 JSON／SHA 和 RefV3 装配。parts 的拥有型字段、metadata 定位摘要和 RTDS 原载荷摘要仍保留；AnchorRule、TWAP、历史修复走原完整来源构造。校验 Err 仍按 `unwrap_or(true)` 保留原行，后遍正式来源构造继续承担原错误，不能按裸时间丢弃窗口外的缺源事件时间 PM 行。文件准入、两遍完整解码和入选行的正式身份／排序不变。

所属[范围筛选十二分钟验收](docs/received-time-filter-12m.md)只运行一次原材料试制：完整trial业务差异0，两遍完整物理量保持；合并445.432秒对历史475.669秒，仅跨版本／共享缓存时点约6.357%差异，不是稳定倍率或全研究预算。


## 离线产物构建、发布与读取

`dpt-gate-a build --manifest <清单> --condition-id <市场> --received-start-utc-ms <开始> --received-end-utc-ms <结束> --candidate-dir <新目录>` 构建显式市场/时间范围；`--manifest` 可重复。`dpt-gate-a publish --candidate-dir <已有候选> --output-dir <目标目录>` 同步文件并以 `NOREPLACE` 重命名发布。输入只取调用者明确列出的 manifest：读取一次，以头部 `utc_day` 和文件条目的来源/schema/hour/part 为事实，从清单父目录定位列出文件。目录和文件可改名、改变层级；不要求个人数据根、日期下限或目录闭集，未列出的旁文件不参与输入。合法历史日期不会自动被选中，市场和时间范围仍由调用者给出，日历/小时换算与实际文件解码仍按真实消费执行。

发布目录只需要 `artifacts/manifests` 中五份定位清单及其数据：规范事件、事件时间索引、可见时间索引、市场观测、归约报告。不再生成质量/测试/资源证明、capability matrix、receipt 或额外 run manifest；没有“训练就绪”布尔或准入 API。两种原发布用途使用同一实际格式。普通构建不要求固定个人输出目录、文件系统类型、特定服务/进程数、隔离工具或源码身份，也不执行扰动物理布局的第二次构建。

`open_published_gate_a_final(root)` 返回 `PublishedGateAFinal`，通过 `artifacts()` 获取已读的定位材料。打开不扫描数据文件，不重算候选、质量和报告摘要；真实数据打开、Parquet/合同载荷解析、索引页定位与事件关联在游标消费时执行。两个时间索引分别服务平台时间与模型可见时间。SIM 有限宿主使用原正式发布一致的读取参数（1024 行、64 MiB 批内存），库游标仍由调用者显式给出读取参数。

新 artifact manifest 不再写 `dependencies` 与 `canonical_record_stream_sha256` 证明副本；读取忽略历史清单中的这些多余字段，真实 manifest 文件摘要仍用于适配器来源身份。因此新清单字节和来源身份摘要会变化，旧程序也不能读取缺少旧必填字段的新清单。旧目录中额外报告不参与新读取；这不承诺所有旧物理格式都兼容，也不再发现仅靠旧全文件摘要复核才能发现的未消费部分改动。实际产物的数据摘要、逻辑摘要及行数继续供诊断消费，不作为每次打开前的自证门。

构建清理仅限本次成功创建的目录；既有目录不会因构建失败被删除。显式发布失败保留调用者目录；如果重命名成功后父目录同步失败，仍返回 I/O 错误，但产物已经位于目标位置，不承诺回滚。后台 public recording 的自有 segment 提交沿原独立生命周期处理。

旧报告失去消费者后，归约不再维护四份规范流摘要或临时排序逻辑摘要，也不提前为摘要编码两套索引投影。真正捕获记录/报告时才编码所需载荷；仅运行暂存归约、不捕获输出时，相应编码字节峰值为0。实际事件时间索引与可见时间索引仍在物化时分别构建；输入行摘要仍用于冲突排序，不改变其排序选择。构建结果只保留实际scope/文件选择与归约计量、五份产物定位和L2峰值，不再复制整份构建配置或证书资源状态。

输入集合保存首次清单和文件摘要，供来源身份、排序和 lineage 使用；不锁定或复制输入文件。每次实际打开选中的文件仍检查字节数、SHA、schema/footer，读取前同长度损坏仍失败。归约结束不再重新读取全部清单和文件：原清单删除/变化、未选文件变化、最后读取后变化不会据此否决结果；也少了一次发现“打开摘要检查后、解码期间原地修改并持续到结束”的机会。旧末尾扫描同样不保证文件连续不变。

公共 V6/TWAP 的显式 admission 入口采用相同路径处理，保留其实际公共格式读取能力；旧 `freeze_direct_source_input_set`/GateA canonical reducer 的支持来源没有增加，不因此获得 TWAP 构建能力。本次不改变 Parquet 或 manifest 字段格式，删除的是目录资格 API、日期资格类型和末尾重验 API。

## 仅测试构建的既有重放成本诊断

[#202十二分钟诊断](docs/public-replay-internal-cost-12m.md)沿原`PublicReplayV1::next_prepared_applied`计量一级阶段及候选嵌套读取，计量模块、状态、时钟/计数、环境参数与报告代码全部`cfg(test)`；普通生产构建不包含这些代码，普通测试默认不激活。专用真实材料测试显式ignored，不在固定回归中自动消费研究材料。

先以`cargo test -p data-pipeline-training --release --lib --no-run --locked --offline`取得并固定准确优化库测试二进制及源码/锁文件身份，再对该二进制显式运行`--ignored --exact public_recording::tests::replay_cost_tests::existing_twelve_minute_replay_internal_cost --test-threads=1 --nocapture`。配置路径由`DPT_REPLAY_COST_CONFIG`、报告路径由`DPT_REPLAY_COST_REPORT`提供；配置字段见[机器结果的effective_config](docs/public-replay-internal-cost-12m.json)，其中public_root在实际本地运行时填写真实材料路径。`expected_normal_prefix`和`initialization_input_positions`仅记录本次选用身份与分母，不增加原reader以外的准入/拒绝门。

报告区分原入口、候选嵌套、返回后摘要、返回对象释放、reader随`into_core`释放与末尾core释放；原业务错误先保留，诊断写入错误不能覆盖它。此诊断不更改选择、因果时间、稳定前缀或EOF，不是生产优化/收益验收。实际运行范围由准确任务合同指定，不能用此显式入口替代范围授权。

所属[#206深度组内共享对照](docs/binance-depth-shared-owner-12m.md)复用同一原测试驱动，在同配置既有十二分钟public上串行基线/修改后各一次。只有公共观察深度的组内Arc所有权改变，计量/扫描/EOF不改；完整JSON兼容及原值由独立改动前夹具验证。一次对照不是稳定比例，也不解决完整研究持久规模。

当前 core 在该组内共享基础上保留一个当前不可变的 Binance 完整观测投影。未发生深度处理时，后续组复用该 Arc；每次 Snapshot/Update 进入原归约器前及应用真实深度事实时失效，下一组沿原完整公式懒建。唯一归约器仍拥有数量和证据，投影不参与归约；旧在途组自行保留旧值，释放后不由当前引用永久保留。每个观察自己的组位置与可见前缀继续逐组生成，原质量、锚点、事实、完整 JSON 和持久录制保持。一个当前强引用不是全进程单份值或内存上限。原完整值/JSON、生命周期反例及本次单对照174.54→140.28秒的覆盖和限制见[当前投影复用报告](docs/binance-depth-current-projection-12m.md)及[机器投影](docs/binance-depth-current-projection-12m.json)。

所属[#208记录布局与容量分解](docs/public-recording-layout-12m.md)只在测试构建用原RunCursor/SequentialCaptureWriter执行有限4096候选；原key/payload不重序列化，稳定分片/header/prefix/初始化保持。两个布局ignored测试通过`DPT_RECORDING_LAYOUT_CONFIG` / `DPT_RECORDING_LAYOUT_REPORT`显式提供本次原根、新根、recording和读取参数，配置字段见[机器结果](docs/public-recording-layout-12m.json)。唯一实际顺序为重编码、原新字节/前缀比较、原#202新布局重放；第二段同遍历复用原observe/EOF前缀闭合，不声称完整走next_row的I/O/错误路径。结果仅支持本次内容等价可行，文件减0.732%且未见明确重放改善，不修改生产默认或作为长期预算。普通回归不运行真实材料；实际范围仍由准确任务合同指定。

所属[#210 payload页/压缩候选](docs/public-payload-page-compression-12m.md)复用以上三个原测试入口，测试配置`payload_page_8m_zstd9=true`仅选择固定payload列8MiB软页目标／ZSTD9，缺省仍走原属性；生产属性不变。4096、容量计数、字典和WriterVersion保持，Canonical子类只计完整原存储payload长度。新候选captured减54.997%，原字节及业务保持，但本次重放288.75秒／RSS174,452KiB高于#208历史参照，非受控稳定变化；保留为后续容量优先场景的候选，不自动改生产默认或闭合长期预算。完整构建缓存增量因首次编译前未采而未知，准确已测阶段和三段GNU资源见报告。

前段[#214有限参考候选读取复用](docs/public-reference-candidate-reuse-12m.md)在一次有限 `PublicReplayV1` 中按实际页请求推进原校验 reader，只保存已验证参考规范记录的定宽物理位置与TWAP类型；正文按原文件有界定位读取，同查询复用行组，页满、非TWAP越界、当前Input及原错误/EOF前沿保持。该段在线候选、保存依赖和历史身份扫描沿原入口，不新增生产诊断接口。既有成本测试增加仅 `cfg(test)` 的 `original_candidate_reader`基线开关（缺省false）及 `progress_interval_seconds`（缺省30秒；普通计量会话默认关闭）；阶段进度对未知量不造比例/ETA。本次固定串行各一次315.32→197.04秒、完整33页相等，仅为该单对照，不外推小时吞吐或研究存储预算；全部成本和覆盖边界见[机器结果](docs/public-reference-candidate-reuse-12m.json)。

后续[#214保存依赖与候选共用验证前沿](docs/public-shared-reference-frontier-12m.md)将单次有限重放的非空保存依赖接入原私有前沿，首次仍完整校验原稳定视图至EOF后才按原流/位置返回所有匹配Reference；空、重复、缺失、多事件和候选未来边界保持，独立`read_dependencies`/历史身份/在线入口不变。仅cfg(test)的`original_dependency_reader`选择本次旧依赖基线，生产没有开关。固定串行各一次192.48→179.86秒、完整依赖返回及33页候选相等，消除额外候选验证而保留约19秒首次EOF责任；索引40B、完整13,032项、实际容量655,360B及新正文定位成本见[机器结果](docs/public-shared-reference-frontier-12m.json)，不外推长期预算或稳定加速比例。

## 正式有限重放进度

`PublicReplayV1::set_progress` 提供单次可选 JSON 行进度，旧库调用默认关闭；`RecordedPublicGroupSourceV1` 只转交。公共统计和原两市场／首小时研究工具默认30秒，前置 `--progress-interval-seconds 0` 可关闭。验证行、定位行、已应用输入及已生成组分别计量，未知总量／ETA不推算，输出故障只停止日志。准确字段、合作式频率及未覆盖prepare阶段见[中文说明](docs/finite-public-replay-progress.md)。本段只有小夹具与工程检查，不含新的真实材料运行。
