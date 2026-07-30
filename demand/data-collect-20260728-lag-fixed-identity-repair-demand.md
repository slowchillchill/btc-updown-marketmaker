# DC-INCIDENT-20260728-LAG-IDENTITY-REPAIR 需求

> 生命周期：`HISTORICAL_EVIDENCE_ONLY`
>
> 最终快照：`FROZEN_AT_20260730_CONSOLIDATION`
>
> 当前执行 authority：`none`
>
> superseded_by：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 读取规则：本文仅用于审计历史事实；内部“当前、下一步、待执行、命令”均属于
> 当时检查点，不得驱动当前实现、测试、部署或远端操作。
>
> 2026-07-30：本文件保留事故事实和最终用户验收；内部阶段状态、游标和命令只描述
> 当时过程。当前整理 authority 为 `DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`。
>
> 状态：`CLOSED_BY_USER_ACCEPTANCE_20260730`
>
> 用户首次批准时间：2026-07-29
>
> Stage 6R B2 修复批准时间：2026-07-29
>
> 用户最终验收时间：2026-07-30
>
> 对应计划：`plan/data-collect-20260728-lag-fixed-identity-repair-plan.md`
>
> 对应 changelog：`changelog/data-collect-20260728-lag-fixed-identity-repair-changelog.md`

## 一、目标

修复 2026-07-28 数据采集器连续两次因 Polymarket SDK receiver lag
退出、随后因 fixed identity 冲突以退出码 76 停止重启的问题；同时修复第一次
clean-root 重部署后，pending-only 受控重启把已有 fixed authority 的 fresh current
错误路由为 `candidate_to_active`、导致
`candidate_to_active:fixed_identity_not_validation` 退出的问题，并在新的空数据根上
重新部署后持续采集。

本事项不补写缺失数据，不把 UTC28 故障日改写为健康训练日，也不兼容或迁移已经
发生 fixed identity 冲突的旧远端数据根。

## 二、已确认事实与证据边界

1. 两个采集进程分别以 `receiver lagged by 1364 messages` 和
   `receiver lagged by 781 messages` 返回，并各触发一次
   `SourceInnerReturn(polymarket_market)`。现有 manifest 不能把两个计数分别绑定到
   两个退出时间。
2. 第一次退出与 UTC rollover paused fence 高度吻合，但没有 fence 持续时间证据，
   不能称为已确认唯一触发原因。
3. 第二次运行存在 receive 到 admission 的端到端处理迟滞，但现有证据不能定位到
   writer、queue 或其他具体环节，也不能证明该迟滞促成 receiver lag。发生在 source
   返回之后的 `writer_queue_backpressure_timeout` 是停机阶段次生错误，不能冒充
   receiver lag 的直接原因。
4. 启动恢复读取全部 v3 列、使用 batch 64 并将约 755 万个 source key 全部保存在
   `BTreeSet`。该实现违反有界恢复内存合同并解释约 119 秒启动延迟和高内存峰值，
   但没有证据证明它是第二次 receiver lag 的直接原因。
5. fixed identity 最终冲突链已经确认：同一 slug 的 condition、CLOB、ordered
   assets、start 和 end 一致，`fixed_identity_verified_at_utc` 却从首次值
   `2026-07-27T23:55:00.053Z` 被重启后的 discovery 改写为
   `2026-07-28T00:02:13.160Z`。严格冲突校验本身必须保留。

## 三、启动恢复扫描

1. `polymarket_orderbook_v3` 恢复只投影既有需求规定的 schema、event discriminator、
   envelope、identity、winner 和 claimant coordinate 列；不得对普通行情行构造完整
   canonical domain。
2. 所有恢复 reader 固定使用 `RECOVERY_BATCH_MAX_ROWS=16`，每个 batch 处理完立即
   释放，不得把完整列或完整 source key 集合常驻内存。
3. 删除全量 `(source_sequence_epoch_id, collector_admission_sequence)` 集合。
   允许按规范文件顺序维护有界的 `epoch -> last_sequence`：
   - epoch 非空；
   - sequence 非零；
   - 同 epoch 严格递增；
   - sequence 允许空洞，不要求 `previous + 1`。
4. 恢复 scanner 只验证或拒绝持久化事实，不生成、修复、重排或重新分配 source
   sequence；writer 仍是唯一 admission/sequence authority。
5. metadata v4 terminal/summary、唯一 claimant coordinate、winner、时间因果、
   source-only、late row、cross-pair 和损坏文件的既有 fail-closed 矩阵保持不变。
6. startup/manifest owner 已在 dataset lease 内建立 manifest/file authority；
   recovery 不重复建立 manifest snapshot、receipt 或第二套文件权威。

## 四、paused fence 与错误归因

1. 唯一 SDK receiver 由同一个 fast-drain pump 持有。收到 Fence 后、processor
   尚未 resume 时，pump 仍必须继续接收并按同一 FIFO 转交消息，不得停止排空 SDK
   broadcast。
2. 数据 row/byte 上限保持不变；队列只增加 Fence 与 Terminal 所需的固定控制容量，
   不扩大业务数据预算。
3. paused 期间发现 SDK terminal 或 capacity failure 时，必须先调用既有
   `RawDayGate` 一次性 close authority，再把 Terminal 按 FIFO 交给 processor。
   不得新增第二个 terminal latch、close registry、CAS authority、sidecar 或
   manifest 字段。
4. `Fence -> fence 后已接收消息 -> Terminal` 的 FIFO 顺序必须保持。stop/unclean
   路径必须收敛已经取得的 ticket，不得丢弃 future，也不得把所有 ticket 强制改写为
   `Defer`。
5. source handle 已完成时，supervisor 必须直接读取既有 `SourceTaskReport` /
   `SourceTaskTermination` 的精确错误。`RawDay` first close 仍是唯一 primary cause；
   后续 writer/finalize/shutdown 错误只记录为 secondary，不得覆盖 primary。
6. 错误归因修复不得新增 cause registry、manifest schema、receipt、sidecar 或第二套
   shutdown 状态机。

## 五、prospective fixed identity

1. 同一次 finalized metadata bounded scan 必须恢复仍可能作为 current/next 的固定
   identity 候选；不得为此增加第二次扫描、持久化 map 或新 artifact。
2. fresh Gamma discovery 继续提供当前平台事实。只有以下 immutable 字段全部精确
   相等时，才复用已恢复的首次 `fixed_identity_verified_at_utc`：
   - slug；
   - condition id；
   - CLOB market id；
   - 按 Up/Down 固定顺序的 asset ids；
   - market start/end。
3. 多候选、任一 immutable 字段冲突、首次时间非法或 current/next authority 不唯一时
   继续 exit 76；不得放宽严格 fixed identity 校验。
4. 本节新增的 fixed-identity authority 不得被用来额外恢复旧 runtime role、动态
   状态、pending 行、poll slot 或 terminal owner；既有 resolution-followup recovery
   按其独立合同恢复仍在窗口内的 pending，不属于本节新增 authority。
5. 本要求只保证新数据根未来 clean restart。旧 UTC27/UTC28 数据只在本地离线保留，
   不得重新作为部署后生产 recovery authority。

## 六、pending-only restart B2 路由

1. recovered pending 必须继续先建立唯一 Polymarket market subscription；fresh
   Gamma current discovery 保持异步，不得改成阻塞 pending subscription 的同步启动
   门。
2. recovered authority 与 fresh discovery 的 immutable exact match 继续只使用既有
   canonical 比较逻辑。该次比较必须同时产出仅存在于内存和当前调用栈的
   `fresh discovery.current matched` 路由结果；不得新增第二套 comparator、schema
   字段、持久化 marker、sidecar、恢复 map 或第二状态机。
3. fresh `discovery.current` 精确命中未消费的 recovered current，或精确命中随时间
   已成为 current 的 recovered next 时：
   - 写既有 V5 current `metadata_snapshot`；
   - 保留首次 `fixed_identity_verified_at_utc`；
   - 保存本次 fresh source receive、payload、digest 和
     `row_evidence_validation_completed_at_utc`；
   - 不再写 `candidate_to_active`。
4. 只有 fresh `discovery.next` 命中 recovered next、但 fresh current 没有命中任何
   recovered authority 时，current 仍是普通 late candidate；next 可写保留首次 fixed
   time 的 snapshot，不得利用 next 的 recovered match 让 current 绕过 candidate
   equality。
5. fresh current 没有 recovered authority 时，既有
   `source receive <= validation = fixed <= applied` candidate 合同和 validator
   完全不变。不得把 equality 全局放宽为 `fixed <= validation`。
6. 任一 immutable 字段冲突、authority 不唯一或首次 fixed time 非法时，继续在任何
   snapshot/candidate admission 前 exit 76。
7. context 安装在 subscription 或 metadata authority admission 完成前发生错误时，
   不得留下无法可靠重试的 `current-without-authority` 或 `next-without-authority`。
   subscription 尚未开始前失败时恢复 pre-switch current/next；subscription 已开始
   后可能已有目标 row/evidence 被接纳，不得回滚这些事实。此时 context switch 必须
   按同一 row 路由提交 current snapshot、严格 current candidate 或新增 next
   snapshot，再返回原错误；外层重连只重新订阅且不得重复 metadata。普通冷启动
   subscription 失败在同一 UTC 日内保持零 metadata；后续外层重连成功后，必须对
   缺失的 current 或 next authority 恰好补写一次 snapshot，再推进已越过的 market
   boundary。若重连前到达 UTC day fence，则按用户批准的窄范围例外，只对
   `metadata_received_utc_ms < day_boundary_utc_ms` 的旧日 Gamma evidence 在旧日
   关闭前补写 snapshot；只有 current/next authority 随后完整时才推进 boundary，
   不得在旧日关闭后回写。属于新日的 Gamma evidence 不得写入或 defer 到旧日；
   context 必须原样跨过 seal/resume，成功重订阅后在新日恰好补写一次 snapshot。
   initial discovery 与 fence 同时 ready 时必须由 fence 优先。首次 discovery 在
   subscription 开始前已 `Expired` 时，清除未安装的 current/next 并重新 discovery，
   同时保留 recovered authority。
8. recovered authority 只能在对应 snapshot 或 candidate 成功 admission 并注册为
   source authority 后消费；本实现进一步采用保守门：只有完整 context switch
   成功才消费。`Expired`、subscription/processing error、metadata
   validation/admission/register error 均保留 recovered 槽位；错误路径为保持
   context 一致性而已提交的 authority row 不触发消费。
9. B2 不新增、删除或放宽 V3/V4/V5 validator；它只修正 restart rediscovery 的 row
   kind 路由和失败重试一致性。

## 七、数据与部署边界

1. UTC28 故障现场必须先完整复制到：
   `/home/chillchill/new-datasets/2026-07-28-deploy-ec92bd3999cc/typed_raw/utc_day=2026-07-28`。
2. 该副本以 manifest SHA、逐文件 SHA/bytes/rows、Parquet header/footer 和本地/远端
   文件清单一致为验收；其 `open / unclean_end_unknown`、13 gaps、2145 lag events
   和 45 dropped rows 必须原样保留。
3. 完成本地修复、测试、独立审查、commit/push 和本地 release 构建后，停止远端
   `data-collect-training-collector.service`。
4. 测试开发阶段采用用户批准的简单清空：直接删除远端
   `/home/ubuntu/btc-updown-marketmaker/datasets` 和
   `/home/ubuntu/btc-updown-marketmaker/data-collect` 两个目录，不做旧 root migration、
   staged rename 或滚动替换。
5. 保留现有 user systemd unit，重新创建程序目录、复制本地构建的修复 binary 后启动
   该 unit。
6. 新数据根首次启动形成新的 lineage。启动当日若不是从 00:00Z 开始，只作为部分
   验证日，不作为完整健康训练日。

## 八、验收标准

1. UTC28 本地副本与远端 10 个文件逐路径、大小和 SHA 完全一致；9 个 Parquet 均可读，
   总计 1,345,977 行。
2. 生产规模恢复 fixture 不再保存全量 key，恢复期间内存有界且适合约 1.9 GiB
   远端；普通行情行不做完整 domain decode。
3. duplicate/regressed/zero sequence 被拒绝；合法 sequence gap 被接受；唯一
   claimant 和既有 negative recovery matrix 全部通过。
4. pause 期间输入超过 SDK broadcast 容量时唯一 receiver 仍持续排空且不产生
   `Lagged`；pause 期间 terminal/capacity failure 能通过既有 `RawDayGate` 和 FIFO
   收敛，不死锁、不建立第二 authority。
5. 两次 source failure fixture 均把精确 receiver lag 作为 primary，把后续 writer
   shutdown 错误保留为 secondary。
6. clean root 运行必须先形成 recovered pending 及 finalized current/next identity，
   再执行受控重启；recovered current 与 recovered-next-to-current 都各写一个
   current snapshot、零 candidate，首次 verified time 不变且 fresh validation
   前进。只有 next 命中 recovered next 的负例必须让无 recovered authority 的
   current 继续走严格 candidate；immutable 冲突仍 exit 76。
7. 全量 workspace 格式、编译和相关测试通过；唯一允许精确排除的既有测试为
   `four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second`
   在 12:00Z 前运行时把当日 12:00Z receive 当成未来的时间段依赖夹具。该测试不在
   B2 diff 中，排除后其余 workspace 测试必须全部通过；独立阶段审查与最终审查均无
   未解决的可执行问题。
8. 远端删除后只运行新 binary；服务启动成功、六个逻辑 source/writer 任务健康、
   有合格事件的 source 按各自合同推进、内存稳定，且无 receiver
   lag/backpressure/raw-after-close/fixed-identity conflict。合法业务静默或稀疏
   snapshot 不要求人为持续产行，也不得仅因静默判为故障。
9. 真实受控重启、自然小时 finalize、fixed identity 保留、fresh validation 前进及
   零 candidate 的运行证据已通过；用户于 2026-07-30 接受其作为本事故事项的最终
   运行验收，并移除 UTC day rollover 和完整健康 UTC 日这两个完成阻断门。该决策
   不声称两项未记录的运行证据已经发生；后续 rollover/full-day 健康检查属于持续
   运维观察，不再阻断本事项关闭。
10. subscription 安装失败后重试不得留下 `current-without-authority`、不得丢失
    pending 或重复 candidate。`Expired` 测试必须使用单调前进的时钟：首次发现的
    recovered current A 在安装前过期时零 row、零 consume，A/B authority 均保留；
    时间继续前进后，原 recovered next B 成为 fresh current，并成功写入恰好一条
    current snapshot 后才消费 B。不得让已过期的 A 通过倒退测试时钟重新有效。
    冷启动 subscription failure 还必须覆盖跨普通 market boundary 后成功重订阅；
    UTC day fence 必须分别覆盖旧日 evidence 在关闭前补齐 snapshot，以及新日
    evidence 零旧日 row/零 defer、seal/resume 后重订阅恰好补一条 snapshot。
    initial discovery 与 fence 同时 ready 的确定性测试必须证明 fence 先 reply，
    resume 后才返回 discovery。
