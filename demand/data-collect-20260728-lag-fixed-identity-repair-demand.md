# DC-INCIDENT-20260728-LAG-IDENTITY-REPAIR 需求

> 状态：`APPROVED_EXECUTING`
>
> 用户批准时间：2026-07-29
>
> 对应计划：`plan/data-collect-20260728-lag-fixed-identity-repair-plan.md`
>
> 对应 changelog：`changelog/data-collect-20260728-lag-fixed-identity-repair-changelog.md`

## 一、目标

修复 2026-07-28 数据采集器连续两次因 Polymarket SDK receiver lag
退出、随后因 fixed identity 冲突以退出码 76 停止重启的问题，并在新的空数据根上
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
4. 不恢复旧 runtime role、动态状态、旧 pending 行、poll slot 或 terminal owner。
5. 本要求只保证新数据根未来 clean restart。旧 UTC27/UTC28 数据只在本地离线保留，
   不得重新作为部署后生产 recovery authority。

## 六、数据与部署边界

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

## 七、验收标准

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
6. clean root 运行产生 current/next finalized identity 后受控重启；immutable identity
   一致时首次 verified time 不变，冲突时仍 exit 76。
7. 全量 workspace 格式、编译和相关测试通过；独立阶段审查与最终审查均无未解决的
   可执行问题。
8. 远端删除后只运行新 binary；服务启动成功、六个逻辑 source/writer 任务健康、
   有合格事件的 source 按各自合同推进、内存稳定，且无 receiver
   lag/backpressure/raw-after-close/fixed-identity conflict。合法业务静默或稀疏
   snapshot 不要求人为持续产行，也不得仅因静默判为故障。
9. 长跑至少覆盖一次受控重启和一次 UTC rollover；只有随后完整 UTC 日满足 closed、
   无故障 gap/drop/lag 和逐文件完整性时，才可判为健康训练日。
