# DC-INCIDENT-20260728-LAG-IDENTITY-REPAIR 计划

> 状态：`STAGE6_CONTROLLED_RESTART_RUNNING_CONTENT_PENDING`
>
> 对应需求：`demand/data-collect-20260728-lag-fixed-identity-repair-demand.md`
>
> 对应 changelog：`changelog/data-collect-20260728-lag-fixed-identity-repair-changelog.md`

## 阶段 0：冻结证据与执行 authority

- 任务 0.1：核对本地 branch、origin refs、工作树和远端实际 service/binary/data
  路径。
  - 验证：实现基线与 `origin/main`、`origin/fix-rtds-silents` 一致；没有把旧
    `after-data-plan` checkout 冒充当前 authority。
- 任务 0.2：通过固定 UTC28 源路径断点拉取故障现场。
  - 验证：本地/远端逐路径 size/SHA 一致；manifest 及 9 个 Parquet 全部通过
    bytes/SHA/rows/PAR1 校验。
- 任务 0.3：建立同事项 demand/plan/changelog 并记录用户 clean-root 决策。
  - 验证：三份文档 slug、引用、范围、原因证据等级和验收标准一致。
- 任务 0.4：自审与独立阶段审查。
  - 验证：无过度部署流程、无旧 root migration、无未经证明的唯一根因表述。
- 完成门：commit/push 后进入阶段 1。

## 阶段 1：有界恢复扫描

- 任务 1.1：先为当前缺陷增加失败测试。
  - 测试：合法 sequence gap 接受；zero/duplicate/regressed 拒绝；普通非-resolution
    行不走完整 domain decode；claimant matrix 保持。
- 任务 1.2：为 v3 source reader增加精确 projection 和
  `RECOVERY_BATCH_MAX_ROWS=16`。
  - 验证：reader schema/projection 与需求列一致；所有相关 recovery reader 均不再
    使用 64。
- 任务 1.3：删除全量 `source_keys`，使用有界 `epoch -> last_sequence` 游标；删除
  普通行情行完整 canonical decode。
  - 验证：扫描结果不生成/修改 sequence；sequence gap 不被误拒。
- 任务 1.4：使用真实 UTC27/UTC28 只读 fixture 测量时间和峰值 RSS。
  - 验证：完整扫描不触碰原数据；RSS 明显低于远端 1.9 GiB 包络且不随 source row
    数量保存全量 key。
- 任务 1.5：自审与 `gpt-5.6-sol/ultra` 独立阶段审查。
  - 验证：claimant/terminal/source-only negative matrix 无回归，没有新增第二
    manifest/writer authority。
- 完成门：commit/push 后进入阶段 2。

## 阶段 2：paused fence 与 primary cause

- 任务 2.1：增加可复现测试：pump 在 Fence 后 processor pause 期间仍接收超过 SDK
  broadcast 容量的输入。
  - 验证：不产生 Lagged，FIFO 中 Fence 后消息顺序不变。
- 任务 2.2：修改唯一 pump，使 pause 只暂停 processor 上下文切换，不暂停 SDK
  receiver；为 Fence/Terminal 保留固定控制容量。
  - 验证：数据 row/byte 预算不变，没有第二 receiver、第二业务 processor或第二
    close authority。
- 任务 2.3：增加 pause 期间 terminal、capacity failure、stop/unclean 和 ticket
  收敛测试。
  - 验证：先准备既有 RawDay close，再 FIFO 交付 Terminal；不死锁、不丢已接纳
    ticket。
- 任务 2.4：source handle 完成后读取现有精确 report，单向精化 primary cause。
  - 验证：receiver lag 保持 primary；writer/finalize/shutdown error 只为 secondary。
- 任务 2.5：自审与 `gpt-5.6-sol/ultra` 独立阶段审查。
  - 验证：不新增 cause registry/latch/schema/sidecar，错误先后与 RawDay authority
    一致。
- 完成门：commit/push 后进入阶段 3。

## 阶段 3：prospective fixed identity

- 任务 3.1：增加 clean-root current/next restart 复现测试。
  - 验证：首次运行写 finalized identity，第二次 discovery 结构相同但 receive time
    更新时，首次 verified time 保持原值。
- 任务 3.2：从同一次 finalized metadata scan 返回有界 current/next immutable
  identity 候选。
  - 验证：无第二 scan/map/artifact，不恢复 role/pending/poll/terminal state。
- 任务 3.3：fresh discovery exact structural match 时继承首次 verified time；冲突
  继续 fail closed。
  - 验证：字段逐项 mismatch、多候选、非法首次时间均 exit 76；strict equality 不被
    放宽。
- 任务 3.4：保留 UTC28 冲突副本为只读 negative fixture，证明旧 root 不会被自动
  迁移或猜测修复。
- 任务 3.5：自审与 `gpt-5.6-sol/ultra` 独立阶段审查。
  - 验证：只解决 prospective clean restart，不把旧 root 变成兼容目标。
- 完成门：commit/push 后进入阶段 4。

## 阶段 4：全量验证与 release

- 任务 4.1：运行格式、相关 crate 测试、workspace 全量测试和 diff 检查。
  - 验证：命令、退出码和测试数量记录到 changelog；工作树只有预期变更。
- 任务 4.2：执行 1.9 GiB 包络的恢复资源验证和 isolated clean-root 两次启动测试。
  - 验证：bounded RSS、首次 identity 稳定、无 lag/deadlock。
- 任务 4.3：最终 `gpt-5.6-sol/ultra` 独立审查。
  - 验证：需求当时原有的 9 项验收逐项有代码/测试证据，无未解决 finding；后来新增
    的验收 10 由阶段 6R 单独关闭。
- 任务 4.4：commit/push 后从 clean commit 本地构建 release。
  - 验证：记录 commit、构建命令、binary 路径、SHA256、Build ID、GLIBC requirement、
    `--help` 和 `--version`。

## 阶段 5：直接清空并重部署

- 任务 5.1：确认 UTC27 与 UTC28 本地副本仍存在且 UTC28 验收结果不变。
- 任务 5.2：停止 `data-collect-training-collector.service` 并确认无 collector PID。
- 任务 5.3：使用精确绝对路径直接删除远端
  `/home/ubuntu/btc-updown-marketmaker/datasets` 与
  `/home/ubuntu/btc-updown-marketmaker/data-collect`。
  - 验证：只检查这两个目标已不存在；不增加 staging/atomic replacement 流程。
- 任务 5.4：创建 `data-collect/bin`，复制 release binary，校验远端 SHA，启动现有
  user unit。
  - 验证：MainPID 非零，`/proc/<pid>/exe` SHA 为新值，新数据根由新程序创建。

## 阶段 6R：pending-only restart B2 修复

- 任务 6R.1：先增加真实遗漏状态的失败测试。
  - 测试：`recovered pending + current none + recovered current + fresh validation`
    写一个 current snapshot、零 candidate；recovered next 随时间成为 current 时
    结果相同。
  - 测试：只有 synthetic discovery.next 命中 recovered next 时，无 recovered
    authority 的 discovery.current 仍写严格 candidate。
- 任务 6R.2：让既有 recovered immutable exact-match 在同一次比较中返回
  fresh-current 角色结果。
  - 验证：不新增第二 comparator、schema、marker、sidecar、恢复 map 或 validator；
    mismatch 仍在 row admission 前 exit 76。
- 任务 6R.3：修正 late rollover row-kind 路由。
  - 步骤 6R.3.1：fresh current 命中 recovered current 或 recovered-next-to-current
    时，使用 `CurrentSnapshot` 路由。
  - 步骤 6R.3.2：snapshot admission/register 且完整 switch 成功后消费 authority，
    并跳过 `candidate_to_active`。
  - 步骤 6R.3.3：fresh current 未命中时继续走原 candidate，且只在 candidate
    admission/register 与完整 switch 均成功后消费匹配的 recovered next。
- 任务 6R.4：修正安装失败重试一致性。
  - 步骤 6R.4.1：普通冷启动 subscription 失败继续零 metadata；context switch
    一旦开始 subscription，不回滚可能已接纳的 row/evidence，而按调用栈内
    `ExistingCurrent / CurrentSnapshot / CurrentCandidate` 路由补齐新 context 的
    current/next authority 后返回原错误。
  - 测试：subscription ack 前发生可重试终止时，recovered current 写一条 snapshot，
    普通 late current 写一条严格 candidate，只新增 next 时只写 next snapshot；
    pending 和已处理 pending evidence 保持。新连接只重新订阅，不重复 metadata。
  - 测试：普通冷启动首次 subscription 失败时零 metadata 且不消费 recovered 槽位；
    即使已跨普通 market boundary，外层重连成功后也只补写一条缺失 current
    snapshot，再推进 boundary 并消费对应 recovered 槽位。
  - 步骤 6R.4.2：UTC day fence 遇到无 authority 的冷启动 context 时，只对
    `metadata_received_utc_ms < day_boundary_utc_ms` 的旧日 evidence 复用同一缺失
    metadata 补写；authority 完整后才执行既有 boundary transition。新日 evidence
    保持 context，不写入或 defer 到旧日；seal/resume 后由成功重订阅在新日补写。
  - 测试：day fence reply 前恰好 admission 一条旧日 current snapshot，随后
    current-to-pending transition 进入既有 rollover defer；无 UTC day regression。
  - 测试：新日 missing current 的旧日 fence reply 成功，旧日零 snapshot、零
    transition、零 deferred ingress；seal/activation/resume 后首次重订阅恰好写一条
    current snapshot 并消费 recovered authority。
  - 步骤 6R.4.3：initial discovery 与 fence 同时 ready 时，生产 select 明确
    control-first；不新增测试 pause 或生产状态。
  - 测试：ready discovery 与已排队 fence 同轮就绪时，先收到 fence reply，resume
    前 discovery future 不返回，resume 后返回同一 discovery。
  - 步骤 6R.4.4：首次 discovery 在 subscription 前已 `Expired` 时，清除未安装
    current/next 后重新 discovery；recovered authority 不消费。
  - 验证：错误返回不消费 recovered 槽位；完整 switch 成功后才消费。
  - 测试：使用单调前进时钟；recovered current A 在安装前 `Expired` 时零
    snapshot/candidate/consume 且 A/B authority 均保留，时间前进后 recovered next
    B 成为 fresh current，成功写一条 snapshot 后才消费 B。不得让 A 通过倒退时钟
    重新有效。
  - 验证：不回滚已 admission 的 row，不新增 transaction/state machine。
- 任务 6R.5：运行定向测试、相关 crate 测试、workspace 测试、clippy、fmt 和
  diff 检查。
  - 验证：普通 candidate equality negative、snapshot time negative、immutable
    mismatch exit 76、pending-first subscription 和既有 FIFO/fence tests 全部通过。
  - 例外：当日 12:00Z 前运行时，精确排除既有动态 12:00Z receive 负载夹具；记录
    原始失败并要求其余 workspace 测试全部通过，不修改 B2 生产代码掩盖该夹具。
- 任务 6R.6：自审与 `gpt-5.6-sol/ultra` 独立阶段审查。
  - 验证：需求第六节和验收 6/10 全部有代码与测试证据，无可执行 finding。
- 完成门：commit/push 后从 clean commit 构建 release，按阶段 5 的相同简单边界再次
  直接清空两个远端目录并重部署，然后重新进入阶段 6。

## 阶段 6：长跑验收

- 任务 6.1：启动后验证六个逻辑 source/writer 任务健康；有合格事件的 source 对应
  counter、running/finalized 文件和 manifest 按各自合同推进，RSS 保持稳定。
  合法业务静默和稀疏 snapshot 不要求持续产行。
- 任务 6.2：在当前部分验证日形成 finalized current/next identity 后执行一次受控
  restart；还必须先形成至少一个可恢复 pending。
  - 验证：PID 更新、`NRestarts` 符合受控动作、首次 verified time 不改写、fresh
    validation 前进、写 current snapshot 而非 candidate，并继续写入。
- 任务 6.3：跨一次 UTC rollover。
  - 验证：旧日协调关闭、新日激活、唯一 receiver 无 Lagged、无 fence deadlock。
- 任务 6.4：继续运行到产生一个完整 UTC 日并审计。
  - 验证：完整日 closed，六源文件/manifest 校验通过，无本事故相关 gap/drop/lag。
- 任务 6.5：同步 changelog，独立审查运行证据，commit/push closure。
