# 数据采集可靠性修复计划

> 对应需求：`demand/data-collect-reliability-repair-demand.md`
>
> 对应 changelog：`changelog/data-collect-reliability-repair-changelog.md`
>
> 事项编号：`DC-REPAIR-20260713-COLLECTOR-RELIABILITY`
>
> 状态：`SUPERSEDED_AFTER_HEARTBEAT_BASELINE`。本计划已完成并通过审查的CLOB `10s/20s/require_text_pong=true`继续作为保持基线；旧RTDS `5s/15s PING/PONG`已由`plan/data-collect-source-availability-lifecycle-repair-plan.md`阶段9 `RTDS-PONG-A`取代，该阶段已于2026-07-19完成本地实现、全量门禁、香港线路复测及独立终审，当前RTDS为固定`5s/require_text_pong=false`。本计划未执行的业务静默恢复预算、D2 one-shot reconnect、CLOB freshness退出、有限总恢复、旧SDK patch白名单、daily coverage及生产阶段全部被新计划取代，不再提供实施、构建、部署、远端操作或数据清理授权。历史执行记录仍以原changelog为准。
>
> 当前训练collector首次正式冷启动的独立执行authority为`plan/data-collect-training-collector-direct-deployment-plan.md`；本旧计划中的`Restart=no`、gate、UTC边界等待与数据清理步骤均不可执行。

## 一、执行原则与成功定义

本计划只保留已完成的heartbeat基线及此前计划演进记录。当前实施必须严格按新专项阶段0--8执行；每阶段测试、独立审查、commit和push通过后才进入下一阶段。生产release构建、远端预置、清理、切换和长跑均不在当前授权内。

当前本地成功证据由仓库测试输出、每日manifest临时fixture、git commit与新专项changelog证明；测试使用自动删除临时目录，不新增receipt、sidecar或transport episode artifact。

## 二、已冻结路径与命名

```text
本地仓库                  /home/chillchill/btc-updown-marketmaker
本地 release binary       target/release/data-collect-training-collector
本地 unit 权威源          data-collect/deploy/systemd/data-collect-training-collector.service
远端 binary               ~/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector
远端 unit                 ~/.config/systemd/user/data-collect-training-collector.service
远端 CLI dataset root     /home/ubuntu/btc-updown-marketmaker/datasets
远端 effective output     /home/ubuntu/btc-updown-marketmaker/datasets/training/typed_raw
UTC 日目录                utc_day=YYYY-MM-DD
source 目录               source_kind=<source>
正常小时文件              <source>_HH.parquet
同小时重启文件            <source>_HH-part-N.parquet
日清单                    utc_day=YYYY-MM-DD/manifest.json
```

上述路径只保留为旧计划的历史命名记录，不构成当前build/deploy authority。2026-07-18首次正式冷启动已另建并批准直接长期部署三件套；本专项不得恢复旧同日staging/gate/清理步骤。

## 三、阶段 1：冻结合同并通过计划审查

- 任务 1.1：核对真实代码、旧需求、远端运行与两日数据审查证据。
  - 步骤 1.1.1：列出程序错误、平台/网络诱因、未知来源与当前补救缺口。 -> 验证：需求第二节逐项区分事实、责任与剩余未知，不把推断写成事实。
  - 步骤 1.1.2：列出旧产物合同被取代的位置，并按用户 2026-07-14 决策同步 `demand/rebuild-core-demand.md` 数据与证据第 10 项。 -> 验证：新需求明确取代 data-collect per-segment/run/receipt/ResourceWatermark/PASS/required-files 合同；核心需求同步为每日单一事实 manifest，其他模块 run manifest 不变，旧 changelog 不被篡改。
- 任务 1.2：冻结最小 production contract。
  - 步骤 1.2.1：冻结 hourly Parquet + 每日唯一 manifest + journald。 -> 验证：manifest 只含采集事实，data-pipeline 负责内容校验、训练准入和语义去重；没有新增 sidecar/report/receipt。
  - 步骤 1.2.2：冻结单 binary、单 unit、单数据根和远端删除白名单。 -> 验证：所有路径都是明确绝对/`~` 路径，无宽泛父目录删除。
- 任务 1.3：独立计划审查。
  - 步骤 1.3.1：派生至少两名独立子代理，使用已确认的 `gpt-5.6-sol`/`ultra`，分别审查正确性/SDK-first 与简化程度/部署安全。 -> 验证：两者都明确返回 PASS，Critical/Important 为 0。
  - 步骤 1.3.2：修复审查问题后重复审查。 -> 验证：最终计划状态改为 `REVIEW_PASSED`，changelog 记录审查结论而不生成独立报告文件。
  - 步骤 1.3.3：只 stage 本事项文档，运行 `git diff --check`，创建 commit 并 push。 -> 验证：远端分支包含同一 commit，用户既有 dirty files 未被纳入。

## 四、后续实施authority已迁移

本计划原阶段2--5全部停止执行，现由`plan/data-collect-source-availability-lifecycle-repair-plan.md`阶段0--8逐项取代。原changelog保留当时批准、实现、远端状态和未执行步骤的历史事实，不从本文件恢复旧命令。

### 4.1 当前SDK与source生命周期

1. SDK patch语义以source-availability需求第4.7节最终获批逐项矩阵为authority，不再使用固定“六项/八项patch”合同；当前版本、Cargo/lock、vendor路径与provenance由`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`管理。当前Stage1 worktree为官方0.7.0底座、唯一`third_party/polymarket_client_sdk_v2-0.7.0-p1`根，相对官方源码差异恰好为`src/ws/connection.rs`、`src/ws/config.rs`、`src/clob/ws/types/response.rs`、`src/rtds/subscription.rs`；保留sticky parser fault、CLOB all-or-error、`SimpleParser::Default`、generation隔离能力和RTDS有界脱敏诊断目标。历史阶段2/4接入与用户B trace决定保持，不因版本身份迁移而重写；SDK070 Stage1独立审查已PASS且用户已批准发布，其闭合以承载记录的commit成功推送且远端SHA与本地HEAD/upstream一致为生效条件。
2. 阶段2让strict heartbeat timeout/旧PONG结束旧socket并进入既有recoverable loop，同时让最后一个manager/bound sender关闭成为不重连的normal teardown；该strict语义当前只适用于`require_text_pong=true`的CLOB。阶段9 RTDS `require_text_pong=false`固定发送PING且no-PONG不重连；不得增加第二owner或公共shadow API。
3. 阶段3原子删除RTDS业务静默专用`request_reconnect()`的vendor能力、runtime wrapper、生产调用和ticket专属测试；每个真实transport新generation仍只replay一次latest complete filtered desired。
4. CLOB删除30/60秒freshness watchdog、unsubscribe/resubscribe budget与第三次退出；Gamma外部失败/空候选持续恢复。Binance aggTrade/depth区分“有合法控制frame但无业务数据”和“完全无入站frame”，业务静默不控制网络；confirmed ID/update hole、parser/schema和本地book不变量继续按完整性合同处理。
5. 各source单次connect/HTTP/read/backoff等待有界并使用capped backoff+jitter；source-local外层总attempt和总elapsed无限，普通external availability、业务静默、日志失败或既有诊断计数饱和不得结束source/collector。

### 4.2 当前daily manifest与共同coverage

1. 四个logical producer固定映射六个writer source：`polymarket -> polymarket_market, polymarket_market_metadata`；`polymarket_rtds -> polymarket_rtds_chainlink_btc_usd`；`binance_depth -> binance_depth_snapshot, binance_depth_update`；`binance_aggtrade -> binance_aggtrade`。全部mandatory，训练collector没有CLOB user source。
2. 每日唯一顶层`data_collect_daily_manifest_v3`直接嵌套一个`data_collect_collection_day_coverage_v1`；六个既有source counters职责不变，不复制coverage，不新增transport scope/reason/episode、extensions、receipt或sidecar。
3. v1只含`not_started|active|ended_known|unclean_end_unknown`、三个nullable `i64` UTC毫秒时间和一个必填`reactivated_within_day`布尔值。legacy缺对象只读为unknown；新writer必须写，missing/malformed/unknown nested字段或版本fail closed；所有ManifestOwner mutation必须保留对象。
4. current-day读取/恢复前先持有dataset-root级OS-backed独占lease。queue/唯一receiver/ingress前置完成，六writer与四producer wrapper全部ready并阻塞共同gate后，owner只执行一次atomic activation；commit决定持久归属，ack只控制network/raw release。
5. 正常rollover依次关闭旧raw generation、drain、收齐四producer barrier与六writer finalize ack、原子提交旧nested end和顶层closed、创建新日active commit并在ack后release。可靠known-close候选first-close-wins；共同fence前的非协调panic/abort/join failure执行unclean-dominates。PendingExit、task级ack/abort permit、同日四态恢复、prior state-aware closure，以及首次activation为`reactivated_within_day=false`、同日后续activation为`true`并保持sticky、新UTC日重置为`false`，全部按新需求第4.2节测试。

### 4.3 当前验证与生产边界

1. 阶段1--7的定向loopback、组合退化和至少100-cycle资源测试，以及阶段8的`cargo fmt --all -- --check`、`cargo clippy -p data-collect --locked --all-targets -- -D warnings`、`cargo test -p data-collect --locked`、vendored SDK测试/lint、vendor diff与`git diff --check`，均由新计划执行并逐项映射28项验收。
2. 旧D2同日running-upgrade、当日删除、gate和26小时放行步骤全部撤销；M8-A的UTC边界首次启用候选也已被2026-07-18独立直接部署事项取代。多日停服、旧PID/lease退出且当前UTC日无任何collector产物的首次正式冷启动可任意时刻直接启用唯一`Restart=on-failure` unit；future coverage-aware升级/回滚仍须另行批准。
3. same-day binary/version/identity takeover、identity migration、旧binary接管open v1日、生产构建、部署、远端服务操作、数据清理和重采均不在当前计划内。相同identity持有独占lease的同日进程恢复属于collector状态机，不是部署takeover。
