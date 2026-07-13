# 数据采集可靠性修复 changelog

> 对应需求：`demand/data-collect-reliability-repair-demand.md`
>
> 对应计划：`plan/data-collect-reliability-repair-plan.md`
>
> 事项编号：`DC-REPAIR-20260713-COLLECTOR-RELIABILITY`

## 2026-07-13：计划落盘前证据与决策

- 用户要求先制定并落盘全面修复计划，计划经子代理审查通过后才执行；实现完成后再次审查，通过后才停止并清理远端旧采集、部署新程序并长期运行。
- 用户明确要求不增加多余校验/验证产物，编译、产物、采集路径和命名保持简单。因此本事项选择 `hourly Parquet + 每 UTC 日唯一 manifest.json + journald`，取消 per-segment/run manifest、runtime receipts、ResourceWatermark 大字段、独立 validation report 与重复 sidecar。
- 2026-07-11、12 拉取数据的文件传输与远端一致；重复与缺口不是日归档复制产生。两日 Polymarket 存在大量完全相同 payload 重复，Binance aggTrade 有已被官方 REST 证明存在的 6 个缺失 aggregate trade ID，RTDS 存在陈旧 payload。
- 当前代码核实出 current/next 全有或全无、synthetic discovery 伪装 `new_market`、fetch-start 回填 receive time、RTDS 任意消息刷新 freshness、queue/writer 错误被吞、segment manifest 硬编码 Complete 等程序问题。
- 官方 Polymarket Rust SDK 0.6.0 高层 typed stream 会隐藏 broadcast lag；SDK 同时公开 `ConnectionManager`、`SubscriptionManager`、`InterestTracker` 与 `WsMessage`。计划先做小型编译原型，只在官方 SDK 公共组件内实现单 receiver；若不成立则触发用户决策门。
- 远端 `ubuntu@3.255.156.183` 的 `data-collect-training-collector.service` 当前 enabled/active，运行旧 release `3a1b7254fc1c-aggtrade-v3-20260710T143830Z`，旧训练数据约 14GB。服务当前仍在写 2026-07-13，未执行停止、删除、重启或部署。
- 远端旧数据只有 2026-07-09、11、12 已核实存在完整本地镜像；06、07、08、10 和当前 13 日没有完整镜像。用户已明确授权实现复审通过后清除旧程序和旧数据；新计划据此采用白名单永久删除，不额外生成备份包或 inventory/validation 文件，本地既有备份保持不动。
- 本事项不重建归档/阿里云盘脚本。collector 取消自动 raw 删除，达到 50GB 时 fail closed；简单日拉取、打包、上传与已上传日清理由后续独立归档事项负责。
- 首轮正式计划审查为三路 FAIL：source/SDK 审查 Critical=`3`、Important=`5`；最小产物/边界审查 Critical=`0`、Important=`7`；远端部署安全审查 Critical=`4`、Important=`5`。按门禁保持未执行状态。
- 已接受并修订的阻塞项：精确 supersede 旧权威条款；SDK 无 force-close 时用有限重订阅后协调进程退出；按 market/asset freshness；UTC 日 barrier 与唯一 manifest owner；aggTrade 进程重启游标、pending buffer 与 REST 时间语义；RTDS 30 秒/future-skew 边界；depth runtime resnapshot；失败 gate 清理；nominal market 时槽；新 binary/unit 在停旧服务前预置校验；完整删除预检与 symlink 顺序；固定 CLI、SIGTERM 和 systemd 合同。
- 新部署前必须停用本机当前 enabled 但 service failed 的 `data-collect-new-datasets-archive.timer/service`，因为旧脚本依赖被取代 schema。极简归档替换仍按用户要求作为 collector 稳定后的独立事项，不让旧脚本处理新数据。
- 自审发现旧服务运行时必然可能持有 executable/当前数据文件，不能把“无打开句柄”设为停服前条件；部署门已改为停服前全目标结构/持有者核对，停服后且任何删除前再做全目标无句柄复核。
- 第二轮正式复审仍为三路 FAIL：source/SDK 审查 Critical=`0`、Important=`4`；最小产物/边界审查 Critical=`0`、Important=`4`；部署安全审查 Critical=`0`、Important=`8`。门禁继续关闭。
- 第二次修订补齐：Binance WS/REST 共有字段与 `M`；[Polymarket 官方 RTDS 文档](https://docs.polymarket.com/market-data/websocket/rtds)（访问日 2026-07-13）规定的 Chainlink `type=update`；单 receiver target asset 过滤；跨进程 dedupe 恢复；跨午夜 `unclean_exit_recovery`；唯一 repo unit 与删除 release/gate/retention shadow path；exit 75 coordinated finalize；失败 gate 立即清理；同一 MainPID 26 小时及 Binance connection 子窗口；阶段 4 独立审查。
- 第三轮正式复审仍为三路 FAIL：source/SDK 审查 Critical=`0`、Important=`2`；最小产物/边界审查 Critical=`0`、Important=`3`；部署安全审查 Critical=`0`、Important=`8`。门禁继续关闭。
- 本地已存在 ignored `data-collect/releases/train-data-collect-ver1-0/`（约 81MB）及 `.gitignore`。用户本次不可逆授权限定于数据采集服务器，因此第三次修订明确保留这两个本地用户对象、排除出权威 build/deploy 输入；未获另行授权不删除。
- 第三次修订补齐：[Binance 官方 WS](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~/)与[官方 REST](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market)中 `M` 的不同语义、nullable WS-only 字段、25,000-ID payload recovery；稳定 producer 跨 backoff 日 barrier；PM lifecycle 与 RTDS 60 分钟去重保证范围；exit 76/StartLimit；fsync tmp/rename/fsync dir；远端无副作用兼容探针；停服前预置回滚；阶段 4 FAIL 立即停写清理。
- 第四轮正式复审仍为三路 FAIL：source/代码范围审查 Critical=`0`、Important=`4`、Minor=`2`；最小产物审查 Critical=`1`、Important=`1`、Minor=`1`；部署/最终审查 Critical=`0`、Important=`5`、Minor=`1`。门禁继续关闭。
- 第四轮确认两条新代码事实：官方 SDK 0.6.0 的 RTDS `SubscriptionManager::subscribe` 也会捕获 `RecvError::Lagged(n)` 后继续，不能用于需要显式丢消息故障的采集；现有 Polymarket Parquet layout 会丢失 `BestBidAsk.spread` 以及 `NewMarket/MarketResolved` 的部分 typed 字段，不能重建跨进程 exact key。
- 第四次修订改为：CLOB 与 RTDS 都先证明官方公共 `ConnectionManager + SubscriptionManager + 单 receiver + state_receiver`；Polymarket 每行持久化完整 SDK typed canonical `source_native_payload_json`，内存使用 SHA-256 fingerprint；Binance merged availability 使用较晚 ingress receive time，unresolved gap 使用 continuity epoch；RTDS 去重窗口冻结为首次 collector receive time且重复不续期。
- 生产去重 hard bound 采用编译常量而不是新增配置或产物。2026-07-11/12 本地数据只读统计中，Polymarket 单 slug 原始行数最大分别为 `389,059` 与 `420,078`，因此固定每 active lifecycle `1,000,000` 个 SHA-256 fingerprint、同时最多两个 lifecycle；RTDS 单 UTC 小时原始行数最大分别为 `3,572` 与 `3,578`，因此固定 60 分钟窗口 `100,000` 个 key。达到上限统一 gap + coordinated finalize + exit 76，不做静默淘汰。
- 第四次部署修订使用 digest-named exclusive staging，区分首次切换与已知失败版本重试；停旧服务前重新核验当下 unit/cgroup/executable 身份；production binary 从已 push commit 的 detached clean worktree 构建；阶段 5 发现正确性问题必须立即 disable/协调停服，不能让错误版本继续写入。
- 第四次修订冻结前只读复核：远端旧主服务仍为 enabled/active，MainPID=`980080`、NRestarts=`4`，executable 仍是旧 release 且 SHA-256=`78c59f31ce8a9803d72c791c1ff066ced6660f7072137e6719b0dd9570c3058e`，旧 training 数据仍约 `14G`；本地旧 archive timer 仍 enabled/active，配套 service 仍 failed。未执行 stop/disable/delete/deploy。
- 当前状态：`REVISION_4_AWAITING_REVIEW`；尚未修改采集代码、停止服务、删除数据或部署。

## 2026-07-14：第五轮审查、用户决策与第五次修订

- 第五轮正式复审仍为三路 FAIL：source/代码范围审查 Critical=`0`、Important=`4`、Minor=`1`；最小产物/资源合同审查 Critical=`0`、Important=`4`、Minor=`0`；部署/最终审查 Critical=`0`、Important=`6`、Minor=`0`。门禁继续关闭，未修改采集代码或远端状态。
- 用户明确批准三项冲突处理：以最新聚焦需求取代 `rebuild-core-demand.md` 中 data-collect per-run manifest/validation verdict/required-files 条款；Binance merged row 同时保留 WS 与 REST 两个真实 ingress receive time，并以较晚值作为整行 availability；批准基于官方 SDK 0.6.0 的最小固定版本 parser-error patch，同时允许 SDK 内部 outgoing/ping 控制通道作为有限生产者约束下的唯一 unbounded 类型例外。
- `demand/rebuild-core-demand.md` 已同步为：collector 每 UTC 日只维护一个事实 manifest；`data-pipeline` 对 closed 日做内容校验、语义去重和训练准入，并在自身 run manifest 引用输入日 manifest identity。training/offline-evaluation/paper/live/sim-calibration 的 run manifest 合同不变。
- SDK patch 来源冻结为 crates.io `polymarket_client_sdk_v2 0.6.0`，原始 `.crate` SHA-256=`e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`、官方 VCS commit=`3ae1aae5e9ded38f984464c9fc0f307f8a9f41fb`，仓库路径为 `third_party/polymarket_client_sdk_v2-0.6.0-p1/`。允许的实质改动只限 `ConnectionManager` sticky parser fault/terminal exit，以及 CLOB interested array element all-or-error；禁止 raw WS/REST、手写订阅或扩大 patch。
- 第五次修订当时把项目自有 data-bearing 缓冲冻结为全进程 `100,000 rows + 128 MiB`、2 秒 permit timeout，writer row group 为 `50,000 rows + 16 MiB`，并曾把恢复 projection 写成 `4,096 rows + 16 MiB`；第六轮审查证明后两者对大单行不能同时成立，第六次修订已在下文用 1MiB row hard cap 与 16-row recovery 取代该恢复表述。全程不增加 sidecar、report 或 receipt。
- Polymarket 去重上限修订为每 lifecycle `2,000,000` 个 `[u8;32]` SHA-256、最多 current+next 两组；message-level fingerprint 必须在 row fan-out 前检查。该容量在 `5,000 unique events/s` 下覆盖 400 秒，超过 5 分钟市场加 T-30 的约 330 秒合同；本地 2026-07-11/12 观察峰值仍分别为 389,059/420,078 rows/slug。
- Binance continuity recovery 改为先对账并原子关闭所有早于当前日的 open manifest，再选择当前日 open 或此前最近 closed manifest 为 base，叠加 finalized 文件并跨 UTC 日反向恢复最近 25,000 ID；当日 manifest/首个 Parquet 尚未产生时不再误判为空根。merged row 新增语义不同的 `websocket_timestamp_received`、`rest_backfill_timestamp_received`，`timestamp_received` 为两者最大值。
- production build 改为已 push commit 的 detached clean worktree + 独立 target + `--locked` + `DATA_COLLECT_GIT_COMMIT=<完整 commit>`，构建后再次检查 HEAD/clean，再经本地 digest-named staging 原子安装固定 binary；不复用主仓库 target cache。
- 远端部署状态机改为 4.2 全部门只检查 staging，不提前覆盖 final；失败入口明确区分 `failed_gate`、`failed_long_running`、`partial_replace`。停止并无句柄后才依次原子替换 final binary/unit，任何部分替换都记录 old/new digest 实际组合并保持 inactive。
- 删除白名单补入 `/home/ubuntu/btc-updown-marketmaker/data-collect/current`，它只允许指向 `/home/ubuntu/btc-updown-marketmaker/data-collect/releases/3a1b7254fc1c-aggtrade-v3-20260710T143830Z`；条件性空目录只允许对绝对路径 `/home/ubuntu/btc-updown-marketmaker/data-collect/releases` 执行 `rmdir`。停服前后都必须枚举所有递归删除目标自身与 descendant mount，发现 bind/submount 或 namespace 不可确认立即停止。
- 2026-07-14 只读复核的旧主 unit 基线：`FragmentPath=/home/ubuntu/.config/systemd/user/data-collect-training-collector.service`；unit file type=`regular file`、owner=`ubuntu`、group=`ubuntu`、mode=`0644`；unit SHA-256=`0440846d3feb79ea3897889ec818c42c7eca1ba1943d6601a46c90f82113c3bb`；`ControlGroup=/user.slice/user-1000.slice/user@1000.service/app.slice/data-collect-training-collector.service`；executable=`/home/ubuntu/btc-updown-marketmaker/data-collect/releases/3a1b7254fc1c-aggtrade-v3-20260710T143830Z/bin/train-data-collect-ver1-0`；executable SHA-256=`78c59f31ce8a9803d72c791c1ff066ced6660f7072137e6719b0dd9570c3058e`；current symlink 及 target 如上。规范化 ExecStart argv 为 `/usr/bin/env bash -lc exec /home/ubuntu/btc-updown-marketmaker/data-collect/current/bin/train-data-collect-ver1-0 --dataset-root /home/ubuntu/btc-updown-marketmaker/datasets --run-id training-longrun-$(date -u +%Y%m%dT%H%M%SZ) --collector-region eu-west-1 --run-forever --retention-delete-mode enabled --retention-threshold-bytes 50000000000`；启动时间和 MainPID 不属于冻结 identity。
- 第六轮三路复审仍为 FAIL：source/代码范围 Critical=`0`、Important=`3`、Minor=`0`；最小产物/资源 Critical=`0`、Important=`1`、Minor=`1`；部署 Critical=`0`、Important=`1`、Minor=`0`。门禁继续关闭。
- 第六次修订增加 vendor allowlist diff：在自动删除的临时目录解包已校验原始 `.crate` 并比较全树，只允许 `src/ws/connection.rs` 与 `src/clob/ws/types/response.rs` 不同；不生成 diff 报告文件。
- 2026-07-11/12 的 537 个 `polymarket_market` Parquet、`143,198,804` 行只读扫描中，既有所有 string 字段单行合计最大 `2,470` bytes，单个 bids/asks 最大 `2,055` bytes。基于该证据，第六次修订增加 `MAX_TYPED_ROW_BYTES=1,048,576`（超过观察最大值约 424 倍）；row group 保持 16MiB logical hard bound，恢复 batch 改为最多 16 行，从而消除 16MiB 与原 128MiB 单行之间的不可执行冲突。全局 in-flight 128MiB 不变。
- 旧四源合并 `2k-5k events/s` 处理能力明确保留，5k 同时只作为容量 sizing 上界；复用自动删除临时目录中的 benchmark，汇总只写 changelog，不恢复 profiler/ResourceWatermark/report artifact。
- 第六次修订三路独立复审全部 PASS：source/代码范围、最小产物/资源、构建/部署三路均为 Critical=`0`、Important=`0`、Minor=`0`；冻结 demand SHA=`2a0f7e0604bf83129d3ca6774a6266927c7d333e3abcb75279ad4bb031782d15`、plan SHA=`eea9dd3d3e0fc1daa7730cd0a39b14a898dc459e0bb32d4febeac9a55de48d5d`、changelog SHA=`a41812813e9ef93bd1f041595e3015259dc4595408678cdc55410d23a7887b1e`。计划审查门通过。
- 同次只读复核旧服务仍为 enabled/active，MainPID=`980080`、NRestarts=`4`；未 stop、disable、delete、deploy。文档状态更新为 `REVIEW_PASSED`，可以进入代码实现；远端门继续关闭。
