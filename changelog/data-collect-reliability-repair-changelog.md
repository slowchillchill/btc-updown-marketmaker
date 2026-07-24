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

## 2026-07-14：阶段 2 SDK public API 决策门

- 实现 SDK patch contract test 时得到确定编译证据：官方 `SimpleParser` 是 `#[non_exhaustive] pub struct SimpleParser;`，没有 `Default` 或公开构造器；Rust 因此禁止下游 crate 使用隐式 unit-struct 构造。公开的 RTDS `SubscriptionManager::new` 却精确要求 `ConnectionManager<RtdsMessage, SimpleParser>`，而高层 RTDS Client 不公开内部 connection，原两文件 patch 无法实现已批准的官方低层组合。
- 按计划 2.1.5 暂停后，用户批准把 vendored SDK allowlist 从两个文件扩大为三个文件；新增的唯一允许路径为 `src/rtds/subscription.rs`，唯一允许语义为给 `SimpleParser` 增加 `Default` 构造能力，不改变 parser、subscription 或平台行为。`src/ws/connection.rs` 的 sticky parser fault/terminal exit 与 `src/clob/ws/types/response.rs` 的 interested array all-or-error 边界不变。
- 当前 allowlist 恰为 `src/ws/connection.rs`、`src/clob/ws/types/response.rs`、`src/rtds/subscription.rs`。仍禁止 raw WebSocket/REST、手写订阅、unsafe 构造或第四处 vendor 修改。官方上游贡献另由隔离子任务只做本地 Draft PR 准备；未获最终确认前禁止 fork、push、创建 Issue/PR 或其他外部写入。

## 2026-07-14：阶段 2 任务 2.1 SDK patch 本地自审

- 已固定并唯一引用 crates.io `polymarket_client_sdk_v2 0.6.0` 的仓库内 p1；原始 `.crate` SHA-256 复核为 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`，自动删除临时目录中的全树比较只得到获批三个差异路径，`cargo tree -p data-collect -i polymarket_client_sdk_v2 --locked` 只得到该 path 实例。
- patch contract integration test 共 3 项通过：interested CLOB 数组采用 all-or-error 且合法前缀广播为 0；CLOB/RTDS whole-frame parser fault 均为 sticky、terminal `Disconnected` 且不自动重连；unknown/uninterested element 仍被忽略，下游可用 `SimpleParser::default()` 合法构造 RTDS 低层连接。
- vendored SDK `--locked --lib --features clob,rtds,ws` 原生测试 `148/148` 通过，`--test websocket reconnection` 的 transport 重连/重订阅测试 `4/4` 通过；官方 Rust 1.88 CI 的 default 与 all-features 两组 `--all-targets` clippy 均以 `-D warnings` 通过。workspace fmt、vendor fmt 与 `git diff --check` 通过。未生成独立验证报告，远端服务、程序和数据仍未改动；任务 2.1 独立复审通过前不进入依赖它的 collector 改造。

## 2026-07-14：阶段 2 任务 2.1 首轮实现复审与修正

- 首轮实现复审分为两路：provenance/scope/tests 审查 PASS，Critical=`0`、Important=`0`、Minor=`0`；correctness/production-integration 审查 FAIL，Critical=`0`、Important=`2`、Minor=`1`。两个 Important 分别是生产 `live_runtime.rs` 仍使用七条 CLOB 高层 filtered stream 和 RTDS 高层 client，尚未证明单 receiver/lag/terminal 合同；以及尚未实现串行订阅 actor、latest-value desired-state watch 与断线压力证明。Minor 是 parser 不重连测试窗口短于默认 backoff。
- 修正后新增项目公共低层 runtime：CLOB 精确组合官方 `ConnectionManager<WsMessage, Arc<InterestTracker>>`、CLOB `SubscriptionManager`、一个业务 broadcast receiver、state/fault receiver 和一个串行订阅 actor；desired assets 通过单槽 revision watch 合并，断线时只保留最新状态，恢复后 additions-before-removals，并丢弃未轮询的 SDK filtered stream。RTDS 精确组合官方 `ConnectionManager<RtdsMessage, SimpleParser>`、RTDS `SubscriptionManager` 和一个业务 receiver/state/fault；生产代码不再引用 CLOB/RTDS 高层 client。
- 第二轮 correctness 复审为 FAIL，Critical=`0`、Important=`1`、Minor=`0`；scope/resource 复审也独立发现同一问题：初版同时启动官方 reconnection handler 和项目 actor，真实重连时可能并发重发旧 assets 与应用新 diff，不是唯一串行订阅发送路径。门继续关闭。修正后不再启动 CLOB reconnection handler；唯一 actor 按 `ConnectionState::Connected { since }` 识别 connection epoch，每个新 epoch 创建新的官方 `SubscriptionManager`，只以空 server state 对最新 desired assets 发送一次官方订阅，动态更新仍在同一 actor 内应用最小 diff。
- CLOB runtime contract 的 7 项测试通过：首次断线期间 10,000 次 desired-state 更新恢复后只发送一次最终订阅且无中间 JSON 请求；已订阅旧 assets 后真实断线、离线更新 10,000 次再重连时只发送一次最终 assets，不重放已由旧 socket 消费的旧订阅；在线动态增删 asset 依次得到初始订阅、最小新增与最小退订；合法 typed 消息可达；`Lagged(n)`、sticky parser fault 优先分类与 retry-exhausted terminal disconnected 都可见。RTDS runtime contract 的 3 项测试通过：parser fault、`Lagged(n)` 与 retry-exhausted terminal disconnected 都可见，且 parser case 只有一次 manager subscription；整份 runtime contract 为 `10/10` 通过。
- 生产 RTDS 改为单进程只创建一个长生命周期 SDK session；健康期先等待 60 秒，之后最多在同一 manager 重订阅两次、每次等待 3 秒，第二次仍无消息则返回错误并由新进程重建，避免无法主动 close 的 SDK session 在进程内累积。该变更只提前接通计划 2.5.1 已冻结的有限恢复骨架；fresh matching payload、age/status 与去重语义仍属于后续任务 2.5，当前不宣称完成。
- parser 不重连测试改用 10--20ms 测试 backoff，并保留 300ms 观察窗口。actor error watch 改为保留结构化 `SdkStreamError`，channel-close 分支会重新读取 sticky fault/actor error/state；actor 顶部只观察 state，未处理的 terminal 版本仍由 `states.changed()` 消费。retry terminal 分类重复 `100/100`、runtime contract 重复 `20/20` 通过。
- 第三轮两路独立复审均为 FAIL，Critical=`0`、Important=`1`：官方 `ConnectionManager` 的 unbounded outgoing `sender_rx` 跨 connection epoch 复用，旧 socket Close 与 actor send 竞争时，旧 epoch 请求可能未被消费而在新连接先发送；当前新 epoch 再订阅 latest desired 不能移除旧请求带来的 stale assets，快速 transport flap 也缺少严格积压上界。该事实不由当前成功重连测试覆盖，已按计划 2.1.5 重新关闭 SDK 能力门，正在评估是否存在不改变已批准语义的官方公共 API 组合；未获用户新决策前不得扩大 vendor patch。
- 独立决策门评估确认，项目层“新 epoch 保守对账 + 稳定窗口”只能最终修正状态，不能阻止旧请求跨 epoch 或严格限制反复 flapping 的 queue；“每进程硬限制 epoch/mutation 后退出”也只能作为保险丝，并会在过严时改变普通 transport 自动重连合同。最小正确方向需要扩大 `src/ws/connection.rs` 的获批语义：outbound request 携带 connection generation，新连接只允许写入本 generation 请求，旧 generation 在写 socket 前丢弃，同 generation 保持 FIFO；初次连接行为不变，Connected 后仍由官方 manager/唯一项目 actor重建 latest desired。该方案有改变其他调用方“断线请求延迟到下一连接”隐含行为的风险，必须补初连、CLOB/RTDS 重订阅、旧 generation 丢弃、同 generation FIFO、快速 flap 和认证订阅回归；当前等待用户批准或拒绝，不自行实施。
- 用户批准上述 connection-generation SDK patch。vendor 差异文件 allowlist 仍恰为原三个路径，不增加第四个文件；`connection.rs` 获批语义由 parser terminal 扩大为 parser terminal 加 outbound generation 隔离。需求和计划已同步冻结：generation 0 保留初连前请求，成功重连在发布 `Connected` 前切换 generation，socket writer 丢弃旧 generation、保留同 generation FIFO；必须在新增回归和两路独立复审通过前保持任务 2.1 与远端部署门关闭。
- 当前本地结果为 patch contract `3/3`、runtime contract `10/10`、data-collect lib `32/32`、clippy `-D warnings`、fmt 与 `git diff --check` 通过，但上述 Important 未关闭，任务 2.1 仍为 FAIL。远端旧服务、程序和数据仍未改动，也未新增验证报告或 sidecar。

## 2026-07-14：阶段 2 任务 2.1 connection-generation patch

- 按用户批准在既有 allowlist 的 `src/ws/connection.rs` 内实现私有 `OutboundRequest { generation, payload }` 与 `OutboundQueue`；公开 `send`/`send_authenticated` API 不变。generation 0 保留首次连接前已排队请求；每次后续成功握手先递增并以 Release 发布 generation，再发布 `Connected`；sender 以 Acquire 读取 generation；active socket writer 只写与本连接 generation 相同的请求，旧 generation 即使在切换后才完成 enqueue 也会被丢弃，同 generation 继续由原 mpsc FIFO 排序。generation 溢出时 terminal `Disconnected`，不允许 wrap 后误接收旧请求。
- 初版把 generation 作为新增函数参数，官方 Rust 1.88 `-D warnings` 因两个私有函数达到 8 个参数而失败；随后把 receiver、published generation 与 active generation 合并为单一私有 `OutboundQueue`，既恢复官方 lint，也把队列/generation 同步更新固化为一个内部不变量，没有增加公共抽象或第四个 vendor 差异文件。
- patch contract 新增 3 项 generation 证明，当前总计 `6/6`：generation 0 的 pre-connect 普通请求与 authenticated subscription 按 FIFO 到达；8 次受控快速 flap 中每个旧 generation 请求均未进入最终 socket，最终 generation 的普通与 authenticated 请求仍按 FIFO 到达；官方 authenticated CLOB `SubscriptionManager` 在新 generation 成功重订阅且 auth/market 不变。旧 generation 快速 flap 测试重复 `50/50`，authenticated 重订阅重复 `20/20` 通过。
- runtime contract 新增 RTDS 官方 manager 跨 generation 重订阅与 `wait_desired_applied` sticky parser fault 优先回归，当前总计 `12/12`；RTDS 重订阅重复 `20/20`、wait parser 优先重复 `50/50` 通过。CLOB 真实断线/latest desired、lag、terminal 与结构化 actor error 合同继续通过。
- data-collect 全量 lib/integration tests 通过，其中 lib `32/32`；vendored SDK lib `148/148`、all-features websocket reconnection `4/4` 通过；官方 nightly-2025-11-24 fmt、Rust 1.88 default/all-features 两组 `--all-targets` clippy `-D warnings`、workspace clippy/fmt、`git diff --check` 全部通过。自动删除临时目录中的原始 `.crate` 全树比较仍恰为获批三个文件，SDK dependency tree 仍只有仓库内 p1，vendor 无 `target/` 残留。
- 当前状态为 `TASK_2_1_GENERATION_PATCH_AWAITING_REVIEW`；以上均为本地测试事实，不代表 Task 2.1 已通过。两路独立复审前不进入 Task 2.2，远端旧服务、程序和数据仍未改动，也未新增独立报告、receipt 或 sidecar。

## 2026-07-14：阶段 2 任务 2.1 第四轮复审与修正

- 第四轮三路独立复审均为 FAIL。correctness 为 Critical=`0`、Important=`1`、Minor=`1`；scope/resource/test 为 Critical=`0`、Important=`1`、Minor=`0`；并发专项为 Critical=`0`、Important=`2`、Minor=`2`。共同 Important 是 generation equality filter 只会在 active socket writer dequeue 时丢弃旧请求；若每个 socket 握手成功后立即 Close，每个 `Connected` 触发的新控制请求可能来不及 dequeue，跨 epoch 复用的 unbounded queue 仍会线性积压。现有 server-side flap 测试只证明旧请求没有写入，不能证明内部 queue 不增长，因此门继续关闭。
- 并发专项另确认官方 RTDS `start_reconnection_handler()` 的首次轮询竞态：它在 spawned task 首次执行时才创建 state receiver 并初始化 `was_connected`；若 runtime 已在首个连接订阅、首个 socket 又在该 task 首次执行前断开，handler 会从 `Reconnecting` 开始并把下一次 `Connected` 当成第一次连接，不调用 `resubscribe_all()`。旧订阅又会被 generation filter 丢弃，稳定 socket 可在业务 watchdog 介入前保持未订阅。原测试先等待首个请求才 Close，遮蔽了该合法调度。
- 最小修正仍只改获批 vendor allowlist 中既有的 `src/ws/connection.rs`：后续握手成功时，`OutboundQueue` 在 published generation 仍为旧值期间捕获 `receiver.len()` 的有限快照并只 drain 该快照，再递增并 Release-store generation、最后发布 `Connected`。writer equality filter 保留，用于丢弃已经 load 旧 generation、但 drain 后才完成 enqueue 的有限 late-old 请求；有限快照避免持续 producer 饿死 `Connected` 发布。`send` 与 `send_authenticated` 的 Acquire load 同时前移到序列化之前，缩小序列化跨切代窗口，不改变公开签名、认证内容或平台语义。
- 新增 vendor 私有单元证明：10,000 条旧请求快照在切代发布前归零，late-old 与 current generation 可区分；10,000 次模拟 immediate-close 切代的 queue high-water 固定为 `1`、不随 epoch 增长；`u64::MAX` generation 不 wrap。SDK writer 及既有集成测试继续证明 late-old 在新 socket 写入为 0、当代 FIFO 不变。
- RTDS 不再启动官方 self-spawn reconnect handler。新增唯一项目 RTDS 串行 actor，以 latest-value watch 保存 `{revision, Subscription}`；每个新的 `Connected { since }` 创建新的官方 RTDS `SubscriptionManager`，从空 server state 只应用一次最新 desired；同 epoch 的两次业务重订阅也进入同一 actor，应用完成后才开始 3 秒业务等待。单一 `ConnectionManager`、单一业务 receiver、官方 typed subscription 与 parser/state/fault 路线不变，没有 raw WebSocket/REST、第二 session 或 shadow subscription。
- runtime contract 新增两项确定性回归：current-thread 中在 actor 首次 poll 前设置 RTDS desired 仍会应用；连接任务首次运行前的 10,000 次 desired 更新会合并为 `btc/usd-10000`，随后 8 次握手后立即 Close，最终稳定 epoch 只收到一次该最新 subscription。当前 targeted runtime contract 为 `14/14`，vendored SDK lib 为 `151/151`；尚需完成全量 lint/test/provenance 与修正后独立复审，状态为 `TASK_2_1_FOURTH_REVIEW_FIX_AWAITING_VALIDATION`。远端旧服务、程序和数据仍未改动。
- 修正后全量自审通过：data-collect lib `32/32`、patch contract `6/6`、runtime contract `14/14` 及其余全部 integration tests 通过；vendored SDK lib `151/151`、all-features websocket reconnection `4/4` 通过；workspace fmt/clippy、vendor nightly fmt、官方 Rust 1.88 default/all-features `--all-targets` clippy `-D warnings` 与 `git diff --check` 通过。RTDS immediate-close/latest desired、actor 首次 poll 前 desired、旧 generation flap 三项各重复 `50/50`。原始 `.crate` SHA-256 仍为 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`，临时解包全树比较仍恰为获批三个文件，dependency tree 只有仓库内 p1，vendor 无 `target/` 残留。当前状态更新为 `TASK_2_1_FIFTH_REVIEW_PENDING`；第五轮独立复审通过前不进入任务 2.2。

## 2026-07-14：阶段 2 任务 2.1 第五轮复审与修正

- 第五轮三路独立复审均为 FAIL。RTDS correctness 与 scope/test 各为 Critical=`0`、Important=`1`、Minor=`1`；generation 专项为 Critical=`0`、Important=`1`、Minor=`2`。代码 Important 是 CLOB/RTDS actor 只在 `desired_rx.changed()` 分支更新缓存：若 reconnect `Connected` 与 desired A→B 同时 pending 而 state 分支先执行，下一轮会先把缓存 A 发到新 socket，再消费 B；这违反“新 epoch 从空 server state 只应用一次 latest desired”。文档 Important 是计划停止条件仍残留 `parser-error-only patch` 旧称谓，与用户已批准的四项最小 patch 边界冲突。
- CLOB 与 RTDS actor 均在识别新的 `Connected { since }` 后、创建官方 manager 或发送任何请求之前，直接 `borrow_and_update()` 合并 watch 当前最新值。新增两个确定性 lib 单元回归：让 state 与 desired 同时 pending并先消费 state，随后用于新 epoch 的 market/RTDS desired 仍分别为最新 B。该线性化点之后才到达的更新继续作为同 epoch 动态 diff 处理，不增加第二 actor、sender、manager task 或 session。
- 计划停止条件已机械改为“需要扩大 `3.1`/`2.1.2` 已批准的四项最小 patch 边界”时停下，不再把当前 patch 错称为 parser-only。RTDS runtime contract 新增同 epoch `resubscribe()` 回归，直接证明方法返回前 actor 已按 FIFO enqueue `subscribe(initial) -> unsubscribe -> subscribe`；业务 3 秒等待因此从重订阅实际应用后开始。
- 同轮关闭 generation 专项两个 Minor：新增 sender 全部关闭时 `advance_generation()` 返回 `SenderClosed` 且 active/published generation 不变的单元回归；新增真实 loopback WebSocket 回归，在 generation 切换后才 enqueue 的 late-old 请求由 active writer 丢弃，server 第一条且唯一业务请求为 current generation。当前 targeted data-collect lib `34/34`、runtime contract `15/15`、vendored SDK lib `153/153` 通过；尚需重跑全量验证与第六轮独立复审，状态为 `TASK_2_1_FIFTH_REVIEW_FIX_AWAITING_VALIDATION`。远端旧服务、程序和数据仍未改动。
- 第五轮修正后的全量验证通过：data-collect lib `34/34`、patch contract `6/6`、runtime contract `15/15` 与全部 integration tests 通过；vendored SDK lib `153/153`、all-features websocket reconnection `4/4` 通过；workspace fmt/clippy、vendor nightly fmt、SDK default/all-features clippy `-D warnings` 与 `git diff --check` 通过。两个 new-epoch latest-watch 单元、RTDS 同 epoch resubscribe、vendor late-old loopback 各重复 `50/50`。原始 `.crate` SHA 与唯一依赖未变，全树仍恰为三个获批差异文件，vendor 无 `target/` 残留。状态更新为 `TASK_2_1_SIXTH_REVIEW_PENDING`；第六轮独立复审通过前仍不进入任务 2.2。

## 2026-07-14：阶段 2 任务 2.1 第六轮复审与修正

- 第六轮 generation 专项 PASS，Critical=`0`、Important=`0`、Minor=`0`；RTDS correctness 与 scope/test 均为 FAIL，Critical=`0`、Important=`1`、Minor=`1`。Important 不在业务实现，而在回归没有绑定真实修复点：第五轮两个 state-first 测试只手工调用 latest helper，删除 actor 新 epoch 的 refresh 仍会通过。Minor 是 RTDS 同 epoch测试只在方法返回后等待 server 最终收齐三条请求，错误的“提前返回、稍后发送”实现也可能通过。
- CLOB/RTDS actor 的 select 现以 state-first biased 顺序处理同时 ready 的状态与 desired；两个回归改为实际启动对应 actor 和真实官方 ConnectionManager，以手工 state watch 构造 `Reconnecting -> Connected`，在 actor 已等待后同步提交 desired B 与 Connected。真实 loopback socket 必须只收到一条请求，且第一条分别为 market asset `2` 与 RTDS filter `btc/usd-2`。删除任一 actor 新 epoch 的 `latest_watch_value()` 调用会改发缓存 A，使测试失败；测试现与生产修复点直接绑定。
- RTDS 同 epoch重订阅测试在 current-thread executor 上创建 `resubscribe()` future 后先同步 poll 一次，断言为 Pending；actor 不能在该 poll 内并发运行，因此任何不等待 applied revision 的提前返回实现都会直接失败。随后 await 成功并继续证明 server FIFO 为 `subscribe(initial) -> unsubscribe -> subscribe`。当前 targeted data-collect lib `34/34`、runtime contract `15/15` 通过；尚需全量验证与第七轮复审，状态为 `TASK_2_1_SIXTH_REVIEW_FIX_AWAITING_VALIDATION`。远端旧服务、程序和数据仍未改动。
- 第六轮修正后的 workspace fmt/clippy、`git diff --check`、data-collect lib `34/34`、patch contract `6/6`、runtime contract `15/15` 与全部 integration tests 通过；vendor 源码未再变化，前轮同一 connection SHA 的 lib `153/153`、all-features websocket reconnection `4/4`、两组 clippy 与三文件 allowlist 证据继续有效，vendor 无 `target/`。两个真实 actor/state-first loopback 回归与 RTDS resubscribe 返回边界各重复 `50/50`。状态更新为 `TASK_2_1_SEVENTH_REVIEW_PENDING`；第七轮通过前不进入任务 2.2。

## 2026-07-14：阶段 2 任务 2.1 第七轮复审与修正

- 第七轮 generation/文档边界审查 PASS，Critical=`0`、Important=`0`、Minor=`0`；correctness 与 scope/test 再次为 FAIL，Critical=`0`、Important=`1`、Minor=`1`。Important 是真实 actor 测试仍只用一次 `yield_now()` 猜测 actor 已读取缓存 A，而 Tokio 不保证让另一任务先 poll；合法调度下 actor 可首次直接读 B，使删除生产 refresh 点的 mutation 仍假通过。Minor 是 resubscribe 首次 poll Pending 只能排除零-await 立即返回，不能排除“yield 一次后在 actor 尚未应用时返回”。
- 两个 actor 回归移除 `yield_now()` 假设。手工 state 初始即为 epoch A 的 Connected，真实 actor 先发送 A，loopback server 收到 A 后通过 oneshot 建立确定性 actor-ready/cache-A 屏障；测试任务再在同一 current-thread 调度片段内同步提交 desired B 与不同 `since` 的 Connected。server 随后只允许一条新 epoch 请求且必须为 B。无论 actor 此时正位于 select 还是刚完成 A apply，删除新 epoch refresh 点都会先重放缓存 A并使断言失败。
- 保留同 epoch首次 poll Pending/FIFO 回归，并新增真实断线门：初始 subscription 到达后 server 关闭连接且暂不接受下一次 WebSocket handshake；`resubscribe()` future 在 actor 无 connected epoch 时经过 100ms timeout 仍不得完成，只有 server 放行重连、actor 在新 epoch 应用最新 subscription 后才返回。该测试可抓住 `replace -> yield_once -> Ok` 提前返回。当前 targeted data-collect lib `34/34`、runtime contract `16/16` 通过；两项 actor-ready回归、断线返回边界与同 epoch FIFO 各重复 `50/50`。尚需完整验证与第八轮复审，状态为 `TASK_2_1_SEVENTH_REVIEW_FIX_AWAITING_VALIDATION`。远端旧服务、程序和数据仍未改动。
- 第七轮修正后的完整 workspace 门禁通过：`cargo fmt --all -- --check`、`git diff --check`、data-collect 全 targets clippy `-D warnings`、lib `34/34`、patch contract `6/6`、runtime contract `16/16` 与其余全部 integration tests 均成功；vendor `target/` 不存在，`connection.rs` SHA-256 仍为 `002ca1ad43302bb8887230786bd8c38adc18fa62dc2385e5c7454bbb36d4d28c`。状态更新为 `TASK_2_1_EIGHTH_REVIEW_PENDING`；第八轮通过前不进入任务 2.2，远端旧服务、程序和数据仍未改动。

## 2026-07-14：阶段 2 任务 2.1 第八轮复审通过

- generation/文档边界、correctness/mutation、provenance/scope/tests 三路独立复审全部 PASS，均为 Critical=`0`、Important=`0`、Minor=`0`。审查确认两个真实 actor 回归的 initial-A wire receipt 屏障可确定性约束新 epoch refresh；RTDS 断连回归对同一 future 证明 actor 无 connected epoch 时 100ms 不得提前返回，恢复且实际应用后才完成。
- 第八轮复核结果：data-collect lib `34/34`、patch contract `6/6`、runtime contract `16/16`、vendor lib `153/153`、websocket reconnection `4/4` 通过，四项关键回归各重复 `20/20`；workspace/vendor fmt、workspace/vendor clippy `-D warnings`、`git diff --check` 均通过。原始 crate/VCS、唯一 path dependency、三文件 allowlist、四项 patch 边界与 production single receiver/single actor 均未回归；临时 target 已删除，无额外报告、receipt 或 sidecar。
- 任务 2.1 状态更新为 `TASK_2_1_REVIEW_PASSED`，允许严格串行进入任务 2.2。远端旧服务仍为 enabled/active，旧程序和旧数据均未改动；远端部署门继续关闭。

## 2026-07-14：阶段 2 任务 2.2 discovery、订阅与语义修复

- production discovery 改为 current 必需、next optional：current 验证成功后立即启动采集，不再被 next 的缺失、无效或 metadata 请求阻塞；next 在同一 lifecycle 内由至多一个可取消的异步 Gamma 查询每 10 秒独立发现，出现后通过既有单一 SDK actor 动态加入。rollover 用新的 desired asset 集合自然保留已预取并晋升的 next、退订过期 current；current-only、next 晚到、晋升和旧 asset removal 合同均有回归。
- 单 receiver dispatch 现返回精确 matched asset 集合：单 asset variant 只接受 current/next asset，mixed `PriceChange` 在写入前删除非 target entries，`NewMarket`/`MarketResolved` 至少一个 asset 匹配才写入；unmatched/global/user 消息写入 0 行且不能刷新健康状态。Gamma discovery 只写 metadata，production 不再创建 synthetic `market_discovery_switch`；legacy 输入即使出现也转换为 0 个 market rows，原生匹配 `new_market` 仍只产生一个 market row。
- 每个已订阅 asset 独立维护订阅后 30 秒首业务事件与后续 60 秒 freshness；next 尚不存在时没有虚假 watchdog，晚到 next 从实际订阅完成时独立计时，一个健康 asset 不能掩盖另一个。静默 asset 最多经同一 Market SDK actor 串行执行两次 `unsubscribe -> subscribe`，再次超时即返回错误，由后续 systemd 新进程恢复；`Lagged(n)`、parser fault 与 terminal state 仍沿 Task 2.1 typed fatal 路径退出。
- Task 2.2 自审门通过：workspace/data-collect fmt、全 targets clippy `-D warnings`、`git diff --check`、lib `39/39`、market discovery contract `6/6`、SDK runtime contract `17/17` 及其余全部 integration tests 通过。新增真实 dispatch 回归证明 mixed target 过滤、unmatched lifecycle 0 写入与 native `new_market` 单行；SDK loopback 回归证明 market resubscribe 的 subscribe/unsubscribe/subscribe 顺序。状态为 `TASK_2_2_SELF_REVIEW_PASSED`，允许严格串行进入任务 2.3；远端仍未改动。

## 2026-07-14：官方 SDK 上游 Draft PR

- 用户最终批准外部 fork、push 和正式创建 Draft PR。隔离干净工作区已向官方 `Polymarket/rs-clob-client-v2:main` 创建 [Draft PR #81](https://github.com/Polymarket/rs-clob-client-v2/pull/81)，fork 为 `qiuqiuaiweb3/rs-clob-client-v2`，分支为 `fix/rtds-simple-parser-downstream`，commit 为 `cacec3e0240f0210cb9d43c495953e335f69e16d`；GitHub API 复核 `draft=true`。
- 上游变更只包含 `SimpleParser: Default` 与一个下游构造回归，共两个文件、`+9/-1`；没有带入本项目专属的 parser terminal、CLOB partial-array 或 connection-generation patch。Rust 1.88 default/all-features build、tests、clippy，RTDS focused test、nightly rustfmt、`cargo sort` 与 diff check 均通过。该外部贡献不改变本仓库 Task 2.1 的三文件 vendor allowlist，也不代表 collector 已获部署批准。

## 2026-07-14：阶段 2 任务 2.3 精确去重与跨进程恢复

- 新增统一的有界精确去重实现：Polymarket 按 current/next lifecycle 各自保存最多 2,000,000 个完整 SDK typed canonical payload 的 SHA-256，rollover 保留晋升 lifecycle、删除退役 lifecycle；Binance 按最近 25,000 个 native aggregate trade ID 保存同源完整 payload fingerprint，完全相同才删除、同 ID 不同 payload 保留并记 conflict；RTDS 使用首次接受时 collector receive time 锚定的 60 分钟/100,000-key 窗口，重复不续期、乱序恢复不改变最早锚点。
- Polymarket 七种 market `WsMessage` 变体均在 target fan-out 前生成一次不含 receive time/derived slug 的完整 `source_native_payload_json`；typed schema 升为 `polymarket_orderbook_v2` 并把该列设为必需、可解析。当前与前一 UTC 日 finalized Parquet 使用 16-row projection 恢复 active lifecycle 集合；测试证明仅 numeric text 精度或旧 layout 曾丢弃字段不同的 payload 不会误删，进程 A 写入后进程 B 的同 lifecycle 重放只保留一次。
- Binance aggTrade typed schema 升为 `binance_aggtrade_v4`，保存含 WS-only `M` 的完整 source-native payload；重启从 finalized Parquet 尾部按数值 part 顺序反向恢复最近 25,000 个 ID。RTDS schema 升为 `polymarket_rtds_chainlink_btc_usd_v3`，保存 `{source,topic,type,timestamp,payload}` canonical 并从当前/前一日 finalized Parquet 恢复 60 分钟窗口。所有恢复都在连接网络前执行，只读取必要列，不生成 key sidecar、报告或 receipt。
- Task 2.3 自审门通过：`cargo fmt --all`、data-collect 全部 lib/bin/integration tests、全 targets clippy `-D warnings`、fmt check 与 `git diff --check` 均成功；lib 为 `46/46`，dedupe contract 为 `4/4`。测试临时数据自动删除，生产路径没有新增验证产物；远端旧服务、程序和数据仍未改动。状态为 `TASK_2_3_SELF_REVIEW_PASSED`，允许严格串行进入任务 2.4。

## 2026-07-14：阶段 2 任务 2.4 Binance aggTrade 连续性核心修复

- 按 [Binance 官方 REST market data 文档](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market) 实现 `GET /api/v3/aggTrades` 回补：固定 `BTCUSDT`、`fromId` inclusive、每页最多 1,000 行，完整响应收到后才采样 REST receive time；严格校验 ID、顺序、边界与 `a,p,q,f,l,T,m,M`。回补最多 3 次且总 elapsed 不超过 20 秒，429、timeout、空响应、乱序和超过 25,000 行均进入明确失败路径。
- 新增跨网络连接保持的 continuity epoch：`epoch_start_id/last_observed_id/last_contiguous_id` 分别保存新连续区间起点、已接受的最大向前 ID 和已补齐尾部；pending WS 行会立即单调推进 observed 但不伪进 contiguous，旧 replay 不改变游标。回补最终失败或 25,000-row pending overflow 会保存精确 unresolved range，从第一条已接受的未来 WS 事实开启新 epoch，且保留更高的已观察 pending ID，后续不再追逐永久旧 gap。
- WS 与 REST overlap 只比较共有同义字段 `a,p,q,f,l,T,m`。一致时写一条 `websocket_rest_merged`，分别保留 WS `M`、REST `M` 与两个真实 ingress receive time，并以较晚值作为整行 availability；不一致时保留两条事实并记录 conflict。typed schema 升为 `binance_aggtrade_v5`，REST-only 的 `event_type/event_time/stream` 为 null，未伪造 WS 字段。
- 启动网络前先对账并原子关闭早于当前日的 open manifest，再从 current open 或最近 closed continuity base 叠加 finalized Parquet；dedupe 反向读取最新文件/row group，并把 WS/REST fingerprint 限定在共同的最近 25,000 native-ID 窗口。测试覆盖 prior-day open recovery-close、当日 manifest 无文件、Parquet rename 领先 manifest、同一大 row group 的固定尾窗、跨进程 payload identity 和无残留 `manifest.json.tmp`。
- 自审通过：data-collect 全部 lib/bin/integration tests、全 targets clippy `-D warnings`、fmt check 与 `git diff --check` 均成功；lib 为 `50/50`，aggTrade continuity contract 为 `11/11`。唯一日 manifest owner、动态 backfill/merge/continuity 计数与跨午夜 barrier 仍按既定依赖顺序在任务 2.7 接线并接受最终阶段审查，因此当前状态为 `TASK_2_4_CORE_SELF_REVIEW_PASSED_PENDING_TASK_2_7_MANIFEST_WIRING`；远端旧服务、程序和数据仍未改动。

## 2026-07-14：阶段 2 任务 2.5 RTDS、metadata 与 depth 时间语义修复

- RTDS typed schema 升为 `polymarket_rtds_chainlink_btc_usd_v4`，保存真实 `message_type`、按 collector receive time 计算的 `age_ms` 与 `freshness_status`。只有 timestamp 有效、`topic=crypto_prices_chainlink`、`type=update`、`symbol=btc/usd` 且 `-5_000 <= age_ms <= 30_000` 的新 payload 才能刷新业务健康；持续 wrong type/topic/symbol、陈旧、未来偏移、无效时间戳或重复 payload 都不能延长 watchdog。60 秒后只经唯一 RTDS actor 重订阅两次，每次实际应用后等待 3 秒，再无恢复即返回错误，由后续任务 2.6 的协调退出与 systemd 新进程恢复。
- 删除 metadata 的重复 `token_ids`、`metadata_recv_time_utc` 以及 depth 的派生 `local_u_after`；内部 discovery 仍使用 SDK 原生 `clob_token_ids` 建立可信 UP/DOWN 映射。Gamma 主动查询和 Binance depth snapshot 分别在发起官方 SDK/HTTP 请求前采样 fetch-start，在完整响应接收后采样 receive；WebSocket `new_market` 与 fallback metadata 的 fetch-start 保持 null，不再用 receive time 伪造。
- Binance depth snapshot 固定官方 endpoint，并以最多 3 次有限重试处理 429/timeout；snapshot 或 resnapshot 最终失败会向上返回错误。完整 mock REST+WebSocket 回归证明 snapshot 100、更新 101--103、gap 105、resnapshot 104、桥接 105--106 的写入与触发顺序，同时证明真实 fetch-start/receive 边界和已删除列不会回归。
- Task 2.5 自审门通过：data-collect 全部 lib/bin/integration tests、全 targets clippy `-D warnings`、fmt check 与 `git diff --check` 均成功；lib 为 `57/57`。RTDS row 还会交叉验证 `age_ms` 和状态与实际 timestamp/topic/type/symbol 一致；针对性 RTDS 测试为 `6/6`。动态日 manifest 的 freshness/depth 计数仍由唯一 owner 在任务 2.7 接线，因此当前状态为 `TASK_2_5_CORE_SELF_REVIEW_PASSED_PENDING_TASK_2_7_MANIFEST_WIRING`；远端旧服务、程序和数据仍未改动。

## 2026-07-14：阶段 2 任务 2.6 writer、恢复监督与协调退出

- 新增全进程共享的 `100,000 rows + 134,217,728 bytes` 待写预算和 `1,048,576 bytes` 单行上限；每条 typed event 在 source ingress 前按结构本体及全部 owned string capacity 计量并取得 permits，随后携带同一 reservation 依次移动 through ingress、batch 与 writer，只有 writer 消费 event 后才释放。owned payload 的复制独立计量，Binance WS pending 与 REST backfill 行也纳入同一预算；预算等待、queue send、writer command、parser 与 finalize 错误不再记录后继续。
- 四条网络采集路径改为有限 inner recovery cycle 加无限 outer supervisor：inner 使用 bounded attempts、120 秒 elapsed、指数 backoff、cap 与稳定 jitter，outer cycle 同样有非零 capped delay。只有新的匹配业务 payload 才重置恢复状态；ping、wrong topic/type、重复、未应用 depth update 与 connection-only 事件不能伪造恢复。Polymarket SDK lag/parser/subscription actor 仍保持进程级 fatal 边界，Binance 普通 transport/snapshot 网络失败则进入外层恢复而不永久终止 source。
- 启动时只在规范化 `training/typed_raw` 根内清理当前 UID 的 data-collect 进程未持有的 `.tmp`、`.running` 与历史 running/digesting Parquet；目录或子项 symlink、越界路径及仍被进程打开的文件均 fail closed。writer 提前完成、channel 缺失/关闭、flush 超时和 finalize 超时现在都会被主 runtime 观测，并进入同一 drain/finalize 错误路径。
- SIGTERM 与 runtime fault 共用 90 秒应用内截止：停止 source、drain ingress/batch、finalize writer，并区分 operator 成功日志与 fault 非零结果；capacity 使用 75、不可恢复 config/path/recovery/dedupe 使用 76、其余错误使用 1。Task 2.7 仍需删除旧 retention/receipt 生产路径、把 50GB 检测只接到该协调退出，并由唯一 manifest owner 完成 open-day 更新及最终 systemd 75/76 策略。
- Task 2.6 核心自审门通过：workspace 全部 lib/bin/integration tests、全 targets clippy `-D warnings`、fmt 与 `git diff --check` 均成功；data-collect lib 为 `64/64`，新增测试覆盖精确 row/byte 边界、复制计量、短背压、closed queue、recovery cycle、stale temp containment/持有句柄、writer 提前失败和 75/76/1 映射。当前状态为 `TASK_2_6_CORE_SELF_REVIEW_PASSED_PENDING_TASK_2_7_PRODUCTION_WIRING`；远端旧服务、程序和数据仍未改动。

## 2026-07-14：阶段 2 任务 2.7 writer 与生产产物收敛

- 训练 cold path 现只有一个生产 binary、一个 repo-tracked user unit 和一个 effective output root。训练 CLI 删除 `run-id`/retention 参数，`--help`/`--version` 在创建 Tokio runtime、解析数据根或连网前无副作用返回；仓库已删除旧 archive service/timer/脚本、release export、training gate example、`.service.example`、retention admin/policy 与相关合同测试。paper/live 仍保留独立源码 binary 与运行边界，未被训练部署路径捕获。
- writer 只按 UTC hour 结束文件，同小时重启用 `part-N`；`50,000 rows/16,777,216 logical bytes` 只结束 row group。四个稳定 logical producer 通过同一 ingress queue 发送日 barrier，六个 writer 回传 finalize ack；唯一 manifest owner 串行合并 finalized delta，按 `fsync tmp -> atomic rename -> fsync parent` 更新每 UTC 日唯一 `manifest.json`。断线恢复跨 UTC 日时，gap 只分别归入受影响的相邻两日，不再把其他日的 gap 复制到当日。
- 日 manifest schema 为 `data_collect_daily_manifest_v3`；它保留 finalized file facts、六个 source 的零值默认项、`written_rows/backfill_rows/structural_dedupe_dropped_events/lag_events/reconnect_events/dropped_rows`、有界 gap 与紧凑 Binance continuity epoch。早于当日的 open manifest 在网络 ingress 前以磁盘 finalized Parquet 对账并用 `unclean_exit_recovery` 关闭；当日 SIGTERM/capacity/fault 只 finalize 并更新 open manifest。数据根用现有目录的独占进程锁防止两个 owner，不新建 lock/receipt/sidecar 文件。
- 生产目录合同收敛为小时 Parquet、每日唯一 manifest 和瞬时 `.running`/`manifest.json.tmp`；不生成 segment/run manifest、startup/retention/resource/backpressure receipt、coverage/gap/reconnect/rollover sidecar 或 validation report。达到 50GB 后复用协调 drain/finalize，更新 open manifest 后退出 75；不发生 collector 内删除。验收矩阵中残留的 ResourceWatermark/reconnect sidecar 训练生产表述已同步删除，旧 Phase 0 文档仍作为历史事实保留。
- 完整本地门禁通过：`cargo test -p data-collect --lib --tests -- --nocapture` 中 lib `71/71` 及全部 integration contracts 成功；workspace fmt、data-collect 全 targets clippy `-D warnings`、三个 binary build 与 `git diff --check` 通过。四源合成 throughput benchmark 两个窗口分别约 `1.66M`/`1.73M events/s`，source hourly layout 四组 20,000-row probe 通过，当次临时目录均自动删除。官方 crate SHA 仍为 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`，全树比较仍只有获批三文件，依赖树只有仓库 p1；vendored SDK 的 locked lib `15/15`、Rust 1.88 default/all-features 全 targets clippy `-D warnings` 与格式检查通过，临时 target 已删除。本机 Rust 1.88 未安装 rustfmt，因此 vendor 格式检查使用已安装的 stable rustfmt，未修改系统 toolchain。
- 2026-07-14 自审后只读复核：远端旧 collector 仍为 enabled/active，MainPID=`980080`、NRestarts=`4`，executable SHA=`78c59f31ce8a9803d72c791c1ff066ced6660f7072137e6719b0dd9570c3058e`，unit SHA=`0440846d3feb79ea3897889ec818c42c7eca1ba1943d6601a46c90f82113c3bb`，`current` 仍指向旧 release；未 stop、disable、delete、deploy。本机旧 archive timer 仍为 enabled/active，只在远端新 collector 部署前按计划停用。当前状态为 `TASK_2_7_SELF_REVIEW_PASSED_PENDING_INDEPENDENT_REVIEW`。

## 2026-07-14：阶段 2 首轮独立 source 审查修正与验证

- 首轮独立 source/SDK correctness 审查为 FAIL，Critical=`0`、Important=`4`、Minor=`1`。五项具体问题是：aggTrade 的 pending/backfill 未在日 barrier 与 SIGTERM 前完整 fenced/drained；depth resnapshot 会丢弃触发 resnapshot 的 frame，UTC 日切换也有同类风险；Binance frame parser/schema 错误被当成可恢复 transport 结果吞掉；depth resnapshot 后重新采样 collector receive time；RTDS timestamp 未显式拒绝秒级或其他非毫秒单位。独立审查门保持关闭，未进入部署。
- aggTrade 新增 source-local coordinated flush：先中止并丢弃正在执行的 REST backfill，再逐段关闭全部 pending gap，依次保留 WS 事实、记录 unresolved range 并写入旧日 continuity，且 flush 期间不启动新 backfill。日切换与停机都经 bounded control channel 请求该动作；runtime 在 90 秒协调退出期限内继续 drain ingress，避免 source 因队列背压无法完成 flush。invalid JSON、缺失 `e` 和非 `aggTrade` event type 现在都是 process-fatal，只有官方定义的 `serverShutdown` 等 transport 事件进入重连。
- depth 在解析并保留原 frame 与首次 `recv_time_utc_ms` 后才判断 UTC 日或 stitch gap resnapshot；snapshot 成功后以同一 frame 重新执行 bridge，不再重读、丢弃或重采样。一次 resnapshot 后仍无法 bridge 会返回 recovery，避免无限循环。parser/schema 错误提升为 process-fatal；snapshot、触发 frame 与 barrier 的顺序有真实 mock HTTP+WebSocket 回归。
- RTDS outer timestamp 与 payload timestamp 均增加显式 Unix-millisecond 范围检查，秒级值固定为 `invalid_timestamp`。该语义由 [Polymarket 官方 RTDS 文档](https://docs.polymarket.com/market-data/websocket/rtds)（访问日 2026-07-14）确认；Binance 的毫秒时间、`serverShutdown`、aggTrade payload 和 depth snapshot/缓存/replay 顺序由 [Binance 官方 WebSocket 文档](https://developers.binance.com/zh-CN/docs/products/spot/testnet/web-socket-streams)（访问日 2026-07-14）交叉核对。
- 修正后的自审继续发现通用 producer 在等待 queue lock 时可能与 barrier 互相等待，因此 barrier 等待会继续 poll source 直至其旧事件完成；随后针对 depth in-flight HTTP resnapshot 与 aggTrade source-local pending 增加专用 fence。初版专用 fence 使用 `yield_now()` 交还调度权，关键测试顺序重复时首轮即复现“确认后下一条业务事件抢在 barrier 前入队”，证明该方式不构成严格栅栏。最终改为两阶段握手：source 完成 flush/fence 后回复并暂停，barrier 实际入队后 owner 才发送恢复信号；depth 与 aggTrade 的后续业务 frame 因而都不能越过 barrier。
- 修正后 6 个关键 barrier/flush/stop 回归严格顺序重复 20 轮，共 `120/120` 通过。`cargo test -p data-collect --lib --tests -- --nocapture` 中 lib `77/77`、aggTrade continuity `12/12`、SDK patch `6/6`、SDK runtime `17/17` 及其余 integration contracts 全部通过；workspace fmt、all-targets clippy `-D warnings`、check 与 `git diff --check` 通过。三个 production binary 可构建；throughput 两个窗口分别为 `1,621,136.47` 与 `1,769,058.56 events/s`，四组 20,000-row source-hourly probe 的 finalize failure 为 0，临时目录均已删除。
- vendored SDK 原始 `.crate` SHA-256 仍为 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`；自动删除临时目录中的完整树比较严格断言只有已批准的 `src/ws/connection.rs`、`src/clob/ws/types/response.rs`、`src/rtds/subscription.rs` 三个文件不同。vendor locked lib `15/15`、default/all-features clippy `-D warnings` 与格式检查通过，仓库内无 vendor `target/`。
- 本轮修正落实既有 demand/plan 合同，没有改变需求、范围、生产产物或部署门，因此需求与计划无需语义修订。远端旧 collector、本机旧 archive timer、旧程序和旧数据均未改动。当前状态为 `TASK_2_7_REVIEW_FIX_VALIDATED_PENDING_SOURCE_REVIEW`。

## 2026-07-14：阶段 2 source 独立复审 FAIL 与第二次修正

- 修正后的 source/SDK 独立复审仍为 FAIL，Critical=`0`、Important=`3`、Minor=`1`。三个 Important 是：depth 把官方 `serverShutdown` 当成 schema fatal；RTDS 秒级正数 timestamp 虽分类为 `invalid_timestamp`，但 hourly validator 仍把它当作“有效时间”并拒绝该状态；已订阅 `crypto_prices_chainlink` 的 payload schema 错误只记 gap 后返回 `Ok(false)`。Minor 是 aggTrade 20 秒总 backfill budget 只在重试 sleep 前检查，未中止接近单请求 timeout 的在途请求。未能确定复现的午夜/SIGTERM gap 归日猜测未定级、未据此扩大实现。
- 三个 Important 均先加入行为回归并确认旧实现失败：outer-seconds/payload-seconds typed raw validation、有效 RTDS 消息后紧接 malformed payload 的 process-fatal、raw/combined depth `serverShutdown` transport exit。随后把 Unix 毫秒合理范围收敛为 `source_format` 内唯一共享函数，classifier 与 hourly validator 使用同一判断；正数秒级异常现在能作为 `invalid_timestamp` typed fact 持久化，不会伪装 fresh/stale，也不会令 writer 失败。
- RTDS 现在先执行已订阅 payload 的 typed decode，再进入 fingerprint dedupe；decode 失败仍记录有界 gap，但立即向上传播错误并进入协调非零退出，不再静默省略事实。depth 在任何业务 schema 解析前只识别 raw/combined envelope 的官方 `e=serverShutdown` 并返回可重连 transport exit；其他 malformed JSON、缺字段或未知 event type 仍为 process-fatal。
- aggTrade backfill 使用单一 absolute Tokio deadline 包裹整个分页请求 future；每轮开始及 retry sleep 前都核对剩余预算，deadline 到达会取消在途请求并返回 `RetryBudgetExhausted`。测试以 30ms 注入预算证明 5 秒 client timeout 的在途请求被第一轮 deadline 取消；production 仍固定 3 attempts、20 秒总预算，没有新增配置。
- 四项新增回归严格顺序重复 20 轮，共 `80/80` 通过。完整 `cargo test -p data-collect --lib --tests -- --nocapture` 中 lib `80/80`、aggTrade continuity `12/12`、source-hourly `9/9`、SDK patch `6/6`、SDK runtime `17/17` 及其余 integration contracts 全部通过；all-targets clippy `-D warnings`、fmt、check、三个 binary build 与 `git diff --check` 均通过。vendor 内容和三文件 allowlist 未变，未生成新报告、sidecar、receipt 或持久验证目录。
- 本次修正仍是既有 demand/plan 的直接实现，不改变语义或部署门。远端旧 collector、本机旧 archive timer、旧程序和旧数据均未改动。当前状态为 `TASK_2_7_SECOND_REVIEW_FIX_VALIDATED_PENDING_SOURCE_REREVIEW`。

## 2026-07-14：阶段 2 source/SDK 最终独立复审通过

- 新的 source/SDK 独立复审最终 PASS，Critical=`0`、Important=`0`、Minor=`0`。审查逐行确认 aggTrade 日切/SIGTERM coordinated flush、两阶段 pause/barrier/resume、停机 ingress drain、depth trigger frame/原 receive time/serverShutdown、RTDS timestamp/schema fatal 与 aggTrade absolute deadline 均闭合，未发现新的越 barrier、死锁、静默丢事实或错误恢复路径。
- 审查独立重算原始 crate SHA-256=`e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`，确认 vendor 全树无新增/缺失且只允许三个文件不同；依赖树只有仓库内 `polymarket_client_sdk_v2 0.6.0-p1` 一个实例，production 只组合官方 `ConnectionManager`、`SubscriptionManager`、单 receiver/state/fault，没有 Polymarket raw WS/REST 或 shadow subscription，vendor 内无 `target/`。
- 可协调范围外的 SIGKILL/主机掉电仍由 open manifest 与下次启动 recovery-close 处理，符合已批准合同，不构成审查问题。source/SDK 审查门现已通过；远端旧服务、旧程序、旧数据与本机 archive timer 仍未改动。当前状态为 `TASK_2_7_SOURCE_REVIEW_PASSED_PENDING_WRITER_SCHEMA_DEPLOY_REVIEW`。

## 2026-07-14：阶段 2 writer/schema/deployment 独立审查 FAIL 与命名决策门

- writer/schema/deployment 独立审查为 FAIL，Critical=`0`、Important=`5`、Minor=`0`。审查门保持关闭；未进入 commit、push、停服、删除或部署。
- Important 1：`daily_manifest` 重启对账只按磁盘现状重建文件集合，会静默删除 manifest 已记录但磁盘缺失的文件，并用当前 bytes/rows/SHA 覆盖同路径旧事实；孤儿 Parquet 恢复又缺少 schema、时间边界和事件计数等完整事实。必须改为既有事实严格比对，缺失/变异 fail closed；孤儿文件只有能完整重建所有要求事实时才允许纳入。
- Important 2 触发强制方案不一致决策门：已批准需求与计划冻结 `<source>_HH.parquet`、`<source>_HH-part-N.parquet`，实现、测试与部署说明却使用 `<source>_YYYY-MM-DDTHH.parquet`、`<source>_YYYY-MM-DDTHH--part=N.parquet`。两者不能同时成立；在用户明确选择权威命名之前，禁止自行修改需求、计划或实现来调和冲突。
- Important 3：daily manifest 的 `required_typed_fields_present` 由字段/事件计数计算 PASS/FAIL 式判断，属于用户已明确交给 data-pipeline 的数据校验结论，不是采集事实；必须从 schema、计算和测试中移除，保留原始事实计数。
- Important 4：writer queue 缺失、断连、timeout 等路径只更新临时 tracker，tracker 最终结果被丢弃，没有把实际丢弃量按来源/接收日合入 daily manifest `dropped_rows`；writer parse/flush/finalize 失败也需按实际未持久化量记入统一 operational counter，并增加 manifest 级回归。
- Important 5：stale temporary cleanup 只检查当前 UID 且 holder 命令行含 `data-collect` 的进程，不能证明其他同 UID/其他 UID holder 不存在；同时未严格验证 root、日期目录、source 目录和临时文件 owner。必须验证路径链 owner，并检查所有可检查 holder；无法证明无人持有时 fail closed，补非 data-collect 同 UID holder 与 owner mismatch 回归。
- 已确认通过的边界包括 Parquet fsync/atomic rename/目录同步、row-group/global/source inflight hard bounds、4 producer+6 writer 日关闭屏障、唯一 manifest owner、paper/live 独立入口、单一 training unit、退出码与部署身份/mount/holder/artifact allowlist 门。
- 当前状态为 `TASK_2_7_WRITER_SCHEMA_DEPLOY_REVIEW_FAILED_AWAITING_NAMING_DECISION`。远端旧 collector、本机 archive timer、旧程序和旧数据均未改动。

## 2026-07-14：小时文件命名决策

- 用户选择保留已批准需求与计划的简洁命名：同一 UTC 日目录内首个小时文件为 `<source>_HH.parquet`，同小时重启分片为 `<source>_HH-part-N.parquet`。不得反向修改需求或计划来迁就当前实现中的冗余日期与 `--part=N` 形式。
- 后续修正必须同步实现、测试和部署说明；daily manifest 的相对路径及恢复排序也必须消费同一命名，不保留兼容写入或第二套生产命名。当前状态为 `TASK_2_7_NAMING_DECIDED_REVIEW_FIX_IN_PROGRESS`；远端状态仍未改动。

## 2026-07-14：writer/schema/deployment 审查问题修正与自审

- 小时文件实现、排序、测试和部署说明已统一为用户批准的 `<source>_HH.parquet` 与 `<source>_HH-part-N.parquet`；UTC 日继续由父目录表达，manifest 中的 logical `utc_hour` 继续保存完整日期小时。生产代码不接受或生成冗余日期、`--part=N` 或其他兼容命名。
- finalized Parquet 现在在 footer 中嵌入同一份完整恢复事实：source、UTC hour、part、schema、rows、首末 receive/source timestamp、逐事件类型计数和 collection facts。重启对账严格要求 manifest 已记录路径仍存在且事实逐项相同；缺失、重复或变异立即 fail closed。磁盘孤儿文件只有在 approved filename、目录、Parquet metadata、时间范围、事件计数和完整 footer facts 全部一致时才允许恢复，bytes 与 SHA-256 仍由当前文件重新计算。
- 从 daily manifest schema、计算和测试中删除 `required_typed_fields_present`，只保留 raw metadata complete/unavailable 等采集事实计数，不再由 collector 输出训练准入或 PASS/FAIL 判断。
- writer 缺失、channel 断开、背压 timeout、parser/write/finalize 失败现在把确实未持久化的行按 event source 与 receive UTC 日合入唯一 manifest 的 `dropped_rows`。writer failure 会在同一短 gate 下停止接收并 drain 队列，避免失败判定与新命令入队竞态；已经成功 finalized、仅 manifest send 失败的文件不虚报为 dropped，由严格 orphan recovery 接管。
- stale temporary cleanup 现在逐级验证 dataset root、`training/typed_raw`、UTC 日目录、source 目录和目标文件均属于当前运行 UID且不是 symlink；同时扫描可访问的全部 `/proc/*/fd` holder，并在删除前取得 Linux write lease。根据 [Linux `F_SETLEASE` 文档](https://man7.org/linux/man-pages/man2/F_SETLEASE.2const.html)（访问日 2026-07-14），存在其他打开描述符时 write lease 不能取得；holder 无法排除、lease 不支持或状态不可确认均 fail closed。回归覆盖当前进程 holder、普通 `sh` 同 UID holder、owner mismatch 与 symlink escape。
- 稳定性循环暴露 `binance_ping_does_not_reset_business_message_freshness` 的测试服务器会把客户端按预期 freshness timeout 断开产生的 `ConnectionReset` 当成 panic，导致偶发未接受第二次连接。最小修正只让夹具在旧连接 Ping 发送失败后继续等待重连；业务断言仍要求 Ping 不刷新时钟并在 1 秒无业务消息后重连。该定向测试随后连续 `50/50` 通过，完整 lib 加 stale-cleanup 循环再连续 20 轮通过，共 lib `1660/1660`、stale cleanup `80/80`。
- 本轮修正后，data-collect lib `83/83`、全部 integration contracts、all-targets clippy `-D warnings`、locked check、workspace fmt、三个 binary build、两个四源 throughput benchmark与四组 source-hourly probe均通过；benchmark 为 `1,765,139.79` 与 `1,791,968.02 events/s`，finalize failure 为 0。vendored SDK 原始 crate SHA、仅三文件 allowlist、Rust 1.88 default/all-features clippy、locked lib `15/15` 与唯一仓库 p1 依赖仍通过，临时 target/probe 均删除。
- 以上修正没有增加 production report、receipt、sidecar 或第二套 manifest；生产合同仍只有小时 Parquet、每日唯一 manifest、瞬时临时文件和 journald。当前状态为 `TASK_2_7_WRITER_SCHEMA_DEPLOY_FIX_VALIDATED_PENDING_REREVIEW`；远端旧 collector、本机 archive timer、旧程序和旧数据均未改动。

## 2026-07-14：writer/schema/deployment 第二轮复审与修正

- 第二轮 writer/schema/deployment 独立复审为 FAIL，Critical=`0`、Important=`2`、Minor=`1`。两个 Important 分别是：prior closed-day manifest 仍未在恢复扫描前严格验证 schema、完整 identity 与精确文件集合；多 writer 停止遇到首个 error/timeout 会提前返回，使迟到 drop/finalize 可能越过 counter drain 与 manifest shutdown。Minor 是部署 README 与验收矩阵仍各残留一处 `part=N`。提交、push 与远端门继续关闭。
- daily manifest 现在只接受 `data_collect_daily_manifest_v3`，并要求非空 `build_commit`、binary SHA、config digest、host 与 region identity。所有 prior closed-day 在 continuity/dedupe 扫描前只读重算完整磁盘 facts，manifest 文件缺失、变异或额外孤儿均 fail closed且不重写；prior open-day 可恢复孤儿及 current-day open/无 manifest rename 空窗也必须由 Parquet footer 证明与所属 manifest 或当前 runtime identity 完全一致。未来 UTC 日目录不得参与恢复。
- 多 writer 停止现在先关闭全部 sender，再在一个共享 deadline 内收集每个 writer 的 success、terminal error、disconnect 或 timeout，并在全部 writer 到达终态后才聚合错误、drain operational counters 和发送 manifest Shutdown。一个 writer 先失败不会丢弃其他 handle；其他 writer 的迟到 finalize/drop 会先进入 owner/counter。若 deadline 内仍有 writer 未终止，则不 drain counters、不发送 Shutdown，直接 abort manifest owner、保留 open-day 恢复语义并失败退出，避免产生关闭后孤儿或不完整计数。
- 部署 README、验收矩阵及当前 changelog 语义已统一为 `part-N`；历史审查记录中的旧 `--part=N` 只作为已否决冲突的事实保留。新增回归覆盖 closed-day missing/mutated/extra、错误 schema、缺失 identity、orphan footer identity mismatch，以及一个 writer 先失败而另一个迟到终止并记录 manifest-level drop。
- 针对性恢复与多 writer 回归连续 20 轮共 `60/60` 通过；随后 data-collect 全量 lib/integration tests、all-targets clippy `-D warnings`、locked check、workspace fmt 与 `git diff --check` 全部通过。当前状态为 `TASK_2_7_SECOND_WRITER_SCHEMA_FIX_VALIDATED_PENDING_FINAL_REREVIEW`；远端旧 collector、本机 archive timer、旧程序和旧数据均未改动。

## 2026-07-14：启动恢复午夜竞态复审与修正

- 下一轮 writer/schema/deployment 最终复审仍为 FAIL，Critical=`0`、Important=`1`、Minor=`0`。审查确认此前 schema/identity/closed-day 对账、多 writer 终态、命名、validation verdict、drop、stale cleanup 与部署门均已闭合，但发现启动只采样一次 UTC 日：若严格恢复从 23:59 跨到 00:01，旧日刚被 reconcile 为 open，而 producer 会从新日计算首个 barrier，可能令旧日一直 open 到下次重启。提交、push 与远端门继续关闭。
- 启动恢复现为 day-stable 循环：在同一 blocking pre-ingress 任务内采样 UTC 日、执行全部 prior/current manifest 与 Binance continuity/dedupe 恢复、reconcile 当前 open manifest，再次采样 UTC 日；如已跨日则以新日期完整重跑，让刚成为 prior 的 open 日先以 `unclean_exit_recovery` 关闭。日期向后跳变直接 fail closed；只有前后日期一致才返回当前 manifest 与 next-boundary cursor。
- 四个 logical producer 不再各自用启动瞬间重新初始化 UTC 日，而是共同继承已确认 startup day 的 next-boundary。每次 poll source 前先同步检查 cursor：若启动确认后又跨日，Polymarket、RTDS、Binance depth 与 aggTrade 都先把已越过日的 barrier 入队，再允许 source future 首次 poll；尚未启动的 depth/aggTrade 没有 pending/in-flight 状态，因此初始 catch-up 不需要先连接网络，正常运行后的 barrier 仍保留原两阶段 fence/flush 握手。
- 可控时钟回归把恢复从 `2026-07-13 23:59` 推进到 `2026-07-14 00:00`，断言 07-13 在任何 source 构造前已 closed、07-14 为 open；四 producer 顺序回归断言 startup barrier 严格先于首条 source event。两项回归连续 20 轮共 `40/40` 通过；随后全量 lib/integration tests、all-targets clippy `-D warnings`、locked check、workspace fmt 与 `git diff --check` 全部通过。当前状态为 `TASK_2_7_MIDNIGHT_RECOVERY_FIX_VALIDATED_PENDING_FINAL_REREVIEW`；远端状态仍未改动。

## 2026-07-14：阶段 3 实现最终独立复审通过

- source/SDK 最终独立复审已先行 PASS，Critical=`0`、Important=`0`、Minor=`0`；writer/schema/deployment 在午夜竞态修正后的最终只读复审也已 PASS，Critical=`0`、Important=`0`、Minor=`0`。两路审查均未修改文件、未访问远端。
- writer/schema/deployment 最终复审逐行确认 day-stable 启动恢复循环、同一稳定迭代绑定的 current manifest/aggTrade recovery/initial boundary、四 producer 首次 poll 前 barrier catch-up，以及正常运行中的 generic 锁安全等待、depth fence/flush、aggTrade 两阶段 flush/resume 均成立。
- 审查同时确认严格 manifest schema/identity/file 对账、continuity/dedupe、writer terminal gate、批准的 `<source>_HH.parquet` / `<source>_HH-part-N.parquet` 命名、validation verdict 移除、writer drop 入账、stale temporary owner/holder gate、最小部署产物和 systemd unit 合同均未回退。
- 阶段 2 实现与阶段 3 两路独立复审现已全部通过，状态更新为 `IMPLEMENTATION_REVIEW_PASSED_READY_FOR_TASK_3_3_1`。远端旧 collector、旧程序、旧数据与本机 archive timer 仍未改动；只有本事项 commit/push 完成并通过阶段 4 预检后，才允许执行破坏性切换。

## 2026-07-14：阶段 3 提交推送与阶段 4.1 生产构建

- 本事项实现精确提交为 `3538f6b5ecd64a9b1e297367a1b8f501a6f3ad27`（`fix(data-collect): rebuild reliable training collector`），并已推送到 `origin/after-data-plan`；远端 ref 与本地完整 commit 一致。用户既有 `AGENTS.md`、`demand/rebuild-core-demand.md` 和 `推荐开发顺序.md` 未进入提交。
- 从该已推送 commit 创建 `/tmp/data-collect-build-3538f6b` detached clean worktree，设置 `DATA_COLLECT_GIT_COMMIT=3538f6b5ecd64a9b1e297367a1b8f501a6f3ad27`，使用独立 `/tmp/data-collect-target-3538f6b` 执行 locked release build。构建前后 HEAD 均精确匹配且 worktree clean，binary 中可检出完整嵌入 commit。
- 唯一生产产物 `target/release/data-collect-training-collector` 大小为 `25,081,576` bytes，mode=`0775`，SHA-256=`69fec19ef6ba03e39e7aa67814bab944b42128379849e0f0b153732d8eb26b40`；本地 digest-named staging 复核同 SHA 后原子替换固定路径。临时 detached worktree、独立 target 与 local staging 均已删除，没有生成 release receipt 或验证报告。
- 当前状态为 `TASK_4_1_PRODUCTION_BUILD_PASSED_PENDING_REMOTE_PREFLIGHT`。远端旧 collector、旧程序、旧数据与本机 archive timer 仍未改动。

## 2026-07-14：阶段 4.2 首次切换预检与新产物预置

- 首次切换入口成立：远端 final binary 与本轮两个 digest-named staging 路径原先均不存在；旧主服务仍为 enabled/active/running，MainPID=`980080`、NRestarts=`4`、ControlGroup=`/user.slice/user-1000.slice/user@1000.service/app.slice/data-collect-training-collector.service`。旧 unit SHA-256=`0440846d3feb79ea3897889ec818c42c7eca1ba1943d6601a46c90f82113c3bb`，PID executable SHA-256=`78c59f31ce8a9803d72c791c1ff066ced6660f7072137e6719b0dd9570c3058e`，`current` 仍只指向获批旧 release；argv、cgroup、owner、mode 与冻结基线一致。
- 三个旧 release、旧 marker、旧 gate/main unit 和四个旧数据目录均为 `ubuntu` 所有的预期文件/目录，递归内容没有非 `ubuntu` owner、symlink 或异常文件类型。全机 10 个可见 mount namespace 的 mountinfo 均未发现删除目标自身或 descendant mount；全 `/proc` holder 扫描只发现冻结 PID `980080` 持有其旧 executable 与当小时 typed raw 文件。
- 清理前只读大小：三个旧 release 分别为 `84,059,917`、`84,158,290`、`84,164,941` bytes；旧 `typed_raw` 为 `16,385,520,036` bytes，`run_manifests` 为 `2,148,080` bytes，`runtime_artifacts` 为 `6,172` bytes，retention validation 目录为 `0` bytes。未创建备份或 inventory 文件，已批准删除后不完整旧日不可恢复。
- 在 final 路径保持不存在、旧进程继续运行的前提下，新建空 `data-collect/bin`，以 exclusive-create 语义预置 binary `data-collect-training-collector.upload-69fec19e...26b40` 与 gate unit `data-collect-training-collector.staging-69fec19e...26b40.service`。远端 binary SHA-256=`69fec19ef6ba03e39e7aa67814bab944b42128379849e0f0b153732d8eb26b40`、size=`25,081,576`、mode=`0755`；gate unit SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`、mode=`0644`。
- 远端 staging binary 为 x86-64 ELF，loader=`/lib64/ld-linux-x86-64.so.2`，全部动态库可解析，主机 glibc=`2.39`；直接执行 `--version` 与 `--help` 均成功。gate unit 由 repo 权威 unit 只把 `Restart=on-failure` 机械改为 `Restart=no`，临时改指 staging binary 的 `systemd-analyze --user verify` 通过且临时文件已删除；生产 staging unit 本身未被改写。
- 当前状态为 `TASK_4_2_STAGED_PREFLIGHT_PASSED_PENDING_FINAL_IDENTITY_RESAMPLE`。旧服务、旧程序、旧数据尚未停止或删除，本机 archive timer 尚未停用。

## 2026-07-14：阶段 4.3 白名单清理、首次 gate 失败与安全收口

- 最终身份、路径、owner/type、全机 10 个 mount namespace 与 holder 门分别在 `04:42:00Z` 至 `04:42:51Z` 再次通过；本机旧 archive timer 随后 disabled/inactive，失败 archive service reset 为 inactive/dead，本地 `/home/chillchill/new-datasets` 保留未删。
- 在 `04:45:20Z` 窗口开始切换。旧主服务与旧 gate unit 均成功 disable/stop；旧主进程 Result=`success`、ExecMainStatus=`0`、MainPID=`0`。停服后 holder=0，10 个 mount namespace 与全部白名单 owner/type 再次通过；随后严格删除已批准的三个旧 release、`current`、marker、旧 gate unit、`16,385,520,036` bytes typed raw、run manifests、runtime artifacts 与空 retention-validation 目录，并重建空 `training/typed_raw`。没有删除白名单外对象，也未创建备份或 inventory 文件。
- 预置 binary 与 gate unit 分别原子替换 final 路径并复核 SHA；gate unit SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`、`Restart=no`。新进程在 `04:48:23Z` 启动，但于 `04:48:24Z` 收到 RTDS 缺少 `timestamp` 的真实 payload后输出 `source_task_error:polymarket_rtds_chainlink_btc_usd:RTDS Chainlink payload invalid: missing field \`timestamp\``，以 status `1` fail closed。systemd 未重启，MainPID 回到 0。
- 失败 gate 已成功 finalize 三个文件；manifest 与 Parquet footer 均证明 build commit=`3538f6b5ecd64a9b1e297367a1b8f501a6f3ad27`、binary SHA-256=`69fec19ef6ba03e39e7aa67814bab944b42128379849e0f0b153732d8eb26b40`、host=`ip-172-26-10-0`、region=`eu-west-1`，因此该 UTC-day 为本轮 build 独占。确认无 holder/descendant mount 后，按步骤 4.4.3 删除整个失败日并重建空 typed raw 根。
- 当前远端是已记录 `failed_gate`：service inactive/dead、disabled、MainPID=`0`；final binary SHA-256=`69fec19ef6ba03e39e7aa67814bab944b42128379849e0f0b153732d8eb26b40`，final gate unit SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，无 staging、无旧 release/current/gate unit、生产数据根为空。本机旧 archive timer 继续 disabled/inactive。
- 当前状态为 `PRODUCTION_GATE_FAILED_RETURNED_TO_STAGE_2`。不得重启该 failed pair；先核实缺失 timestamp payload 的真实类型与官方 RTDS 合同，做最小代码修正并重新执行阶段 2/3 全量验证和独立复审。

## 2026-07-14：首次 gate RTDS 根因确认与最小修正

- Polymarket 官方 RTDS 页面要求价格 payload 含 `symbol/timestamp/value`，同时官方 `real-time-data-client` README 明确说明使用 filter 时 CryptoPrice 会先下发 initial data dump，形状为 `{symbol,data:[...]}`。核对的官方 TypeScript client main commit 为 `c937d9c11cdd2b771aa4818392a1b6dda65c25de`。
- 通过仓库既有 `RtdsSdkRuntime`、即官方 Rust SDK `ConnectionManager<RtdsMessage, SimpleParser>` 与单 receiver 做一次 ignored live probe，首帧精确为 `topic=crypto_prices,type=subscribe,payload={symbol:"btc/usd",data:[...]}`，随后连续 7 帧为 `topic=crypto_prices_chainlink,type=update` 且含合法 payload timestamp/value。临时 probe 测试已删除，未使用 raw WebSocket 或第二套订阅实现。
- 根因是 collector 在检查 envelope topic/type 之前无条件把每条 SDK 消息 decode 为 `ChainlinkPrice`；因此官方初始历史 snapshot 被错误升级为目标 update schema fatal。最小修正只在非目标 topic/type 且 payload 又不是单价形状时返回 ignored：不进入 dedupe、不写 typed 行、不记 gap、不刷新 freshness。price-shaped wrong-topic/wrong-type 仍保留原 typed invalid 状态；精确目标 `crypto_prices_chainlink/update` 缺字段仍记录 gap 并 process-fatal。
- 新增真实 snapshot 形状回归，断言 snapshot `Ok(false)`、queue 空、gap 空，紧接正常 update `Ok(true)` 并入队；既有目标 malformed fatal 回归继续通过。随后 20 秒真实官方 SDK 端到端临时运行成功，六个 source kind 全部 finalize，其中 RTDS 写入 `15` 行；临时目录自动删除。
- 当前状态为 `TASK_2_5_RTDS_INITIAL_SNAPSHOT_FIX_VALIDATED_PENDING_FULL_REVIEW`。远端保持已记录 inactive `failed_gate`，生产数据根为空，本机 archive timer 保持 disabled/inactive。

## 2026-07-14：RTDS gate 修正完整本地门禁

- data-collect 全量 lib/integration tests 通过，lib=`89/89`；all-targets clippy `-D warnings`、locked check、workspace fmt 与 `git diff --check` 全部通过。SDK patch contract=`6/6`，vendor tree 未生成 `target/`，获批三文件 patch 边界未变化。
- 20 秒真实官方 SDK 临时运行是本轮唯一 live source 验证，已自动删除数据根；没有保留 probe 源码、报告、sidecar 或 receipt。修正 diff 只涉及 RTDS decode-error 分类、真实 snapshot 回归及对应需求/计划/changelog，不修改 typed schema、SDK vendor、writer、manifest、systemd unit 或部署路径。
- 当前状态为 `RTDS_GATE_FIX_VALIDATED_PENDING_REVIEW`。先进行 source/SDK 独立复审，再进行 failed-gate retry/部署状态独立复审；严格串行，二者全部 PASS 前远端 failed pair 不得启动或替换。

## 2026-07-14：RTDS gate 修正首轮独立审查措辞修正

- source/SDK 独立审查为 FAIL，Critical=`0`、Important=`1`、Minor=`0`。唯一问题是需求、计划与测试名声称“在 Chainlink payload decode 前忽略”，而真实代码是先尝试 typed decode，再在 decode 失败升级为 fatal 前依据 envelope 归类 ignored；运行时修复本身、target update fatal、wrong-topic/type typed 行、watchdog、dedupe、single receiver 与 SDK fault/lag 边界均通过。
- 按审查建议保留当前单一 serde decode，不新增重复 raw JSON shape parser；只把需求、计划、测试名和 changelog 改为准确的“decode 失败后、升级 fatal 前按 envelope 分类”。运行语义与 production binary 输入代码没有其他变化。
- 当前状态为 `RTDS_GATE_FIX_WORDING_ALIGNED_PENDING_SOURCE_REREVIEW`。远端 failed pair 与空数据根未改动。

## 2026-07-14：RTDS gate 修正 source/SDK 独立复审通过

- 对唯一措辞问题完成修正后，原 source/SDK 审查代理复审为 PASS，Critical=`0`、Important=`0`、Minor=`0`；审查未修改文件、未访问远端。
- 复审确认实现只使用一次 `ChainlinkPrice` decode：decode 失败且 envelope 不是精确 `crypto_prices_chainlink/update` 时返回 ignored；精确目标 envelope 的 schema 错误仍 process-fatal。需求、计划、测试名与该真实顺序一致。
- 针对真实 snapshot ignored 与目标 malformed fatal 的两条回归分别 `1/1` 通过，workspace fmt 与 `git diff --check` 通过。当前状态为 `RTDS_GATE_FIX_SOURCE_REVIEW_PASSED_PENDING_FAILED_GATE_STATE_REVIEW`；远端 failed pair 未在本轮复审中启动或替换。

## 2026-07-14：RTDS gate 修正两路独立复审全部通过

- failed-gate retry/部署状态第二路独立审查为 PASS，Critical=`0`、Important=`0`、Minor=`0`；只读审查未修改本机或远端状态。
- `2026-07-14T05:10:53Z` 远端事实为 service inactive/dead、disabled、MainPID=`0`，enable symlink 与 holder 均不存在；failed binary SHA-256=`69fec19ef6ba03e39e7aa67814bab944b42128379849e0f0b153732d8eb26b40`，failed gate unit SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c` 且 `Restart=no`。
- `training/typed_raw` 为空，无 upload/staging 残留；旧 releases/current/marker/gate unit 与旧 run/runtime/retention 数据目录均不存在。本机旧 archive timer disabled/inactive，`/home/chillchill/new-datasets` 保留。
- 审查确认失败重试必须跳过已完成的旧对象删除，先提交/push、从已推送 commit 干净构建、重新预置并通过精确身份门；不得复用或直接启动当前 failed pair。当前状态为 `RTDS_GATE_FIX_IMPLEMENTATION_REVIEW_PASSED_READY_FOR_COMMIT`。
- 提交前再次执行完整门禁：data-collect lib=`89/89` 与全部 integration contracts 通过，SDK patch=`6/6`、SDK runtime=`17/17`；all-targets locked clippy `-D warnings`、all-targets locked check、workspace fmt、`git diff --check` 均退出 0。依赖树只有仓库内 `polymarket_client_sdk_v2 0.6.0-p1`，vendor tree 未生成 `target/`。

## 2026-07-14：RTDS gate 修正提交推送与重试生产构建

- RTDS gate 修正提交为 `ef34c223fc12a6b95ef5c65cc4eebc1f629dc6d1`（`fix(data-collect): ignore RTDS subscription snapshots`），已推送到 `origin/after-data-plan`；本地 HEAD、remote-tracking ref 与 `ls-remote` 三者精确一致。用户既有 `AGENTS.md`、`demand/rebuild-core-demand.md` 和 `推荐开发顺序.md` 未进入提交。
- 从该已推送 commit 创建 `/tmp/data-collect-build-ef34c22` detached clean worktree，设置 `DATA_COLLECT_GIT_COMMIT=ef34c223fc12a6b95ef5c65cc4eebc1f629dc6d1`，以独立 `/tmp/data-collect-target-ef34c22` 执行 locked release build。构建后 HEAD 未变、worktree clean，binary 可检出完整嵌入 commit。
- 新 binary 大小=`25,081,400` bytes、SHA-256=`249dc5802fb0aba185adf365b0e0523f86d39e626a9440fbd05164bfd8c5d9f6`、固定本地 mode=`0755`；经 digest-named local staging 复核后原子替换 `target/release/data-collect-training-collector`。临时 detached worktree、独立 target 与 staging 已删除，没有创建 release receipt 或验证报告。
- 当前状态为 `TASK_4_1_RETRY_BUILD_PASSED_PENDING_REMOTE_RESTAGE`。远端仍是第二路审查确认的 inactive failed_gate；新 binary 尚未上传、替换或启动。

## 2026-07-14：失败重试预检、预置与原子替换

- 重试前再次精确核实 failed_gate：service inactive/dead、disabled、MainPID=`0`、无 holder；旧 failed binary/unit SHA 分别为 `69fec19ef6ba03e39e7aa67814bab944b42128379849e0f0b153732d8eb26b40` 与 `76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，数据根为空、无 staging，旧 release/current/gate unit 与旧 run/runtime/retention 目录仍不存在。本机 archive timer disabled/inactive，既有下载数据保留。
- `systemctl --user reset-failed <unit>` 因 disabled/inactive unit 已被 manager 卸载而返回 `Unit ... not loaded`；现场 `Result=success` 且不存在 failed state，因此没有为形式要求加载或启动 unit。远端 `dd` 不支持 `oflag=excl`，首次 upload 在创建文件前失败；随后使用操作系统 `O_CREAT|O_EXCL` 打开语义创建精确 digest-named staging，没有改动 final pair或遗留失败文件。
- 新 staging binary 大小=`25,081,400`、mode=`0755`、SHA-256=`249dc5802fb0aba185adf365b0e0523f86d39e626a9440fbd05164bfd8c5d9f6`；远端确认为 x86-64 ELF、loader=`/lib64/ld-linux-x86-64.so.2`、动态库无 `not found`、glibc=`2.39`，`--version`/`--help` 成功且数据根保持为空。staging gate unit mode=`0644`、SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`、`Restart=no`，远端 `systemd-analyze --user verify` 通过。
- 在最终 inactive/no-holder/empty-root 与 old/new digest 门再次通过后，于 `2026-07-14T05:22:28Z` 依次同目录原子替换 final binary 与 unit并 daemon-reload。final binary/unit SHA 精确为上述新 binary 与 gate unit，staging 全部消失；service 继续 inactive/dead、disabled、MainPID=`0`，尚未启动。
- 当前状态为 `TASK_4_4_RETRY_PAIR_INSTALLED_PENDING_START_WINDOW`。只能在非 23 UTC 的 45–50 分钟窗口启动，并以同一 `Restart=no` 进程跨下一 UTC 小时运行至少 15 分钟。

## 2026-07-14：RTDS 修复生产 gate 启动

- 启动前最终复采样再次确认 service inactive/dead、disabled、MainPID=`0`、无 holder、数据根为空、无 staging，final binary/unit SHA 分别为 `249dc5802fb0aba185adf365b0e0523f86d39e626a9440fbd05164bfd8c5d9f6` 与 `76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，unit 为 `Restart=no`。
- 在非 23 UTC 的批准窗口 `2026-07-14T05:45:53Z` 启动 gate。MainPID=`1007135`，`/proc/1007135/exe` SHA 精确等于新 binary，effective argv 与冻结 ExecStart 一致；启动后三秒 service active/running、NRestarts=`0`、UnitFileState=`disabled`，journal 只有正常 systemd start 记录。
- 当前状态为 `TASK_4_4_RETRY_GATE_RUNNING`。必须维持 PID `1007135` 跨 `06:00 UTC` 且总运行至少 15 分钟；任何退出均按 failed_gate 停服、记录并清理 build-exclusive UTC-day。

## 2026-07-14：阶段 4.5 稀疏 snapshot 小时关闭审查失败与收口

- gate 从 `2026-07-14T05:45:53Z` 以 MainPID=`1007135` 持续跨过 `06:00 UTC` 并运行超过 15 分钟，NRestarts=`0`。五个连续/事件 source 成功 finalize `05.parquet`；RTDS 为 `830` 条合法 fresh update、exact duplicate=`0`，initial snapshot 未误写。manifest build/binary/host/region 与文件 footer facts 一致，`gaps=[]`、全部 lag/drop/reconnect=`0`，aggTrade 无 unresolved range；journal 只有正常启动。
- 唯一阻塞事实是 `binance_depth_snapshot_05.parquet.running` 从 `05:45:55Z` 保持 0-byte 且 manifest snapshot counters 为 files/rows=`0`，而 finalized depth update 已引用 bootstrap `snapshot_last_update_id=97380343272`。实现只在下一条跨小时 snapshot、UTC 日 barrier 或停机时 finalize；正常无 resnapshot 路径没有定时小时关闭，因此旧 running 会跨完整小时，异常退出时还可能由 stale cleanup 删除唯一 bootstrap snapshot。
- 阶段 4.5 独立审查结论为 FAIL，Critical=`0`、Important=`1`、Minor=`0`；其余 binary/unit/SHA、RTDS、continuity、manifest、命名、journal、旧对象删除与最小产物合同均通过。最小修复边界是为 writer 增加与 ingress 顺序一致的 UTC 小时 watermark/finalize：不得制造空 snapshot、sidecar，亦不得把同进程正常小时关闭误写成 `part-N`。
- 收到确认 FAIL 后于 `06:12:30Z` 立即 SIGTERM 停服；journal 记录 `coordinated_shutdown_complete`，Result=`success`、ExecMainStatus=`0`、MainPID=`0`、NRestarts=`0`，unit 继续 disabled 且 `Restart=no`。协调停服把已缓冲 snapshot 与当前小时 writer 正常 finalize。
- 失败 UTC 日大小=`107,445,128` bytes、文件=`12`；manifest 与全部文件 facts 证明其只属于 commit `ef34c223fc12a6b95ef5c65cc4eebc1f629dc6d1`、binary SHA-256=`249dc5802fb0aba185adf365b0e0523f86d39e626a9440fbd05164bfd8c5d9f6`。确认 service inactive、holder=`0`、descendant mount=`0`、owner/type/symlink 门通过后，于 `06:14:47Z` 删除整个失败日并恢复空 `training/typed_raw`。
- 当前远端为新一轮已记录 `failed_gate`：service inactive/dead、disabled、MainPID=`0`；failed binary SHA-256=`249dc5802fb0aba185adf365b0e0523f86d39e626a9440fbd05164bfd8c5d9f6`，failed gate unit SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`、`Restart=no`，数据根为空。状态为 `PRODUCTION_GATE_FAILED_SNAPSHOT_HOURLY_FINALIZE_RETURNED_TO_STAGE_2`。

## 2026-07-14：稀疏 snapshot 小时关闭修复与本地门禁

- 用户再次确认保留已批准的简洁命名：正常小时主文件继续为 `<source>_HH.parquet`，只有同一小时进程重启造成已完成文件名碰撞时才使用 `<source>_HH-part-N.parquet`；不得为修复无事件小时关闭改用第二套命名或让同一进程正常关闭生成 `part-N`。
- 最小实现复用现有 depth source fence：depth outer supervisor 在 UTC-hour 边界先暂停 source，再把只针对 `binance_depth_snapshot` 的 finalize 控制按同一 ingress 顺序排在此前 snapshot 后，随后恢复 source。ingress 只 flush 该 source batch并把小时命令排入原 writer FIFO；旧小时有 active segment 时按 `utc_hour_boundary` finalize，没有行时 no-op。没有新增线程实例、数据根、sidecar、report、receipt 或 manifest 字段。
- 新增回归分别证明：边界前 snapshot 先于 finalize 控制；仅一条 bootstrap snapshot 且没有后续事件时仍关闭一行主文件并发出 manifest delta；空小时不生成 Parquet；同小时 bootstrap+resnapshot 仍只写主文件、没有 `part-N`。需求第 107 行和计划步骤 2.7.2 已同步明确该合同。
- 完整本地门禁通过：data-collect lib=`93/93` 与全部 integration contracts 通过，SDK patch=`6/6`、SDK runtime=`17/17`；all-targets locked clippy `-D warnings`、all-targets locked check、workspace fmt、`git diff --check` 均退出 0。依赖反向树只出现仓库内 `third_party/polymarket_client_sdk_v2-0.6.0-p1`，vendor tree 未生成 `target/`。
- 当前状态为 `SNAPSHOT_HOURLY_FINALIZE_FIX_VALIDATED_PENDING_REVIEW`。必须先按顺序通过源码/顺序/writer 正确性审查，再通过远端 failed-gate 身份与重试部署状态审查；两路均 PASS 前禁止提交、替换或启动远端 pair。

## 2026-07-14：稀疏 snapshot 小时关闭独立源码审查

- 第一轮独立 `gpt-5.6-sol`/`ultra` 只读审查结论为 PASS，Critical=`0`、Important=`0`、Minor=`0`；审查未修改文件、未访问远端、未派生子代理。
- 审查确认 depth source fence、共享 enqueue lock 与 ingress FIFO 保证旧小时 snapshot 先于 hour finalize；ingress 先 flush snapshot batch，再把 finalize 排入同一 writer FIFO。空小时 no-op、同一进程正常小时关闭保留主文件、`part-N` 只由既有文件碰撞触发；午夜 day/hour barrier、追赶、断线/backoff 与 writer fail-closed 未发现绕过或死锁。
- 审查独立重跑完整 data-collect 测试、all-targets locked clippy、workspace fmt 与 `git diff --check`，全部通过。当前状态为 `SNAPSHOT_HOURLY_FINALIZE_SOURCE_REVIEW_PASSED_PENDING_RETRY_STATE_REVIEW`；第二轮远端 failed-gate 身份与重试部署状态审查 PASS 前，提交与远端替换/启动门仍关闭。

## 2026-07-14：稀疏 snapshot 修复的 failed-gate 重试状态审查

- 第二轮独立 `gpt-5.6-sol`/`ultra` 只读审查结论为 PASS，Critical=`0`、Important=`0`、Minor=`0`；证据采样窗口为远端 `2026-07-14T06:39:10Z–06:41:21Z`、本地 `2026-07-14T06:37:47Z–06:40:52Z`，审查未修改本地或远端状态。
- 远端 service 精确为 inactive/dead、disabled、MainPID=`0`、NRestarts=`0`、`Restart=no`，无 enable symlink、进程、cgroup 或打开句柄。failed binary 为 `ubuntu:ubuntu`、mode `0755`、大小 `25,081,400` bytes、SHA-256=`249dc5802fb0aba185adf365b0e0523f86d39e626a9440fbd05164bfd8c5d9f6`；failed unit 为 `ubuntu:ubuntu`、mode `0644`、SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，FragmentPath/ExecStart 一致。
- `training/typed_raw` 为空且无 mount/holder；无 upload/staging、第二数据根、validation artifact 或已删除旧对象复现。本机 archive timer disabled/inactive，archive service static/inactive，`/home/chillchill/new-datasets` 保留。
- 审查确认 HEAD、tracking ref 与远端分支仍为 `ef34c223fc12a6b95ef5c65cc4eebc1f629dc6d1`，暂存区为空；四个本事项文件未提交，三个用户文件未混入。重试顺序继续是提交/push、从 pushed commit 干净构建、精确 restage/replace、非 23 UTC 的 45–50 分启动并跨小时至少 15 分钟；不得复用当前 failed binary。
- 当前状态为 `SNAPSHOT_HOURLY_FINALIZE_IMPLEMENTATION_REVIEW_PASSED_READY_FOR_COMMIT`。两路独立复审已通过，允许只提交并推送四个本事项文件；新生产构建仍须严格从推送后的 commit 产生。

## 2026-07-14：稀疏 snapshot 小时关闭提交推送与生产构建

- 稀疏 snapshot 小时关闭修复提交为 `af57615e9694af3e930da5e648f058a1ad1cb321`（`fix(data-collect): finalize sparse snapshots hourly`），已推送到 `origin/after-data-plan`；本地 HEAD、remote-tracking ref 与 `ls-remote` 三者精确一致。提交只含 `live_runtime.rs` 和本事项 demand/plan/changelog，用户的 `AGENTS.md`、`demand/rebuild-core-demand.md`、`推荐开发顺序.md` 未进入提交。
- 从该已推送 commit 创建 `/tmp/data-collect-build-af57615` detached clean worktree，设置 `DATA_COLLECT_GIT_COMMIT=af57615e9694af3e930da5e648f058a1ad1cb321`，以独立 `/tmp/data-collect-target-af57615` 执行 locked release build。构建后 worktree HEAD 未变化且 clean，binary 中可检出完整嵌入 commit。
- 新 binary 大小=`25,125,208` bytes、SHA-256=`ef29d9ed09ce9f7d05a1df1a0110f9c4ac4f37ba1e6257a7dacdc07d4aadd5c4`、固定本地 mode=`0755`；经 digest-named local staging 复核后原子替换 `target/release/data-collect-training-collector`。临时 detached worktree、独立 target 与 staging 已删除，没有创建 release receipt 或验证报告。
- 当前状态为 `TASK_4_1_SNAPSHOT_HOUR_FIX_BUILD_PASSED_PENDING_REMOTE_RESTAGE`。远端仍是第二轮审查确认的 inactive failed-gate，当前 failed binary SHA 为 `249dc5802fb0aba185adf365b0e0523f86d39e626a9440fbd05164bfd8c5d9f6`；新 binary 尚未上传、替换或启动。

## 2026-07-14：稀疏 snapshot 修复的远端预置与原子替换

- 重试前于 `2026-07-14T06:47:20Z` 再次核实 failed-gate：service inactive/dead、disabled、MainPID=`0`、NRestarts=`0`、`Restart=no`，数据根为空且无 executable holder；failed binary/unit SHA 分别为 `249dc5802fb0aba185adf365b0e0523f86d39e626a9440fbd05164bfd8c5d9f6` 与 `76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，新 digest staging 路径不存在。
- 使用 shell noclobber 的独占文件描述符创建新 staging，没有先创建再覆盖。staging binary 为 `ubuntu:ubuntu`、mode `0755`、大小 `25,125,208` bytes、SHA-256=`ef29d9ed09ce9f7d05a1df1a0110f9c4ac4f37ba1e6257a7dacdc07d4aadd5c4`；ELF/loader/动态库、嵌入 commit、`--version`/`--help` 均通过。staging gate unit 为 `ubuntu:ubuntu`、mode `0644`、SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`、`Restart=no`，systemd verify 通过。
- 第一次原子替换尝试在任何 `mv` 前因 disabled/inactive unit 已被 manager 卸载而由 `reset-failed` 返回 `Unit ... not loaded`，脚本安全退出。复核确认 final/staging 四个 SHA 均未变化、user manager 无 failed unit，因此记录该形式动作不适用并重新执行全部身份门，没有为此加载或启动 unit。
- 于 `2026-07-14T06:49:50Z` 依次同目录原子替换 final binary 与 unit并 daemon-reload；final SHA 精确等于新 binary/gate unit，staging 全部消失。service 继续 inactive/dead、disabled、MainPID=`0`，数据根仍为空。

## 2026-07-14：稀疏 snapshot 小时关闭 production gate

- 在批准的非 23 UTC 窗口 `2026-07-14T06:50:15Z` 启动最终 `--run-forever` gate。MainPID=`1014179`，运行中 `/proc/1014179/exe` SHA 精确为 `ef29d9ed09ce9f7d05a1df1a0110f9c4ac4f37ba1e6257a7dacdc07d4aadd5c4`；service active/running、disabled、`Restart=no`、NRestarts=`0`。
- 边界前唯一 snapshot 临时文件为 `binance_depth_snapshot_06.parquet.running`。在没有后续 snapshot 和人工触发的情况下，`2026-07-14T07:00:20Z` 采样已自然变为 `binance_depth_snapshot_06.parquet`；没有 `_06-part-N`、没有旧小时 `.running`，也没有生成空的 snapshot `_07` 文件。同一 PID 于 `07:05:21Z` 已跨边界运行超过 15 分钟，并持续稳定到至少 `07:11:39Z`。
- 唯一 open manifest 精确记录 build `af57615e9694af3e930da5e648f058a1ad1cb321`、binary SHA、config digest、host/region 与 `gaps=[]`。六个 source 的 `_06.parquet` 全部 finalized、均为 `part=0` 与 `rollover_reason=utc_hour_boundary`；footer rows、文件 bytes、SHA-256 与 manifest 逐项一致。snapshot 为 1 行，fetch-start=`06:50:16.488Z`、receive=`06:50:16.699Z`、`last_update_id=97381662665`、bid/ask 各 `1,000` 档；depth update 的 5,833 行全部引用同一 snapshot ID且 gap reason 为空。
- RTDS finalized 582 行全部为 `crypto_prices_chainlink/update/btc/usd/fresh`，age=`881..2442ms`，exact payload duplicate=`0`；initial subscription snapshot 未写入。aggTrade finalized 3,761 行，ID=`4013780103..4013783863` 完全连续且唯一，manifest continuity 无 unresolved range。六源 dropped/lag/reconnect 均为 0，journal 自启动后无 warning/fatal。
- Polymarket 初次粗扫描以“同 canonical message、不同 receive millisecond”计数得到 9,917，会把首次合法多 target `PriceChange` 的不同 fan-out 行误判为重放；按需求规定的完整 typed 业务行重新比较后，609,029 行中的跨接收时间重复业务行为 `0`。这一中间误判只输出到终端，没有写入生产或仓库产物。
- 数据根只含 hourly Parquet、唯一 manifest 与当前小时合法 `.running`；旧小时 `.running`、`part-N`、空 snapshot 小时文件、tmp、report/receipt/sidecar、第二数据根和 staging 均为 0。本机临时拉取的 finalized 文件与临时 gate unit已删除。
- 当前状态为 `TASK_4_4_SNAPSHOT_HOUR_GATE_PASSED_PENDING_STAGE_4_5_REVIEW`。必须由独立 `gpt-5.6-sol`/`ultra` 只读复核真实 gate；审查 PASS 前 unit 继续 disabled 且 `Restart=no`，不得进入长期运行切换。

## 2026-07-14：阶段 4.5 首轮审查与证据文字修正

- 阶段 4.5 首轮独立 `gpt-5.6-sol`/`ultra` 只读审查暂不 PASS：Critical=`0`、Important=`0`、Minor=`1`。代码、hour-finalize 顺序、真实 snapshot、六源 Parquet/manifest、typed 重复、continuity、journal、最小产物、旧对象删除和长期切换门均通过；唯一 Minor 是上一节把 PyArrow 返回的 bids/asks JSON 字符串字符数 `32,001` 误写成盘口档数。
- 独立审查从真实 typed JSON 与 endpoint `limit=1000` 核实 bids/asks 各恰为 `1,000` 档；已只修正该 changelog 事实，没有改代码、数据、schema、manifest、unit 或 gate 进程。当前状态为 `TASK_4_5_SNAPSHOT_HOUR_GATE_DOC_CORRECTED_PENDING_REREVIEW`，定向复审 PASS 前继续保持 PID `1014179`、disabled、`Restart=no`。
- 定向复审结论为 PASS，Critical=`0`、Important=`0`、Minor=`0`。复审确认真实 Parquet bids/asks 各 1,000 档、两段 JSON 字符串各 32,001 字符、endpoint `limit=1000`，首轮 Minor 与修正范围记录准确，`git diff --check` 通过。
- 截至复审采样 `2026-07-14T07:26:53Z`，远端仍为 PID `1014179`、active/running、disabled、`Restart=no`、NRestarts=`0`；binary、进程与 gate unit SHA 未变化，journal 无 warning/error。当前状态为 `TASK_4_5_SNAPSHOT_HOUR_GATE_REVIEW_PASSED_READY_FOR_LONG_RUNNING_SWITCH`；先提交/push 阶段 4 事实，再只切换长期 unit、reload/enable且不得重启 PID。

## 2026-07-14：长期 unit 无重启切换与阶段 5 启动

- 阶段 4 gate、独立审查和文档定向复审事实已提交为 `97bc804`（`docs(data-collect): record snapshot gate pass`）并推送到 `origin/after-data-plan`；提交只包含本事项 demand/plan/changelog，三个用户文件未进入提交。
- 以 repo 权威 unit 创建 digest-named exclusive staging；文件为 `ubuntu:ubuntu`、mode `0644`、SHA-256=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`、`Restart=on-failure`，远端 systemd verify 通过。切换前 service 仍为 PID `1014179`、active/running、disabled、`Restart=no`、NRestarts=`0`，gate unit SHA 与 binary/proc SHA 精确匹配。
- 于 `2026-07-14T07:29:26Z` 只原子替换 unit、daemon-reload 并执行 `enable`，未执行 start/restart/`--now`。切换前后 MainPID 均为 `1014179`，`ExecMainStartTimestampMonotonic=10114954997118` 未变化，原始启动时间仍为 `06:50:15Z`；切换后 unit enabled、`Restart=on-failure`、NRestarts=`0`，binary SHA 仍为 `ef29d9ed09ce9f7d05a1df1a0110f9c4ac4f37ba1e6257a7dacdc07d4aadd5c4`。
- 阶段 5 从 unit 切换时间保守计时至少 26 小时，最早完成点为 `2026-07-15T09:29:26Z`；这也覆盖从原进程/首个 Binance connection 启动后超过 24 小时的 lifecycle、UTC 日切换与至少 312 个 nominal 5 分钟时槽。任何 MainPID 变化都从新 PID 重置整个 26 小时窗口；任何确认的代码、unit、数据或产物合同错误立即停服并按 `failed_long_running` 身份门处理。
- 当前状态为 `TASK_5_LONG_RUNNING_STARTED_20260714T072926Z`。服务保持 enabled/running，后续按小时只读检查 systemd、journal、finalized 文件、manifest、continuity 与 stale running，不生成验证报告或 sidecar。

## 2026-07-14：首个长期窗口因 RTDS parser fault 重置

- `2026-07-14T07:33:34.487861Z`，PID `1014179` 收到 RTDS SDK parser fault：`Internal: missing field topic at line 1 column 593`；runtime 记录 `source_task_error:polymarket_rtds_chainlink_btc_usd` 后以 status 1 退出。systemd 于 `07:33:44Z` 按 `Restart=on-failure` 启动 PID `1017813`，NRestarts 从 0 变为 1；binary/proc SHA 和长期 unit SHA 未变化。
- 错误发生在官方 SDK `SimpleParser` 解出 typed envelope 之前，当前没有 raw frame，证据不足以唯一归因为平台坏帧或官方 SDK schema 覆盖不足；不得虚构唯一责任。它不经过本轮 snapshot hour-finalize 代码路径，且需求 3.3 第 4 项明确要求 parser fault 作为 schema fatal 使进程退出，由 systemd 新进程恢复，因此目前未确认代码、数据或产物合同错误，不触发 `failed_long_running` 停服清理门。
- 旧进程在退出前已 finalize 07 小时的 aggTrade、depth update、PM market、metadata 与 RTDS 主文件并更新 open manifest；该小时旧进程未产生 snapshot，故没有空 snapshot finalized 文件。新进程只对已有 finalized 文件使用合法 `part-1`，新 bootstrap snapshot 使用仍空闲的 `binance_depth_snapshot_07.parquet.running` 主文件。manifest 已记录 RTDS parser fatal、source task error/ended、PM/RTDS recovery事实；Binance continuity 仍无 unresolved range。
- `2026-07-14T07:36:12Z` 复核 PID `1017813` active/running、enabled、`Restart=on-failure`、NRestarts=`1`，启动后无新 warning。按需求与计划，整段 26 小时窗口和两个 Binance connection 的 24 小时子窗口均从 `07:33:44Z` 重置，新的最早完成点为 `2026-07-15T09:33:44Z`；后续任何 PID 变化继续重置。
- 当前状态为 `TASK_5_LONG_RUNNING_RESET_20260714T073344Z`。继续按小时只读监控新 PID，不把这次已记录且按合同恢复的外部/schema fault 误报为 restart loop，也不放宽最终同一 PID 26 小时验收门。

## 2026-07-14：长期运行 Polymarket receiver 容量失败与停服清理

- PID `1017813` 从 `07:33:44Z` 运行到 `12:31:26Z` 后报告 `source_task_error:polymarket_market:receiver lagged by 6 messages` 并以 status 1 退出；systemd 于 `12:31:36Z` 启动 PID `1041257`，停服前 `NRestarts=2`。manifest 同时记录 `polymarket_sdk_stream_fatal:receiver lagged by 6 messages`、market `lag_events=6` 与 `backfill_rows=0`，Binance aggTrade continuity 仍为 `unresolved_ranges=[]`。
- Tokio `Lagged(6)` 表示 receiver 已跳过被 broadcast 覆盖的 6 条旧消息；Polymarket market stream 没有 offset/replay/REST 回补，重启只能接收后续事件。失效关闭、非零退出、协调 finalize 与 systemd 恢复机制本身按合同工作，但不能恢复这 6 条原生消息，因此这是已确认的数据正确性错误，不能作为普通平台/网络瞬态继续重置 26 小时窗口。
- 失败前 `polymarket_market_12.parquet` 为 `1,754,723` 行、`117,261,893` bytes，覆盖约 `1,833.284s`；按相同 `(timestamp_received, source_native_payload_json)` 合并 fan-out 后为 `946,011` 条 native message。峰值约 `1,899 msg/s`、`3,573 rows/s`，仍在批准的四源 `2k-5k events/s` sizing 内。`timestamp_received - 平台 timestamp` 的秒级 p50 从稳定期约 `11ms` 连续升至末秒 `3,224ms`、max=`3,317ms`；该口径还含网络/平台时延，但结合本地 `Lagged(6)` 足以证明 receiver 下游消费积压。未发现 OOM、磁盘耗尽、writer fatal 或平台断线；远端 2 vCPU/1.9GiB 资源条件可能放大积压，但不改变消息已丢失的事实。
- 两名独立 `gpt-5.6-sol`/`ultra` 子代理分别完成根因诊断和计划停机门审查，结论均为 FAIL：当前串行 receiver 在每次 recv 后同步执行 canonical JSON、结构去重、fan-out 与 bounded ingress 等待，真实链路容量不足；现有合成 benchmark 没有覆盖这一完整瓶颈。最小修复应在项目层将 SDK 单 receiver 快速排水与后续处理有界解耦并纳入全局 row/byte 预算，不扩大 vendored SDK patch；只增大 SDK 1024 broadcast 容量不能修复持续积压。
- `2026-07-14T12:47:04Z` 停服前再次确认 PID=`1041257`、active/running/enabled、`Restart=on-failure`、binary SHA-256=`ef29d9ed09ce9f7d05a1df1a0110f9c4ac4f37ba1e6257a7dacdc07d4aadd5c4`、unit SHA-256=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`。随即执行 `disable --now`；`12:47:05Z` journal 记录 `coordinated_shutdown_complete`，Result=`success`、ExecMainStatus=`0`、MainPID=`0`、service inactive/dead/disabled，无进程、cgroup holder、打开句柄或残留 `.running/.tmp`。
- 删除前逐项 SHA 复核 manifest 所列全部 `48` 个 Parquet，磁盘与 manifest 路径、bytes、SHA-256 一一相符；唯一 `manifest.json` 为 `state=open`，顶层与每个文件 facts 均只属于 commit `af57615e9694af3e930da5e648f058a1ad1cb321` 和上述 binary SHA。失败日共 `49` 个文件、`1,409,722,377` bytes，目录与内容均为 `ubuntu` 所有，无 symlink、descendant mount、holder 或未列入 manifest 的持久文件，因而通过 build-exclusive 删除门。
- 于 `12:49:23Z` 删除失败 build 独占的整个 `training/typed_raw` 并以 `ubuntu:ubuntu 0755` 重建空根；没有移动、打包或生成 inventory/report/sidecar。失败 binary/unit 保留为精确可识别的 `failed_long_running` pair，service inactive/dead/disabled、MainPID=`0`、`Restart=on-failure`，数据根为空。
- 当前状态为 `FAILED_LONG_RUNNING_PM_RECEIVER_CAPACITY_RETURNED_TO_STAGE_2`。下一步只能先完成项目层有界快速排水、真实全链路容量回归、全量本地门禁与独立实现审查；审查 PASS 后才可提交/push、从新 commit 干净构建并按 failed-long-running 重试门替换远端 pair。

## 2026-07-14：Polymarket fast-drain 容量修复与本地门禁

- 根因修复保持 SDK-first 和单 receiver：`MarketSdkRuntime` 的唯一 receiver 只交给一个 fast-drain task；每次 recv 成功立即冻结 wall-clock/monotonic receive time并取得共享全局与 `25,000 messages/64 MiB` decoded 局部 permits，再按 FIFO 交给唯一串行 processor。canonical、dedupe、target filter、双 target fan-out 与 shared ingress 仍只有原生产语义路径；没有增大 SDK broadcast、扩大 vendored patch、创建第二 receiver 或 raw WebSocket/REST 旁路。
- lifecycle/订阅切换、UTC 日 barrier、故障与 SIGTERM 使用同一 FIFO source fence：marker 前已接收消息先由旧 context 处理完成，marker 后消息才进入新 context；停机先停止 intake并有界 drain，不直接 abort 已接受消息。processor fatal 与 ingress closed 对已能计算 fan-out 的消息精确按 receive UTC 日记录 message/row drop/gap；decoded/global capacity 在规范化前只诚实记录一条 native message、normalized rows unknown。SDK lag/parser/terminal 与 pump join 保留首个不可恢复错误，retryable SDK 错误不能掩盖 drain 中出现的 schema/dedupe fatal。
- fan-out 改为先完整 measure/build，再一次性取得真实 owned bytes 的 permits，随后提交 dedupe并执行无失败 send；容量失败不提前污染 dedupe。retained lifecycle 的 fingerprint 与 freshness 在 A+B -> B+C 切换中保留，退役 A tail 被过滤，新 C 在订阅等待期间到达也按已预装 freshness context 处理；排队消息继续使用 fast-drain 冻结的 receive time，早于本轮 `subscribed_at` 的消息不能刷新健康。
- 新增或强化回归覆盖：5,000-message SDK burst/FIFO、持续 stream fence、decoded queue/global/local capacity terminal、atomic fan-out 与实际 byte 计数、processing/closed-ingress/error-priority drain、跨 UTC 日精确 abandonment、restore worker cancel/join、初始及 activity 后 disconnected、sticky parser fault、A+B -> B+C loopback、retained lifecycle/freshness 与 stop 后 permit 全归还。rollover loopback 修正夹具后连续 6 次通过；跨日 abandonment 与 capacity error-kind 定向回归通过。
- 生产 helper 四源容量门同时运行 producer、processor、shared ingress 与真实 writer：Polymarket 固定 `2,100 source-native messages/s`、每条真实双 target `PriceChange` fan-out 两行，RTDS/depth/aggTrade 各 `840 messages/s`，持续总量约 `4,620 messages/s`，并在开始时增加 `1,025` 条 Polymarket burst。debug 实测 sustained=`4,688.4 msg/s`、actual=`4,482.8 msg/s`；release 为 sustained=`4,695.9 msg/s`、actual=`4,489.4 msg/s`。两者均精确完成 `28,745` 条 native message、`42,370` 行及各 source Parquet 行，drop/lag/operational counter 为 0，pump/decoded/ingress/batch/writer 均排空且 row/byte permits 全归还。
- 该容量门在初始 burst 后以 100ms 周期采样 50 次 Polymarket offered-minus-processed、decoded permits、shared ingress、各 writer queue 与 global inflight；任意两秒窗口不得正向单调增长，后十个样本均不得超过前十个样本加一个生产 tick/一个有界 batch 的对应容差。source/decoded/ingress/writer 实测没有依靠 25k/40k 缓冲后延迟排空来通过；global inflight 只呈现测试断言容差内的有界 batch 锯齿。
- 本地完整门通过：`cargo test -p data-collect --tests --locked` 的 lib=`121/121` 及全部 integration contracts 成功；all-targets locked clippy `-D warnings`、all-targets locked check、workspace fmt 与 `git diff --check` 均退出 0。generic writer probe 为 10,000 rows/3ms；既有 throughput 两窗分别约 `1,669,193.74` 与 `1,765,936.14 events/s`，finalize failure=0，临时目录均自动删除。
- SDK 供应链重新核实：原始 `.crate` SHA-256 仍为 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`，自动删除临时目录的完整树比较仍恰为获批三个差异文件；`cargo tree` 只有仓库内 p1。vendor nightly fmt、Rust 1.88 locked lib=`153/153`、default/all-features 两组 all-targets clippy `-D warnings` 全部通过，vendor 无 `target/`。
- 远端只读复核仍为 failed-long-running 身份：service inactive/dead、disabled、MainPID=`0`，binary SHA-256=`ef29d9ed09ce9f7d05a1df1a0110f9c4ac4f37ba1e6257a7dacdc07d4aadd5c4`，unit SHA-256=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`，生产数据根为空。本轮未改远端、未生成 report/receipt/sidecar；当前状态为 `TASK_3_FAST_DRAIN_REPAIR_VALIDATED_PENDING_FINAL_REVIEW`，两路最终独立复审均 PASS 前提交、production build 与远端替换门保持关闭。

## 2026-07-14：fast-drain 修复两路最终独立复审通过

- source/SDK-first 最终独立审查 PASS，Critical=`0`、Important=`0`、Minor=`0`。审查独立连续三次运行四源容量门并复跑完整 lib、SDK runtime contract、all-targets clippy、fmt 与 diff check；确认官方 SDK 唯一 receiver、单一 processor、receive time 冻结、FIFO fence、全局/局部预算、atomic fan-out、dedupe commit、跨日 drop 与 pump/task 清理均成立，没有 raw WebSocket/REST 或第二业务路径。
- writer/schema/deployment 最终独立审查 PASS，Critical=`0`、Important=`0`、Minor=`0`。审查确认 permits 在复制和所有失败路径精确归还、writer/Parquet/operational counter 与 receive UTC 日一致、四源测试不能依靠大 buffer 延迟排空、vendor 仍只有三文件差异、依赖树只有仓库 p1、权威 unit 的退出码/StartLimit/SIGTERM 合同未回退；远端只读身份仍为已记录 failed-long-running pair且空数据根。
- 两路审查均未修改文件或执行远端写操作。阶段 3 实现审查门现已通过，当前状态为 `TASK_3_FAST_DRAIN_REPAIR_REVIEW_PASSED_READY_FOR_COMMIT`；只允许精确提交并推送本事项文件，随后从该已 push commit 的干净 detached worktree 构建新 production binary。

## 2026-07-14：fast-drain 修复提交推送与 production 构建

- fast-drain 修复精确提交为 `a2d92d48f93301edfe15d1dcd9f53971ef89dae7`（`fix(data-collect): drain Polymarket receiver promptly`）并已推送到 `origin/after-data-plan`；本地 HEAD、remote-tracking ref 与 `ls-remote` 三者一致。提交只含本事项 7 个文件，用户既有 `AGENTS.md`、`demand/rebuild-core-demand.md` 与 `推荐开发顺序.md` 未进入提交。
- 从该已 push commit 创建 `/tmp/data-collect-build-a2d92d4` detached clean worktree，设置 `DATA_COLLECT_GIT_COMMIT=a2d92d48f93301edfe15d1dcd9f53971ef89dae7`，使用独立 `/tmp/data-collect-target-a2d92d4` 执行 locked release build。构建前后 HEAD 精确匹配且 worktree clean，binary 中可检出完整嵌入 commit。
- 新 production binary 大小=`25,434,096` bytes、mode=`0755`、SHA-256=`5634ad72a5f507c590af52b62196ab310d7511e04ea7df84990ea90d5101ed91`；经不存在的 digest-named local staging 复核后原子替换主仓库固定 target 路径。临时 detached worktree、独立 target 与 local staging 均已删除，没有生成 release receipt 或验证报告。
- 当前状态为 `TASK_4_1_FAST_DRAIN_BUILD_PASSED_PENDING_REMOTE_RESTAGE`。远端尚未上传、替换或启动新 binary；仍须先按 failed-long-running identity、空数据根、无 holder/mount、exclusive staging 与 `Restart=no` gate unit 完成阶段 4.2 全量预检。

## 2026-07-14：fast-drain 修复远端重试预检与原子替换

- `2026-07-14T15:29:30Z` 起复核已记录 failed-long-running pair：service inactive/dead、disabled、MainPID=`0`、NRestarts=`0`、`Restart=on-failure`；旧 binary SHA-256=`ef29d9ed09ce9f7d05a1df1a0110f9c4ac4f37ba1e6257a7dacdc07d4aadd5c4`、旧 unit SHA-256=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`，owner/mode/ExecStart 与 changelog 一致。生产 `training/typed_raw` 为空，final binary、数据根及 unit 无 executable/fd holder；114 个可读 `/proc/*/mountinfo` 均无目标自身或 descendant mount，逐级父目录均为非 symlink 预期目录。
- 新 digest staging 路径确认不存在后使用 shell noclobber exclusive-create 上传。staging binary SHA-256=`5634ad72a5f507c590af52b62196ab310d7511e04ea7df84990ea90d5101ed91`、大小=`25,434,096`、mode=`0755`；为 x86-64 ELF，loader 与全部动态库可解析，远端 glibc=`2.39`，内嵌 commit、`--version` 与 `--help` 均通过。staging gate unit SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`、mode=`0644`，仅把权威 unit 的 `Restart=on-failure` 机械改为 `Restart=no`，systemd verify 通过。
- 替换前再次核对 inactive/disabled、MainPID=0、旧 pair SHA、新 staging SHA、空数据根、无 holder/mount；随后先同目录原子替换 final binary并复核 SHA，再原子替换 final unit、daemon-reload并复核 digest。新 final binary/unit owner 均为 `ubuntu:ubuntu`，staging 全部消失；service 继续 inactive/dead、disabled、MainPID=`0`、NRestarts=`0`、`Restart=no`，数据根仍为空。
- 当前状态为 `TASK_4_2_FAST_DRAIN_RESTAGE_PASSED_WAITING_GATE_WINDOW`。新 pair 尚未启动；只允许在非 23 UTC 小时的 45–50 分窗口启动最终 `--run-forever` gate，并在 `Restart=no` 下跨小时至少 15 分钟。

## 2026-07-14：fast-drain production gate 启动

- 在批准的非 23 UTC 窗口 `2026-07-14T15:46:21Z` 启动最终 `--run-forever` gate。MainPID=`1046442`，运行中 `/proc/1046442/exe` SHA-256 与 final binary 均为 `5634ad72a5f507c590af52b62196ab310d7511e04ea7df84990ea90d5101ed91`；gate unit SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，service active/running、disabled、`Restart=no`、NRestarts=`0`。
- 启动后唯一生产根创建 2026-07-14 UTC 日、六个 source 目录、六个 15 时 `.running` 与一个 `manifest.json`；journal 当前只有正常 Started 记录。当前状态为 `TASK_4_4_FAST_DRAIN_GATE_STARTED_20260714T154621Z`，必须保持同一 PID 跨 16:00 UTC 且运行至少到 `16:01:21Z` 后再判定 gate，并直接核验 finalized Parquet、manifest、continuity、重复、journal 与无额外产物。

## 2026-07-14：fast-drain production gate 因 RTDS parser fault 失败并清理

- systemd 精确记录 PID `1046442` 于 `2026-07-14T15:46:21.496539Z` 启动，于 `16:01:16.948313Z` 输出 `source_task_error:polymarket_rtds_chainlink_btc_usd:parser error: Internal: missing field \`topic\` at line 1 column 593` 并以 status 1 退出。该进程跨过 16:00 UTC，但只连续运行 `14分55.451774秒`，距 15 分钟验收约差 `4.548226秒`；gate 不通过。`Restart=no` 正确阻止自动重启，NRestarts=`0`。
- 退出时唯一 manifest 为 `state=open`，顶层 build=`a2d92d48f93301edfe15d1dcd9f53971ef89dae7`、collector binary SHA-256=`5634ad72a5f507c590af52b62196ab310d7511e04ea7df84990ea90d5101ed91`、host=`ip-172-26-10-0`、region=`eu-west-1`；gaps 精确为 RTDS parser fatal、source task unexpected end 与 source task error 三项。所有活跃 writer 已按 shutdown finalize，磁盘只有 manifest 所列 `11` 个 Parquet，共 `873,107` 行、`59,561,249` bytes，没有 `.running/.tmp/.partial`、额外文件或 sidecar。
- 删除前逐项复核 manifest 所列 11 个文件与磁盘路径、bytes、SHA-256 一一相符；每个文件 facts 的 code version/binary SHA 与顶层一致。逐级路径均为非 symlink 预期目录，数据根和 UTC 日均为 `ubuntu` 所有；没有目标内 fd/cwd/root/exe holder，遍历全部可读 `/proc/*/mountinfo` 也没有目标或 descendant mount。因此该 UTC 日只属于本轮失败 build。
- 按步骤 4.4.3 删除整个失败 UTC 日并以 `ubuntu:ubuntu 0755` 重建空 `training/typed_raw`；没有移动、打包或生成 inventory/report/receipt/sidecar。随后仅执行 `reset-failed`，service 当前 inactive/dead、disabled、MainPID=`0`、`Restart=no`；failed binary/unit SHA-256 仍分别为 `5634ad72a5f507c590af52b62196ab310d7511e04ea7df84990ea90d5101ed91` 与 `76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，生产根为空。
- 独立 `gpt-5.6-sol`/`ultra` 只读审查确认：parser fault 按需求被正确 fail closed，但正确退出不能替代 15 分钟 gate；计划 4.4.3 只定义 failed pair 供“下一修复版”在 4.2.1 替换，既往记录也禁止直接复用 failed binary。阶段 5 的 `Restart=on-failure` 自动恢复语义不能移植到阶段 4 的 `Restart=no` gate。现有文档不存在“无代码变化、同一 artifact 再认证”转换，直接重跑未获批准。
- 截至 2026-07-14 查询，Polymarket 官方 RTDS 文档仍声明所有消息都有 `topic/type/timestamp/payload`；官方 Rust SDK main 与本仓库 0.6.0 一样，把 `RtdsMessage.topic` 设为必需并对单对象或整数组直接 serde decode。官方 TypeScript RTDS client 只对含 `payload` 的帧执行 `JSON.parse` 后类型断言，不做运行时字段校验。官方 Rust 仓库没有检索到对应的 RTDS missing-topic issue/PR。上述证据只能证明 Rust SDK 严格 parser 对真实帧失败，不能在没有 raw frame 时唯一判断平台坏帧、未文档化控制帧或 SDK schema 覆盖不足。
- 相同 `missing field topic at line 1 column 593` 已在当天 `07:33:34Z` 和 `16:01:16Z` 两次出现。盲目重跑即使偶然通过 15 分钟，也没有证据支持后续同一 PID 26 小时门能够通过；但采集 raw frame、改变 `SimpleParser` 行为或新增同 artifact 重试例外都会超出现有已批准合同。当前状态为 `TASK_4_4_FAST_DRAIN_GATE_FAILED_RTDS_PARSER_CLEANED_DECISION_REQUIRED`，保持停机并请求用户决策。

## 2026-07-15：用户批准 RTDS 失败时有界结构诊断

- 对上一节“官方 Rust 仓库没有检索到对应 issue/PR”作范围纠正：当前维护的官方 v2 仓库 [`Polymarket/rs-clob-client-v2`](https://github.com/Polymarket/rs-clob-client-v2) main 仍是本项目固定的 `3ae1aae5e9ded38f984464c9fc0f307f8a9f41fb`/v0.6.0，`RtdsMessage.topic` 仍为必填字段，未发现覆盖该信封的当前 v2 修复；但归档的官方旧仓库已有 [`Polymarket/rs-clob-client#291`](https://github.com/Polymarket/rs-clob-client/issues/291)，记录同型 `Internal: missing field 'topic'`，当时实际帧是 `message/connectionId/requestId` 组成的服务端错误信封 `Invalid request body`。因此先前结论只对 v2 仓库检索成立，不能表述为整个官方仓库历史不存在对应 issue。
- 使用现有官方 SDK 与项目 `RtdsSdkRuntime` 做了一次临时生产形态的 `unsubscribe -> subscribe` live probe，10 秒内收到 1 条 update 且没有 parser error；临时 probe 源码与编译产物均已删除。该单次证据否定“每次 resubscribe 都必然产生 invalid request”的强归因，但不足以证明长期 resubscribe 与服务端错误信封无关，根因仍未确定。
- 用户批准只在现有 allowlist 文件 `third_party/polymarket_client_sdk_v2-0.6.0-p1/src/rtds/subscription.rs` 增加失败时有界诊断，不新增第四处 vendor 差异。诊断只在 `parse_messages` 失败后运行，最大 `2,048` UTF-8 bytes，只包含 frame byte length、SHA-256、顶层 JSON shape、数组中第一个 contract mismatch index、字段名和固定标量白名单 `message/status/code/topic/type/connectionId/requestId/error`；不得输出任意 payload、完整 frame 或 frame 前缀，不写 Parquet，不新增 sidecar/report/receipt。
- 原始 parser error 必须保留，parser fault 继续 sticky、terminal、fatal，每条失败连接最多经既有 error/journald 路径输出一次；成功帧解析结果和订阅行为不得变化。远端继续保持 failed pair inactive/dead、disabled、MainPID=`0`、NRestarts=`0`、`Restart=no`，binary SHA-256=`5634ad72a5f507c590af52b62196ab310d7511e04ea7df84990ea90d5101ed91`、unit SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，生产根为空。当前状态为 `TASK_2_1_RTDS_BOUNDED_DIAGNOSTIC_APPROVED_IMPLEMENTING`；完成本地验证和独立实现复审前不得部署或启动。
- 实现只改既有 allowlist 文件 `src/rtds/subscription.rs`：成功时直接返回原 `parse_messages` 结果；失败时保留原 error kind/source chain，再解析一次 JSON 生成结构摘要并以 UTF-8 boundary 安全截断。标量值只从冻结白名单读取，object/array 值不展开；完整 payload、frame 与 frame prefix 均不进入诊断。项目 loopback contract 改用与官方旧 issue #291 同形的服务端错误信封，验证 sticky parser fault、terminal disconnected、不重连和 payload 值不泄露。
- 初次定向测试发现逐字符刚好填满上限时没有空间写入 `[truncated]`；修正为必要时从既有 UTF-8 内容尾部回退后写完整固定后缀。随后 Rust 1.88 all-features clippy 发现直接 UTF-8 string slice lint；改用已核实 boundary 的 `str::get`，没有放宽 lint。两项均由回归覆盖，未改变诊断合同。
- 本地验证通过：新增 SDK diagnostic tests=`5/5`，vendored SDK locked lib=`158/158`；data-collect lib=`121/121` 及全部 integration tests 通过，其中 SDK patch contract=`6/6`、SDK runtime contract=`17/17`。data-collect all-targets locked clippy `-D warnings`、vendored SDK Rust 1.88 default/all-features all-targets clippy `-D warnings`、workspace fmt、pinned nightly vendor fmt 与 `git diff --check` 均通过。原始 `.crate` SHA-256 仍为 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`，自动删除临时目录的全树比较仍恰为 `src/ws/connection.rs`、`src/clob/ws/types/response.rs`、`src/rtds/subscription.rs` 三个差异文件；依赖树只有仓库内 p1，vendor 无 `target/`。
- 当前状态更新为 `TASK_2_1_RTDS_BOUNDED_DIAGNOSTIC_VALIDATED_PENDING_INDEPENDENT_REVIEW`。远端 pair 和空生产根未改动；必须先由独立 `gpt-5.6-sol`/`ultra` 审查实现、测试与文档，PASS 后才能提交、构建和进入 4.2.1。

## 2026-07-15：RTDS 有界诊断首轮审查修正

- 首轮独立 `gpt-5.6-sol`/`ultra` 审查结论为 FAIL：Critical=`0`、Important=`1`、Minor=`1`。Important 指出初版用手写必填字段存在性规则定位数组 mismatch；对于 `[{"topic":"a","topic":"b","type":"update","timestamp":1,"payload":{}}]`，真实 serde typed decode 因重复 `topic` 失败，但诊断错误报告 `first_contract_mismatch_index=none`。Minor 指出测试尚未显式锁定 error kind/source chain、控制字符转义和刚好 `2,048` ASCII bytes 上限。
- 修正没有扩大 patch 文件或诊断输出范围：删除手写字段规则，直接在原始数组 bytes 上用 serde sequence visitor 逐项执行 `RtdsMessage` typed decode，首个 typed error 的真实数组位置就是 mismatch index；用于摘要显示的 JSON 仍只输出字段名和获批标量白名单。成功路径仍只执行原 `parse_messages`；失败仍保留原 error kind/source chain并继续 sticky、terminal、fatal。
- SDK diagnostic tests 从 `5/5` 增至 `8/8`，新增重复必填字段真实索引、error kind/source chain、控制字符 JSON 转义、精确 `2,048` ASCII bytes 与多字节安全截断回归。修正后 vendored SDK locked lib=`161/161`，data-collect lib=`121/121` 及全部 integration tests 通过，其中 SDK patch contract=`6/6`、SDK runtime contract=`17/17`；data-collect all-targets locked clippy `-D warnings`、vendored SDK Rust 1.88 default/all-features all-targets clippy `-D warnings`、workspace fmt、pinned nightly vendor fmt 与 `git diff --check` 均通过。
- 供应链边界未变：原始 `.crate` SHA-256 仍为 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`，自动删除临时目录的全树比较仍恰为 `src/ws/connection.rs`、`src/clob/ws/types/response.rs`、`src/rtds/subscription.rs` 三个差异文件；依赖树只有仓库内 p1，vendor 无 `target/`。计划停止条件中陈旧的“四项”计数已依据同文已批准的第五项机械更正为“五项”，不改变 patch 内容。
- 当前状态更新为 `TASK_2_1_RTDS_BOUNDED_DIAGNOSTIC_FIX_VALIDATED_PENDING_REREVIEW`。远端仍是已记录 failed pair，生产根仍为空；独立复审 PASS 前不得提交、构建、替换或启动。

## 2026-07-15：RTDS 有界诊断独立复审通过

- 修正后由同一独立 `gpt-5.6-sol`/`ultra` 审查代理复审，结论 PASS，Critical=`0`、Important=`0`、Minor=`0`。审查确认原始数组 bytes 逐项 `RtdsMessage` serde typed decode 能让重复 `topic` 正确命中 index 0，不再存在影子字段合同；8 项 diagnostic tests 完整锁定首轮缺口。
- 审查同时确认成功解析、原错误 kind/source chain、sticky/terminal/fatal、禁止自动重连均未改变；诊断只输出获批结构、字段名与白名单标量，没有 payload、完整 frame 或 prefix 泄露。loopback contract=`6/6`、三份文档、五项 patch 边界、官方 issue #291 的有限表述、原始 crate SHA、三文件 vendor allowlist、唯一仓库 p1 依赖和无 vendor `target/` 全部通过；审查未修改文件或远端。
- 当前状态更新为 `TASK_2_1_RTDS_BOUNDED_DIAGNOSTIC_REVIEW_PASSED_READY_FOR_COMMIT`。下一步只允许精确提交并 push 本事项 5 个文件，再从该已 push commit 的干净 detached worktree 构建 production binary；用户既有 `AGENTS.md`、`demand/rebuild-core-demand.md` 与 `推荐开发顺序.md` 必须继续排除。

## 2026-07-15：RTDS 有界诊断提交、production 构建与远端预置

- 有界诊断精确提交为 `ce87684e0ff4ef6787a96cabc33be12aa659bd75`（`fix(data-collect): diagnose RTDS parser failures safely`）并已 push 到 `origin/after-data-plan`；本地 HEAD、remote-tracking ref 与 `ls-remote` 三者一致。提交只含本事项 5 个文件，用户既有 `AGENTS.md`、`demand/rebuild-core-demand.md` 与 `推荐开发顺序.md` 未进入提交。
- 从该已 push commit 创建 `/tmp/data-collect-build-ce87684e` detached clean worktree，设置完整 `DATA_COLLECT_GIT_COMMIT` 并使用独立 `/tmp/data-collect-target-ce87684e` locked release 构建。首次编译成功后，`strings | grep -q` 在 `pipefail` 下因正常 SIGPIPE 被误判为 141，脚本在本地 staging/替换前退出并自动删除临时目录；改用直接固定字节搜索后重新干净构建通过。最终 binary 大小=`25,438,728` bytes、mode=`0755`、SHA-256=`4e2bf2d8ce4c613ee3a54b8fc5aed26e674c41bfee148f3c4c7aa4b1da86adb1`，内嵌完整 commit；本地 digest staging 原子替换固定 target 后，临时 worktree/target 均已删除。
- 远端重试入口与冻结 failed pair 精确匹配：旧 binary SHA-256=`5634ad72a5f507c590af52b62196ab310d7511e04ea7df84990ea90d5101ed91`、gate unit SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，service inactive/dead、disabled、MainPID=`0`、NRestarts=`0`、`Restart=no`，生产根为空。路径逐级 owner/type 合法，binary/数据根 holder 为 0；两次全量复核分别读取 114/116 个可读 `/proc/*/mountinfo`，目标及 descendant mount 命中均为 0。本地 archive timer/service 继续 disabled/inactive。
- 新 binary 与机械生成的 gate unit 使用不存在的 digest-named staging exclusive-create 上传。binary 的 SHA/size/mode、x86-64 ELF、loader/动态库、远端 glibc=`2.39`、内嵌 commit、无副作用 `--version`/`--help` 均通过；unit SHA 未变，ExecStart、Restart=no、RestartPreventExitStatus、SIGTERM/TimeoutStopSec 及 `systemd-analyze --user verify` 通过。第一次 unit staging 名不以 `.service` 结尾，仅使 verify 在读取内容前拒绝文件名；final 未改，核对本轮 staging SHA 后删除并以合法 digest unit 名重新 exclusive-create。inactive unit 被 systemd 垃圾回收时 `reset-failed` 返回 not loaded，但已核实 Result=success、NRestarts=0，没有失败状态需要清除。
- `2026-07-14T22:31:44.223786800Z` 最终预检再次确认 binary/root holders=0、116 个可读 mountinfo 无命中、旧 final 与新 staging identity 精确，随后原子替换 final binary/unit并 daemon-reload。远端 final binary/unit owner 分别为 `ubuntu:ubuntu`、mode=`0755/0644`，SHA 分别为 `4e2bf2d8ce4c613ee3a54b8fc5aed26e674c41bfee148f3c4c7aa4b1da86adb1` 与 `76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`；staging 全部消失，service 仍 inactive/dead、disabled、MainPID=0、NRestarts=0、Restart=no，生产根为空。
- 当前状态为 `TASK_4_2_RTDS_DIAGNOSTIC_RESTAGE_PASSED_WAITING_GATE_WINDOW`。只允许在非 23 UTC 小时的 45–50 分之间启动最终 `--run-forever` gate，并让同一 PID 跨小时至少运行 15 分钟。

## 2026-07-15：RTDS 有界诊断 production gate 启动

- 在批准的非 23 UTC 窗口 `2026-07-14T22:45:44.815054547Z` 启动最终 `--run-forever` gate。MainPID=`1054097`，运行中 `/proc/1054097/exe` 与 final binary SHA-256 均为 `4e2bf2d8ce4c613ee3a54b8fc5aed26e674c41bfee148f3c4c7aa4b1da86adb1`；gate unit SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，service active/running、disabled、Restart=no、NRestarts=0。
- 启动后唯一生产根只创建 `utc_day=2026-07-14`、六个 source kind 目录和一个 `manifest.json`；journal 当前只有正常 Started 记录。当前状态为 `TASK_4_4_RTDS_DIAGNOSTIC_GATE_STARTED_20260714T224544Z`，必须保持同一 PID 跨 23:00 UTC 且连续运行至少到 `23:00:44.815054547Z` 后，再直接核验 finalized Parquet、manifest、四类外部源/六个 source kind、continuity、重复、journal 与无额外产物。

## 2026-07-15：RTDS 有界诊断 production gate 通过

- MainPID=`1054097` 保持不变并跨过 23:00 UTC；`2026-07-14T23:01:15.027743486Z` 已连续运行约 15分30秒，超过严格门槛。`23:05:12.872653492Z` 再采样时已运行 1,168 秒，仍 active/running、disabled、Restart=no、NRestarts=0，运行中 executable SHA、final binary SHA 和 unit SHA 与部署记录一致；journal 从启动起只有 1 条正常 Started，error/fatal/panic/lagged/drop/source_task 命中为 0。
- 22 UTC 小时六个 source kind 均生成 finalized Parquet，manifest=`data_collect_daily_manifest_v3`、state=open、build commit/binary SHA/host/region 正确，files=6、gaps=0。逐项核验磁盘 path/type、bytes、SHA 与 manifest 全部一致，总行数=`929,409`；生产根同时只有当前小时 5 个 `.parquet.running`、1 个 manifest 和上述 6 个 Parquet，unexpected artifact=0。`binance_depth_snapshot` 是事件稀疏源，22 时已有 1 个真实 snapshot；23 时无新 snapshot 因而没有空 `.running`，不属于缺失 source。
- 分源事实为：aggTrade=`4,469`、depth snapshot=`1`、depth update=`8,537`、Polymarket market=`915,550`、metadata=`8`、RTDS=`844`。每源 dropped_rows=0、lag_events=0、reconnect_events=0、source_gaps=[]；aggTrade finalized IDs `4014421704..4014426172` 唯一、严格连续、0 gap，manifest 当前 continuity 也为 unresolved_ranges=[]。depth 首更新桥接 snapshot，8,537 行全部 stitch、gap_rows=0、resnapshot=0。
- RTDS 844 行全部为 `topic=crypto_prices_chainlink`、`message_type=update`、`symbol=btc/usd`、fresh，age_ms=`822..2703`；未再出现 parser fault。Polymarket market 四种业务事件均有真实行，metadata 为 complete=6/unavailable=2 并覆盖 4 个真实 5 分钟 slug。对 finalized PM/RTDS 必要列做临时、自动删除的 bounded scan：PM 475,027 个唯一 source-native payload 只出现同一接收时刻的合法 fan-out 440,523 行，跨接收时刻 exact payload 重复=0；RTDS 844 个 payload 全唯一、跨接收时刻重复=0。所有本地临时副本已删除，没有生成报告/receipt/sidecar。
- 当前状态更新为 `TASK_4_4_RTDS_DIAGNOSTIC_GATE_PASSED_PENDING_STAGE_REVIEW`。gate 继续以同一 PID 和 Restart=no 运行；必须先由独立 `gpt-5.6-sol`/`ultra` 复核阶段 4 部署、数据、journal、文档与最小产物，PASS 后才可提交本轮 changelog并无重启切换长期 unit。

## 2026-07-15：RTDS 有界诊断阶段 4 独立审查通过

- 独立 `gpt-5.6-sol`/`ultra` 阶段审查结论 PASS，Critical=`0`、Important=`0`、Minor=`0`。截至 `2026-07-14T23:13:56Z`，PID `1054097` 已连续运行 1,691 秒，仍 active/running、disabled、Restart=no、NRestarts=0；commit/本地与远端 binary SHA/运行中 executable SHA/gate unit SHA 全部一致。
- 审查独立确认唯一生产根只有 1 个 manifest、6 个 finalized Parquet、5 个当前小时 running，额外文件为 0；manifest 与磁盘 6 文件总计 `929,409` 行、`61,959,580` bytes，rows/bytes/SHA 全部一致、gaps=0。旧 release、旧 gate unit、旧 manifest/receipt 路径均不存在。
- 审查独立复核 aggTrade ID 严格连续，depth snapshot `97416961394` 被首条 update `U=97416961395` 正确桥接且后续连续，RTDS 844 行全部 fresh/正确 topic/type/symbol且无 parser fault，PM/RTDS 跨接收时刻 exact duplicate=0；journal 仍只有 Started，lag/drop/fatal/panic/parser/reconnect 命中=0。临时 Parquet 副本均自动删除，未生成报告或 sidecar。
- 审查确认远端 gate unit 与仓库权威长期 unit 仅 `Restart=no`/`Restart=on-failure` 一行不同；阶段 4 文档提交并 push 后，可在机械复核 PID/executable/gate unit SHA 后原子替换 unit、daemon-reload、enable，禁止 restart 当前 PID。当前状态更新为 `TASK_4_5_RTDS_DIAGNOSTIC_STAGE_REVIEW_PASSED_READY_FOR_DOC_COMMIT`。

## 2026-07-15：阶段 4 文档提交与长期 unit 无重启切换

- 阶段 4 三份对齐文档精确提交为 `9ec514d68ebfc82ab37ab1bcc1a321174ae70d86`（`docs(data-collect): record RTDS diagnostic gate`）并已 push 到 `origin/after-data-plan`；本地 HEAD、remote-tracking ref 与 `ls-remote` 一致，用户 3 个既有 dirty 文件仍未纳入。
- 切换前机械复核 gate PID=`1054097`、NRestarts=0、运行中 executable/final binary SHA=`4e2bf2d8ce4c613ee3a54b8fc5aed26e674c41bfee148f3c4c7aa4b1da86adb1`、gate unit SHA=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c` 全部一致。仓库权威长期 unit SHA=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795` 使用不存在的 digest-named `.service` staging exclusive-create 上传，`systemd-analyze --user verify`、ExecStart、Restart=on-failure 与 RestartPreventExitStatus 均通过。
- `2026-07-14T23:16:45.469798996Z` 原子替换 unit、daemon-reload并执行 `enable`，没有调用 restart/stop/start。最终 unit SHA=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`，service enabled、active/running、Restart=on-failure、NRestarts=0；MainPID 仍为 `1054097`，ExecMainStartTimestamp 仍为 `2026-07-14T22:45:44Z`，运行中 executable SHA 未变，staging 消失。
- 当前状态更新为 `TASK_5_1_LONG_RUNNING_ACTIVE_PID_1054097`。26 小时同 PID 窗口从真实进程启动时间计算；同时必须观察至少一个 Binance WebSocket connection 的 24 小时 lifecycle 与 source 内恢复、UTC 日切、至少 312 个 nominal 5 分钟时槽，并按小时直接核对 service/journal/files/manifest，不生成监控报告文件。

## 2026-07-15：长期门因 RTDS 非契约对象失败

- 长期监控前段事实通过：`2026-07-15T00:01:42Z` 首个 UTC 日切正确关闭 07-14 manifest，并只创建 07-15 唯一 open manifest；closed 日 11 个 Parquet 的 path/bytes/SHA、690 row groups、`4,495,341` 行与 manifest 完全一致，旧日无 running。01 UTC 检查时 00 时六源已 finalized，01 时只有五个持续源 running，稀疏 snapshot 未生成空文件；PID/SHA/NRestarts 未变，gaps、agg unresolved、六源 lag/drop/reconnect 均为 0。
- 原 PID `1054097` 于 `2026-07-15T01:23:48.342129Z` 输出 `missing field topic at line 1 column 593` 并以 status 1 退出。有界诊断为 `frame_bytes=594`、SHA-256=`5b03c32e6b41344d8e332cefbcdb3a924e7892d7f0ffd3fba947e610b32b6f4c`、`top_level=object`、`selected={}`、字段名仅 `body/statusCode`；没有输出 body、statusCode 值、原 frame 或 prefix。该证据证明 frame 不是正常 `RtdsMessage` 契约，但不能单独证明其正文、产生者、触发请求或唯一根因。
- systemd 于 `01:23:48.355718Z` 记录旧 PID exit status 1，并在 `01:23:58.604648Z` 按长期 unit 启动 PID `1066927`、restart counter=1。因此同 PID 26 小时门立即失败；当时 Binance 24 小时 lifecycle 尚未完成、reconnect=0，finalized metadata/market 只覆盖约 33 个 slug，未达到 312。新进程继续使用同一 binary SHA，并以 part-1 恢复；manifest 诚实记录 RTDS fatal/source-task 三项 gaps及 RTDS lag/reconnect。

## 2026-07-15：failed-long-running 协调停服与数据清理

- `2026-07-15T01:34:16.062437342Z` 对 PID `1066927` 执行 `disable --now`；`01:34:16.632542Z` 程序输出 `coordinated_shutdown_complete`，`01:34:16.646650624Z` 命令返回。最终 service inactive/dead、disabled、MainPID=0、Result=success、ExecMainStatus=0；没有再次自动重启。failed pair binary SHA=`4e2bf2d8ce4c613ee3a54b8fc5aed26e674c41bfee148f3c4c7aa4b1da86adb1`、长期 unit SHA=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795` 保留。
- 删除门逐项证明两个 UTC 日均只属于 commit `ce87684e0ff4ef6787a96cabc33be12aa659bd75` 和上述 binary：07-14 closed 为 11 Parquet/`4,495,341` 行/`299,943,175` bytes/gaps=0；07-15 open 为 17 Parquet/`6,397,424` 行/`426,424,992` bytes/gaps=3。合计 28 Parquet、`10,892,765` 行、`726,368,167` Parquet bytes；每个 manifest path/bytes/SHA 与磁盘一致，manifest 文件数等于磁盘 Parquet 数，无 running/tmp/partial、symlink、未列入 manifest 的持久文件或其他 artifact。
- 删除前 binary holder=0、数据根 holder=0，114 个可读 `/proc/*/mountinfo` 对目标及 descendant mount 命中=0；最终身份、owner/type/realpath 与空闲 service 再次匹配。`2026-07-15T01:36:27.032581892Z` 删除整个 failed-build `training/typed_raw`，`01:36:27.252515845Z` 以 `ubuntu:ubuntu 0755` 重建空根；没有移动、打包或生成 inventory/report/receipt/sidecar。
- 当前状态为 `TASK_2_RTDS_ERROR_ENVELOPE_DIAGNOSIS_IN_PROGRESS_SAFE_STOPPED`。下一步只读核对现有 RTDS request 序列、SDK parser/connection 行为、官方协议与历史 issue；在证据不足时不得把 `body/statusCode` frame 擅自忽略、重试或改判为普通 transport。若需要改变已批准的 parser fatal 合同，必须先向用户列出冲突与选项并获得决定。

## 2026-07-15：RTDS 响应信封只读归因与用户决策门

- 运行时序已进一步收敛但尚未形成唯一根因。失败进程的 manifest 同时记录 RTDS `lag_events=1`、`reconnect_events=1`；代码只会在 60 秒 fresh wait 到期后先记 lag，再于第一次恢复动作记 reconnect并调用 `runtime.resubscribe()`。同一 connected epoch 中该调用经项目串行 actor 先调用官方 Rust SDK `unsubscribe`，再调用 `subscribe`，随后才等待 3 秒业务恢复。因此 594-byte `body/statusCode` frame 与第一次业务恢复周期具有直接时序关系；但现有日志没有逐请求服务端确认，不能断言是 unsubscribe、subscribe、限流、平台内部故障或其他服务端路径中的哪一个唯一产生了该 frame。
- 当前固定且仍为官方 v2 main 的 Rust SDK v0.6.0 实际发送：初始/恢复 subscribe 为 `topic=crypto_prices_chainlink,type=*,filters={"symbol":"btc/usd"}` 的 JSON 字符串；同 epoch unsubscribe 的公共 API 只接受 `TopicType`，实际发送相同 topic/type但不含 filters。官方 RTDS 文档仍示例 Chainlink `type=*` 与 JSON-string filters，并称 unsubscribe 使用相同结构；旧官方 `real-time-data-client` 也把调用方完整 subscription 原样透传。归档 Rust SDK issue #291 已证明 RTDS 会把 `Invalid request body` 这类非业务错误对象发进同一数据 socket；旧 TypeScript issue #21 也报告 Chainlink unsubscribe 收到该错误，但其示例使用大写 `BTC/USD`，不足以证明本项目小写请求必然失败。
- 2026-07-13 更新的官方统一 `ts-sdk` beta 与当前 `py-sdk` 又采用另一套已编码语义：Chainlink 固定发送 `topic=crypto_prices_chainlink,type=update`、不带 filters，客户端按 symbol 过滤，unsubscribe 也只发送 topic/type；TypeScript 对 `RealtimeEventSchema.safeParse` 失败的非业务 frame 直接不分发，Python 记录 dropped event 后继续。该行为与当前公开 RTDS 文档和固定 Rust SDK 的 `type=*`/server filter 路径并不一致。它只能证明平台与官方客户端契约正在演进，不能直接授权本项目改用 beta SDK 语义，更不能覆盖本项目已批准的 malformed target update/parser fault 必须 fatal 合同。
- “只给 Rust SDK unsubscribe 补回 filters”因此不是已证实修复；“直接 drop 并重建 `RtdsSdkRuntime`”也不是安全旁路：Rust `ConnectionManager` 没有公共 close/force-reconnect API，活跃连接对 sender 关闭没有明确退出分支，官方 v2 issue #39 另有 drop 后后台任务泄漏报告。直接忽略 `body/statusCode` 则会掩盖服务端拒绝或故障并违反 fail-closed。把该对象识别为 recoverable server response并强制重连是可行候选，但会改变 parser fatal 合同并扩大 SDK patch，尚未获批。
- 当前证据下推荐的最小下一步不是修改恢复语义，而是对同一既有有界诊断再补一层：仅允许输出顶层 `statusCode` 的标量、`body` 的类型/byte length/SHA-256；若 body 本身是 JSON 字符串或 object，只输出其顶层 shape、排序字段名和原已批准白名单 `message/status/code/error/connectionId/requestId` 的标量，仍不得输出任意 body、完整 frame 或 prefix，总诊断仍不超过 2,048 UTF-8 bytes，parser fault 继续 sticky/terminal/fatal。该增量能区分 4xx invalid request、429/rate limit 与 5xx/platform failure，再决定应修请求还是做有界 transport recovery；但它扩大了用户已冻结的字段白名单，必须先获得明确批准。
- 其他可选项均需独立授权：直接把 Chainlink 改为最新 beta SDK 的 `type=update`、无 filters、客户端过滤会改变订阅与 typed-raw 选择语义；把已识别服务端错误信封改为 recoverable 并新增 SDK 强制 reconnect 会改变 parser/连接合同；忽略错误信封不作为可接受选项。远端继续保持 failed pair inactive/dead、disabled、MainPID=0 与空生产根，不启动探针、collector 或新 gate。当前状态更新为 `TASK_2_RTDS_RESPONSE_ENVELOPE_DECISION_REQUIRED_SAFE_STOPPED`。

## 2026-07-15：用户批准第二层 RTDS 响应信封有界诊断

- 用户批准推荐的第二层有界诊断 patch。该批准只扩大失败诊断白名单，不批准改变 Chainlink `type`/filters、同连接重订阅顺序、parser fatal、sticky fault、terminal disconnected 或进程级恢复语义；也不批准忽略 `body/statusCode` 信封、进程内 drop/recreate runtime 或 raw WebSocket 旁路。
- 诊断合同同步冻结为：外层新增 `statusCode` 标量；`body` 为 string 时只记录 decoded UTF-8 byte length/SHA-256，其他 JSON 类型只记录递归排序 object keys 后的 canonical JSON byte length/SHA-256并使用明确的 `canonical_json` 字段名。string 正文能解析为 JSON或 body 本身为 object 时，只记录正文顶层 shape、排序后的顶层字段名及 `message/status/code/error/connectionId/requestId` 标量；禁止输出正文、prefix、嵌套字段/payload 或任意非白名单值，总诊断仍不超过 `2,048` UTF-8 bytes。
- 独立归因审查重新核对后结论为该窄 patch PASS、Critical=0、Important=0。删除前只读监控曾直接核对原 manifest 中最后一次 fresh RTDS receive 与 fatal 相差约 `60.843` 秒；该原 manifest 随 failed-build 数据按批准流程永久删除，因此这里只把当时读数作为“故障处于首次业务恢复时间窗”的强时序推断，不把 `lag_events=1/reconnect_events=1` 单独写成因果证明，也不宣称已唯一定位触发请求。
- service 继续保持 inactive/dead、disabled、MainPID=0，生产根为空。当前状态更新为 `TASK_2_RTDS_RESPONSE_ENVELOPE_DIAGNOSTIC_PATCH_APPROVED_IN_PROGRESS_SAFE_STOPPED`；源码、测试、独立实现审查及新 artifact 门全部通过前不重新部署。

## 2026-07-15：第二层 RTDS 有界诊断实现与阶段审查通过

- 本次源码只修改既有 vendor allowlist 内的 `src/rtds/subscription.rs`：外层顶级对象额外允许 `statusCode` 标量；string body 记录 decoded UTF-8 bytes/SHA-256并在可解析时读取正文顶层结构；其他 JSON body 递归排序 object keys 后记录明确命名的 canonical JSON bytes/SHA-256。正文 object 只允许排序字段名与 `message/status/code/error/connectionId/requestId` 顶层标量，嵌套值、payload、非白名单值、原 body/frame/prefix 均没有输出路径。成功 `parse_messages`、错误 kind/source chain、sticky parser fault、terminal disconnected、订阅与恢复代码未改。
- 新增 3 个专门测试并扩展 1 个边界测试，覆盖 string JSON body、非 JSON string、object canonical fingerprint、排序字段、嵌套 sensitive sentinel、长多字节正文与 `2,048` UTF-8 bytes 上限；原成功单对象/数组、畸形 JSON、typed array mismatch、控制字符和错误链回归继续通过。vendor `--lib --features clob,rtds,ws` 为 `164/164`；data-collect `--lib --bins --tests` 的库 `121/121` 及全部 integration tests通过，其中 SDK patch=`6/6`、SDK runtime=`17/17`。
- Rust `1.88` vendor default/all-features 两组 `--all-targets` clippy `-D warnings`、workspace all-targets clippy、workspace/vendor fmt、`git diff --check` 与唯一依赖树均通过。原始 `.crate` SHA-256 仍为 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`；临时解包全树比较仍只有获批的 `src/ws/connection.rs`、`src/clob/ws/types/response.rs`、`src/rtds/subscription.rs` 三个差异路径。
- 首次验证误用 `cargo test --all-targets`，在全部库与 integration tests 通过后进入默认 45 分钟的真实网络 benchmark；发现后立即以 SIGINT 终止，没有把未完成 benchmark 当作 PASS。该命令生成的 `data-collect/market-discovery-artifacts` 与 `data-collect/real-source-alignment-artifacts` 已精确删除，后续以不执行 bench 的 `--lib --bins --tests` 完整重跑并 exit 0；最终没有新增 report/receipt/sidecar 或 untracked验证目录。
- 两名独立 `gpt-5.6-sol`/`ultra` 审查者分别检查诊断安全/资源边界与 SDK-first/文档/错误传播，均结论 PASS，Critical=0、Important=0、Minor=0；审查源码 SHA-256=`3e777846f209e1b9f28b6d7722e481fcce5339856b32eec4dbea8e8ec7f7db72`。审查确认 2,048-byte UTF-8 bound、canonical 命名、嵌套不泄漏、成功解析、原 error chain、sticky/terminal/fatal 与同连接 `unsubscribe -> subscribe` 均未退化，文档没有把响应信封夸大为唯一根因。
- 远端只读复核仍精确匹配已记录 failed-long-running pair：service inactive/dead、disabled、MainPID=0、Result=success、ExecMainStatus=0、NRestarts=0；旧 binary SHA-256=`4e2bf2d8ce4c613ee3a54b8fc5aed26e674c41bfee148f3c4c7aa4b1da86adb1`、长期 unit SHA-256=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`，生产根为空且无 upload/staging 文件。当前状态更新为 `TASK_2_RTDS_RESPONSE_ENVELOPE_DIAGNOSTIC_STAGE_REVIEW_PASSED_READY_FOR_COMMIT`；精确提交并 push 前不构建或部署。

## 2026-07-15：第二层 RTDS 有界诊断提交与干净构建

- 四个批准文件精确提交为 `6e6759b8d6138e081b69ada3fd69be1ead43d133`（`fix(data-collect): inspect RTDS response envelopes safely`）并 push 到 `origin/after-data-plan`；本地 HEAD、remote-tracking ref 与 `ls-remote` 一致。提交只含 demand/plan/changelog 与既有 vendor allowlist 内的 `src/rtds/subscription.rs`；用户 `AGENTS.md`、`demand/rebuild-core-demand.md` 和 `推荐开发顺序.md` 仍留在主工作区且未纳入。
- 从已 push commit 创建 `/tmp/data-collect-build-6e6759b8d613` detached worktree，HEAD 精确匹配且构建前后 clean；以独立 `CARGO_TARGET_DIR=/tmp/data-collect-target-6e6759b8d613` 和 `DATA_COLLECT_GIT_COMMIT=6e6759b8d6138e081b69ada3fd69be1ead43d133` 执行 locked release build。产物 SHA-256=`078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`、size=`25,467,504` bytes，为 x86-64 PIE ELF、loader=`/lib64/ld-linux-x86-64.so.2`；内嵌完整 commit、无副作用 `--version`/`--help` 均通过。
- 产物经不存在的 digest-named local staging、SHA 复核与 atomic rename 放到主仓库固定 `target/release/data-collect-training-collector`，mode=`0755`；detached worktree、独立 build target 与前述 vendor 验证 target 均已删除，无 local staging 残留。当前状态更新为 `TASK_4_1_RTDS_RESPONSE_ENVELOPE_DIAGNOSTIC_BUILD_PASSED_PENDING_REMOTE_RESTAGE`；步骤 4.2 全部门通过前不得修改远端 final binary/unit 或启动服务。

## 2026-07-15：第二层 RTDS 诊断版远端预置与全量预检

- failed-long-running 入口再次机械复核通过：service inactive/dead、disabled、MainPID=0、Result=success、ExecMainStatus=0、NRestarts=0；旧 final binary/unit SHA 分别仍为 `4e2bf2d8ce4c613ee3a54b8fc5aed26e674c41bfee148f3c4c7aa4b1da86adb1` 与 `7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`。binary 与 `training/typed_raw` holder=0，114 个可读 mount namespace 中目标及 descendant mount 命中=0；逐级 path owner/type/non-symlink 均匹配，生产根为空，新 digest staging 原先不存在。
- 首次尝试以 `dd oflag=excl` 上传时，远端 coreutils 明确报 `invalid output flag: excl` 并 exit 1；随即复核目标仍不存在且未留下空文件。之后使用 POSIX shell `noclobber` 对 digest 路径单次独占打开，并将 binary/unit 分别写入该已独占 fd；任一写入/权限步骤失败均由 trap 删除本轮未完成 staging。没有降低 fail-if-exists 语义，也没有覆盖未知对象。
- 新 binary staging SHA-256=`078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`、size=`25,467,504`、mode=`0755`；为 x86-64 PIE ELF，loader=`/lib64/ld-linux-x86-64.so.2`，全部动态库可解析，远端 glibc=`2.39`。内嵌 commit、`--version`、`--help` 均通过且没有创建数据。新 gate unit staging SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`、mode=`0644`，`systemd-analyze --user verify` 通过；与现有长期 unit 的唯一 diff 仍为 `Restart=on-failure` 改成 `Restart=no`，ExecStart、TimeoutStopSec、RestartPreventExitStatus 与 StartLimit 均未变。
- 预检后旧 final binary/unit、service 状态与空生产根仍未改变。当前状态更新为 `TASK_4_2_RTDS_RESPONSE_ENVELOPE_DIAGNOSTIC_REMOTE_PREFLIGHT_PASSED_WAITING_GATE_WINDOW`；只允许在 `2026-07-15 02:45–02:50 UTC` 重新复核身份后执行 atomic replace、daemon-reload 与 start，不提前启动。

## 2026-07-15：第二层 RTDS 诊断版 gate 原子切换与启动

- `02:45 UTC` 窗口的首次完整切换脚本在任何 rename 前因 unit-specific `systemctl --user reset-failed data-collect-training-collector.service` 返回 `Unit ... not loaded` 而安全停止；四个冻结 SHA 逐项证明 final/staging 均未变化。对旧 unit 执行 daemon-reload 后再次 unit-specific reset 仍因 disabled+inactive+Result=success unit 被立即 GC 而无对象可重置。随后先证明 user manager failed units=`0`，再执行无目标 `reset-failed` 并证明前后 failed units 均为 0；该操作是可验证 no-op，没有清除其他 unit 的失败状态。
- 最终脚本在窗口内重新执行完整门：日期/小时/分钟、service inactive/dead/disabled/MainPID=0/NRestarts=0、四个 SHA、owner/mode/普通文件/单硬链接/同 filesystem、空生产根、holder=0、115 个可读 mount namespace 中 descendant mount=0、failed units=0 全部通过。随后先 atomic rename binary并复核 final SHA，再 atomic rename gate unit并复核 unit SHA，daemon-reload 后确认 loaded `Restart=no` 且 service 仍 inactive，最后只执行一次 start；没有 enable、没有提前换长期 unit。
- start request=`2026-07-15T02:48:10.136517216Z`，MainPID=`1074411`，service active/running、disabled、Restart=no、NRestarts=0；运行中 `/proc/1074411/exe`、final binary SHA 均为 `078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`，final gate unit SHA=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，两个 staging 路径消失。初始 journal 只有正常 Started；约 30 秒采样仍为同 PID/NRestarts=0，生产根已有唯一 manifest 与六源 writer 路径中的实际初始文件，没有 error/fatal/parser/reconnect 日志。
- 当前状态更新为 `TASK_4_4_RTDS_RESPONSE_ENVELOPE_DIAGNOSTIC_GATE_ACTIVE_PID_1074411`。从真实进程启动时间计算不少于 15 分钟，并跨 `03:00 UTC` 小时边界直接检查六源、manifest、Parquet、journal 与新诊断；通过前不切换长期 unit。

## 2026-07-15：第二层 RTDS 诊断版 gate 跨小时与直接内容审计

- gate 从 `2026-07-15T02:48:10.136517216Z` 持续跨过 `03:00 UTC`；截至 `03:21:28.786113399Z` 已运行超过 33 分钟，仍为同一 MainPID=`1074411`、active/running、disabled、`Restart=no`、NRestarts=0，`/proc/1074411/exe` SHA-256 仍为 `078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`。journald 只有正常 Started，未出现 parser/error/fatal/panic/lag/reconnect；数据根没有 `*.tmp` 或 `*.partial`。
- 02 UTC 六个 finalized Parquet 与唯一 open manifest 直接复核通过：实际 file size、SHA-256、row count、物理 schema、内嵌 `data_collect_daily_file_fact_v1` 与 manifest 六条 file fact 全部一致；build commit/binary/host/region 正确，`gaps=[]`，六个 source 的 drop/lag/reconnect/backfill 均为 0。03 UTC 五个连续/事件 source 正在各自唯一 `*.running` 继续写入；稀疏 depth snapshot 没有制造空 03 文件。
- Binance 内容审计确认 aggTrade `2,019` 行的 native ID 从 `4014507211` 到 `4014509229` 严格连续且唯一，typed fields 与嵌套 websocket source-native JSON 逐行一致，unresolved range 为空；depth snapshot lastUpdateId=`97422622048`，`7,082` 条 update 的首条 `u=97422622065` 正确桥接，之后每条 `local_u_before` 都等于前一 `u`，最终 `u=97422797079`，gap/resnapshot 均为 0。
- Polymarket metadata 共 8 行，覆盖 4 个 slug/8 个 asset；6 行完整，2 行明确记录 `market_resolved_not_observed_before_window_end`，不存在无理由缺字段。market Parquet 共 `619,367` 行，对应 `322,295` 条唯一 source-native 消息；其中 `297,072` 行是同一 price-change 消息的合法双资产 fan-out，最大 fan-out=2。逐消息重建的 normalized rows 与 source-native payload 完全对应，没有 payload 在另一接收时刻重放。RTDS 共 `692` 条，全部 `type=update`、topic=`crypto_prices_chainlink`、symbol=`btc/usd`、`freshness_status=fresh`，age=`829..2388ms`；source-native payload 全部唯一且与 typed timestamps/value 逐行一致。
- 审计只在自动删除的 `/tmp` 目录读取远端副本，不生成 report/receipt/sidecar。两次前置断言失败分别来自审计脚本误读 schema-level 而非 file-level Parquet metadata，以及错误地把 manifest 规范化键 `depth_update` 当作物理原生字段（实际为 `depthUpdate`/`btcusdt@depth`）；另一次前置断言未先解开 aggTrade 的 websocket/rest 双证据外层。三处都在读取阶段 fail closed、由 trap 清理临时目录，未修改远端数据或运行状态；按真实 schema 修正后相关完整断言均通过，不能把辅助脚本错误记成 collector 缺陷。
- 当前状态更新为 `TASK_4_4_RTDS_RESPONSE_ENVELOPE_DIAGNOSTIC_GATE_DATA_AUDIT_PASSED_PENDING_STAGE_REVIEW`。下一步只允许按步骤 4.5 派生独立 `gpt-5.6-sol`/`ultra` 只读审查；PASS 且本轮文档提交/push 前不切换长期 unit。

## 2026-07-15：第二层 RTDS 诊断版阶段 4 独立审查通过

- 独立 `gpt-5.6-sol`/`ultra` 审查结论为 PASS，Critical=`0`、Important=`0`、Minor=`0`，无需停止 gate。审查者没有复用主线程统计，而是在自动删除的 `/tmp` 中独立下载并全量复算六个 02 UTC Parquet；manifest/footer/SHA/counters、aggTrade 2,019 个连续唯一 ID、depth 7,082 条桥接与连续性、metadata 8 行、Polymarket 619,367 行/322,295 条 source message/零跨 receive 重放/297,072 条合法 fan-out、RTDS 692 条 fresh/unique/typed-native parity 全部通过。
- 审查者首次 RTDS canonical 辅助断言因同时使用 `parse_float=str` 后再序列化而把 JSON number 错变为 string；该临时审查口径 fail closed 且目录由 trap 清理。随后以普通 JSON parse 检查 canonical bytes、另用保留数值文本的 parse 核对 value，完整重跑通过；这不是 collector 或数据问题。全部 `dc-stage45-*` 临时目录已删除，没有持久报告、receipt 或 sidecar。
- 截至审查最终复采 `2026-07-15T03:37:41Z`，service 仍为同一 MainPID=`1074411`、active/running、disabled、`Restart=no`、NRestarts=0；运行中 executable/final binary SHA=`078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`，gate unit SHA=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，journal 仍只有正常 Started。旧 release/current/gate shadow、旧数据产物和 staging 均不存在；生产根只有唯一 manifest、六个 finalized、五个当前小时 running，tmp/partial/report/receipt/sidecar/validation 均为 0。
- demand/plan/changelog 的事项编号、状态、事实和步骤 4.5 门一致，`git diff --check` 通过。当前状态更新为 `TASK_4_5_RTDS_RESPONSE_ENVELOPE_DIAGNOSTIC_STAGE_REVIEW_PASSED_READY_FOR_COMMIT`；下一步精确提交并 push 本轮三份文档，然后才能按步骤 5.1.1 在不重启 MainPID 的前提下切换长期 unit。

## 2026-07-15：阶段 4 文档提交与无重启切换长期 unit

- 阶段 4 的 demand/plan/changelog 精确提交为 `dd6775a62289fcfcc4b6e96ed9c69731651151fa`（`docs(data-collect): record diagnostic gate review`）并 push；本地 HEAD、`origin/after-data-plan` 与 `ls-remote` 一致。提交只含这三份权威文档，用户 `AGENTS.md`、`demand/rebuild-core-demand.md` 与 `推荐开发顺序.md` 未纳入。
- repo 权威长期 unit `data-collect/deploy/systemd/data-collect-training-collector.service` SHA-256=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`。切换前重新证明当前 gate unit SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，两者唯一 diff 为 `Restart=no` → `Restart=on-failure`；ExecStart、RestartSec、RestartPreventExitStatus、StartLimit、KillSignal 与 TimeoutStopSec 均不变。长期 unit 以不存在的 digest-named staging 路径独占写入，remote owner=`ubuntu:ubuntu`、mode=`0644`、单硬链接、与 final 同 filesystem，SHA 与 `systemd-analyze --user verify` 均通过。
- 最终切换前 service 为 MainPID=`1074411`、启动 monotonic timestamp=`10186829825114`、active/running、disabled、`Restart=no`、NRestarts=0；运行中 executable/final binary SHA-256 都是 `078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`。随后只 atomic rename unit、daemon-reload 和 enable，没有 stop/start/restart。
- `2026-07-15T03:41:30.130725317Z` 切换后，MainPID 仍为 `1074411`，启动 monotonic timestamp 仍为 `10186829825114`，active/running、enabled、`Restart=on-failure`、NRestarts=0；executable/binary SHA 未变，final unit SHA 精确为 `7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`，staging 消失，enable symlink 指向唯一 final unit，journal 仍只有原始 Started。
- 当前状态更新为 `TASK_5_1_LONG_RUNNING_ACTIVE_PID_1074411_WINDOW_START_20260715T024810Z`。步骤 5.1.2 的同 PID 26 小时从真实启动 `2026-07-15T02:48:10.136517216Z` 计算，最早验收时刻为 `2026-07-16T04:48:10.136517216Z`；同时必须覆盖 Binance 24 小时 connection lifecycle 与 source 内恢复、UTC 日切和至少 312 个 nominal 5 分钟时槽。任何 MainPID 变化重置整个 26 小时窗口，connection 提前重连只重置其 24 小时子窗口。

## 2026-07-15：长期运行窗口首次重置与 RTDS 服务端错误证据

- 小时监控在 `05:06:49Z` 因 PID/NRestarts 冻结断言失败而报警；拆解复核确认 MainPID=`1074411` 已于 `04:11:25.574760Z` 输出唯一 RTDS parser fatal。第二层有界诊断直接证明收到的是 594-byte 外层 object：`statusCode=400`、body 为 object，canonical body SHA-256=`58cf46af0d28de31ba9f01a10e002a683fa804a58d9c71e986825e64cc448ede`；允许的 message 标量明确为服务端 `leger AddSubscriptions` 的 `ERROR #23503 ... connection_id_fk` foreign-key constraint error。没有输出完整 frame/body/payload，2,048-byte bound 与既有 fatal 语义均按批准设计生效。
- 当前证据只证明平台在处理该次 AddSubscriptions 时返回后端约束错误，不能据此宣称唯一根因在 collector、SDK 或平台某一方；但没有 binary/unit/schema/writer 不变量失败。需求已明确 RTDS parser fault 必须 fatal，SDK 0.6.0 无公开 force-reconnect，进程退出后由 systemd 新进程重建 SDK client。因此本次属于冻结恢复合同覆盖的外部响应失败，不是需要删除有效数据的 failed-build/code/unit/data-correctness gate。
- 旧进程协调 shutdown 后，04 UTC 五个连续/事件 source 以 `rollover_reason=shutdown_finalize` 完整封口；systemd 在 10 秒 RestartSec 后于 `2026-07-15T04:11:35.607659Z` 启动同 binary/unit 的 MainPID=`1078240`。当前 executable/binary SHA 仍为 `078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`，unit SHA 仍为 `7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`，active/running、enabled、`Restart=on-failure`、NRestarts=1。
- manifest 把同一事件分别记录为 parser fatal、source task unexpected end 与 source task error，共 3 个可追踪 gap；RTDS lag/reconnect 各为 1。新进程恢复最近 finalized 数据后继续写入：aggTrade 自动 backfill 25 行，latest observed/contiguous 相等且 unresolved ranges 为空；depth 新取 1 个 snapshot；同一 04 小时的续写使用计划允许的 `part-1`，05 小时回到主文件名。没有 drop、stale running、tmp/partial 或额外 report/receipt/sidecar。
- 按步骤 5.1.2，“任何 systemd 进程退出/MainPID 变化都从新 MainPID 重置整个 26 小时窗口”；因此窗口和 Binance connection 子窗口均从 `2026-07-15T04:11:35.607659Z` 重新计算，最早 24 小时 connection checkpoint 为 `2026-07-16T04:11:35.607659Z`，最早 26 小时验收时刻为 `2026-07-16T06:11:35.607659Z`。当前状态更新为 `TASK_5_1_LONG_RUNNING_RESET_TO_PID_1078240_WINDOW_START_20260715T041135Z`，继续逐小时监控；若出现 code/unit/data correctness 问题才按步骤 5.2 fail gate，外部失败或新的 PID 变化继续如实记录并重置窗口。

## 2026-07-15：相同 RTDS 服务端错误第二次复现与方案决策门

- `2026-07-15T11:14:54.483660Z`，MainPID=`1078240` 再次收到 594-byte RTDS 外层 object：frame SHA-256=`5b03c32e6b41344d8e332cefbcdb3a924e7892d7f0ffd3fba947e610b32b6f4c`、`statusCode=400`、body canonical JSON SHA-256=`58cf46af0d28de31ba9f01a10e002a683fa804a58d9c71e986825e64cc448ede`，message 仍为 `leger AddSubscriptions` 的 PostgreSQL `#23503` / `connection_id_fk` foreign-key constraint error。它与 `04:11:25.574760Z` 第一次响应的长度、两个 SHA、shape、字段与白名单 message 完全相同；这不是根据相似文字推断，而是有界诊断的精确重复指纹。
- 第二次 parser fatal 后，systemd 于 `11:15:04.604599Z` 以同 binary/unit 启动 MainPID=`1081425`。截至 `12:24:27Z`，service active/running、enabled、`Restart=on-failure`、NRestarts=2；binary SHA-256 仍为 `078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`，unit SHA-256 仍为 `7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`。11 UTC 主文件以 `shutdown_finalize` 封口并由合法 `part-1` 续写，12 UTC 当前文件继续增长；RTDS lag/reconnect 各为 2，aggTrade 累计 backfill 208 行且 `last_observed_id=last_contiguous_id`、unresolved ranges 为空，depth 已重新取 snapshot，drop 仍为 0。manifest 的 3 个 gap 字符串因内容完全相同而去重，没有把第二次发生伪装成新错误种类。
- 代码路径与冻结合同一致：项目 RTDS freshness 恢复在 60 秒无 fresh event 后调用官方 `SubscriptionManager` 的 `unsubscribe -> subscribe`；vendor `ConnectionManager` 当前把所有 `parse_messages` 错误设置为 sticky parser fault 并 terminal disconnected，项目再按 fatal error 协调退出。Polymarket [官方 RTDS 文档](https://docs.polymarket.com/market-data/websocket/rtds) 明确说明相同结构可用于 unsubscribe，且订阅可在不断线时增删修改；官方 Rust SDK [CHANGELOG](https://github.com/Polymarket/rs-clob-client/blob/main/CHANGELOG.md) 也记录了 RTDS unsubscribe/reference-counting 支持。因此现有证据不支持把这两次后端约束错误归因为 collector 使用了文档禁止的订阅动作；同时它仍不足以唯一判定是平台 backend、连接生命周期竞态还是 SDK 错误分类中的哪一项根因。
- 方案冲突已触发：需求 3.1.3/3.1.5 要求该 parser fault 保持 fatal，3.1.6 与计划停止条件 6 禁止未经批准扩大固定五项 vendor patch；需求验收 6 与计划 5.1.2 又要求同一 PID 覆盖 26 小时并包含一个 Binance connection 的 24 小时生命周期。完全相同的外层服务错误约 7 小时内两次触发进程恢复，说明“只继续重置窗口直到碰巧通过”是低把握且无界的完成策略，这三组约束不能由执行者自行调和。
- 等待用户决策的选项为：`A（建议）`批准窄化扩大 vendor patch，只把严格匹配的 RTDS 外层服务错误信封分类为可恢复 connection fault，使 SDK 关闭当前 socket 并进入既有同进程 backoff/reconnect/generation replay；目标 topic/type 的 malformed data/schema 仍必须 fatal，项目仍记录 gap/reconnect，不使用 raw WS、手写协议或 SDK 旁路，并补精确 observed-envelope、false-positive、schema-fatal 与 reconnect/replay 测试。`B` 不改代码，把生产验收改为同 binary/unit 在有界 systemd 恢复间连续 26 小时；这会放弃同 PID 内存稳定性与单个 Binance connection 24 小时生命周期证据。`C` 保持全部合同并继续反复重置窗口；鉴于两次实证，不建议采用。A、B 都会改变已批准需求/计划，未获用户明确选择前不实施。
- 当前服务继续 enabled/running 采集，未停止、未重启、未替换 binary/unit、未清理有效数据；步骤 5.1.2 的最终验收结论暂停在 `TASK_5_1_RECURRING_RTDS_SERVER_ERROR_DECISION_REQUIRED_PID_1081425`，等待用户决定后再同步修改需求/计划并进入实现或验收。

## 2026-07-15：第三次相同故障、独立方案审查与 D2 批准

- `2026-07-15T12:30:31.062283Z`，MainPID=`1081425` 第三次收到完全相同的 594-byte RTDS 服务信封：frame SHA-256=`5b03c32e6b41344d8e332cefbcdb3a924e7892d7f0ffd3fba947e610b32b6f4c`、body canonical JSON SHA-256=`58cf46af0d28de31ba9f01a10e002a683fa804a58d9c71e986825e64cc448ede`、`statusCode=400`，白名单 message 仍为 `AddSubscriptions` / PostgreSQL `#23503` / `connection_id_fk`。systemd 于 `12:30:41.104605Z` 恢复为 MainPID=`1082455`、NRestarts=3；截至 D2 实施启动前仍 active/running、enabled，binary/unit SHA 分别保持 `078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc` 与 `7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`。
- 三名独立 `gpt-5.6-sol`/`ultra` 审查者分别核对运行触发链、需求/验收冲突与官方资料；事件诊断和三次同指纹均 PASS，但原 A 方案因范围过宽且 existing reconnect attempt/backoff 在 successful handshake 后重置而不能直接批准。审查进一步确认固定 Rust SDK 的 `unsubscribe(&[TopicType])` 构造服务请求时把原 filtered Chainlink subscription 的 `filters` 写为 `None`；这是与官方 RTDS “same structure” unsubscribe 语义不对称的真实 SDK 表达缺口，但无 outbound 抓包，不能把它夸大为 `connection_id_fk` 的唯一根因。
- 两轮交叉复审一致推荐 D2：不给 service envelope 增加 recoverable 分类，而是在官方公共 `ConnectionManager` 增加 generation-aware one-shot reconnect；60 秒无 fresh 时移除同 socket `unsubscribe -> subscribe`，当前 generation 安全结束后由新 `Connected` epoch 的唯一 actor 从空 server state重放一次未改变的完整 `btc/usd` desired。ticket必须绑定generation、stale/future/非Connected no-op、同generation合并且控制状态有界；applied ack携带generation+revision；每个unresolved episode最多两个新generation，自然flap也计数，仍无fresh则整进程退出。所有service envelope、parser与目标schema错误继续sticky/terminal/fatal。
- 用户明确回复“批准 D2”。需求与计划已把 vendor patch 边界从五项扩大为固定六项；D2 仍坚持 SDK-first，不使用 raw WS、手写 subscription、manual signing、第二个 SDK client/receiver/actor，也不修改官方 topic/type/filter。A+ 只保留为 D2 新 socket首次replay仍收到同类服务信封时的后续新决策门，B/C均不采用。当前状态更新为 `TASK_2_D2_GENERATION_AWARE_RECONNECT_APPROVED_IMPLEMENTATION_IN_PROGRESS`；本地测试和阶段3独立审查PASS前不构建或替换远端运行版本。

## 2026-07-15：D2 运行中升级冲突与用户选择方案 2

- 两名独立`gpt-5.6-sol`/`ultra`文档审查者一致判定D2技术合同基本完整，但现有阶段4只接受首次切换或inactive失败重试，无法合法覆盖当前final binary/unit已存在且旧服务active/running的真实状态；错误复用首次切换还会再次进入已完成的旧release与全数据根清理。该Important问题在解决前阻止实现和提交。
- 代码复核确认`reconcile_open_day_manifest`要求当前UTC日manifest的build commit、binary SHA、config、host与region全部等于新runtime identity，因此不同build不能接管同一open日。远端于`2026-07-15T13:08:09Z`仍为MainPID=`1082455`、NRestarts=3、active/running；`utc_day=2026-07-15`manifest为old build commit=`6e6759b8d6138e081b69ada3fd69be1ead43d133`、binary SHA=`078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`、state=open、files=74、gaps=3。
- 用户在“跨UTC日保留当前数据”与“删除当前日后立即切换”之间明确选择方案2。新增`approved_running_upgrade`入口：全部实现审查、隔离构建及远端staging预检通过前旧服务继续运行；之后只可协调停服并删除old build独占的绝对路径`/home/ubuntu/btc-updown-marketmaker/datasets/training/typed_raw/utc_day=2026-07-15`。`2026-07-14`及更早closed日期必须保留，本轮不再执行首次部署的旧program或全training根清理。
- 删除前后均须复核service/PID、old binary/unit、manifest/footer identity、owner、句柄与descendant mount；删除前中断可在old pair未变时恢复旧服务，删除后记录`approved_running_upgrade_data_cleared`并保持inactive，只能按已知old/new pair继续D2切换。审查者指出的旧同socket RTDS重订阅文字已直接改写；service/parser/schema fault也明确为新generation replay期间或之后从异步fault路径观察，而非误称`subscribe()`同步返回。
- 第一轮部署复审另发现4.3.1仍把已经不存在的旧gate shadow当成必停对象；D2分支现已改为只`disable --now`唯一main unit，并把旧gate文件、enable symlink均不存在及`LoadState=not-found`作为正向门。意外存在必须停止，不能靠忽略unit-specific命令错误继续。
- 第二名复审者在运行中升级删除门继续发现三个Important：非原子递归删除的部分状态可能被误判为可恢复old service；只核对manifest/footer identity不能证明未知树对象有权删除；通用“干净数据根”措辞可能误删必须保留的prior closed日期。文档因此继续保持FAIL，未提交也未进入代码实现。
- 修订后先要求manifest file facts与磁盘finalized Parquet的路径、数量、bytes、SHA及footer identity双向完全相等，并拒绝symlink、额外目录/文件、running/tmp/digesting与非普通对象；停服后的排序tree facts只记录digest/计数到既有changelog，不生成inventory/report。完整树随后同filesystem原子rename为唯一digest-bound`.deleting-*`，按Parquet/source目录、manifest、空tombstone顺序清除。中断只能落在原路径完整、tombstone部分清除或两者都不存在三类可识别状态；未知组合保持inactive。
- “干净数据根”已按部署入口拆分：D2失败只清理其独占current-day与tombstone，全部冻结prior closed日期及`typed_raw`父目录必须保留；首次部署的历史空根语义不再套用D2。上述修订后重新请求两名独立审查者复核。
- D2实现落点只读审计发现`demand/data-collect-demand.md`第119、226行仍保留每3秒同socket重发、每两次重建SDK client与最多20次的旧RTDS合同；它与用户已批准的单client/connection、observed-generation ticket、最多两个新generation不能同时成立。旧需求现已在原位置明确由本事项D2取代，本需求的supersession清单同步引用，future paper/live仍由各自需求决定；未改动其他source合同。
- 同一轮检索还发现验收5与计划5.3.2残留“重建干净根”泛化措辞；现均按入口分支，D2 gate/long-running失败不得清空prior closed日期或重建`typed_raw`父目录。四份需求/计划/changelog随后一起进入重新独立审查。
- 两名最终复审者一致指出初始升级的固定Jul15/open-manifest门不能被26小时long-running失败泛化复用；后者可能涉及Jul16以后一个或多个open/closed日。初始old-build清理仍严格固定Jul15，另增加D2 failed-build参数化逐日门：必须先显式枚举有限`affected_days[]`及每个target day/build/state/tree/tombstone，禁止glob/range，按UTC日升序一次只处理一个；Jul14及更早初始冻结日期与全部非目标日永不进入清理。
- D2 gate失败明确实例化`affected_days=[2026-07-15]`与本轮D2 SHA；long-running失败按实际缺陷范围逐日实例化。每个target复用完整树、atomic tombstone、manifest-last和互斥中断状态机，当前tombstone未消失前不处理下一日；计划中的“四份权威文档一致”计数也已修正。上述修订再次进入最终独立复审。
- 两名独立文档审查者最终均对四份权威文档给出PASS，Critical=`0`、Important=`0`、Minor=`0`；初始Jul15门、failed-build逐日门、旧RTDS需求supersession、异步fatal与无额外产物均通过。提交前远端只读采样又发现旧版本于`2026-07-15T13:40:38Z`第4次收到完全相同的594-byte服务信封，frame/body SHA仍分别为`5b03c32e6b41344d8e332cefbcdb3a924e7892d7f0ffd3fba947e610b32b6f4c`与`58cf46af0d28de31ba9f01a10e002a683fa804a58d9c71e986825e64cc448ede`；systemd于`13:40:48Z`恢复MainPID=`1084549`、NRestarts=4，服务active/running，binary/unit未变。
- D2实现审计确认裸`RtdsMessage` broadcast不携带generation，旧socket已排队fresh可能在新generation applied ack后才被消费；若直接接受会错误恢复episode。获批语义所需的最小项目层修复是不增加receiver、不丢行：观察更高generation ack时以`Receiver::len()`建立`freshness_ineligible_remaining`，fence消息继续typed/dedupe/落盘但不恢复健康；切代时重置为当前len，同generation revision不重置，消息返回前二次检查ack，`Lagged(n)`仍fatal。该fence可能保守排除极早到达的新generation消息，但只造成false negative，不会让旧消息false positive恢复；验收已补相应竞态测试。
- 两名聚焦复审者随后均以Critical=`0`、Important=`1`、Minor=`0`判定上述文字仍有单消息off-by-one：若唯一旧消息已被`recv()`取得，二次检查才首次看到新ack，此时`Receiver::len()==0`；或取得前remaining=1却先减为0，均可能错误放行当前旧消息。合同已改为独立判定当前消息：二次检查首次发现更高generation时当前消息无条件ineligible、`len()`只约束后续消息；否则以取得前remaining决定当前消息并在标记后再减一。需求与计划同时补“唯一旧消息/len=0”和“最后一条fence/remaining=1”确定性回归，修订后重新进入聚焦复审。
- 修订后两名独立`gpt-5.6-sol`/`ultra`审查者均给出PASS，Critical=`0`、Important=`0`、Minor=`0`；确认current-message与后续queue fence状态转移闭合，单receiver、不丢行、Lagged fatal、两代上限和方案2删除边界均未退化。文档状态更新为`TASK_2_D2_DOCUMENT_REVIEW_PASSED_READY_IMPLEMENTATION`，可以进入D2代码实现；远端构建、停服、删除和部署门仍关闭。

## 2026-07-15：D2 本地实现、自审修正与实现审查门

- 第六项 vendor patch 只在既有 allowlist 的 `src/ws/connection.rs` 增加 generation-aware one-shot reconnect。公共 `ConnectionManager` 公开 Acquire-load generation；latest-value watch ticket 在入口与 active socket 消费点都复核 exact active generation，stale/future/非 Connected请求 no-op，同 generation 重复请求合并。新握手在发布 `Connected` 前清理旧 ticket；parser fault仍优先进入原 sticky/terminal 路径，outbound generation、同 generation FIFO 与公开 send/auth API 均未改变。
- 项目 RTDS runtime 删除 freshness 路径中的同 socket `unsubscribe -> subscribe`。唯一 actor 在每个新 `Connected` generation 新建一个官方 `SubscriptionManager`，只发送一次完整 latest filtered desired；applied watch 携带 `(generation, revision)`。唯一 broadcast receiver 以 `Receiver::len()`建立 freshness-only fence，fence 内消息仍正常 decode、dedupe与落盘，`Lagged(n)`仍 fatal；60秒无 fresh 后的 episode 最多接受两个新 generation，自然 transport flap 同样计数，只有 eligible、匹配、fresh、非重复业务消息恢复健康。
- 自审发现并修正一处更窄的 ack/message 竞态：消息取得后的二次 ack 检查与随后再次读取 applied watch 之间，新的 ack 可能到达并在尚未建立 fence 时被误用于资格判断。修正后资格只比较二次检查已经登记在 fence 内的 applied generation 与当前 active connection generation；迟到但尚未被该检查观察的 ack 不能放行当前消息。新增确定性回归覆盖该窗口，并保留 `len()==0` 当前消息与取得前 `remaining=1` 最后一条 fence 两个边界。
- 修正后 `cargo test -p data-collect --locked` 全部通过：lib=`126/126`、SDK patch contract=`7/7`、SDK runtime contract=`17/17`及其余 integration tests 全部成功；data-collect locked clippy `-D warnings`、workspace fmt与`git diff --check`通过。vendored SDK Rust 1.88 all-features lib=`179/179`，default/all-features两组 all-targets clippy `-D warnings`与 pinned nightly fmt通过。
- crates.io原始 `.crate` SHA-256 复核为 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`；自动删除临时目录中的全树比较仍恰为获批的 `src/ws/connection.rs`、`src/clob/ws/types/response.rs`、`src/rtds/subscription.rs`三个差异文件，dependency tree只有仓库内p1，vendor无`target/`残留。当前状态更新为`TASK_2_D2_IMPLEMENTATION_REVIEW_PENDING`；两路独立实现审查均PASS前不提交、构建或触碰远端。

## 2026-07-15：D2 首轮实现审查 FAIL、逐项修正与复审门

- 三名独立 `gpt-5.6-sol` / `ultra` 审查者均给出 FAIL：deployment/gate 审查 Critical=`0`、Important=`4`、Minor=`0`；incident/state-machine 审查 Critical=`0`、Important=`4`、Minor=`0`；contract/race 审查 Critical=`0`、Important=`6`、Minor=`0`。合并后的真实问题包括：消息在异步落盘后可能被当前 generation 错误晋级；message 分支吞掉同时观察到的 applied ack；connection 与 applied generation 混用；接收 deadline 被误报为 fatal timeout；通用 SDK read-first `biased` 可能饿死 outbound/PING/reconnect；actor send 的跨 generation TOCTOU；首次观察 ack 已跳代时未建 fence；迟到 applied 与自然 flap 共享旧窗口；以及 100 次人工 one-shot、文件描述符/任务生命周期和真实两代无 fresh 的覆盖不足。实现审查门保持关闭，未提交、构建、停服、删除或部署。
- 修正后每条 `RtdsRuntimeMessage` 固定携带取得时的 `freshness_generation` 和同次新观察到的 applied ack；异步 emit 后只允许同一 generation 仍 active 的消息恢复。freshness fence 保存初始 generation，首次 ack 若已跳代仍按唯一 receiver 当前 `len()` 建 fence。`recv()` / `recv_event()` 的自身期限耗尽统一返回 `None`，不再伪造 SDK fatal。
- episode 分开维护 base、最新 connection 与最新 applied generation；reconnect counter 只采样单调 connection generation，迟到 applied 不再倒退 connection 轨迹，自然跳代会在 replay 判断前真实计数并执行两代上限。每个新 applied generation 重新获得最多 3 秒观察窗口，整个 episode 仍受 12 秒绝对上限约束。自审另发现 episode 初始化先读 connection、后读 applied 仍有切代竞态，现改为先取 applied 快照、再取单调 connection generation，避免合法自然切代被误报为 `applied > base`。
- 官方 SDK 公共 connection loop恢复公平 `tokio::select!`；新增 generation-bound `ConnectionManager` clone 仅让官方 `SubscriptionManager` 的控制请求固定携带 actor 已观察 generation，旧请求由后代 socket 丢弃，actor 只在 exact generation 仍 Connected 时发布 applied。该接口直接落实已批准的第六项“跨连接陈旧控制请求不得进入新 socket”语义，差异仍只在既有 `src/ws/connection.rs`，没有 raw WS/REST、手写 subscription、第二 client、receiver、actor 或 connection loop。
- 新回归覆盖持续入站下 outbound/PING/reconnect 公平性、generation-bound sender、首次 ack 跳代 fence、接收期限、消息绑定 generation、connection/applied 分离、迟到 applied、新 applied 独立窗口和两代真实 loopback。两组 100 次人工 one-shot 分别证明每代唯一 socket/replay、单 actor/receiver、Tokio 存活任务不增长，并在 Linux `/proc/self/fd` 上拒绝按 generation 累积文件描述符。
- 修正后 `cargo test -p data-collect --locked` 全部通过：lib=`132/132`、SDK patch contract=`10/10`、SDK runtime contract=`18/18`及其余 integration tests 全部成功；两项 100-cycle 回归以单测试线程再次通过。data-collect locked clippy `-D warnings`、workspace fmt与`git diff --check`通过。vendored SDK Rust 1.88 all-features lib=`179/179`、default/all-features两组 all-targets clippy `-D warnings`与 pinned nightly fmt通过。
- 供应链复核仍固定：原始 crate SHA-256=`e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`；自动删除临时目录的全树比较恰为获批三个差异文件；依赖树只有仓库内 p1；精确 `cargo clean` 后 vendor `target/` 不存在。
- `2026-07-15T15:12:30Z` 远端只读复核确认旧服务仍为 MainPID=`1084549`、NRestarts=`4`、active/running/enabled、`Restart=on-failure`；运行中 executable 与 final binary SHA-256 均为 `078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`，unit SHA-256=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`。当前状态为 `TASK_2_D2_FIRST_IMPLEMENTATION_REVIEW_FIX_VALIDATED_PENDING_REREVIEW`；三路原审查者全部 PASS 前，方案 2 的提交、构建、停服、删除和部署门继续关闭。

## 2026-07-15：D2 第二轮实现复审 FAIL 与两代终止验收修正

- 三路原审查者第二轮复审均为 FAIL，且一致只剩同一个问题：每路 Critical=`0`、Important=`1`、Minor=`0`。真实 loopback 已建立两个 one-shot generation、确认每代唯一 replay 且无 fresh，但原测试到 `generations_used()==2` 后正常结束，没有调用生产 `WaitExpired` 的 terminal 分支；删除或反转生产退出分支仍会绿，因此尚未证明固定 fatal、`rtds_business_message_timeout_after_two_new_generations` gap 和禁止第三次 ticket/socket。其余首轮问题、SDK-first、三文件 allowlist、资源生命周期、文档与方案 2 门均获三路确认通过。
- 最小修正把“generation budget 是否仍可继续”抽成生产与测试共用的纯决策 helper；第一代明确返回可继续，达到第二代则返回携带固定 gap reason 与 error message 的 terminal `Err`。生产 `WaitExpired` 分支直接消费该 `Err`、关闭 recovery 状态、记录同一 gap 并返回非零 source error，没有增加恢复次数、配置、线程、连接或产物。
- 真实两代 loopback 现在在第二代 applied 且无消息后调用同一 helper，精确断言 terminal `Err`、gap=`rtds_business_message_timeout_after_two_new_generations`、error=`RTDS business stream did not recover`；随后服务端继续有界等待并断言没有第三个 socket。定向回归通过，完整 `cargo test -p data-collect --locked` 仍为 lib=`132/132`、SDK patch contract=`10/10`、SDK runtime contract=`18/18`及其余 integration tests 全部成功；locked clippy `-D warnings`、fmt 与 `git diff --check`通过。
- 当前状态为 `TASK_2_D2_SECOND_IMPLEMENTATION_REVIEW_FIX_VALIDATED_PENDING_REREVIEW`。原三路审查者最终全部 PASS 前，仍不得提交、构建、上传 staging、停旧、删除 Jul15 或部署。

## 2026-07-15：D2 最终实现复审通过

- 三路原 `gpt-5.6-sol` / `ultra` 审查者最终均给出 PASS，每路 Critical=`0`、Important=`0`、Minor=`0`。最后一个 Important 已闭合：生产 `WaitExpired` 和真实两代 loopback 使用同一 budget-terminal helper；第一代可继续，第二代无 fresh 时返回固定 terminal error、记录固定 gap，且测试有界证明不存在第三个 socket。
- 三路同时确认首轮全部问题没有回归：异步 emit 只使用消息绑定且仍 active 的 generation；message 不吞 applied；connection/applied 轨迹分离；deadline 不伪造 fatal；通用 SDK 公平 select 不饿死 CLOB/RTDS 控制；generation-bound 官方 `SubscriptionManager` 消除 send TOCTOU；首次跳代 ack 建 fence；迟到 applied 与逐代窗口正确；100 次 one-shot 后 task/FD/socket/actor/receiver 不按代累积。
- SDK-first 与范围审查通过：D2 新 vendor 代码仍只进入既有 allowlist 的 `src/ws/connection.rs`，完整 vendor tree 仍只含获批三个差异文件；没有 raw Polymarket WS/REST、手写 subscription、第二 client/connection loop/actor/receiver或同 socket RTDS unsubscribe。service envelope、parser、目标 schema、`Lagged(n)` 与 terminal disconnected 仍 fail closed。
- `2026-07-15T15:18Z` 部署审查者只读复采确认旧服务仍为 MainPID=`1084549`、NRestarts=`4`、active/running/enabled，旧 binary/unit SHA 未变；Jul15 仍为旧 build open 日，D2 staging 与 `.deleting-*` 均不存在。当前状态为 `TASK_2_D2_IMPLEMENTATION_REVIEW_PASSED_READY_FOR_COMMIT`；下一步只允许精确 stage 本事项 8 个文件并 commit/push，用户既有 `AGENTS.md`、`demand/rebuild-core-demand.md` 与 `推荐开发顺序.md` 继续排除。

## 2026-07-15：D2 提交、隔离生产构建与运行中升级预检

- D2 实现与对应需求/计划/changelog 已精确提交为 `ddc5f01739482a63055cd7fd938cbba44519875a`（`fix(data-collect): reconnect RTDS by generation`）并推送到 `origin/after-data-plan`；本地 HEAD、tracking ref 与 `git ls-remote` 一致。提交只包含本事项 8 个文件，用户既有 `AGENTS.md`、`demand/rebuild-core-demand.md` 与 `推荐开发顺序.md` 未被暂存或提交。
- 从该已 push commit 创建 `/tmp/data-collect-build-ddc5f017` detached clean worktree，设置完整 `DATA_COLLECT_GIT_COMMIT` 并使用独立 `/tmp/data-collect-target-ddc5f017` 执行 locked release 构建。最终 binary 大小=`25,489,992` bytes、mode=`0755`、SHA-256=`ec910ccc008187b72ebcd3a2466623db1d5e6abcb27a24c2881a76e435047852`，为 x86-64 PIE、loader=`/lib64/ld-linux-x86-64.so.2`、BuildID=`9e4a8c156fcb64444dce98927f7bf29e79a31f62`，内嵌完整 commit；产物原子放入本地固定路径后临时 worktree/target 均已删除。
- 运行中升级入口于 `2026-07-15T15:31:50Z` 重新冻结旧 pair：service MainPID=`1084549`、NRestarts=`4`、active/running/enabled、`Restart=on-failure`、启动 monotonic timestamp=`10225988536166`；运行中 executable 与 final binary SHA 均为 `078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`，final unit SHA=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`。旧 gate 文件/enable symlink 均不存在且 LoadState=`not-found`；Jul15 为唯一 old-build open 日，prior day、staging 与 tombstone 当时均不存在。
- binary 与机械生成的 `Restart=no` gate unit 已以不存在的 digest-named 路径 exclusive-create 预置。远端 binary SHA/size/mode/owner/nlink、ELF/loader/动态库、glibc=`2.39`、内嵌 commit、`--version`/`--help` 均通过；gate unit SHA-256=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`、mode=`0644`，与 repo 权威 unit 的唯一差异为 `Restart=on-failure` 改为 `Restart=no`，`systemd-analyze --user verify` 通过。一次兼容性脚本因 `pipefail` 下的 `head` SIGPIPE 在只读探针中提前停止，未改 final 或 service；改用完整消费输出后全部重跑通过。
- Jul15 open manifest 并发原子更新使首次全量扫描的完整 bytes 快照失效，另一次采样恰遇合法的瞬时 `manifest.json.tmp`，两次都按门禁拒绝采用且未改变远端。稳定重采要求 identity 与 `files[]` 在完整扫描期间不变，最终确认 90 个 finalized Parquet、`3,110,032,652` bytes 与 manifest 路径/数量/bytes/SHA 双向一致；90 个 footer 的 rows、schema、source、完整 file fact 与 old build identity一致。五个当前小时 `.running` 只由 PID `1084549` 持有，finalized/manifest无持有者；树无 symlink、额外目录/文件、stale running、tmp/digesting，10 个 mount namespace 对目标及 descendant mount 均无命中，prior days为空。
- 本地旧 `data-collect-new-datasets-archive.timer` 已再次执行 disable/stop并确认 disabled/inactive，archive service inactive，`/home/chillchill/new-datasets` 保留。`2026-07-15T15:48:30Z` 最终复采仍为同 PID/start/NRestarts 与旧 final pair；新 staging digest未变且 inactive/disabled，正式 binary/unit、旧服务与 Jul15 数据未改。当前状态为 `TASK_4_2_D2_REMOTE_PREFLIGHT_PASSED_PENDING_INDEPENDENT_METHOD_REVIEW`；独立预检方法审查 PASS 前不得停服或删除。
- 独立 `gpt-5.6-sol`/`ultra` 预检方法审查给出 PASS，Critical=`0`、Important=`0`、Minor=`0`，确认稳定投影、90 文件/footer、holder 与 namespace 证据足以进入协调停服门。审查同时冻结正确边界：open manifest 高频原子更新时不冻结整份 bytes；瞬时 `manifest.json.tmp` 只使该次快照重试；停服前投影只授权 `disable --now`，不得直接授权删除；停服后必须重新对账并生成唯一权威 tree digest。按审查补做 10 个 mount namespace 的 mountpoint 与非根 bind-source 双向扫描，均无目标自身、后代或别名命中。当前状态更新为 `TASK_4_2_D2_REMOTE_PREFLIGHT_REVIEW_PASSED_READY_FOR_COORDINATED_STOP`。

## 2026-07-15：D2 协调停服与 Jul15 权威删除门

- 为避免扩大数据中断，旧服务持续运行至下一 gate 窗口前；从 `15:54:29Z` 至 `16:34:30Z` 的连续监控始终为 MainPID=`1084549`、NRestarts=`4`、active/running，并正常跨过 `16:00 UTC`。最终停服前重采包含 95 个 finalized、`3,371,600,381` bytes 与 95 个 footer，identity/files/holder/mount/bind-source 全部通过。第一次 stop 脚本在调用 systemd 前因再次命中瞬时 `manifest.json.tmp` 而安全退出；复采证明旧服务仍 active/enabled、PID/final/staging 全部未变。
- `2026-07-15T16:36:25.205784372Z` 执行唯一主 unit 的 `disable --now`，于 `16:36:26.313446953Z` 协调返回；Result=`success`、ExecMainCode/Status=`0/0`、inactive/dead、MainPID=`0`、disabled、enable symlink消失，无残留 collector 进程。旧 gate 文件与 symlink继续不存在且 LoadState=`not-found`；未执行 unit-specific reset，也未触碰首次部署 4.3.4–4.3.5 的程序或全根清理。
- 停服后重新从最终静态树执行完整门：manifest state=`open`、SHA-256=`33439bbf9d722f2e5c76cac61c51f71a394e3978315dd4f5c0e9d3c3e5d66a76`；100 个 finalized Parquet、`3,521,371,501` bytes 与 manifest 的 path/count/bytes/SHA 双向一致，100 个 footer完整 file fact均属于 old commit/binary/host/region/config。树只有 day 根、六个 source目录、manifest和100个Parquet，共108个对象；无 holder、进程、running/tmp/digesting、symlink、未知对象、mountpoint或bind-source。
- canonical tree facts 按 `path/type/owner/group/mode/nlink/size/sha256` 排序后的权威 digest=`ef669c97a2d7989819cd3365d8bce6089892ce00f20ed17a204e5ecf1251e8f2`；prior-days列表为空，digest=`4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`；`typed_raw`父目录冻结为 device/inode/mode/uid:gid=`66305/262157/755/1000:1000`。指定 tombstone事前不存在且与日目录同 filesystem。当前状态记录为 `approved_running_upgrade_data_clearing`；只批准把完整 Jul15 日原子 rename 到指定 old-binary digest tombstone，随后按 manifest-last 白名单删除。
- 第一次把 tree 复算与 rename 放在同一长会话时，命令在本地 30 秒 yield 后继续异步运行但未返回可采用的最终证据；只读状态确认原目录仍完整、tombstone仍不存在，且随后独立重算 tree digest、holder与mount/bind门仍全部匹配。为避免不透明重试，动作被拆成单一短命令；`2026-07-15T16:42:15.151007271Z`，完整原目录已同 filesystem 原子 rename 为 `/home/ubuntu/btc-updown-marketmaker/datasets/training/.deleting-utc_day=2026-07-15-078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`，原路径消失、tombstone存在。状态保持 `approved_running_upgrade_data_clearing`，旧服务禁止恢复；下一动作只能按已冻结manifest白名单继续完成清除。

## 2026-07-16：用户人工删除整个 training 根后的状态收口

- manifest-last 删除脚本在任何 unlink 前曾报告 tombstone digest=`86b70e67bfb92315a6a4f8014968849c82e5c59ceae0e2aceb5534f4f21f9f2b`，与冻结 `ef669c97a2d7989819cd3365d8bce6089892ce00f20ed17a204e5ecf1251e8f2` 不同，因此正确 fail closed；当时 manifest与100个Parquet仍完整。之后用 manifest facts 和实际逐文件 SHA 两次重算均恢复为原冻结 digest，独立审查也确认 rename 未改变权威 facts、不得采用 `86b70...`。用户随后要求停止全部操作，主流程和仍运行的审查子代理均已停止，没有执行 unlink、final replace、daemon-reload或gate start。
- 用户恢复对话后先表示可从原子替换开始；在被指出 tombstone仍约3.28GiB后，用户自行于远端 `~/btc-updown-marketmaker/datasets` 执行 `rm -rf training/`。该人工动作删除了完整 tombstone，同时也删除了计划要求保留的空 `training/typed_raw` 父目录；它没有走已批准的逐Parquet、空source目录、manifest-last、空tombstone顺序，是明确的 operator deviation，且不可恢复。
- `2026-07-15T17:01:47Z` 只读复核确认 `datasets`仍为`ubuntu:ubuntu`、mode=`0775`的真实空目录；`training`、`typed_raw`、原Jul15路径与唯一tombstone全部不存在，dataset根下无其他对象。此前停服后完整门已证明 prior-days为空、tombstone只含old-build Jul15，因此当前证据未显示人工命令额外删除了已批准保留的历史日期，但旧Jul15数据已永久丢失。
- service仍inactive/dead、disabled、MainPID=`0`且无collector进程；old final binary/unit SHA仍分别为`078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`与`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`。new staging binary/gate unit仍为`ec910ccc008187b72ebcd3a2466623db1d5e6abcb27a24c2881a76e435047852`与`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，owner/mode/size均未变。
- 代码只要求CLI dataset根存在：缺失`training`时容量扫描返回0，stale cleanup与UTC-day恢复返回空；随后`reconcile_open_day_manifest`对新`training/typed_raw/utc_day=<day>`执行`create_dir_all`。因此代码具备从当前空dataset根启动的明确语义，但是否接受人工偏差并进入4.4仍交由独立审查。当前状态为`TASK_4_3_OPERATOR_DELETED_TRAINING_PENDING_RESUME_REVIEW`；审查PASS前不替换final、不启动gate。
- 独立`gpt-5.6-sol`/`ultra`恢复审查给出PASS，Critical=`0`、Important=`0`、Minor=`0`。审查确认当前满足D2中断状态机的“原日与tombstone均不存在表示cleared”，缺失空父目录不会阻塞代码；`training_dataset_size_bytes`、stale cleanup、UTC-day恢复均把NotFound视为空，新的D2日目录由`reconcile_open_day_manifest`创建。当前状态更新为`TASK_4_3_D2_OPERATOR_DELETE_REVIEW_PASSED_READY_FOR_ATOMIC_REPLACE`；不得人工预建目录，4.4前仍须即时核对service/空dataset/mount/holder与old-final/new-staging四个digest。
- 第二名独立`gpt-5.6-sol`/`ultra`复审给出FAIL，Critical=`0`、Important=`2`、Minor=`0`。`2026-07-15T17:06Z`只读复采发现用户交互bash PID=`1092719`的cwd仍为`/home/ubuntu/btc-updown-marketmaker/datasets`，不满足4.4.1无句柄门；必须由用户先`cd`到dataset根之外或退出，禁止强制终止用户shell。
- 同次复审发现预置命名错误：需求第127行、计划第34行及deploy readme第103行均冻结binary瞬时名`data-collect-training-collector.upload-<sha256>`，实际远端却为`data-collect-training-collector.staging-ec910...`，获批upload路径不存在。内容SHA仍正确，但命名冲突不能解释为合规。当前状态改为`TASK_4_3_D2_RESUME_REVIEW_FAILED_BLOCKED_BEFORE_ATOMIC_REPLACE`；用户shell释放cwd后，只能先把错误staging纠正为获批upload名并重做全门，不能直接采用错误名称替换final。
- 用户随后退出交互shell；最终只读复核确认原PID=`1092719`不再持有dataset根。`2026-07-15T17:13:11.811389912Z`在service仍inactive/disabled、final未变的前提下，把错误的`.staging-ec910...` binary同目录原子纠正为需求冻结的`.upload-ec910...`；纠正后错误路径不存在，获批upload路径的SHA=`ec910ccc008187b72ebcd3a2466623db1d5e6abcb27a24c2881a76e435047852`、size=`25,489,992`、mode=`0755`、owner=`ubuntu:ubuntu`、nlink=`1`均精确。
- `2026-07-15T17:18:40.760965218Z`最终替换门通过：service inactive/dead、disabled、MainPID=`0`、NRestarts=`0`；dataset根为真实空目录，`training`、原Jul15与tombstone均不存在；root级扫描确认holder=`0`、collector进程=`0`，11个mount namespace无target或bind-source命中。old final binary/unit SHA分别为`078fdc32bf4992bc7393b4151c1326fc726090662a5440c8829e167196df16bc`与`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`，获批upload binary/gate unit SHA分别为`ec910ccc008187b72ebcd3a2466623db1d5e6abcb27a24c2881a76e435047852`与`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`。
- 随后依次同目录原子替换final binary与unit并daemon-reload；`2026-07-15T17:18:43.491091925Z`复核final binary/unit SHA精确为上述D2 binary与gate unit，ExecStart保持最终`--run-forever`合同、`Restart=no`，全部本轮staging消失。service继续inactive/dead、disabled、MainPID=`0`、NRestarts=`0`，dataset根仍为空。当前状态更新为`TASK_4_4_D2_ATOMIC_REPLACE_PASSED_WAITING_GATE_WINDOW`；下一动作只允许在`2026-07-15 17:45–17:50 UTC`即时复核后启动gate。
- 定时会话持续读取远端UTC且未提前启动；`2026-07-15T17:45:01Z`进入合规窗口后，瞬时门再次确认service inactive/disabled、MainPID=`0`、NRestarts=`0`、final binary/unit SHA精确、数据根为空、staging不存在，并以root权限扫描得到holder=`0`、collector进程=`0`、11个mount namespace无target或bind-source命中。`systemctl --user start`只调用一次，systemd于`17:45:08.583543Z`记录正常Started。
- 初始验证脚本在`17:45:09.873219888Z`返回rc=`1`，但当时service已经active、MainPID=`1103255`、NRestarts=`0`且`Restart=no`，没有发生第二次start或systemd重启。只读定位证明这是验证竞态而非collector失败：四个source目录及其`.running`文件mtime为`17:45:10.737–17:45:10.741Z`，晚于失败断言；脚本在manifest刚出现后即要求六目录齐全。`17:45:41Z`复采时六个固定source目录已全部存在，唯一manifest为state=`open`、build commit=`ddc5f01739482a63055cd7fd938cbba44519875a`、binary SHA=`ec910ccc008187b72ebcd3a2466623db1d5e6abcb27a24c2881a76e435047852`、region=`eu-west-1`，source counters已含六类；Polymarket market running文件已有真实字节，其余writer已创建。
- 同次复采确认MainPID仍为`1103255`、active/running、disabled、`Restart=no`、NRestarts=`0`；运行中executable与final binary SHA均为`ec910ccc008187b72ebcd3a2466623db1d5e6abcb27a24c2881a76e435047852`，final unit SHA=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`，生产根无tmp/partial/report/receipt/sidecar/validation，`17:45:00Z`后的journal只有正常Started。当前状态更新为`TASK_4_4_D2_GATE_RUNNING_PENDING_CROSS_HOUR`；保持同一PID跨`18:00 UTC`并至少运行至`18:00:09Z`后再执行4.4.2。
- gate以同一MainPID=`1103255`跨过`18:00 UTC`，`18:00:29Z`仍active/running、disabled、`Restart=no`、NRestarts=`0`，已超过15分钟；运行中executable/final binary/unit identity未变。17 UTC小时已无旧小时`.running`，manifest记录六个finalized且磁盘集合精确相等：aggTrade `11,601`行/`454,703` bytes/SHA=`3c92f5bee5340d93ab3e2fecedf97088a8e2ba74c23368d6be336c8e4922ad7f`；depth snapshot `1`行/`17,088` bytes/SHA=`b105d74b23978d5213d1ee3f3257d1b94a6b2bf42b89336ecd0b191eb9691317`；depth update `8,899`行/`1,500,142` bytes/SHA=`07031893f50cc99bfc9d9cda1be2b2326eade2beceaead7d026a3657be0a32b1`；Polymarket market `927,860`行/`62,284,949` bytes/SHA=`f7c1bd855a4ddb4f4430ea82d166e74a435029f42731ee69243ec08a53a5edd2`；metadata `10`行/`11,875` bytes/SHA=`9dc229c96474b4f2cc362f8825a56b7b55f9e87817e78c208cd15d3d78c8c21d`；RTDS `888`行/`48,469` bytes/SHA=`37d6cb6c19890b1f90814ef6503a4a0500d06eb8b4ea07882175a16d5347fc7f`。
- 本地PyArrow `23.0.1`通过SSH内存流直接读取上述六个生产文件，没有创建临时文件或报告；六个实际bytes/SHA、Parquet footer rows均与同一manifest快照一致，六类均有真实行，总计`949,259`行。18 UTC当前有五个`.running`；depth snapshot只有17 UTC bootstrap快照一行且在没有resnapshot时不创建连续小时writer，属于已冻结稀疏语义，不是缺源。
- `2026-07-15T18:03:05.877776750Z` 4.4.2最终机械门PASS：manifest仍为唯一Jul15 open、D2 build/binary/config/host/region身份正确，六类counter均files=`1`、written_rows=`rows`且rows>0，gaps/lag/drop/reconnect均为`0`；journal自start只有正常Started，旧小时running、未知对象、symlink、tmp/partial/report/receipt/sidecar/validation均为`0`。当前状态更新为`TASK_4_5_D2_GATE_PASSED_PENDING_INDEPENDENT_STAGE_REVIEW`；独立审查PASS并提交/push三份事项文档前不得切换长期unit。
- 独立`gpt-5.6-sol`/`ultra`阶段审查明确PASS，Critical=`0`、Important=`0`、Minor=`0`。审查者于`2026-07-15T18:10:07Z`只读复采：MainPID仍为`1103255`、start=`17:45:08Z`、active/running、disabled、`Restart=no`、NRestarts=`0`、NeedDaemonReload=`no`；运行中executable/final binary SHA均为`ec910ccc008187b72ebcd3a2466623db1d5e6abcb27a24c2881a76e435047852`，unit SHA=`76ae2883dee5bd95132e4d306eaac2507756936759082661065942a5f8c3e27c`。同一PID已连续约25分钟；单一内存manifest快照与六个finalized双向一致，bytes/SHA/footer rows及总计`949,259`行均匹配，六源非空，17 UTC无running，18 UTC五个running符合snapshot稀疏合同，journal仍只有正常Started。
- 审查同时确认人工`rm -rf training/`只被记为operator deviation、未伪称manifest-last合规；获批upload命名纠正、atomic replace、staging消失、ExecStart与三份文档均和远端事实一致，无影子路径、错误断言或过度设计。当前状态更新为`TASK_4_5_D2_STAGE_REVIEW_PASSED_READY_FOR_DOC_COMMIT`；只允许提交/push本事项三份文档，随后按5.1.1无restart/stop切换长期unit，并必须确认MainPID仍为`1103255`。
- 阶段4三份事项文档精确提交为`b63982fc889f4aae08220b41a33737e8f716b97c`（`docs(data-collect): record D2 production gate`）并push到`origin/after-data-plan`；本地HEAD、tracking ref与`git ls-remote`一致。提交只包含本事项demand/plan/changelog，用户既有`AGENTS.md`、`demand/rebuild-core-demand.md`与`推荐开发顺序.md`继续未暂存、未提交。
- repo权威长期unit SHA=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`，与运行中gate unit唯一差异为`Restart=no`改为`Restart=on-failure`。以exclusive-create上传digest-named staging后，第一次验证因脚本把权威文件实际`590` bytes误写成`598`而在任何final替换前fail closed；只读复核确认staging为`ubuntu:ubuntu`、mode=`0644`、nlink=`1`、size=`590`且SHA精确，service仍为同一PID、disabled、`Restart=no`，final unit未变。修正静态size断言后`systemd-analyze --user verify`通过。
- `2026-07-15T18:14:03.954578892Z`再次冻结MainPID=`1103255`、start monotonic timestamp=`10240648265130`、NRestarts=`0`、运行中executable/final binary/gate unit身份；随后只原子替换unit、daemon-reload并执行`enable`，未调用stop/start/restart/`--now`。`18:14:04.418713958Z`复核MainPID与start timestamp均未变化，service active/running、enabled、`Restart=on-failure`、NRestarts=`0`、NeedDaemonReload=`no`；final unit SHA=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`，运行中executable SHA仍为`ec910ccc008187b72ebcd3a2466623db1d5e6abcb27a24c2881a76e435047852`，staging消失且自start journal仍无restart。
- 当前状态更新为`TASK_5_1_D2_LONG_RUNNING_MONITOR_ACTIVE`。同PID 26小时窗口继续从`2026-07-15T17:45:08Z`起算，最早于`2026-07-16T19:45:08Z`完成；同时必须覆盖至少一个Binance WS connection的24小时生命周期与source内恢复、UTC日切及至少312个nominal 5分钟时槽，任一MainPID变化都重置整个26小时窗口。
- 用户将监控频率从55秒调整为15分钟。只终止并替换本地只读监控会话，未触碰远端service；新会话对齐每个UTC季度后的约30秒，每15分钟核对MainPID/start timestamp/NRestarts/Restart/enabled、两类Binance reconnect累计值与manifest日期，每小时追加executable/unit SHA、manifest聚合、running/stale/extra/journal与磁盘汇总。等待由不超过56秒的可中断片段组成，不落盘监控日志或报告。`2026-07-15T18:21:13Z`新基线仍为MainPID=`1103255`、NRestarts=`0`、Jul15 open、finalized=`6`、agg/depth reconnect均为`0`；`18:21:17Z`小时复核stale/extra/journal error均为`0`。
- 从`18:30:35Z`到`20:45:34Z`的15分钟监控均为MainPID=`1103255`、NRestarts=`0`、Binance agg/depth reconnect=`0/0`。`19:00:39Z`小时门为finalized=`11`、rows=`4,571,479`、bytes=`307,437,903`；`20:00:39Z`为finalized=`16`、rows=`8,176,229`、bytes=`549,233,123`，两次均为stale/extra/journal error/gap/lag/drop/reconnect=`0`且executable/unit SHA精确。
- `2026-07-15T20:56:19.499769Z`原MainPID=`1103255`输出`source_task_error:polymarket_rtds_chainlink_btc_usd:RTDS business stream did not recover`并以status=`1`退出；systemd记录failed后于`20:56:29.604627Z`按长期unit启动MainPID=`1109425`、NRestarts=`1`。`21:00:33Z`监控检测到PID/start timestamp/NRestarts变化并正确fail closed；原同PID 26小时窗口永久失效，不能继续累计。
- Jul15 open manifest仍为D2 identity并诚实记录十个gap：RTDS transport generation `0→1→2→3`、generation `1/2`两次one-shot reconnect均attempted/published、`rtds_business_message_timeout_after_two_new_generations`及source terminal/error；另有同一时段Polymarket market SDK timeout/recovery-cycle restart。RTDS累计lag/stale=`92`、reconnect=`3`，drop=`0`；旧RTDS part-0最后receive=`20:56:05.907Z`、source timestamp=`20:55:23Z`，新PID part-1首receive=`20:57:17.431Z`、source timestamp=`20:56:41Z`，随后已恢复到实时数据。Polymarket market receive gap约25秒；Binance aggTrade通过`25`行backfill恢复且`last_observed_id=last_contiguous_id=4015144557`、unresolved ranges为空，depth新snapshot bridge/gap_rows=`true/0`，没有identity混入或未解决Binance序列缺口。
- `21:01:02Z`新PID仍active/running、enabled、`Restart=on-failure`，binary/unit SHA继续为`ec910ccc008187b72ebcd3a2466623db1d5e6abcb27a24c2881a76e435047852`与`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`；20 UTC part-0均以shutdown_finalize封口，part-1及21 UTC writer继续写入。当前状态更新为`TASK_5_1_D2_PID_CHANGED_DIAGNOSIS_IN_PROGRESS`：保持新PID只读监控并从`20:56:29Z`暂时重计，最早`2026-07-16T22:56:29Z`；三路独立诊断确认是普通平台/网络瞬态前不得宣称继续验收，若确认code/unit/data正确性或产物合同问题则立即按5.2.4停服。
- 三路独立`gpt-5.6-sol`/`ultra`只读诊断均给出PASS并选择继续运行，Critical=`0`、Important=`0`。旧RTDS 20点part-0精确为`3,209`条fresh与`91`条stale；manifest的`lag_events=92`是每条stale累计`91`再加60秒freshness timeout一次，不是SDK broadcast `Lagged(92)`，否则必须存在`rtds_sdk_stream_fatal:receiver lagged by N messages` gap，而实际不存在。最后qualifying fresh接收=`20:55:09.824Z`，退出=`20:56:19.499769Z`，与60秒无fresh后两代有界恢复耗尽一致；generation `1/2` ticket均attempted/published，transport推进到`2/3`，没有第三代、parser/service-envelope/schema/writer fatal或资源泄漏证据。
- 新PID并非启动后立即收到实时消息：首条RTDS接收=`20:57:17.431Z`且仍stale，首条fresh=`20:57:32.822Z`，随后逐渐追平。RTDS part-0/part-1各自及跨文件exact payload重复均为`0`；旧/新文件的行数、SHA、footer与manifest一致。Polymarket market跨进程接收gap约`25.159s`且新旧unique payload交集为`0`；Binance aggTrade回补`25`行、continuity unresolved ranges为空，depth新snapshot首更新桥接且gap rows为`0`。因此本次只能定性为“已记录且可恢复的RTDS外部交付链瞬态”，现有证据不能唯一归责Polymarket平台、AWS网络或SDK，不能虚构更窄根因；不执行`disable --now`、数据清理或退回阶段2。
- 完整26小时窗口从systemd启动新MainPID=`1109425`的`2026-07-15T20:56:29.604627Z`重新起算，最早完成点=`2026-07-16T22:56:29.604627Z`；Binance两条24小时连接子窗口也从新进程重计。`21:15:33Z`季度监控仍为同PID/start、NRestarts=`1`、active/running/enabled、`Restart=on-failure`，Jul15 open manifest finalized files=`27`、gaps=`10`、lag=`92`、drop=`0`、agg/depth reconnect=`0/0`、RTDS reconnect=`3`，binary/unit identity未漂移。
- 两项非本次已确认根因的风险进入独立只读复核：其一，D2每个新generation只观察`3s`且episode总上限`12s`，而完整进程本次约`47.8s`才收到首条RTDS、约`63.2s`才收到fresh，恢复预算可能偏激进；其二，生产`bounded_ws_config()`把heartbeat设为`10s`、timeout=`20s`，而当前官方RTDS文档要求每`5s`发送PING且pinned官方Rust SDK默认也是`5s/15s`。旧进程用同一`10s`配置已稳定约3小时、故障期间仍收到业务消息且无heartbeat timeout，新进程同配置也恢复，故现有证据不能把两项风险写成本次事件根因；但在独立复核闭合前也不得静默忽略或擅自改动。当前状态更新为`TASK_5_1_D2_LONG_RUNNING_MONITOR_ACTIVE_PID_1109425_RISK_REVIEW`。
- heartbeat窄审查与主代理一手复核确认：官方CLOB WebSocket总览要求market/user channel每`10s`发送PING，当前官方RTDS文档则两处明确要求每`5s`发送PING；pinned官方Rust SDK `WsConfig::default()`同样为heartbeat=`5s`、timeout=`15s`。生产`bounded_ws_config()`覆盖为`10s/20s`并同时传给MarketSdkRuntime与RtdsSdkRuntime，因此把CLOB节拍错误复用于RTDS，属于与官方协议配置不能同时成立的确定代码正确性错误。旧PID约3小时稳定、故障时仍有stale业务消息且无heartbeat timeout，新PID用同一配置恢复；官方成员也曾在Issue示例使用10秒，因此没有证据把该差异认定为`20:56`故障根因，但计划5.2.4以“任一已确认代码正确性错误”为立即停服门，并不要求它必须是当次事故根因。
- `2026-07-15T21:21:30.171616641Z`停服前MainPID=`1109425`、start monotonic=`10252129286211`、NRestarts=`1`、active/running/enabled、`Restart=on-failure`；运行中executable与final binary SHA均为`ec910ccc008187b72ebcd3a2466623db1d5e6abcb27a24c2881a76e435047852`，unit SHA=`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`。按5.2.4执行唯一一次`disable --now`，`21:21:30.939767Z`应用输出`coordinated_shutdown_complete`，`21:21:30.950426Z`systemd记录Stopped；最终Result/ExecMainCode/Status=`success/0/0`、inactive/dead、disabled、MainPID=`0`，无collector进程及`.running/.tmp/.partial/.digesting`残留。
- 停服后唯一Jul15 open manifest保留本轮D2 identity、files=`32`、gaps=`10`、全部source dropped_rows=`0`；累计RTDS lag=`159`由旧进程91条stale+一次timeout及新进程后续67条stale构成，reconnect=`3`。未执行数据删除、tombstone、binary/unit替换或daemon-reload；本地15分钟只读monitor已终止。当前状态更新为`TASK_5_2_D2_FAILED_LONG_RUNNING_HEARTBEAT_CONTRACT_DECISION_REQUIRED`：必须先完成恢复预算独立审查并由用户裁决最小heartbeat修复，之后才可更新需求/计划、回到阶段2实现/审查、重新构建部署与重新开始完整长跑。
- 两名独立恢复预算审查者均判定`3s/代、12s/episode`为Important，而非理论风险。D2需求只批准最多两个新generation、有限等待和第二代仍无fresh后的fatal，并明确取代旧每3秒合同；生产却把`RTDS_RECONNECT_WINDOW_SECS=3`同时用作新generation建立/replay与applied后的fresh观察，且总上限仅`12s`。项目actor在官方SDK `subscribe()`把请求放入本地outbound channel后即发布`applied`，该状态不是服务端ACK、甚至不证明socket writer已经发送；Polymarket也没有3秒业务恢复SLA。原PID在60秒watchdog后约`9.68s`即耗尽两代，而新进程约`47.8s`才收到首条stale、`63.2s`才收到fresh，确认实际恢复尺度远大于当前预算；这会不必要地重启全部六源并持续破坏同PID 26小时与Binance 24小时验收。
- 两名审查者一致推荐每个新generation在项目观察到当前generation/revision的本地applied并建立freshness fence后获得完整`60s` fresh窗口；stale、duplicate、snapshot或PING均不恢复。第一名认为`150s=2×60s+30s共享transition余量`是最小合理总上限；第二名反证后认为`180s=2×(30s connection/replay+60s fresh)`语义更机械、更能保证第二代完整窗口，且`30/75`、`60/135`过严。主流程据此推荐`30s` connection/applied上限、`60s` applied后fresh观察、两代总`180s`；仍保持base generation不计额度、自然flap计数、第二代耗尽固定gap/error、禁止第三代，以及Lagged/service-envelope/parser/schema立即fatal。
- `21:24:10Z` root级`/proc` cwd/root/exe/fd扫描确认生产根handle hits=`0`，无target descendant mount；`21:30:33Z`停服监控仍为inactive/dead、disabled、MainPID=`0`、collector进程=`0`、临时文件=`0`，Jul15 open manifest仍为files=`32`、bytes=`833,945,863`、gaps=`10`。当前状态更新为`TASK_5_2_D2_FAILED_LONG_RUNNING_TWO_CONTRACT_FIXES_DECISION_REQUIRED`；等待用户决定是否批准分源heartbeat、`30/60/180`两代恢复预算及`affected_days=[2026-07-15]`既有manifest-last清理门，决定前保持停服且不清理/构建/部署。
- 2026-07-16 用户明确只批准先最小修复分源 heartbeat，未批准把恢复预算或数据清理并入本轮。主流程重新核对生产调用、pinned SDK 与当前官方文档：CLOB market生产路径和RTDS生产路径确实共同调用覆盖为`10s/20s`的`bounded_ws_config()`；[CLOB WebSocket总览](https://docs.polymarket.com/market-data/websocket/overview)明确market/user每`10s`发送PING，[RTDS文档](https://docs.polymarket.com/market-data/websocket/rtds)两处明确每`5s`发送PING；pinned SDK `WsConfig::default()`为`5s/15s`。因此“CLOB的10秒interval被错误复用给RTDS”结论再次确认；同时收窄措辞，官方页面没有直接规定CLOB `20s`或RTDS `15s` PONG timeout，RTDS `15s`只采用pinned官方SDK默认。
- 按测试先行增加`websocket_heartbeat_configs_are_channel_specific`：首次定向运行因尚无`bounded_rtds_ws_config()`以`E0425`失败。随后只在`data-collect/src/live_runtime.rs`新增独立RTDS配置并把唯一生产RTDS构造切到该配置；CLOB原helper及所有CLOB调用保持`10s/20s`，RTDS为`5s/15s`，reconnect `max_attempts=3`与`max_backoff=5s`不变。历史bench/smoke probe不是本次生产构造路径，保持原样且不纳入本轮最小修复验收。相同locked定向测试复跑为`1 passed, 0 failed, 132 filtered out`。当前状态更新为`TASK_2_HEARTBEAT_MINIMAL_FIX_IMPLEMENTED_REVIEW_PENDING_RECOVERY_BUDGET_DECISION_REQUIRED`；service继续inactive/dead、disabled，本轮没有清理、生产release构建、部署或改动`3s/12s`恢复预算。
- 完整验证通过：`cargo test -p data-collect --locked`的lib为`133 passed, 0 failed`，所有bin、integration与doc tests同时退出0；`cargo clippy -p data-collect --locked --all-targets -- -D warnings`、`cargo fmt --all -- --check`和`git diff --check`均退出0。两路独立`gpt-5.6-sol`/`ultra`实现复审最终均为PASS，Critical=`0`、Important=`0`、Minor=`0`；确认唯一RTDS生产构造使用独立`5s/15s`、CLOB及reconnect/subscribe/freshness/SDK路径不变，需求/计划/changelog对齐且没有越权纳入恢复预算或清理。远端只读复核为ActiveState=`inactive`、SubState=`dead`、UnitFileState=`disabled`、MainPID=`0`，没有collector进程；未执行任何远端状态变更。当前状态更新为`TASK_2_HEARTBEAT_MINIMAL_FIX_REVIEW_PASSED_RECOVERY_BUDGET_DECISION_REQUIRED`。

## 2026-07-25：同步最终四路径vendor authority

SDK070阶段0盘点发现，本事项demand第3.1节和plan的current说明虽然都明确把
`demand/data-collect-source-availability-lifecycle-repair-demand.md`第4.7节作为
最终authority，却仍残留“三个源码差异路径”的旧枚举，漏掉后来已经批准并实际存在
的`src/ws/config.rs`。用户批准按推荐修正后，current demand/plan现统一列出
`src/ws/connection.rs`、`src/ws/config.rs`、
`src/clob/ws/types/response.rs`、`src/rtds/subscription.rs`四路径。

该修正只消除current authority自相矛盾，不回写本changelog中各历史时点真实的
两路径/三路径执行记录，不修改vendor源码、Cargo/lock、测试或运行行为。

## 2026-07-25：current SDK身份迁移到独立SDK070事项

- `DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME` Stage1已在当前worktree从官方
  0.7.0 crate全新建立唯一
  `third_party/polymarket_client_sdk_v2-0.7.0-p1`，把根patch、data-collect精确
  依赖和lock切换为0.7.0，并删除旧tracked `0.6.0-p1` vendor。相对官方0.7.0的
  源码差异仍恰好为本事项current demand/plan列出的四个路径。
- 本次只同步本事项仍使用现在时表达的SDK identity、path与provenance指针；四路径
  patch语义、SDK-first、唯一owner、CLOB strict heartbeat、RTDS no-PONG、bounded
  outgoing/ping例外及全部历史阶段事实不变。旧changelog中的0.6.0 hash、命令与
  运行证据继续描述其当时时点，不回写。
- SDK070 Stage1离线runtime矩阵已通过并标记
  `SDK_070_RUNTIME_USABLE`；typed fixture因官方合法missing/null帧在admission前
  被SDK整帧拒绝而标记`SDK_070_TYPED_CONTRACT_BLOCKED`。该typed阻断只属于SDK070
  v3入口，不改变本事项已完成source lifecycle行为。
- SDK070 Stage1独立审查已为`Critical 0 / Important 0 / Minor 0 / PASS`，当前仍待
  另行commit/push授权；本同步没有修改本事项生产Rust/schema/RTDS行为，没有运行
  公开网络smoke、release、部署、远端服务/数据操作或清理。

### SDK070 Stage1发布授权同步

- 用户已另行批准SDK070 Stage1 commit/push；其闭合以承载SDK070闭合记录的commit
  成功推送且远端SHA与本地HEAD/upstream一致为生效条件。
- 本记录精确取代同日此前等待发布授权状态；旧段仅保留为历史事实。
- 本同步不修改本事项既有生产行为，也不授权公开网络smoke、release、部署、远端
  服务/数据操作或清理。
