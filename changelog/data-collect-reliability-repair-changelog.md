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
