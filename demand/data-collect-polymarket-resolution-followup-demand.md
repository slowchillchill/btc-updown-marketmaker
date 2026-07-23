# Polymarket 五分钟市场生命周期与结算追踪修复需求

> 事项编号：`DC-REPAIR-20260721-POLYMARKET-RESOLUTION-FOLLOWUP`
>
> 对应计划：`plan/data-collect-polymarket-resolution-followup-plan.md`
>
> 对应 changelog：`changelog/data-collect-polymarket-resolution-followup-changelog.md`
>
> 状态：`PHASE_4_COMPLETE_LOCAL_HK_VPN_AC12_AC13_PASS_2026-07-23`。用户已逐项批准第八节第1至8项需求语义，拒绝并要求删除第9项固定5秒`closedTime` future-skew allowance，并批准同步第10项主authority；随后明确授权完整执行阶段1至阶段4。阶段1至阶段4已完成实现、验证和规定的独立审查；阶段4本地香港线路AC-12与全量AC-13通过，最终三路全面终审均为Critical 0、Important 0、Minor 0。production部署、服务操作和数据删除未获授权，也未执行。

> 2026-07-22真实数据采集验收拓扑修订：用户指定本事项AC-12必须在本地原生Ubuntu环境使用已配置的香港VPN线路执行，不再使用AWS爱尔兰数据采集服务器。用户现已授权在阶段1至阶段3完成并通过各阶段门后执行阶段4及AC-12；该决定不改变production部署拓扑，也不授权production部署、服务操作或数据删除。

## 一、目标

修复 `btc-updown-5m` market metadata 与 market channel 在市场切换和结束后的生命周期采集，使 collector：

1. 尽早发现并预加载 next market，同时保留既有 `T-30s` 最迟覆盖边界；
2. 在 market 计划开始时刻 `T` 触发 `prefetched -> active`，不再人为等待 5 秒；
3. 在 market 结束时刻 `E` 后继续保留该 market 的结算观察资格；
4. 优先采集官方 market channel 的真实 `market_resolved`；
5. 未收到合格 WebSocket 终态时，使用官方 Polymarket Rust SDK 的 Gamma `market_by_slug` 做有界 fallback 检查；
6. 只有取得结构化且身份一致的结算证据才写 `retired`；
7. 重试预算耗尽时只记录 collector 未取得终态证据，不把它伪装成“市场客观未 resolved”。

本需求修复的是 collector 观察事实、时间线和证据完整性，不改变市场规则、模型输入合同、交易动作、paper/live submit path 或盈亏计算。

## 二、已核实事实

### 2.1 2026-07-20 typed raw

检查目录：

`/home/chillchill/new-datasets/2026-07-20/typed_raw/utc_day=2026-07-20/source_kind=polymarket_market_metadata`

已核实：

- 24 个 finalized Parquet、864 行、290 个 unique slug；manifest hash 与文件一致；
- lifecycle 行数为 `prefetched=288`、`active=288`、`unresolved_window_end_observation=288`；
- `fallback_close=0`、`retired=0`、metadata resolved 终态为 0；
- next metadata `prefetched` 的实测中位数为 `T-294.93s`；
- current metadata `active` 的实测中位数为 `T+5.04s`；
- 旧 window-end observation 的实测中位数为 `E+5.00s`；
- 287 组可配对 active/end rows 的 metadata fetch time 和 metadata 内容相同，证明 `E+5s` 行复用了 active 时的缓存 discovery snapshot，不是结束后的新 Gamma 请求。

`T-294.93s` 是该日实现行为产生的统计值，不是平台发布 SLA。取消 current 的 5 秒延迟后，新 next discovery 很可能前移到接近 `T-300s`；因此不得把 `T-294.93s` 固化成调度常量或验收阈值。

### 2.2 当前代码事实

- `data-collect/src/live_runtime.rs` 当前在 `E+5s` 生成 `unresolved_window_end_observation`，随后开始 rollover；
- 该 observation 由缓存的 `DiscoveredBtcUpdownMarket` 合成，不执行结束后 Gamma fetch；
- discovery desired asset set 只包含 current 和 next；context 切换后，SDK subscription actor 会移除旧 current assets；
- WebSocket identity verifier、asset map、`current_market_resolved: bool` 和 lifecycle dedupe 当前都只支持 current+next；
- discovery Gamma 请求固定 `closed=false`，不能用于查询已经 closed 的旧 market；
- `evaluate_retired_predicate()` 目前只有测试调用，没有接入 production runtime，因此其中错误的 `active=false` 门不是 2026-07-20 缺少终态行的直接原因，但会阻断本需求的 fallback 实现。

### 2.3 官方接口与全天终态复核

截至 2026-07-21，本需求依赖的能力均由官方 Polymarket Rust SDK 或其官方类型覆盖：

- market channel 支持 `market_resolved`；订阅必须启用 `custom_feature_enabled=true`；
- Gamma client 支持 `market_by_slug()`；
- Gamma `Market` 类型暴露 `active`、`closed`、`closed_time`、`accepting_orders`、`uma_resolution_status`、`automatically_resolved`、`outcomes`、`outcome_prices` 和 `clob_token_ids`；
- CLOB market endpoint 可提供 closed/accepting-orders 与 token winner 证据；pinned SDK 的 method/types 已静态确认能够表达这些字段。本事项只把 CLOB 结果用于离线事实交叉复核，不新增 production CLOB follow-up 请求。

2026-07-21 逐 slug 复核 2026-07-20 的 288 个 `btc-updown-5m` market：

- 288/288 Gamma 请求成功且 slug/condition/assets identity 匹配；
- 288/288 为 `active=true`、`closed=true`、`acceptingOrders=false`、`automaticallyResolved=true`、`umaResolutionStatus=resolved`；
- 288/288 outcome prices 恰好一胜一负，其中 `[0,1]` 为 150 个、`[1,0]` 为 138 个；
- `closedTime-E` 最小 16 秒、中位数 21 秒、P95 57 秒、最大 150 秒，288/288 不超过 3 分钟；
- 官方 CLOB endpoint 的逐 condition 只读复核同样为 288/288 `active=true`、`closed=true`、`accepting_orders=false`，且每个 market 恰好一个 winner；pinned SDK 的 `Client::market()` 和 token winner 类型已做静态能力确认。

`closedTime` 是平台记录的关闭时间，不是 Gamma 字段首次可见时间，也不是 collector 的 WebSocket 到达时间。上述统计支持把 `E+3m` 作为合理的首次 fallback 检查，但不能代替真实在线可见性验收。

官方参考：

- https://docs.polymarket.com/market-data/websocket/market-channel
- https://docs.polymarket.com/api-reference/markets/get-market-by-slug
- https://github.com/Polymarket/rs-clob-client-v2

## 三、术语与时间语义

1. **`T`**：目标 market 的计划开始 UTC 时刻。
2. **`E`**：目标 market 的计划结束 UTC 时刻；对连续五分钟 market，旧 market 的 `E` 等于下一 market 的 `T`。
3. **调度时刻**：计划应当触发动作的绝对 UTC 边界，例如 `E+3m`。
4. **真实执行时刻**：collector 实际唤醒并开始动作的本地时刻，必须真实记录，不保证与调度时刻同一毫秒。
5. **`timestamp_received`**：v3 row 所表达事实被 collector 真实观察或生成的时刻。网络 row 写完整 WebSocket frame/Gamma response 的真实接收时刻；本地 lifecycle、missed-slot 或 exhausted row 写该本地事实真实生成时刻。它始终非空，不得回填为调度时刻、`closedTime`、旧 snapshot 时间或虚构的网络 receive。
6. **collector admission**：`collector_admission_time_utc_ms` 与非零 `collector_admission_sequence` 表示 row 完成校验并进入既有 bounded writer queue 的真实因果顺序；不得用 Parquet 行序或 `timestamp_received` 替代。
7. **source evidence receive**：`source_evidence_received_at_utc` 只表示该 row 携带或引用的外部证据接收时刻。T promotion 引用原 prefetched response 的 receive；fresh Gamma/WS row 引用本次 response/frame receive；transport、missed-slot 和 exhausted summary 为 null。
8. **本地 lifecycle `active`**：market 已到达 T，且 collector 尚未取得 retired 证据；它可以在 E 后继续保持 active，不能与 runtime current role 混用。
9. **Gamma `active`**：平台 metadata 原始字段。它与本地 lifecycle 不同，也不是 `closed` 或 resolved 的反义条件。
10. **`market_runtime_role=resolution_pending`**：market 已越过 `E`、不再是 current，但 collector 仍在执行有界 terminal follow-up。它不是第五种 `market_lifecycle_state`。
11. **terminal-once**：同一 slug/condition 只允许一个经过校验的终态结果完成 lifecycle；WS 与 Gamma 竞态时不得重复写终态。
12. **有效非终态响应**：请求成功，固定 identity 与二元结构可验证；平台 terminal fields 可以尚未出现、尚未进入终态或不满足 terminal predicate，但不存在 identity/shape contradiction。
13. **transport unavailable**：连接建立（包括官方公开 typed API 只能归入 `connect` 的 DNS 解析失败）、timeout、HTTP status/rate limit 等没有产生可验证 market response 的可用性事实；不得解释为 market 未 resolved。
14. **integrity error**：slug/condition/market/assets/outcome/schema 矛盾；继续按既有范围 fail closed，不作为普通 retryable nonterminal 吞掉。

### 3.1 单个 market 的规范时间线

| 相对边界 | 调度动作 | lifecycle / runtime role | 必须保留的证据 |
|---|---|---|---|
| 可发现后、T 前 | 立即发现并写 verified `prefetched`；`T-294.93s` 只记为历史中位数，健康情况下最迟 T-30 完成 metadata 与 subscription 覆盖 | `prefetched` / `next` | 本次 Gamma discovery fetch、receive、identity validation、可还原payload、digest 与真实 admission；snapshot本身没有state-transition applied字段 |
| T | 由 absolute boundary timer 触发，不等待 fresh 网络请求、不再延迟 5 秒 | `prefetched -> active` / `next -> current` | scheduled T、真实 applied、真实 local observation/admission，并引用原 prefetch evidence |
| `[T,E)` | 作为 current 正常采集；若 T 时缺 identity，只允许在本窗内按 RF-02 的 late path 建立 current | `active` / `current` | 正常 market data；late path 另保留 fresh discovery 与真实 validation/applied |
| E | 不再写 E+5 缓存 observation；下一 market 同时在其 T 边界成为 current | lifecycle 保持 `active`；role `current -> resolution_pending` | scheduled E、真实 role applied；保留旧 WS assets |
| `(E,E+3m)` | 继续等待并优先处理合格 `market_resolved`；不做 Gamma resolution poll | `active` / `resolution_pending`，WS terminal 后才 retired/none | 真实 WS frame、receive、identity/winner 与 terminal-once evidence |
| E+3m | 发起 Gamma initial，ordinal 1 | terminal 则 retired/none；否则继续 pending | scheduled slot、fetch/receive、result/predicate 或 transport/integrity evidence |
| E+4m..E+8m | 每分钟一个绝对 slot，ordinal 2..6，即 retry 1..5；任一合格 WS/Gamma terminal 后取消尚未执行的 slots | 同上 | 每个已调度 slot 最多一条 typed attempt row |
| 最后 slot 分类并完成 accepted-WS drain | E+8m 请求可使用既有 20 秒 timeout，因此该slot约在E+8m20s前分类；terminal/summary只能在随后bounded drain完成后发布并记录真实时刻，不把E+8m20s伪装成整体hard deadline；不得发第7次请求 | terminal 时 `retired` / `none`；否则 lifecycle 保持 `active`、role 退出为 `none` | terminal row，或唯一 `resolution_followup_exhausted` summary；不得写新的 `unresolved_window_end_observation` |

## 四、与现有 authority 的关系

本需求于2026-07-22首次获批时只授权阶段0文档authority同步；同日后续已明确授权阶段1至阶段4，
当前授权状态以页首及对应changelog为准。以下记录批准前的精确冲突及当前处理状态；不得把已完成的
文档同步冒充代码已经实现：

1. `plan/data-collect-plan.md`此前要求fallback同时满足`active=false`；全天真实证据证明该门会拒绝288/288个已结算目标market。主plan任务8.5的当前规范已删除该truth-value gate，旧阶段2执行结果继续作为历史事实保留。
2. `data-collect/src/sources/polymarket.rs` 与对应测试把 `active=true` 当作 fallback 失败条件。必须改为“字段原样记录，但不参与 terminal 判定”。
3. `demand/data-collect-source-availability-lifecycle-repair-demand.md`禁止Gamma恢复后追逐过期slug。该规则继续约束current/next discovery；文档authority现已同步一个有界例外：对活跃期已经验证identity且`market_runtime_role=resolution_pending`的market，可以按exact slug做resolution follow-up，直到terminal或预算结束。该查询不得重新进入current/next discovery。
4. 既有 `T-30s` 是最迟覆盖边界，不是禁止更早 metadata discovery 的门，也不得被实测 `T-294.93s` 替代。
5. 主demand此前要求rollover`current -> retired`；rebuild-core草案已经把lifecycle与runtime role分离，但runtime role只列current/next。两份文档现已保持candidate/prefetched/active/retired四态并补齐独立runtime role`resolution_pending`；E只切换role，取得terminal后才切换lifecycle`active -> retired`。rebuild-core只做该data-collect窄边界补齐，整体仍未获批准。
6. source-availability authority此前只允许transport状态进入source-local runtime/operator log。现已同步严格有界例外：每个resolution slot最多一条`polymarket_market_metadata_v3` attempt row，总计每market最多6条；这些row与其他metadata row一样进入既有Parquet artifact，并由每日manifest的既有file row count、bytes、sha256与schema facts覆盖，但不向manifest增加per-attempt transport字段、artifact mapping、sidecar、receipt或通用transport episode tape。
7. 历史changelog继续保留当时事实、不删除、不重写；本事项与受影响旧authority的changelog已追加supersede决定。未来实现证据仍只能在另获授权并真实执行后追加。

## 五、范围

### 5.1 包含

- training collector 的 `btc-updown-5m` lifecycle 与 current/next/resolution-pending runtime roles；
- `polymarket_market` market channel 的 `market_resolved` 延迟观察；
- `polymarket_market_metadata` 的 T 边界 promotion、Gamma resolution follow-up 和 terminal metadata；
- 官方 Rust SDK subscription actor 的 desired asset union；
- metadata typed schema v3、writer、恢复和相关合同测试；
- 必要的主 demand/plan/source-availability authority 同步；
- 获得后续明确授权后的本地构建、测试，以及在本地原生Ubuntu环境通过已配置香港VPN线路执行、对外部平台只读且仅写本地隔离artifact的live validation。

### 5.2 不包含

- raw REST、manual WebSocket、manual signing、自建 Polymarket client 或其他 shadow implementation；
- 修改 Binance、RTDS、paper、live、模型、ObservationContract、标签、simulator、交易或钱包语义；
- 回填或改写 2026-07-20 历史 Parquet；
- 把 Gamma fallback 伪装成 WebSocket `market_resolved`；
- 新增 crash checkpoint/journal/sidecar，或承诺 unclean process crash 后 exactly-once 恢复；
- production release、部署、systemd操作或任何数据删除；commit/push只按已授权的阶段审查门执行。

## 六、功能与证据需求

### RF-01：next discovery 与 prefetch

- next market 一旦由官方 SDK/Gamma 可发现且 identity 完整，即可进入本地 `prefetched` 并安装 market-channel subscription。
- 必须记录实际 metadata fetch start、receive、planned `T` 和 prefetch evidence。
- 健康情况下必须最迟在 `T-30s` 前具备完整 next metadata 与 subscription desired evidence；更早发现不改变该最迟覆盖合同。
- `T-294.93s` 只作为 2026-07-20 观测统计，不能作为 hard-coded timer 或 pass/fail threshold。

### RF-02：T 边界 promotion

- 使用独立绝对 boundary timer，在 `T` 到期时触发已验证 prefetched context 的 `prefetched -> active`，不得继续使用 `E+5s` 人为延迟。
- promotion 不得等待新的网络请求才能生效；必须分别记录 `lifecycle_scheduled_at_utc=T`、`lifecycle_applied_at_utc=真实处理时刻`。本地 transition row 的 `timestamp_received` 写 transition 真实生成时刻，collector admission 另写真实 admission time/sequence，并通过 `source_evidence_received_at_utc` 与 `source_evidence_raw_sha256` 引用原 prefetched response；不能把计划 T、原 prefetch receive 或 admission 互相冒充。
- 若在 T 同时触发 fresh Gamma refresh，只能在响应实际返回后按真实 fetch/receive 时间另行落盘；不得把 prefetch response 或后来的 response 伪装成恰好在 T 收到。
- 操作系统与 async scheduler 的实际抖动必须可测量，不得承诺真实执行与 T 同一毫秒。
- 如果T到期时没有verified prefetched identity，禁止伪造promotion或active row；旧current按E进入pending role，目标current exact-slug discovery按availability继续异步恢复，其他source继续运行。该late路径只以`lifecycle_applied_at_utc`判窗：`T <= applied < E`才可写`candidate_to_active + none_to_current`；同时必须满足`source_evidence_received_at_utc <= row_evidence_validation_completed_at_utc = fixed_identity_verified_at_utc <= lifecycle_applied_at_utc`。response何时到达本身不替代applied判窗。`applied >= E`一律按expired拒绝，不写active、current、resolution_pending或补做E+3..E+8 slots，只在既有availability/coverage证据中记录该market未在活跃期建立verified identity。不得用E后identity扩大“活跃期已验证pending exact-slug”例外。

### RF-03：E 边界与 pending ownership

- market 到达 `E` 时，`market_runtime_role` 从 `current` 切换为 `resolution_pending`，`market_lifecycle_state` 在没有 terminal 证据时仍保持 `active`，不能仅因时间到达写 `retired`。
- 同时立即将已经 prefetched 的下一 market 提升为 current；旧 market 的 resolution follow-up 不得阻塞新 current/next 的行情采集和 discovery。
- 每个 pending item 必须固定保存 canonical slug、market id、condition id、Up/Down outcome mapping、asset ids、E、首次/后续调度点及 terminal 状态。

### RF-04：旧 market WebSocket 订阅保留

- desired asset set 必须是 current、next 与全部仍处于 `market_runtime_role=resolution_pending` 的 assets 并集，并继续由现有唯一 SDK actor执行精确 additions/removals。
- 旧 assets 只在以下任一条件成立后移除：合格 WS `market_resolved`；合格 Gamma terminal fallback；或 resolution follow-up 预算完成。
- “预算完成”只表示最后slot已经分类，不得直接teardown；任何pending assets移除前都必须向唯一WS pump插入本地fence，drain该fence之前已accepted的旧asset事件并再次运行terminal-once。若其中存在合格WS terminal，则改走`terminal_ws`，不得写exhausted summary；只有drain后仍无terminal，才允许写exhausted并移除assets。该drain不等待fence之后尚未接收的未来网络事件。
- 最后一个 slot 在 E+8m 发起，允许该次既有 20 秒 timeout 正常完成；在该 horizon 下，正常最坏重叠为 2 个 pending old + current + next，即 4 个 market、8 个 assets。
- pending old assets 只把 `market_resolved` 路由到 terminal handler；不为旧 market 继续保留整窗 book/price fingerprint 集合。每个 pending item 使用 compact terminal-only dedupe，最多保存 16 个 terminal fingerprints；两个 pending item 的额外本地状态（不含 SDK socket buffer）峰值必须低于 1 MiB。
- 不得增加第二 WebSocket owner、第二 subscription actor 或按 market 创建无界常驻 task。

### RF-05：WebSocket 终态与 terminal-once 仲裁

- 继续启用 `custom_feature_enabled=true`。
- 合格 `market_resolved` 必须校验 market/condition、exact asset set、winning asset 和 winning outcome；官方 nullable slug/outcomes 存在时必须与固定 identity/mapping 匹配，缺失时不得单独否决由 condition + exact assets + winner 完成的证据。
- 唯一actor接受的每个SDK-decoded `market_resolved` 必须在terminal校验前先把source-native payload、真实receive、endpoint和source-native identifiers写入既有`polymarket_market` raw artifact，并继续由既有manifest file facts覆盖；因此后续校验失败不能抹掉已接收frame。该raw保存不等于terminal claim，也不生成metadata terminal row。
- 合格事件由唯一actor处理时立即保存真实`polymarket_market/market_resolved` raw event、原子claim terminal-once并取消尚未执行的Gamma slots；`resolution_evidence_source=market_resolved_ws`的terminal metadata在下述bounded drain完成、真实state transition应用后enqueue，仍保留平台timestamp与本地receive time。不得把WS路径metadata命名为`fallback_close`。
- WS payload 不完整或 identity 矛盾仍按 integrity 边界fail closed，不降级成合格 fallback；只保留前述既有`polymarket_market` raw evidence及既有错误/manifest/coverage事实，不写`resolution_terminal_ws`或其他伪装成terminal/poll的v3 metadata row。本需求不为无效WS新增第六种metadata row kind。
- terminal validation、typed conversion与terminal-once claim必须由现有唯一Polymarket processing actor串行执行；fetch task和WS pump只提交completion/event，不得在actor外写terminal row。winner是该actor首先处理、完整校验并原子claim的消息，不按source类型或platform/receive timestamp追溯改写。
- 任一source claim terminal后，market仍保持`market_runtime_role=resolution_pending`且旧assets继续留在desired union，直到actor完成accepted-event fence/drain。Gamma先claim时，actor暂存已验证response，drain中的WS只保存为raw late corroboration；drain完成后才原子应用`active_to_retired + resolution_pending_to_none`、enqueue同一Gamma attempt terminal row并unsubscribe。
- WS先claim时，actor必须取消该slug尚未完成的poll，并在state transition前收拢已完成/已入队的Gamma completion；不得让completion越过teardown后再处理。completion仍执行SDK decode、attempt binding、identity/shape、closedTime与完整terminal predicate；任何integrity error按正常attempt evidence fail closed。所有需要保留的late Gamma attempt row先在actor内有界buffer，drain期间不得提前admit到writer。通过integrity检查的response使用该slot唯一`resolution_poll_attempt`，result=`post_terminal_claim_observation`、`terminal_claim_owner=market_resolved_ws`，完整保留source fields、可还原payload、digest与predicate三态，但lifecycle/role transition、`resolution_evidence_source`与terminal followup status均不得宣称Gamma获胜；row state记录transition前的active/resolution_pending事实、followup status=pending。
- WS-first drain成功后，actor必须先原子应用唯一`active_to_retired + resolution_pending_to_none`并把`resolution_terminal_ws`作为该terminal bundle的第一条metadata row admission，再按原completion顺序admit buffered Gamma attempt rows；同一UTC分区必须使用同一有序writer batch，跨分区必须受RF-10 producer fence约束。writer/finalize必须保持prefix durability，使任何finalized `post_terminal_claim_observation`都能在finalized metadata中找到同一fixed identity、且更早admit的唯一WS terminal row；禁止先持久化post row再补terminal。该表达不丢raw-equivalent evidence，也不新增第二terminal metadata。
- budget-exhausted候选没有terminal claim，必须先drain。fence前accepted事件由同一actor处理完后，actor要么由WS claim terminal并完成上述terminal teardown，要么原子关闭pending并enqueue唯一summary；只有完成该状态变更后才释放fence barrier并unsubscribe。关闭后才接收的WS最多保存为post-horizon raw corroboration，不能再与summary并存为第二terminal metadata；其raw `recv_time_utc`必须严格晚于summary的`runtime_role_applied_at_utc`，相等或更早均不能冒充post-horizon。

### RF-06：Gamma fallback 调度

- 只使用 pinned 官方 Polymarket Rust SDK `GammaClient::market_by_slug()` 查询固定 pending identity；不得复用带 `closed=false` 的 current/next discovery 请求。
- `initial_attempt=1`，调度在 `E+3m`；`retry_attempts=5`，分别调度在 `E+4m`、`E+5m`、`E+6m`、`E+7m`、`E+8m`；总计最多 6 次。
- 所有 slot 锚定绝对 `E+n`，不得在上一次请求完成后再 sleep 60 秒造成累积漂移。
- 每个 slug 同时最多一个 in-flight request；沿用当前单次20秒timeout时，E+8最后slot约在`E+8m20s`前完成分类。terminal/summary还必须等待RF-04/RF-05的bounded accepted-event drain并写真实applied/admission时刻，因此`E+8m20s`不是整体发布hard deadline。
- 任一 attempt 获得合格 terminal 后立即完成并取消剩余 slot。
- attempt ordinal 固定为 1..6：ordinal 1 是 initial，ordinal 2..6 分别对应 retry 1..5；不得同时把 initial 与 retry 1 写成同一个编号。
- 每个 scheduled slot 最多写一条 v3 attempt row，记录 scheduled time、ordinal、fetch start、真实完成/记录时间、result class 和 null-safe error/status evidence；没有 SDK response 时 response-derived 字段必须为 null。该有界 typed 证据是第四节第 6 项的精确 authority 例外：row 仍计入既有 artifact/file manifest 事实，但不得扩张 manifest schema、sidecar 或通用 transport episode。
- 停机期间未执行，或 unclean restart 后无法从 finalized artifact 证明已经执行的过去 slot，只可记录 `missed_due_to_downtime`。该值只声明“没有可用的 durable attempt evidence”，不反向断言旧进程一定没有发出网络请求；不得伪造 fetch、response 或平台字段。

### RF-07：terminal predicate

WS event 路径维持 RF-05。Gamma fallback 只有同时满足下列条件才能写 `resolution_followup_status=terminal_gamma_fallback`、`resolution_evidence_source=gamma_by_slug` 与本地 `retired`：

1. exact slug、market id、condition id 与固定 pending identity 一致；
2. 对固定二元 Up/Down market，outcomes、clob token ids 和 outcome prices 长度均为 2，fallback canonical asset set 与原 subscribed asset set 精确相等，不允许额外、缺失或重复 asset；
3. response 绑定本次唯一 attempt，由新的 SDK `market_by_slug` 调用取得而非复用 discovery cache；`fetch_start >= scheduled_at`、`receive >= fetch_start`。单次请求在20秒边界终止并写 `transport_unavailable + timeout`，该slot后到response丢弃且不得参与predicate；attempt无法绑定属于内部integrity error，也不得伪装成source predicate失败；
4. `closed=true`；
5. `acceptingOrders=false`；
6. `closedTime` 存在、可解析，且不早于 E；raw closedTime、解析/下界校验结果和本次 receive time 必须分别保存，但不得增加 `closedTime` 相对 receive time 的 future-skew allowance 或上界门；
7. 本轮只接受去除首尾空白并 ASCII 小写后精确等于 `resolved` 的 `umaResolutionStatus`；不保留未枚举的等价状态口子；
8. outcomes 与 outcome prices 完整，价格恰好一胜一负；`winning_asset_id` 与 `winning_outcome` 只能按同一 exact asset/outcome 数组索引派生，不能读取或注入 Gamma 不存在的独立 winner 字段；
9. `active` 字段原样保存为 nullable；缺失写 null 和 field-unavailable reason，但不影响 terminal 判定，无论 true/false/None 都不能恢复成 truth-value gate。

`automaticallyResolved=true` 是本目标 market 的支持证据，不作为所有 Polymarket market 的通用必要条件。本事项不增加 production CLOB 交叉请求；未来若要把 CLOB winner 加入 runtime predicate，必须另建需求并冻结额外请求、availability、schema 和资源预算。

官方SDK在HTTP成功后发生JSON/body/`Market` decode或schema错误时，没有可评估Market response；该slot写结构化`integrity_error + sdk_decode_or_schema`，predicate保持null，并按既有integrity边界fail closed。不得降级成`sdk_transport`或valid nonterminal。

### RF-08：预算耗尽语义

- 删除新的 `E+5s unresolved_window_end_observation` 生成路径；旧历史 v2 row 不修改。
- budget exhausted 时 market 不得标 `retired`。
- budget 完成后，`market_lifecycle_state` 在无 terminal 证据时保持 `active`，`market_runtime_role` 从 `resolution_pending` 退出为 none；使用独立 `resolution_followup_status` 表达：
  - `budget_exhausted_after_valid_nonterminal`：六次内至少一次取得 identity-valid 非终态 response 且始终没有 terminal；即使后续 slot 为 transport unavailable 也归此类；
  - `budget_exhausted_without_valid_response`：六次均未取得可评估 response，只出现 transport unavailable 或 missed-due-to-downtime；
  - `terminal_ws` 或 `terminal_gamma_fallback`：已经成功完成，不属于 exhausted。
- 对外 observation/event 名称使用 `resolution_followup_exhausted`，不再使用语义错误的 `unresolved_window_end_observation`。
- exhausted summary 必须记录 attempt 总数、valid-response 数、transport-unavailable 数、missed-slot 数和 nullable `last_valid_metadata_received_utc`。没有 valid response 时 terminal 派生字段保持 null；不得复制 T 时缓存的 active/accepting/closed 值冒充 E+8 fresh response。
- exhausted summary只能在最后slot分类完成且RF-04规定的accepted-WS fence/drain确认无合格terminal后发布；若drain得到terminal或integrity error，分别改走terminal或既有fail-closed路径，不得先写exhausted再覆盖。

### RF-09：typed schema v3

`polymarket_market_metadata_v2` 缺少审计 lifecycle、terminal predicate 与 attempt timeline 所需字段。本需求要求升级为 `polymarket_market_metadata_v3`。v3 使用以下五种稳定 `metadata_row_kind`，不再让同一个字符串同时表示 source、row kind 与 lifecycle：

1. `metadata_snapshot`：既有 `GammaClient::markets(MarketsRequest{slug, closed=false})` discovery 产生的 verified prefetch/current snapshot；v3不再新写本地slug-only candidate row，旧v2 candidate只读兼容；
2. `market_state_transition`：本地 T promotion 或 E runtime-role transition；
3. `resolution_poll_attempt`：E+3..E+8 的单个 Gamma slot，Gamma terminal 也由该同一 row 表达，不另写第二条 attempt；
4. `resolution_terminal_ws`：合格 `market_resolved` 的 terminal metadata；
5. `resolution_followup_summary`：预算耗尽后的唯一 summary。

v3 必须新增或显式保留下列 typed fields：

- 非空 row/envelope：`metadata_row_kind`、`timestamp_received`、非零 `collector_admission_sequence`、`collector_admission_time_utc_ms`、`metadata_origin=gamma_markets_discovery|gamma_by_slug|market_resolved_ws|local_market_state_timer|local_followup_scheduler`、collector host/region 和 schema version；
- identity/state：非空 slug、condition id、CLOB market id、canonical exact asset ids、market start/end、`fixed_identity_verified_at_utc`、四态 `market_lifecycle_state`、`market_runtime_role=current|next|resolution_pending|none`；`fixed_identity_verified_at_utc`是该market canonical identity首次完整验证时刻，后续row原值携带且不得改写；v3不允许slug-only candidate伪装成typed snapshot；
- evidence reference：nullable `source_endpoint`、`source_endpoint_unavailable_reason=not_applicable_no_source_evidence`、`source_evidence_origin=gamma_markets_discovery|gamma_by_slug|market_resolved_ws`、`source_evidence_event_type=gamma_markets_discovery_response|gamma_market_by_slug_response|market_resolved`、`source_evidence_channel=market`、`source_evidence_received_at_utc`、`row_evidence_validation_completed_at_utc`、`source_evidence_payload_json`、`source_evidence_raw_sha256`；该validation字段只表示本row外部evidence完成成功或失败校验的时刻，不表示校验必然通过，也不得拿首次identity时间替代；
- state transition：nullable `market_lifecycle_transition=prefetched_to_active|candidate_to_active|active_to_retired`、`lifecycle_scheduled_at_utc`、`lifecycle_applied_at_utc`、`market_runtime_role_transition=none_to_current|next_to_current|current_to_resolution_pending|resolution_pending_to_none`、`runtime_role_scheduled_at_utc`、`runtime_role_applied_at_utc`；lifecycle 与 runtime-role 字段不得跨命名空间写值；
- Gamma/WS source fields：nullable `metadata_fetch_start_utc`、`closed`、`closed_time_raw`、`closed_time_utc`、`closed_time_check_passed`、`closed_time_check_failure_reason=missing|parse_error|before_market_end`、`automatically_resolved`、`uma_resolution_status`、`outcomes_raw`、`outcome_prices_raw`、`active`、`active_unavailable_reason=source_field_absent|not_applicable_for_row_kind`、`accepting_orders`、`ws_platform_timestamp_raw`、`ws_platform_timestamp_utc`；
- winner/terminal：nullable `winning_asset_id`、`winning_outcome`、`resolution_evidence_source=market_resolved_ws|gamma_by_slug`、`resolution_evidence_raw_sha256`、`terminal_claim_owner=market_resolved_ws|gamma_by_slug`；
- attempt：nullable `resolution_poll_attempt_ordinal`（1..6）、`resolution_poll_scheduled_utc`、`resolution_poll_result=terminal|valid_nonterminal|transport_unavailable|integrity_error|missed_due_to_downtime|post_terminal_claim_observation`、`resolution_transport_error_class=connect|timeout|http_status|rate_limited|sdk_transport`、`resolution_transport_http_status`、`resolution_integrity_error_class=attempt_binding_invariant|sdk_decode_or_schema|source_identity_or_shape_contradiction|closed_time_invalid|derived_winner_invariant`；
- follow-up：nullable `resolution_budget_horizon_utc`、`resolution_followup_status=pending|terminal_ws|terminal_gamma_fallback|budget_exhausted_after_valid_nonterminal|budget_exhausted_without_valid_response`、`resolution_poll_attempts_recorded`、`resolution_valid_response_count`、`resolution_transport_unavailable_count`、`resolution_missed_slot_count` 与 `last_valid_metadata_received_utc`；
- predicate：nullable `terminal_predicate_passed` 与 `terminal_predicate_failure_reason`；not-evaluated 必须使用 null，不能冒充 false；
- nullable `response_fields_unavailable_reason=not_applicable_for_row_kind|transport_unavailable|missed_due_to_downtime|sdk_decode_or_schema_error`，用于解释整组 response-derived fields 为 null 的原因。`attempt_binding_invariant`在本需求专指已经decoded但与本slot/fetch context矛盾的stale/cache response，因此不得用该reason清空真实response fields。

字段组的 required/null 合同如下。`R` 表示该 row kind 必须非空，`N` 表示必须为 null，`C` 表示只在表后条件成立时非空。表中 source evidence组不包含两个专门解释null的`source_endpoint_unavailable_reason`与`response_fields_unavailable_reason`，它们的required规则在表后单列：

| `metadata_row_kind` | row time + admission | fixed identity | state transition fields | source response/evidence | row evidence validation | attempt fields | predicate/terminal fields | summary counters |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `metadata_snapshot` | R | R | N | R | R | N | N | N |
| `market_state_transition` | R | R | R | R | C | N | N | N |
| `resolution_poll_attempt` | R | R | C | C | C | R | C | N |
| `resolution_terminal_ws` | R | R | R | R | R | N | R | N |
| `resolution_followup_summary` | R | R | R | N | N | N | N | R |

矩阵条件冻结如下：

- `metadata_snapshot` 只接收verified discovery，origin固定为`gamma_markets_discovery`、event type固定为`gamma_markets_discovery_response`、channel为null；它的`timestamp_received`与`source_evidence_received_at_utc`都是本次`GammaClient::markets` response receive，payload/digest/endpoint/fetch start和完整identity必须存在。v3不生成local candidate row。
- `market_state_transition` 的origin固定为`local_market_state_timer`；`timestamp_received`是本地state transition真实生成时刻，admission是进入writer queue的时刻。正常T路径写lifecycle `prefetched_to_active` + role `next_to_current`，两组scheduled均为T、applied为真实处理时刻，并引用原prefetch evidence；该row不重新验证source，row evidence validation为null。T时没有verified next时，仅当真实`lifecycle_applied_at_utc`落在`[T,E)`且满足source receive <= row evidence validation completed = fixed identity verified <= applied，才写lifecycle `candidate_to_active` + role `none_to_current`并引用fresh discovery evidence；applied >= E一律expired且不生成该row或pending slots。E row同样不重新验证source，row evidence validation为null；lifecycle transition及其时间字段为null，只写role `current_to_resolution_pending`、scheduled E与真实applied。所有local state row不得复制旧`active/accepting/closed/resolution/outcomes`冒充当时平台状态。
- `resolution_poll_attempt` 对被唯一actor接受的decoded Gamma response要求fetch start、source receive、row evidence validation completed、endpoint、可还原payload、digest、response fields与predicate verdict。Gamma自己claim terminal时先保持active/resolution_pending；accepted-event drain完成后才enqueue同一row，并写lifecycle `active_to_retired`、role `resolution_pending_to_none`、两组post-drain applied真实时刻、`terminal_claim_owner=gamma_by_slug`、scheduled为null及retired/none/terminal_gamma_fallback。若WS已先claim，通过integrity检查的Gamma response仍写同一slot唯一attempt，但result=`post_terminal_claim_observation`、owner=`market_resolved_ws`、state=active/resolution_pending、status=pending，transition与`resolution_evidence_source`为null；predicate true/false及failure reason按真实response保存。`transport_unavailable`要求fetch start与transport error class，HTTP/rate-limit时同时要求status；20秒timeout即写timeout并丢弃后到response。SDK decode/schema error写result=integrity_error、integrity class=`sdk_decode_or_schema`、predicate/reason=null；attempt无法绑定同理写`attempt_binding_invariant`。`missed_due_to_downtime`的fetch/response/transport fields全为null。非terminal且无integrity error的attempt保持active/resolution_pending/pending。
- `resolution_terminal_ws` 要求真实frame receive、row evidence validation completed、WS endpoint、`source_evidence_event_type=market_resolved`、`source_evidence_channel=market`、source-native payload、raw digest、exact identity/winner、terminal verdict、`terminal_claim_owner=market_resolved_ws`与terminal_ws；WS先claim后仍保持active/resolution_pending到accepted-event drain结束，再由同一row写lifecycle `active_to_retired`与role `resolution_pending_to_none`，scheduled fields为null、applied为post-drain真实transition时刻，并写retired/none。Gamma-only fields、attempt ordinal和poll scheduled time为null。
- `resolution_followup_summary` 只写fixed identity、active/none、两种互斥 exhausted status、四个counters与条件nullable last-valid receive；source evidence、attempt、predicate和平台terminal fields全部为null。它的lifecycle transition及时间为null，只写role `resolution_pending_to_none`，`runtime_role_scheduled_at_utc=null`，applied为最后slot response/timeout及accepted-WS drain完成后实际退出pending的时刻；独立`resolution_budget_horizon_utc=E+8m`表达最后slot计划边界，不能把poll start伪装成role-exit scheduled time。
- follow-up字段逐row-kind冻结：`metadata_snapshot`及T的`market_state_transition`必须horizon/status均为null；E的`market_state_transition`必须horizon=E+8m、status=pending；每条`resolution_poll_attempt`必须horizon=E+8m，普通非terminal与`post_terminal_claim_observation`为pending、Gamma-owned terminal为terminal_gamma_fallback；pending期`resolution_terminal_ws`必须horizon=E+8m、status=terminal_ws；`resolution_followup_summary`必须horizon=E+8m并取两种exhausted status之一。不得用“任何follow-up row”留给实现自行判断。
- `terminal_claim_owner`仅在三种位置非空：Gamma-owned terminal attempt=`gamma_by_slug`，WS terminal row=`market_resolved_ws`，WS-owned `post_terminal_claim_observation`=`market_resolved_ws`；其余row必须为null。claim owner只说明唯一transition归属，不把corroboration attempt升级成第二terminal evidence source。
- `winning_asset_id`、`winning_outcome`、`resolution_evidence_source`与`resolution_evidence_raw_sha256`只在Gamma-owned terminal attempt或WS claimant terminal row非空。`post_terminal_claim_observation`无论predicate为true或false，这四个ownership-bearing terminal fields都必须为null；其source payload、outcomes/prices、source digest与predicate仍完整保留，可供离线corroboration，但不能成为第二terminal evidence。
- exhausted summary中`resolution_poll_attempts_recorded = resolution_valid_response_count + resolution_transport_unavailable_count + resolution_missed_slot_count = 6`；valid count只统计`valid_nonterminal` Gamma response。`resolution_valid_response_count > 0`时，`last_valid_metadata_received_utc`必须非空且等于六个slot中最大valid source receive；count=0时必须为null。任何`integrity_error`按既有范围fail closed，不生成伪装成正常预算完成的exhausted summary。
- `source_endpoint`对实际发起的请求、实际WS channel或被local transition引用的source evidence必填；因此transport、SDK decode和attempt-binding row只要已发起请求也必须保留Gamma endpoint。只有没有发起请求且没有source reference的missed/summary row才令endpoint为null并写`source_endpoint_unavailable_reason=not_applicable_no_source_evidence`；其他row该reason必须为null。source evidence event type及适用channel只在存在decoded/引用evidence时按上述枚举填写；Gamma evidence的channel必须为null，WS evidence必须为`market`。`active`有真实source field时原样写true/false；source field缺失或该row kind不适用时为null并写对应`active_unavailable_reason`。
- snapshot的`source_evidence_origin=gamma_markets_discovery`；state transition按其固定identity evidence写`gamma_markets_discovery`；decoded poll response（包括stale/cache attempt-binding contradiction）写`gamma_by_slug`且event type=`gamma_market_by_slug_response`；WS claimant terminal写`market_resolved_ws`。transport、decode、missed与summary没有可接受source response evidence时origin、event type、channel、receive、payload和digest都为null，但transport/decode仍按上一条保留已发起请求的endpoint/fetch事实。实际发起过`market_by_slug`但没有decoded response的attempt仍以`metadata_origin=gamma_by_slug`记录请求路径；missed和summary使用`local_followup_scheduler`。
- 对真实source-bearing row，`source_evidence_payload_json`必须可解析并无损重建全部source-native response inputs：WS保存source-native payload；Gamma保存pinned SDK完整decoded `Market`对象的确定性序列化，不得只写digest，也不得把fixed identity或本地attempt/timing context伪装进source payload。完整terminal predicate必须由payload + 本rowtiming/attempt fields + fixed identity共同重建。`source_evidence_raw_sha256`必须对实际存储的payload bytes计算。若SDK类型不能无损表达或序列化本需求使用的source字段，触发RF-11 SDK能力决策门，不得改用raw REST。正常T/E transition不复制payload，但其fixed identity + source receive + digest必须在finalized metadata中精确解析到一条source-bearing v3 row；解析不到、命中多条或digest不一致均为integrity error。`resolution_evidence_raw_sha256`只在owned terminal row非空，并必须等于对应`source_evidence_raw_sha256`。
- `row_evidence_validation_completed_at_utc`逐row冻结：verified `metadata_snapshot`、所有decoded Gamma attempt（包括attempt-binding contradiction）与WS terminal必填，并满足source receive <= row evidence validation completed <= collector admission；首次snapshot还必须使fixed identity verified = validation completed，后续row只携带不变的fixed值。正常T/E local transition、transport、missed、SDK decode与summary必须为null；仅late `candidate_to_active` transition因直接消费fresh discovery evidence而必填，并满足source receive <= validation completed = fixed identity verified <= lifecycle applied。
- 整组response-derived fields因row kind、transport、downtime或SDK decode而为null时，`response_fields_unavailable_reason`必须分别写`not_applicable_for_row_kind`、`transport_unavailable`、`missed_due_to_downtime`或`sdk_decode_or_schema_error`；所有decoded Gamma response row（包括attempt-binding contradiction）该字段必须为null，并保存实际response fields、receive、payload与digest。
- 每个decoded Gamma response都必须独立于first-failure precedence原样保存SDK实际提供的`closed_time_raw`（source缺失才为null），不得因为更早predicate失败而丢弃真实raw evidence。只有进入closedTime gate后才派生parsed/check/reason：存在、解析成功且不早于E时写check=true/reason=null；缺失、解析失败或早于E时写check=false与唯一对应reason。不得将closedTime与本次receive做future-skew上界比较；两者仍分别保存供审计。precedence在closedTime之前失败时parsed/check/reason可为null。没有decoded Gamma response时四者全部为null。

`terminal_predicate_failure_reason` 只取下列稳定枚举，并按顺序保存第一个失败原因：

1. `slug_mismatch`；
2. `market_id_mismatch`；
3. `condition_id_mismatch`；
4. `asset_count_not_two`；
5. `asset_duplicate`；
6. `asset_set_mismatch`；
7. `outcomes_count_not_two`；
8. `outcome_mapping_invalid`；
9. `outcome_prices_count_not_two`；
10. `closed_missing`；
11. `closed_false`；
12. `accepting_orders_missing`；
13. `accepting_orders_true`；
14. `closed_time_missing`；
15. `closed_time_parse_error`；
16. `closed_time_before_market_end`；
17. `uma_resolution_status_missing`；
18. `uma_resolution_status_not_resolved`；
19. `outcome_prices_not_one_hot`。

source predicate 的基础分类先冻结：枚举1..9与15..16归`integrity_error`，并分别写`source_identity_or_shape_contradiction`或`closed_time_invalid`；10..14与17..19在没有既有WS terminal claim时归`valid_nonterminal`。非one-hot已经完整覆盖无唯一winner的可达source状态。winner asset/outcome只由已验证exact数组派生；若source predicate通过后派生后置不变量仍失败，attempt写`integrity_error`、class写`derived_winner_invariant`，`terminal_predicate_passed`与failure reason都为null并fail closed。它不属于19项source failure，也不得向Gamma fixture注入不存在的winner字段来制造predicate failure。

最终`resolution_poll_result`必须按以下上下文顺序决定，不得并行套用两套分类：transport/missed/20秒timeout、SDK decode/schema、attempt binding、上述source integrity和`derived_winner_invariant`始终保留各自既有result，另一source已经claim也不能覆盖或吞掉；只有通过全部integrity gate的decoded Gamma response，才检查WS是否已claim。若已claim，result覆盖为`post_terminal_claim_observation`并原样保留predicate true/false及对应failure reason；若未claim，predicate true写`terminal`，predicate false写`valid_nonterminal`。因此`post_terminal_claim_observation`只覆盖原本会成为terminal或valid-nonterminal的integrity-clean response，从不覆盖integrity/transport/missed。

transport、missed、20秒timeout、SDK decode/schema error和attempt-binding invariant都没有可评估predicate，`terminal_predicate_passed`与failure reason为null；其中timeout归`transport_unavailable`，SDK decode/binding归结构化`integrity_error`。没有既有WS claim时，Gamma terminal为true + null reason，普通valid-nonterminal为false + 上述唯一reason；`post_terminal_claim_observation`则可按真实response为true或false。active null不产生failure reason。

每个被唯一processing actor接受的Gamma response都必须由对应v3 attempt row保留可还原payload、稳定digest与真实receive；另一source已claim不构成丢弃例外，只按上述优先级改变integrity-clean Gamma attempt为`post_terminal_claim_observation`且禁止第二transition。只有通过WS terminal integrity admission并成功claim terminal-once的事件才生成`resolution_terminal_ws`；Gamma-first时后到的合格WS仍只保留既有`polymarket_market` raw corroboration。所有SDK-decoded `market_resolved`无论校验结果都先按RF-05保存既有raw evidence。transport/local row不得伪造response-derived fields。v2与v3不得混写进同一running/finalized Parquet segment。所有v3 row仍由既有每日manifest的file facts覆盖；本需求不增加manifest字段或新artifact。

历史v2只保留read-only consumer/audit兼容，不能提供v3要求的fixed identity validation、source payload/digest reference或terminal bundle，因此不得由v2 row合成v3 row、恢复resolution-pending或创建missed slot。v3启用时必须从fresh SDK discovery建立current/next v3 evidence；若目标market已到E且活跃窗内没有v3 verified identity，则沿用RF-02 expired拒绝并保留coverage事实，不得借schema迁移追逐旧slug。

### RF-10：竞态、恢复与 UTC 日边界

- WS与Gamma同时完成时，由单一actor的terminal-once claim顺序仲裁且只发布一次终态；WS已claim后，actor随后处理的Gamma completion只能写`post_terminal_claim_observation`，不得改写为fallback。Gamma已claim时反向同理，queued WS只作raw corroboration。
- pending state 必须跨 UTC hour/day rollover；row 按真实 `timestamp_received` 进入实际接收日分区，通过 slug/condition 与前一日 market 关联。
- poll scheduler、completed-result queue 和 in-flight result routing 必须参加现有 Polymarket producer fence：seal 旧日前 drain 所有 `receive < boundary` 的已完成结果，`receive >= boundary` 安全 defer 到新日；不得在 resume 后把旧日 receive 事件送入新日 gate。
- producer fence还必须保持terminal bundle的prefix durability：若WS receive在旧日、in-flight Gamma completion receive在新日，fence允许该completion作为bounded drain消息进入actor，但禁止其new-day row提前admit；actor处理并buffer后先完成/finalize旧日WS terminal，再激活新日并admit post attempt，不能让“defer new-day row”与“等待drain”互相死锁。若Gamma completion receive在旧日而WS receive在新日，旧日fence必须先把Gamma交给actor分类，因此该Gamma不可能事后成为WS-owned post observation。任何finalized post row都必须能解析到已finalized的唯一WS terminal；孤儿post属于artifact integrity error，restart必须fail closed，禁止恢复pending、重新claim或写exhausted。
- 同一进程的 source outer recovery 必须保留 pending fixed identity、attempt state 与剩余 slots。
- 本事项不新增 checkpoint/journal/sidecar，也不保证 unclean process crash 后恢复尚在 running Parquet 中的 pending/attempt/terminal。启动时只可从现有finalized v3 metadata与对应既有`polymarket_market` raw artifact做best-effort恢复，历史v2不能作为恢复authority：先校验terminal bundle、source-reference及raw-to-metadata完整性；已finalized metadata terminal不得重放，finalized post没有对应terminal时fail closed。对同一fixed identity，finalized terminal与exhausted summary必须互斥；存在唯一owned terminal时，`[T,recovery_time]` matching WS raw可分别作为WS获胜或Gamma-first losing corroboration。存在唯一exhausted summary时，只有`raw.recv_time_utc > summary.runtime_role_applied_at_utc`的matching raw是合法post-horizon raw-only corroboration；相等或更早的raw说明fence/drain遗漏，必须fail closed。terminal与summary均不存在时，任何matching raw都表示已观察却未完成typed commit，必须fail closed；多个terminal、多个summary或两者并存同样fail closed。上述fail-closed路径禁止恢复pending、Gamma fallback或exhausted，也不得依据raw伪造原transition/applied/admission补metadata。只有metadata/raw均无冲突、durable v3 verified identity存在且至少一个未来slot尚未到期时才恢复未来slots；不得补做已错过slot，不在E+8最后slot以后发起第7次final probe。
- unclean crash 造成的不可恢复窗口沿用现有 unclean manifest/coverage 事实，不能伪称 exactly-once 或完整执行 6 次。
- pending 完成后，其 task/subscription/state/dedupe 必须有界释放，不得产生随 market 数量单调增长的 FD、task 或内存。

### RF-11：SDK-first 与失败边界
- production 只能使用 pinned 官方 Polymarket Rust SDK 已有 market channel、dynamic subscription 和 Gamma 方法；本事项不增加 production CLOB follow-up。
- 若实际实现发现 SDK 类型、请求或响应无法满足 RF-05、RF-07 或 RF-09，必须列出具体缺口、影响和最小可选方案并等待用户决定；不得自行改用 raw REST、manual WebSocket 或 shadow client。
- `resolution_transport_error_class`只按 pinned SDK 和其公开 typed error surface 做 first-match 分类，固定优先级为：typed HTTP status 429 -> `rate_limited`；其他 typed HTTP status -> `http_status`；`reqwest::Error::is_timeout()` -> `timeout`；`reqwest::Error::is_connect()` -> `connect`；其余 SDK transport error -> `sdk_transport`。`connect`覆盖`is_connect()`所报告的DNS/TCP/TLS/proxy连接建立失败；v3不保留独立`dns` subtype，不得读取错误显示文本、private error type/source chain或自建resolver来重新细分。HTTP成功后的SDK decode/schema错误仍按RF-07写`integrity_error + sdk_decode_or_schema`，不进入上述transport分类。
- Gamma network/HTTP/rate-limit 与有效非终态均不得结束整个 collector，也不得阻塞其他 source；identity/schema contradiction 继续按既有完整性范围 fail closed。

## 七、验收标准

### AC-01：时间线

fake-clock/paused-time测试证明next可早于T-30 prefetch、T到期触发promotion、没有新的E+5 sentinel、E+3 initial与E+4..E+8五次retry均锚定绝对时间；late identity只按applied判窗，applied=E及以后expired。

### AC-02：真实时间证据

T promotion row同时保留scheduled T、真实applied time、transition的真实`timestamp_received`、writer admission time/sequence与原prefetch source-evidence receive/digest；fresh Gamma/WS row使用本次真实fetch/receive/admission。测试证明五种row kind不混淆local observation、首次fixed identity验证、本row evidence validation与admission：适用row满足source receive <= row validation completed <= admission，late transition另满足validation completed = fixed identity verified <= applied；不适用row的validation字段为null。任何artifact都不存在伪造的T、E、closedTime、network receive或停机retry timestamp。

### AC-03：subscription 与重叠 lifecycle

loopback/SDK contract 测试证明旧 assets 在 pending 期间保留、terminal/exhausted 后精确移除；2 pending + current + next 时仍由同一 actor 管理最多 8 个目标 assets，无第二 owner、重复 subscribe churn 或 lifecycle 上限错误。

### AC-04：WS 先到

分别覆盖WS由单actor先claim + queued/in-flight Gamma completion、Gamma由单actor先claim + 已入队WS，以及E+8 valid-nonterminal/timeout + 已入队WS三类反向竞态：WS-first时后到Gamma仍完成integrity/predicate处理并在actor内buffer该slot唯一attempt，drain后WS terminal row先完成唯一transition并先admit，随后才admit`post_terminal_claim_observation`；Gamma-first保持pending/assets到drain完成，之后只写terminal_gamma_fallback且queued WS仅为raw late corroboration；exhausted fence在释放barrier前由同一actor原子选择WS terminal或关闭pending+summary。三类都只转换一次terminal metadata/lifecycle，transition applied不早于drain结束，之后才移除旧assets；Gamma integrity error仍按正常attempt evidence fail closed，不得因WS已claim而被吞掉。

### AC-05：Gamma 正例

至少覆盖一个真实形态的 `active=true, closed=true, acceptingOrders=false, umaResolutionStatus=resolved` fixture，并证明它可通过 terminal predicate；`active=true` 不再被拒绝。

### AC-06：Gamma 负例

table-driven测试必须对RF-09冻结的19个source failure枚举逐项各有至少一个fixture，明确覆盖slug、market id、condition id、asset count/duplicate/set、outcome count/mapping、outcome-price count、closed missing/false、accepting-orders missing/true、closedTime missing/parse/before-E、resolution missing/non-resolved与non-one-hot；另覆盖active null和不绑定当前attempt的stale/cache response。组合失败fixture必须证明只保存precedence最前的唯一reason。除active null外均不得retired，active null仍按其他证据判定；每个可达source负例必须得到唯一result class、predicate三态与稳定failure reason。另以不注入Gamma winner字段的纯派生后置不变量单元测试覆盖`derived_winner_invariant`，断言integrity fail closed且predicate/reason均为null。

### AC-07：重试与 exhausted

分别证明initial、第1/中间/第5 retry成功时立即停止；ordinal与retry映射唯一；6次有效非终态以及valid+transport混合序列只在accepted-WS drain无terminal后写一次`budget_exhausted_after_valid_nonterminal`；零valid response同样只在drain后写一次`budget_exhausted_without_valid_response`。queued terminal不得与exhausted并存；不复制旧metadata、不写无证据retired，也不发起第7次probe。

### AC-08：availability 与 integrity 分流

connect/timeout/rate-limit availability统一进入`transport_unavailable`，valid nonterminal 与 identity/schema contradiction 分别进入另外两个结果类；availability和valid nonterminal不结束 collector，identity/schema contradiction 保持既有 fail-closed 边界。测试必须证明RF-11固定的typed分类优先级；DNS失败只在公开`is_connect()`成立时归`connect`，不得恢复独立`dns`标签。

### AC-09：schema 与 writer

五种v3 row kind的逐组required/null matrix、完整枚举、identity验证时刻、各row horizon/status、Arrow/Parquet roundtrip、source event type/channel、可还原payload与digest、local reference解析、writer batch、恢复和consumer compatibility tests全部通过；v2/v3不混写，v2不能合成v3或恢复pending，所有response-derived terminal fields可追溯，任何finalized post都有更早finalized的唯一WS terminal。本事项的attempt rows计入既有Parquet file row count/hash/schema manifest facts，但每日manifest schema与artifact集合不扩张。

### AC-10：UTC 与 restart

23:55 UTC market 的 E+3..E+8 rows 按各 row kind 的真实 `timestamp_received`（网络 receive 或本地事实生成时刻）进入对应次日分区并保持 identity；D-1ms/D/D+1ms 与 in-flight/fence 竞态不产生 UTC regression。对WS-first terminal bundle在raw admission/finalize、actor buffer、metadata writer admission、batch flush、segment finalize和UTC seal每个切点做crash fixture，证明任何finalized post都已有唯一finalized WS terminal。restart matrix另覆盖：raw+unique terminal合法、raw+无terminal/summary失败、raw receive <= exhausted role-exit失败、raw receive > role-exit作为合法post-horizon corroboration、terminal+summary或重复终态失败；失败路径不得恢复pending、fallback或exhausted。process restart只对无terminal/exhausted冲突的finalized v3 verified identity和未来slot做best-effort恢复，错过slot不伪造且没有post-deadline final probe。

### AC-11：资源与持续运行

确定性多 market 测试证明 pending 数、in-flight、task、FD、subscription 和 compact terminal dedupe 有界；两个 pending 额外本地状态峰值低于 1 MiB，连续运行后无随 market 数量单调增长。

### AC-12：真实数据采集验收

阶段4执行授权已于2026-07-22取得；只有阶段1至阶段3分别完成实现、验证、独立审查与commit/push后，才在本地原生Ubuntu环境使用用户已配置的香港VPN线路运行对Polymarket只读、仅写本地隔离validation artifact的live validation，不再上传或改到AWS爱尔兰数据采集服务器执行。测试至少覆盖多个完整五分钟market，并取得以下之一：真实WS terminal；或E+3 Gamma terminal；或可审计的retry/exhausted路径。启动前、运行期间每个完整五分钟market window至少一次、结束后都必须保存不含凭据且可核验的香港VPN/出口线路状态，运行中保留Polymarket market-channel与Gamma endpoint、连接错误及重试证据；VPN不可用、线路无法确认或发生静默直连回退时，AC-12不得通过，也不得把该结果归因于SDK或collector缺陷。报告必须区分platform closedTime、WS platform timestamp、Gamma fetch/receive和本地collector receive，不得用closedTime推断历史可见时间，也不得把本地香港VPN路径的receive/latency冒充AWS爱尔兰paper/live主机延迟。

### AC-13：全量回归与范围

data-collect targeted tests、全量 tests、check、Clippy、fmt、diff check 均通过；静态检查证明没有 raw REST/manual WS/shadow client、第二 subscription owner，也没有修改 Binance、RTDS、paper/live、模型或交易路径。

### AC-12/AC-13 执行结果（2026-07-23）

阶段4在本地隔离artifact中完成真实source执行并通过主代理自审：

- 精确commit `a32226863428dcbd82b016538f36855240a41892` 的release binary连续运行
  `1802` 秒，collector与线路监控退出码均为 `0`，无临时文件；
- `31/31` 份before/running/after线路样本均为
  `8.210.176.1 / HK / HKG`，覆盖 `5` 个完整五分钟market的每个活跃窗；
- collector进程树 `22/22` 次IPv4/IPv6 connect均只到本地香港线路
  `127.0.0.1:20810`，非loopback connect为 `0`；
- 5个完整market中4个在E+3前取得真实WS terminal；5条metadata terminal与
  5条raw `market_resolved`逐条唯一对应；
- v3 metadata为精确68字段、25行，row-kind计数为snapshot 8、transition 12、
  WS terminal 5、attempt 0、summary 0；WS先于E+3完成使attempt/summary为0符合
  terminal-once合同，不要求真实运行人为制造五种row kind；
- manifest 11/11 file facts、Parquet bytes/hash/rows/footer/embedded facts一致，
  full tests、check、Clippy、fmt与diff check全部通过。

真实source报告：
`data-collect/docs/polymarket-resolution-followup-ac12-validation.md`。
该结果仅代表本机进程被强制经既有香港线路的validation，不代表系统默认路由或AWS爱尔兰
paper/live延迟。阶段4需求映射、文档/验收与范围/简洁性三路独立Ultra终审均为PASS，
Critical 0、Important 0、Minor 0；已满足计划4.3最终commit/push门。

## 八、用户审查决定

用户于 2026-07-22 逐项明确决定：

1. 接受 `E+3m` initial + 5 retries，即总计 6 次、最后 slot 为 `E+8m`。
2. 接受废弃新产生的 `unresolved_window_end_observation`，改为独立 `market_runtime_role=resolution_pending` 与 `resolution_followup_exhausted`。
3. 接受 `active` 只记录、不参与 terminal truth-value 判定。
4. 接受对“禁止追逐过期slug”增加仅限活跃窗内已固定并验证identity的pending-resolution有界例外；process restart仍额外要求finalized durable identity。
5. 接受 metadata schema 升级为 v3，采用 RF-09 的五种 row kind、required/null matrix、稳定失败枚举、可还原source payload与digest/reference合同，且不回写历史 v2 数据。
6. 接受旧market assets保留到terminal或budget完成，并只对pending旧market路由terminal事件；唯一actor claim后仍保持pending/assets直到drain结束，Gamma-first时queued WS仅作late corroboration，exhausted则必须被drain中的queued terminal阻止。
7. 接受每个slot最多一条v3 typed attempt row这一对旧transport-evidence authority的有界例外；row仍进入既有Parquet与manifest file facts但不扩张manifest schema/artifact；WS已claim时后到actor的Gamma completion仍完成integrity/predicate处理并保留该slot唯一attempt evidence，但唯一WS terminal必须与`post_terminal_claim_observation`同批或更早finalized，绝不允许post单独先finalized，且不得产生第二terminal ownership、evidence source或transition。
8. 接受 process restart 仅从finalized v3 metadata及既有`polymarket_market` raw artifact做best-effort未来slot恢复，历史v2不作为恢复authority；matching finalized WS raw必须对应唯一owned terminal，或严格晚于唯一exhausted role-exit而只作post-horizon corroboration，其他组合fail closed且不自动补写；不新增checkpoint，也不执行post-deadline第7次probe。
9. 拒绝固定 5 秒 `closedTime` future-skew allowance，并要求删除。RF-07、RF-09 与 AC-06 已删除 `closedTime` 相对 receive time 的上界、对应失败枚举和fixture，只保留存在、可解析、`closedTime >= E`及两条时间各自原样保存的合同。
10. 批准同步主 authority；本次批准不授权 Rust/schema/test 实现、commit、push、release、远端 probe、部署、服务操作或数据清理。
