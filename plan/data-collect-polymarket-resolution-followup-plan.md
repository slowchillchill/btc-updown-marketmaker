# Polymarket 五分钟市场生命周期与结算追踪修复计划

> 事项编号：`DC-REPAIR-20260721-POLYMARKET-RESOLUTION-FOLLOWUP`
>
> 对应需求：`demand/data-collect-polymarket-resolution-followup-demand.md`
>
> 对应 changelog：`changelog/data-collect-polymarket-resolution-followup-changelog.md`
>
> 状态：`PHASE_4_COMPLETE_LOCAL_HK_VPN_AC12_AC13_PASS_2026-07-23__V3_VALIDATE_FIRST_METADATA_V4_IMPLEMENTED_CURRENT__GAMMA_CLOSED_TIME_FORMAT_A_APPROVED_FIX_PENDING_2026-07-27`。阶段0至阶段3已分别完成自审、独立终审与commit/push；阶段4本地香港线路AC-12、本地全量AC-13与任务4.3三路独立全面终审均已通过。用户于2026-07-24批准source schema v3的validate-first文档修订，并于2026-07-25批准metadata v4 claimant schema；二者已由SDK070 Stage3实施并成为当前new-write组合。2026-07-27 Attempt3暴露的Gamma顶层`closedTime`精确格式缺口已由用户批准方案A，本地最小修复、测试和发布待执行。production部署、服务操作和数据删除仍未获授权、未执行。

> 2026-07-22 AC-12执行拓扑修订：真实source validation固定在本地原生Ubuntu环境通过用户已配置的香港VPN线路执行，不再上传到AWS爱尔兰数据采集服务器。用户已授权阶段4在阶段1至阶段3全部通过后执行；不得改变production service或执行数据删除。

> 2026-07-23阶段职责分界：用户批准阶段2只闭合actor语义、竞态矩阵、原子writer admission与有界
> 内存source-reference authority；finalized prefix durability、actor/writer/flush/finalize/UTC seal
> crash cuts及process restart恢复统一归阶段3。该分界不削弱RF-10、AC-09或AC-10，只避免阶段2另造
> 与阶段3重复的durability/recovery框架。

## 一、执行原则与成功标准

本计划阶段0文档authority同步已经完成；现按用户明确授权顺序执行阶段1至阶段4。每个阶段必须先完成自审与独立审查、commit/push后才能进入下一阶段，并保持官方Polymarket Rust SDK-first、唯一market-channel subscription owner、真实时间戳、identity fail-closed和其他source独立运行。

整体成功标准：

1. next market 尽早 prefetch，健康情况下最迟 T-30 已准备；
2. 健康且T前已有verified prefetched identity时，current lifecycle在T到期触发，不再人为T+5；缺identity按RF-02处理且不伪造active；
3. 旧 market 在 E 后继续接收 WS resolution，并按 E+3 initial + E+4..E+8 retry 做 Gamma fallback；
4. `active=true` 的真实 resolved market 可以通过结构化 terminal predicate；
5. budget exhausted、transport unavailable 与 integrity error 不混淆；
6. metadata v3足以重建既有/current source-v2因果时间线；未来source-v3 claimant recovery只使用已批准metadata v4精确外键，metadata v3不能充当该authority；
7. UTC rollover 与同进程 source recovery 不丢失 pending evidence；process restart 只按已冻结的 finalized-artifact best-effort 边界执行且不伪造证据；
8. targeted/full tests、静态检查和后续经授权的真实 live validation 全部满足需求 AC-01 至 AC-13。

## 二、预计修改边界

预计涉及：

- `demand/data-collect-demand.md`；
- `demand/data-collect-source-availability-lifecycle-repair-demand.md`；
- `demand/rebuild-core-demand.md`中与本事项直接冲突的data-collect lifecycle/runtime-role窄边界；该同步不批准rebuild-core其余草案；
- `plan/data-collect-plan.md`；
- 对应历史 changelog 的新追加记录，不改写旧记录；
- `data-collect/src/live_runtime.rs`；
- `data-collect/src/market_discovery.rs`；
- `data-collect/src/polymarket_sdk_runtime.rs`；
- `data-collect/src/sources/polymarket.rs`；
- `data-collect/src/source_format.rs`；
- `data-collect/src/dedupe.rs`；
- `data-collect/src/polymarket_market_metadata_layout.rs`；
- `data-collect/src/writer.rs`；
- 与 metadata、market contract、SDK runtime、writer、UTC/recovery 直接相关的 tests。

不得顺手重构无关 source、manifest、模型或交易代码。若真实实现证明 SDK 能力不足，立即停在 SDK 能力决策门，不创建 raw REST/manual WS 旁路。

## 阶段 0：用户审查、参数冻结与 authority 对齐

### 任务 0.1：冻结需求语义

- 步骤 0.1.1：由用户审查并决定 demand 第八节 10 项。
  - 验证：需求状态从 `DRAFT_FOR_USER_REVIEW` 变更为明确 approved 状态，并在 changelog 记录逐项决定；没有口头推断或默认批准。
- 步骤 0.1.2：冻结 retry 计数为 initial 1 + retry 5，以及最终 observation/status 名称。
  - 验证：demand、plan、changelog 对`E+3..E+8`、总6次、E+8 slot约20秒分类边界及随后bounded drain使用真实完成时刻的表述一致；不把E+8m20s写成整体hard deadline。
- 步骤 0.1.3：按用户第 9 项决定删除固定 5 秒`closedTime` future-skew allowance，不以其他推测性容差替代。
  - 验证：terminal predicate只要求closedTime存在、可解析且不早于E；closedTime与receive分别原样保存，但不存在两者间的上界门、对应failure枚举或fixture。

### 任务 0.2：同步现有 authority

- 步骤 0.2.1：在主 demand 中澄清 Gamma `active` 为观察字段，不要求 false，也不与 closed/resolved 互为反义。
  - 验证：全仓当前需求不存在 fallback 必须 `active=false` 的有效 authority。
- 步骤 0.2.2：在主 demand中把纯时间`current -> retired`改为`current role -> resolution_pending role -> evidence-backed retired lifecycle`；在rebuild-core草案中只补齐同一独立`market_runtime_role=resolution_pending`窄边界。两者都保持四态lifecycle，不借同步批准rebuild-core其余内容。
  - 验证：公共 `MarketLifecycleState` 仍为四态；runtime role 与 lifecycle 不跨域比较。
- 步骤 0.2.3：在source-availability demand中保留current/next discovery不追逐expired slug，并增加仅限活跃窗内已固定、已验证identity的pending-resolution exact-slug有界例外；process restart仍要求finalized durable identity。
  - 验证：文本明确限定目标、身份来源、用途、E+8m 最后 slot 及该 slot 单次 timeout 完成窗口；不会扩张为历史 discovery/backfill 或第 7 次 probe。
- 步骤 0.2.4：明确 supersede transport 只写 operator 日志的窄边界，允许每 market 最多 6 条 v3 resolution-attempt row；它们进入既有 metadata Parquet并由每日 manifest 的既有 file facts覆盖，但不增加 manifest字段、artifact mapping、sidecar或episode tape。
  - 验证：typed nullability 和固定业务 slot 明确；manifest仍只记录既有file row count/bytes/hash/schema facts，不把普通 transport 恢复变成逐 attempt 数据。
- 步骤 0.2.5：在主plan的任务8.5当前规范中supersede旧`active=false` predicate，并补齐结构化winner/resolution evidence、schema v3与live acceptance；阶段2任务2.1步骤2.1.3的历史执行结果保留原文，只由当前authority注释与新changelog追加记录supersede。
  - 验证：plan 步骤和测试逐项映射 demand RF-01 至 RF-11、AC-01 至 AC-13。
- 步骤 0.2.6：只向旧 changelog 追加 supersede 记录，不改写历史事实。
  - 验证：`git diff` 中旧记录只增不删；变更原因、影响和用户决定可追溯。

### 任务 0.3：阶段审查门

- 步骤 0.3.1：主代理自审三类 authority、scope 与 diff。
  - 验证：无 unresolved 文档冲突、无超范围授权、`git diff --check` 通过。
- 步骤 0.3.2：使用 `gpt-5.6-sol`、Ultra reasoning 的独立子代理做全面阶段审查。
  - 验证：Critical/Important 均为 0，或问题修复后复审通过。
- 步骤 0.3.3：只有用户已经授权 git 发布时才创建精确 commit 并 push；未获授权则本阶段停在 review-ready 且不进入下一阶段。
  - 验证：不得把计划批准自动解释为 commit/push 授权。

## 阶段 1：先冻结失败测试与 metadata v3

执行解释：任务1.1在本阶段冻结并实际观察目标行为的预期RED，不把尚未实现的
pending/scheduler行为误报为GREEN；阶段1审查门仍只要求该RED可编译、可复现且失败在
目标行为差异，并要求schema/predicate及既有默认测试保持通过。metadata v3在本阶段只建立
同一layout/writer中的合同能力，production `typed_schema_version`与live emission继续唯一写v2；
不新增第二writer、第二source kind、产品feature或dual-write。阶段2完成pending、terminal actor、
admission与scheduler后再删除旧metadata fanout，并一次性切换production到v3。

### 任务 1.1：建立 fake-clock 时间线测试

- 步骤 1.1.1：新增 paused-time fixture，复现当前 T+5 promotion、E+5 缓存 sentinel 和旧 assets 过早移除。
  - 验证：修复前测试精确失败在用户要求的行为差异，而不是缺少通用 helper。
- 步骤 1.1.2：冻结目标时间线：early prefetch、T promotion、E pending、E+3 initial、E+4..E+8 retry。
  - 验证：测试断言绝对slot，不使用“请求完成后sleep 60s”；late identity只按applied判窗，并覆盖receive<T/verify>T/applied<T+、receive<E/verify<E/applied>E以及applied=E组合，证明applied E-1ms可active、E及以后expired且不创建pending slots。

### 任务 1.2：升级 metadata schema

- 步骤 1.2.1：在保留现有v2 production路径的同时新增 `polymarket_market_metadata_v3` 合同能力，精确实现 RF-09 的五种 row kind、字段名、完整枚举和 required/null matrix。
  - 验证：schema field names、row-kind validator、UTC timestamp unit、source metadata 与需求完全一致；所有v3 row identity非空，旧v2 slug-only candidate只读兼容且不再新写；阶段1没有production v3 emission或dual-write。
- 步骤 1.2.2：在同一layout与既有writer版本分派中扩展v3 row、测试用 raw-to-typed conversion 和 writer batch，分离 `timestamp_received`、source-evidence receive 与 collector admission，冻结 attempt ordinal 1..6、poll result、transport class/status、closedTime raw/check、active unavailable reason、followup summary、counters、nullable predicate/evidence、event type/channel、可还原payload与digest fields；不新增独立v3 writer factory或提前冻结依赖阶段2 actor时序的production constructor。
  - 验证：T/E local rows不复制旧平台状态；真实 response-derived fields只来自对应 response/frame；transport/missed/summary row不含伪造 closed/winner/receive。
- 步骤 1.2.3：增加五类 row 的 table-driven validator、failure-reason precedence、transport subtype稳定枚举与公开typed分类优先级合同、Arrow/Parquet roundtrip、v2/v3 non-mixing、manifest file-facts coverage、schema-level recovery eligibility 与 consumer compatibility tests；startup recovery与crash matrix仍留在阶段3。
  - 验证：AC-02、AC-06、AC-09通过；transport枚举仅保留RF-09五种稳定值并冻结RF-11公开typed优先级；fixed identity时间跨row不变，row-evidence validation completed只在适用row非空并满足source receive <= validation completed <= admission/applied；`polymarket_market_metadata_v3`内部的source-bearing metadata row payload无损重建全部source-native response inputs，payload + row timing/attempt + fixed identity可重建完整predicate，digest重算一致，local reference精确解析到唯一metadata source row；decoded stale/cache binding error也保留payload/receive/digest且predicate为null；WS event type/channel闭合；五类row的horizon/status逐项required/null；post row四个ownership-bearing winner/evidence字段为null；v2只读兼容且不能合成v3/recover pending；attempt rows增加既有file row count/hash但不改变每日manifest schema/artifact集合。该验证不要求未来`polymarket_orderbook_v3` raw-first，也不把metadata digest作为orderbook source recovery authority。

### 任务 1.3：结构化 terminal predicate 测试

- 步骤 1.3.1：先增加 `active=true + closed=true + acceptingOrders=false + resolved + one-hot winner` 正例。
  - 验证：旧代码测试失败，证明命中真实冲突。
- 步骤 1.3.2：按 demand AC-06 对19个source failure枚举逐项增加table-driven fixture，并增加组合失败precedence、active null、20秒timeout、attempt binding与SDK decode/schema测试。本轮不增加Gamma/CLOB runtime交叉分支。
  - 验证：19个枚举各自可达且得到唯一结构化reason，组合失败只取冻结顺序最前项；decoded response即使更早gate失败也保留closedTime raw，且不比较closedTime与receive的future skew；timeout不误报integrity，decode/binding不伪造predicate，winner只从Gamma实际数组派生。独立派生后置不变量测试不伪造Gamma winner字段，并断言`derived_winner_invariant + predicate null`。
- 步骤 1.3.3：移除 `active` truth-value gate，保留字段存在/类型和原值。
  - 验证：AC-05、AC-06、AC-08 通过；event path 不回归。

### 任务 1.4：阶段审查门

- 步骤 1.4.1：运行并记录named timeline RED，确认其失败在目标行为差异；随后运行targeted schema/predicate tests、默认测试、fmt和diff check并自审，默认验证不得因RED fixture而失败。
- 步骤 1.4.2：按阶段 0 相同模型与 reasoning 要求执行独立阶段审查，修复后复审。
- 步骤 1.4.3：只有已授权时 commit/push；否则停在 review-ready 且不进入下一阶段。

## 阶段 2：T/E lifecycle、订阅保留与有界 poll scheduler

### 任务 2.1：拆分 current rollover 与 resolution follow-up

- 步骤 2.1.1：引入最小 per-slug pending-resolution state，保存需求 RF-03 的固定 identity 与 slot 状态。
  - 验证：同一 slug 只存在一个 item，item 完成后全部 ownership 有界释放。
- 步骤 2.1.2：以独立 absolute boundary timer 驱动 T promotion；promotion 直接使用已验证 prefetched context，不等待网络。
  - 验证：AC-01、AC-02；无新 E+5 sentinel。
- 步骤 2.1.3：T 到期后异步 discovery 新 next；旧 market 同时进入 pending。
  - 验证：pending follow-up 不阻塞 current/next WS drain、Gamma discovery 或其他 source。
- 步骤 2.1.4：覆盖 T 到期但 `next=None` 或 Gamma unavailable 路径，禁止伪造 active，按 exact current 异步恢复并保存 scheduled/applied/unavailable 事实。
  - 验证：缺失identity时没有active row；只按真实applied是否落在自身`[T,E)`决定candidate-to-active/none-to-current，receive/verified仅满足因果顺序；applied在E及以后expired且不创建resolution slots；其他source持续运行。

### 任务 2.2：扩展唯一 subscription actor 的 desired union

- 步骤 2.2.1：desired assets 由 current + next + pending union 计算，仍由现有 actor执行 additions-before-removals。
  - 验证：AC-03；同 desired 不产生 wire churn。
- 步骤 2.2.2：扩展 identity/asset routing 和 resolved state 以支持最多 2 pending + current + next；pending 只路由 terminal 事件并使用每 slug 最多 16 个 fingerprint 的 compact dedupe，不扩展整窗 2,000,000-entry lifecycle 集合。
  - 验证：8 assets/2 pending runtime-role fixture 通过，外部/非terminal旧market事件被过滤，目标旧market resolution可路由，额外状态低于1MiB。
- 步骤 2.2.3：terminal/exhausted 后精确移除对应 pending assets，不影响共享/现有目标。
  - 验证：无过早 unsubscribe、无残留 subscription、无第二 owner。

### 任务 2.3：实现 WebSocket terminal-once

- 步骤 2.3.1：让 identity verifier 可查 current、next 与 pending 固定身份。
  - 历史执行验证（当前v2，2026-07-23已完成）：WS exact identity正例与逐项矛盾负例通过；每个SDK-decoded `market_resolved`先进入既有`polymarket_market` raw artifact，无效事件fail closed且不生成任何v3 terminal/poll metadata row。该raw-first仅保留为已完成v2事实；未来source schema v3由本计划“2026-07-24 v3 new-write修订”取代。
- 步骤 2.3.2：用 per-slug terminal-once 仲裁 WS/Gamma 竞态。
  - 验证：AC-04；现有唯一Polymarket processing actor串行执行validation、typed conversion与claim，fetch/WS producer不在actor外写terminal；terminal metadata/lifecycle只转换一次。
- 步骤 2.3.3（当前v2历史执行）：任何terminal claim后保持pending/assets直到accepted-event drain结束。Gamma-first随后写唯一Gamma terminal row，queued WS仅在既有`polymarket_market`保存raw corroboration；WS-first时，queued/in-flight Gamma completion仍完成integrity检查与predicate计算并在actor内buffer该slot唯一attempt evidence，但不取得terminal ownership、不产生第二transition。drain后先应用唯一transition并把WS terminal作为terminal bundle第一条writer admission，再按completion顺序admit buffered attempts；exhausted fence在释放barrier前由同一actor原子terminal或关闭pending+enqueue summary。未来source schema v3不再保存invalid/unmatched/post-horizon raw，按本计划v3修订任务执行。
  - 验证：Gamma-first的transition applied在drain后且Gamma attempt独占status；WS-first分别覆盖late terminal/nonterminal Gamma response及SDK decode、binding、identity/shape、closedTime integrity error，断言同一actor先原子应用唯一transition，再以terminal-first有序batch admission保留该slot唯一attempt evidence且WS独占terminal transition；容量或reference authority失败时整批零admit，内存中不存在可单独admit的孤儿post。E+8 nonterminal/timeout断言barrier释放前已线性化terminal或summary。三者都无过早unsubscribe，不新增candidate/superseded状态。finalized prefix与crash-cut验证按2026-07-23批准分界归任务3.1。

### 任务 2.4：实现 Gamma exact-slug poll scheduler

- 步骤 2.4.1：新增仅供 pending resolution 使用的 SDK `market_by_slug` 查询路径；不复用 `closed=false` discovery。
  - 验证：SDK provenance/static contract证明无 raw REST/manual path。
- 步骤 2.4.2：用最小单一有界 scheduler 驱动绝对 E+3..E+8 slots，每 slug 最多一个 in-flight。
  - 验证：AC-01、AC-07；20 秒 timeout 不产生重叠或 60 秒累计漂移。
- 步骤 2.4.3：按 RF-09 的稳定 precedence 将每个 slot 分类为 terminal、valid_nonterminal、transport_unavailable、integrity_error、missed_due_to_downtime 或 post_terminal_claim_observation；先固定transport/missed/decode/binding/source-integrity/derived-integrity，只有integrity-clean decoded response才由既有WS claim把terminal或valid-nonterminal覆盖为post-terminal observation。transport subtype严格使用RF-11公开typed优先级：429、其他HTTP status、timeout、connect、其余SDK transport；DNS只在`reqwest::Error::is_connect()`成立时归`connect`，不解析错误字符串。
  - 验证：AC-05 至 AC-08；failure reason/result class唯一，typed transport分类逐级命中且v3拒绝`dns`枚举，WS-owned terminal/nonterminal Gamma response均成为post-terminal observation，而WS-owned integrity/transport结果保留原类且不被吞掉；任一 terminal 后取消剩余 slots。
- 步骤 2.4.4：预算耗尽按“至少一次 valid response”与“零 valid response”互斥聚合，只发布一次 `resolution_followup_exhausted` 和 summary counters。
  - 验证：只有accepted-WS fence/drain无terminal后才发布summary；valid+transport混合序列归类唯一，四counter和last-valid条件不变量成立；budget horizon=E+8而role-exit scheduled=null/applied为drain后的实际完成；不写retired、不复制旧snapshot、不产生新`unresolved_window_end_observation`或第7次probe。
- 步骤 2.4.5：在pending、terminal-once、bounded drain、metadata admission与scheduler全部接通后，删除WS `new_market` metadata fanout、E+5 sentinel及旧`fallback_close` metadata conversion，并将现有production writer/converter一次性切换到v3。
  - 验证：production只有一个metadata emission/conversion/writer路径；不dual-write、不把旧事件硬映射为新row kind，切换后的新segment只含v3且历史v2只读。

### 任务 2.5：阶段审查门

- 步骤 2.5.1：将阶段1的同一named timeline RED转为GREEN，并运行fake-clock、subscription loopback、WS/Gamma race、retry/exhausted、production v3 activation/non-mixing tests和自审。
- 步骤 2.5.2：独立阶段审查覆盖 correctness、唯一 owner、资源、过度设计、文档一致性；修复后复审。
- 步骤 2.5.3：只有已授权时 commit/push；否则停在 review-ready 且不进入下一阶段。

## 阶段 3：UTC、restart/recovery 与资源收敛

### 任务 3.1：UTC day/hour rollover

- 步骤 3.1.1：让 pending state、poll scheduler、completed-result queue、in-flight result routing与terminal bundle参加同一 Polymarket producer fence；按真实 receive time drain 旧日或defer新日row admission，并保持WS terminal先于其post attempts的跨分区prefix durability。WS-old/Gamma-new时允许completion作为bounded drain消息进入actor并buffer new-day row，旧日terminal finalize后才放行新日admission。
  - 历史执行验证（当前v2，2026-07-23已完成）：23:55 market fixture 的 E+3..E+8 进入次日且 identity 可关联；D-1ms/D/D+1ms、in-flight timeout、WS-old/Gamma-new、Gamma-old/WS-new与fence race均不产生UTC regression、deadlock或orphan post。在raw admission/finalize、actor buffer、metadata writer admission、batch flush、segment finalize与UTC seal各切点注入crash，任何finalized post都已有同一fixed identity且更早finalized的唯一WS terminal。未来source schema v3的validation/typed-row crash cuts由本计划v3修订任务替代。
- 步骤 3.1.2：确保 schema 切换不会把 v2/v3 混进同一 segment。
  - 验证：hour/day boundary writer recovery tests通过。

### 任务 3.2：outer source recovery 与进程重启

- 步骤 3.2.1：source outer runtime重建时保留或恢复 pending fixed identity与剩余 slots。
  - 验证：WS reconnect、Gamma availability cycle 不丢 pending，不回放已完成 terminal。
- 步骤 3.2.2（当前v2历史执行）：不新增 checkpoint/journal；进程启动先验证finalized v3 terminal bundle/source references及既有`polymarket_market` raw-to-metadata完整性，再只从无terminal冲突的finalized v3 verified identity best-effort恢复仍有未来slot的pending；v2不得作为恢复authority。
  - 历史执行验证：running parquet不被当作durable；finalized post必须解析到唯一已finalized WS terminal，孤儿post fail closed。`[T,recovery_time]`内matching finalized `market_resolved` raw与metadata覆盖完整matrix：unique WS/Gamma terminal合法；无terminal/summary失败；unique exhausted下raw receive严格晚于role-exit才是合法post-horizon corroboration，相等/更早失败；terminal+summary、重复terminal或重复summary失败。失败路径不恢复pending、不发Gamma、不写exhausted且不伪造transition补写；无finalized v3 identity不猜测；past slot只记录“无durable attempt evidence”的missed语义，不断言旧进程未发请求；E+8最后slot后不发起final probe；不承诺unclean crash exactly-once。该raw/post-horizon矩阵不进入未来source schema v3 authority。

### 任务 3.3：资源与持续性

- 步骤 3.3.1：运行大批量确定性 market lifecycle，统计 pending、in-flight、task、FD、compact dedupe、subscription 和额外内存 bytes。
  - 验证：AC-11；两个 pending 额外状态低于 1 MiB，数量在完成后回落，无单调增长。
- 步骤 3.3.2：确认最坏 request budget 为每 market 6 次、正常最大 72 次/小时，不阻塞 WS drain。
  - 验证：queue latency/timeout fixture无 starvation、无 per-market unbounded task。

### 任务 3.4：阶段审查门

- 步骤 3.4.1：targeted + recovery/resource tests、自审。
- 步骤 3.4.2：独立阶段审查并复审至通过。
- 步骤 3.4.3：只有已授权时 commit/push；否则停在 review-ready 且不进入下一阶段。

## 阶段 4：本地全量验证与真实 source gate

### 任务 4.1：本地全量验证

- 步骤 4.1.1：使用独立 target dir 运行 targeted tests。
  - 验证命令在实现时按实际 test target 冻结，至少覆盖 metadata layout、Polymarket contract、SDK runtime、writer、UTC/recovery 和资源。
- 步骤 4.1.2：运行：

```bash
CARGO_TARGET_DIR=target/data-collect-resolution-followup cargo test -p data-collect --locked -- --test-threads=1
CARGO_TARGET_DIR=target/data-collect-resolution-followup cargo check -p data-collect --tests --locked
CARGO_TARGET_DIR=target/data-collect-resolution-followup cargo clippy -p data-collect --locked --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

  - 验证：全部成功且无无关 diff。
- 步骤 4.1.3：执行官方 SDK provenance/static scan。
  - 验证：只有 pinned SDK client/actor；无 raw REST/manual WS/shadow implementation、第二 owner或新增依赖。

### 任务 4.2：本地香港VPN真实数据采集 validation gate

- 步骤 4.2.1：用户已于2026-07-22授权阶段4本地真实source测试；在阶段1至阶段3全部通过后，于本地构建可追溯binary，记录commit/dirty state、sha256、构建命令及本地隔离validation artifact路径；不上传到AWS数据采集、paper或live服务器。
  - 验证：执行前记录binary与本地独立validation目录的canonical path；不覆盖本地production binary/service，不删除或改写既有数据。
- 步骤 4.2.2：只读核验用户已配置的香港VPN线路，保存测试前不含凭据的香港出口/线路状态；随后通过该线路运行对Polymarket只读、仅写本地隔离artifact的live validation，覆盖多个完整五分钟market及其E+3 follow-up，在每个完整五分钟market window内至少再保存一次线路状态，并在结束后保存最终线路状态。
  - 验证：AC-12；至少取得真实WS terminal、E+3 Gamma terminal或完整retry/exhausted证据之一。Polymarket market-channel与Gamma endpoint、连接错误和重试可追溯；线路不可确认、测试期间失效或静默回退本地直连均不通过。
- 步骤 4.2.3：直接从本地只读artifact独立复算时间线、identity、terminal predicate、schema、subscription与资源。
  - 验证：报告明确区分platform timestamp、closedTime、fetch start、本地collector receive和lifecycle effective time，并明确本地香港VPN路径延迟不是AWS爱尔兰paper/live主机延迟。

### 任务 4.3：最终审查门

- 步骤 4.3.1：对照 demand RF/AC逐项生成证据索引，主代理自审。
- 步骤 4.3.2：使用规定模型/推理级别执行独立全面终审；任何可执行问题修复后重新运行受影响验证并复审。
- 步骤 4.3.3：只有用户已授权发布时创建最终 commit并 push；部署 production仍是独立授权门。

### 阶段4执行结果（2026-07-23）

1. 任务4.1全部通过：独立target中的完整lib为347/347，全部integration targets与
   doc-tests通过；`cargo check --tests`、Clippy `-D warnings`、fmt与diff check均退出0。
2. 任务4.2使用commit `a32226863428dcbd82b016538f36855240a41892` 的精确release
   binary在本地隔离artifact运行1802秒并正常finalize。31份线路样本全为
   `8.210.176.1 / HK / HKG`；22次collector connect全部只到
   `127.0.0.1:20810`，非loopback为0。
3. 只读复算识别5个完整五分钟market，其中4个在E+3前取得WS terminal；5条metadata
   terminal与5条raw `market_resolved`逐条唯一对应。manifest file facts、68字段v3
   schema、identity、transition/reference、digest与实际时间线均通过。
4. 真实source validation report已落盘：
   `data-collect/docs/polymarket-resolution-followup-ac12-validation.md`。
5. 审查中修正了smoke与正式run的`/proc/maps`证据归属，并把首版漏掉7个smoke dataset
   文件的inventory扩为artifact根下除自身外全部223个普通文件；223/223 hash通过。
6. 任务4.3的需求映射、文档/验收、范围/简洁性三路独立Ultra终审最终均为PASS，
   Critical 0、Important 0、Minor 0，且明确确认当前方案没有过度设计或过度校验。
7. 本轮没有production部署、AWS操作、system VPN/route/service mutation、系统包安装、
   数据删除或shadow Polymarket网络实现。阶段4已满足获授权的最终commit/push门；
   production部署仍保持独立授权门。

## 2026-07-24 v3 new-write修订映射（文档已批准，持续本地执行已授权）

前述阶段2/3的raw-first和raw-only recovery文本只记录2026-07-23已经完成的`polymarket_orderbook_v2` source行为与验证，不得用于实现v3。未来`polymarket_orderbook_v3` validate-first的唯一执行游标是`plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`阶段3；本计划不复制第二套步骤或审查门。

| 本需求增量 | 唯一执行计划位置 | 验证 |
|---|---|---|
| qualified / unmatched / target-associated contradiction / Gamma-first / role-exit / platform duplicate | 总计划步骤3.1.6至3.1.7 | source writer admission/sequence、terminal metadata、claim、transition和pending边界；gap/error/coverage不得成为payload/digest旁路 |
| 单一validated canonical object与写入顺序 | 总计划步骤3.2.2至3.2.3 | 一次validator、source admission失败时0 claim/0 transition、无rejected/quarantine/sidecar |
| 删除raw-first/raw-only production分支 | 总计划步骤3.4.7 | v3调用图与历史source v2/metadata v3保留边界 |
| typed-only合法/失败/restart/crash/mixed矩阵 | 总计划步骤3.5.1至3.5.5 | 包含0 source row + 唯一合法exhausted summary及一致attempts |

上述总计划步骤只有在其SDK字段门、`2-PM_CLOSED`、最终字段矩阵和持续执行授权全部满足后实施；这些入口现已满足，但只有真实代码、测试和验收完成后才能标记为已实现。

### 2026-07-25 metadata v4 claimant修订映射（文档已批准，持续本地执行已授权）

未来`polymarket_market_metadata_v4`与source v3 recovery的唯一执行游标仍是SDK070总计划阶段3。本计划保留2026-07-23已完成的metadata v3/source v2步骤与证据，不复制第二套schema、recovery、activation或审查步骤。

| 本需求增量 | 唯一执行计划位置 | 验证 |
|---|---|---|
| metadata v3不可变前缀、两个nullable claimant字段及逐row null matrix | 总计划步骤3.1.8、3.5.1 | Arrow/Parquet round-trip、WS-owned terminal pair必填、其他row同时为null、partial/空epoch/zero sequence失败 |
| source v3 claimant外键、restart与cross-pair fail-closed矩阵 | 总计划步骤3.5.2至3.5.5 | 唯一finalized source row匹配、identity/winner核对、platform duplicate唯一claimant、source v3 + metadata v3及source v2 + metadata v4失败 |
| current new-write原子activation | 总计划步骤3.5.6 | activation前仅source v2 + metadata v3，activation后仅source v3 + metadata v4；无dual-write、半切换或第二writer |

该映射只消费总计划的单一执行游标。后续持续授权已覆盖Stage3本地Rust/schema/test、构建、测试、审查、commit和push；它不等于实现已经完成，也不授权production cutover。

## 2026-07-27 Gamma `closedTime`真实格式后继修复

### 任务 5.1：同步精确格式authority

- 步骤 5.1.1：记录Attempt3顶层Gamma `closedTime="2026-07-27 05:25:52+00"`、当前RFC3339-only helper误判、`E+52s`事实与用户批准的方案A。
  - 验证：只允许精确`+00`后缀规范化为`+00:00`后交给同一Chrono RFC3339 parser；raw原样保存；不使用nested event兜底，不降级任何integrity error，不增加任意格式框架。

### 任务 5.2：最小实现与验证

- 步骤 5.2.1：只修改共享`parse_polymarket_gamma_closed_time_utc_ms` helper，在既有RFC3339失败后执行上述窄规范化；evaluator、adapter、fatal路径、SDK和schema保持不变。
  - 验证：精确Attempt3值解析为`1785129952000ms`；既有RFC3339继续通过，非法时间及`closedTime < E`继续按`closed_time_invalid` fail closed，raw仍保存原值。
- 步骤 5.2.2：只运行直接受影响的`polymarket_contract`精确回归、完整integration target、fmt与diff check；按实际编译影响补充必要check/Clippy。
  - 验证：不运行RTDS-only probe、100-cycle、资源矩阵、格式笛卡尔积或默认workspace完整测试；没有SDK、Cargo、schema、writer、evaluator或第二parser authority改动。
- 步骤 5.2.3：自审后执行三路独立审查，成立问题闭环后以独立Rust commit发布。
  - 验证：authority与Rust不混入同一commit；push后本地HEAD、upstream和远端ref一致。

### 任务 5.3：由RTDS专项执行完整collector复验

- 步骤 5.3.1：修复闭合后只以`plan/data-collect-rtds-business-silence-full-reconnect-plan.md`的新Attempt4作为8小时全量运行游标。
  - 验证：新clean/pushed HEAD、新release binary、新artifact和空dataset root；不续写Attempt3，不用短smoke替代8小时，不增加本计划第二套长跑验收。

## 三、需求—计划—验证映射

| 需求 | 计划任务 | 主要验证 |
|---|---|---|
| RF-01 | 0.2、2.1 | AC-01、AC-02 |
| RF-02 | 1.1、2.1 | AC-01、AC-02 |
| RF-03 | 2.1 | AC-01、AC-03 |
| RF-04 | 2.2 | AC-03、AC-11 |
| RF-05 | 2.3（source v2历史）；总计划3.1.6至3.2.3、3.4.7 | AC-04、AC-09、AC-10 |
| RF-06 | 2.4 | AC-01、AC-07 |
| RF-07 | 1.3、2.4、5.1--5.2 | AC-05、AC-06、AC-08 |
| RF-08 | 1.2、2.4 | AC-07、AC-09 |
| RF-09 | 1.2、3.1（metadata v3/source v2历史）；总计划3.1.8、3.5.1、3.5.5至3.5.6 | AC-02、AC-09、AC-10 |
| RF-10 | 2.3、3.1、3.2、3.3（source v2历史）；总计划3.1.6至3.1.8、3.5.1至3.5.6 | AC-04、AC-10、AC-11 |
| RF-11 | 全阶段 | AC-08、AC-13 |
| AC-12 | 4.2 | 真实 source validation report |
| AC-13 | 4.1、4.3 | 全量命令、scope/provenance终审 |

## 四、停止与决策门

执行中出现以下任一情况必须停止相关阶段并请用户决定：

1. 用户没有明确批准 demand 第八节参数或 authority 覆盖；
2. 官方 SDK 无法提供所需动态订阅、exact slug、terminal字段或 winner evidence；
3. 新需求与已批准 authority、真实 schema、SDK行为或 runtime约束仍不能同时成立；
4. v3 需要破坏现有 finalized data、consumer兼容性或部署 identity才能落地；
5. 真实live validation需要production service mutation、数据删除、钱包/交易权限、改变本地VPN/system service状态或其他未授权操作；
6. Critical/Important 审查问题未清零，或验证无法提供真实证据。
