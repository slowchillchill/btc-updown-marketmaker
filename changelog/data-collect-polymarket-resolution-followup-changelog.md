# Polymarket 五分钟市场生命周期与结算追踪修复 Changelog

> 事项编号：`DC-REPAIR-20260721-POLYMARKET-RESOLUTION-FOLLOWUP`
>
> 对应需求：`demand/data-collect-polymarket-resolution-followup-demand.md`
>
> 对应计划：`plan/data-collect-polymarket-resolution-followup-plan.md`
>
> 状态：`AUTHORIZED_PHASE_1_TO_4_EXECUTION_2026-07-22`。阶段0已完成自审和独立终审；用户已明确授权完整执行阶段1至阶段4、每阶段独立审查后的commit/push及最终本地香港VPN AC-12。production部署、服务操作和数据删除未获授权。

## 2026-07-21：数据检查与问题确认

用户要求检查 2026-07-20 的 `polymarket_market_metadata` 数据，重点判断 collector 是否没有等待足够时间采集 market resolved，并理清以市场活跃期为基准的采集时间线。

只读检查确认：

- 目标目录包含 24 个 Parquet、864 行；`prefetched`、`active`、`unresolved_window_end_observation` 各 288 行；
- next prefetch 中位数为 `T-294.93s`，current active 中位数为 `T+5.04s`，旧 end observation 中位数为 `E+5.00s`；
- active/end paired rows 复用相同 metadata fetch 与内容，E+5 observation不是结束后的新 Gamma请求；
- metadata 数据没有 `fallback_close`、`retired` 或经过校验的 resolved terminal；
- 当前 runtime 在 E+5 生成缓存 observation后 rollover，desired assets 只保留 current+next，旧 market随后会退出订阅；
- 当前 fallback predicate 的 `active=false` 门只有测试调用，没有造成该日数据现象，但会阻断未来 Gamma fallback。

## 2026-07-21：官方终态与 SDK 能力复核

逐 slug 查询 2026-07-20 全天 288 个 target market，并用官方 SDK能力交叉核对：

- 288/288 Gamma identity匹配；
- 288/288 `active=true, closed=true, acceptingOrders=false, automaticallyResolved=true, umaResolutionStatus=resolved`；
- 288/288 outcome prices为 one-hot winner；
- `closedTime-E` 最小/中位数/P95/最大为 16/21/57/150 秒；
- 官方 CLOB endpoint 只读状态同样为 active true、closed true、accepting-orders false，且每个 market 唯一 winner；pinned SDK method/types 静态确认可以表达这些字段；
- SDK 已有 market channel `market_resolved`、dynamic subscription 和 Gamma `market_by_slug`，当前没有 SDK 能力缺口。

结论边界：closedTime不是 Gamma首次可见时间或 WS到达时间，因此只支持把 E+3作为合理首次 fallback，不构成历史时点可见性证明。

## 2026-07-21：用户提出的新时间线

用户提出：

- next market 约在 `T-294.93s` 写 prefetched；
- current market 在 T 写 active，删除 5 秒延迟；
- 删除 E+5 observation；
- E+3m首次检查，没有 resolved则每分钟重试，最多重试5次；
- 预算耗尽后写 unresolved observation。

分析后形成以下草案修正，等待用户审查：

1. `T-294.93s` 保留为历史观测值，不固化为 timer/SLA；取消5秒延迟后实际值可能接近 T-300。既有 T-30保持“最迟覆盖”合同。
2. T表示 lifecycle调度有效时刻；实际处理和网络返回必须记录真实时间，不能伪造恰好T收到。
3. E+3为 initial attempt，E+4..E+8为5次 retry，总计最多6次；沿用20秒单次timeout时，E+8 slot约在E+8m20s前分类，terminal/summary在随后bounded drain后按真实时刻发布，E+8m20s不是整体hard deadline。
4. E 后旧 market 的 lifecycle 在没有 terminal 证据时保持 active，独立 runtime role 进入 resolution_pending 并继续保留 WS assets；真实 WS terminal 先到即结束，Gamma 只作为 fallback。
5. budget exhausted不等于 market客观 unresolved；草案改用 `resolution_followup_exhausted`，区分 valid nonterminal 与 transport unavailable。
6. metadata v2无法审计 structured terminal predicate和attempt timeline，草案要求升级v3且不回写历史v2。
7. public lifecycle 保持 candidate/prefetched/active/retired 四态；E 后使用独立 `market_runtime_role=resolution_pending`，只有 terminal 证据才切换 retired。
8. process restart 不新增 checkpoint/journal，只从 finalized artifact best-effort 恢复未来 slot，不执行 post-deadline 第 7 次 probe。

## 2026-07-21：合同冲突复核

### active=false

复核结论为“冲突真实，但之前需要收窄表述”：

- 主 demand只要求保存 active字段，没有明确要求值为false；
- 明确要求 active=false的是主 plan、`sources/polymarket.rs` predicate和测试；
- 真实终态288/288 active=true，因此旧门会拒绝全部目标市场；
- 草案要求 active原样保存、类型合法，但不参与 terminal truth-value判定。

### expired slug

当前 source-availability authority禁止Gamma恢复后继续追逐过期slug。新方案需要对已经在活跃期完成固定identity验证的pending-resolution market做exact-slug查询；process restart则额外要求finalized durable identity。

草案不删除原规则，只增加用途和时间都受限的例外：该查询只能用于 resolution follow-up，不能重新参与 current/next discovery；最后 slot 在 E+8m 发起，只允许该次既有 20 秒 timeout 完成，不能增加第 7 次 probe。

### T-30 与 early prefetch

T-30继续作为健康情况下 next metadata/subscription的最迟覆盖边界；更早metadata discovery不与它互相替代。实测中位数不得写成 hard threshold。

## 2026-07-21：三件套草案创建

已创建：

- `demand/data-collect-polymarket-resolution-followup-demand.md`；
- `plan/data-collect-polymarket-resolution-followup-plan.md`；
- `changelog/data-collect-polymarket-resolution-followup-changelog.md`。

草案统一使用事项编号 `DC-REPAIR-20260721-POLYMARKET-RESOLUTION-FOLLOWUP`，plan与changelog顶部显式引用对应 demand。当前状态均为 `DRAFT_FOR_USER_REVIEW`。

本次草案明确未执行：

- 既有 authority同步；
- Rust/schema/test修改；
- 本地构建或测试；
- commit/push；
- release、上传、远端 probe、部署或服务操作；
- 历史 Parquet修改或删除。

## 2026-07-21：草案首轮独立审查与修订

三路只读审查分别检查文档/schema authority、调度/runtime架构与terminal predicate。首轮发现的关键问题均属文档合同问题，没有修改production代码：

- process restart恢复缺少durable checkpoint authority，原草案却承诺恢复pending并不重放terminal；
- 新Gamma scheduler尚未纳入UTC producer fence，存在午夜receive/fence竞态；
- `resolution_pending` 被误写成第五种lifecycle，违反lifecycle/runtime-role命名空间隔离；
- E+3 initial + 5 retries冻结为6次，但restart final probe会形成第7次；
- scheduled T与真实applied time没有分列；
- exhausted聚合在valid+transport混合序列下不互斥，v3字段/nullability/counters也未闭合；
- CLOB runtime交叉验证在需求中可选、验收中却强制；
- WS nullable slug/outcomes、Gamma exact asset set、freshness、resolved枚举、clock skew与active null策略未冻结；
- 保留旧WS assets、每slot typed attempt row及transport authority例外未进入用户审查门；
- pending若沿用整窗fingerprint集合会放大内存上界。

草案已按保守、最小实现方向修订：

1. 不新增checkpoint/journal/sidecar；process restart只从finalized metadata best-effort恢复未来slot，不承诺unclean crash exactly-once，不执行第7次probe。
2. poll scheduler、completed results和in-flight routing必须参加同一Polymarket UTC fence，并增加D-1ms/D/D+1ms竞态验收。
3. public lifecycle继续四态；E后使用独立`market_runtime_role=resolution_pending`，terminal后才retired。
4. T分列scheduled/applied/received；没有verified next时不伪造active。
5. exhausted按“至少一次valid response”与“零valid response”互斥聚合，补齐v3字段、nullable语义、counters和digest字段名。
6. 本轮production只使用Gamma fallback；CLOB只保留离线事实复核，不加入runtime请求预算。
7. Gamma predicate冻结exact二元asset set、attempt绑定、20秒freshness、5秒future skew、精确resolved状态和active-null非门语义。
8. pending只路由terminal事件并使用compact terminal dedupe，不扩展整窗fingerprint集合。
9. 用户审查门新增旧asset保留、typed attempt authority例外、restart边界和5秒skew决定。

## 2026-07-21：草案 schema/authority 复审与修订

第二轮只读复审继续发现三项会影响实现唯一性的合同歧义：

- `timestamp_received` 同时被用于网络response receive与本地transition emission，却没有独立source-evidence/admission字段；
- v3没有冻结row kind、逐类required/null matrix、transport status、closedTime raw/check、active unavailable reason及完整failure precedence；
- attempt row属于既有metadata Parquet，但“不得进入daily manifest”的旧措辞会错误移除既有file row count/hash/schema coverage。

草案已修订为：

1. `timestamp_received`按row事实冻结为真实观察/生成时刻，另设source-evidence receive/digest与collector admission time/sequence；T promotion显式引用原prefetch证据。
2. v3冻结五种row kind、逐类required/null matrix、transport与closedTime字段、active null reason，以及20项可达且有顺序的source terminal failure枚举/result class映射；SDK decode、attempt binding与派生winner invariant独立归结构化integrity class。
3. 每个slot仍最多一条attempt row；Gamma terminal由同一attempt row表达。所有row进入既有Parquet并由每日manifest既有file facts覆盖，但不增加manifest字段、artifact、sidecar或episode tape。
4. plan的restart成功标准已收窄为同进程recovery不丢、unclean restart仅finalized-artifact best-effort且不伪造；不承诺exactly-once。
5. demand/plan/changelog的用户审查门统一为10项，“文档批准不自动授权代码/git/远端”保留为非编号授权边界。
6. metadata snapshot origin按真实代码冻结为`GammaClient::markets` discovery；v3不再新写本地slug-only candidate，旧v2仅只读兼容。
7. state transition分开记录lifecycle与runtime role：E只切role；T后identity晚到使用candidate-to-active/none-to-current并保留scheduled T，不伪称prefetched。
8. 20秒timeout归transport unavailable，closedTime解析/范围矛盾归integrity；Gamma不存在的独立winner字段不进入source predicate fixture。
9. 任何pending teardown都先fence/drain已被WS pump接受的旧asset事件；E+8 nonterminal/timeout与queued WS竞态时，合格WS terminal优先且禁止写exhausted。
10. T后identity只允许在自身`[T,E)`活跃窗内恢复为active；E及以后按expired拒绝，不扩大pending expired-slug例外。
11. exhausted的runtime-role scheduled为null、applied为最后slot与WS drain后的真实退出时刻；另设E+8 budget horizon，并冻结counter总和与last-valid条件不变量。
12. decoded Gamma response始终保存实际closedTime raw；first-failure precedence不删除raw证据。派生winner后置不变量使用独立integrity class与predicate null，不伪造Gamma字段。
13. terminal来源按首个完成校验并原子claim terminal-once决定：Gamma已claim时queued WS仅作raw late corroboration；exhausted尚无terminal claim，必须被drain中的合格WS阻止。无需新增candidate/superseded schema状态。
14. late identity只以真实lifecycle applied是否落在`[T,E)`判窗；schema拆分首次固定identity验证与本row evidence验证，避免后续poll receive错误约束到旧identity时间。
15. E transition、poll、WS terminal、summary逐类冻结budget horizon与followup status；T transition和snapshot两者均为null，不再由实现猜测“follow-up row”。
16. terminal validation/conversion/claim统一由现有单一Polymarket processing actor串行执行；任何claim后保持pending/assets到drain完成，再应用transition并写terminal row。
17. WS先claim时，后到actor的Gamma completion仍完成integrity检查与predicate计算，并保留该slot唯一attempt evidence但不产生第二terminal ownership、evidence source或transition；Gamma先claim时queued WS仅raw corroboration，不增加losing-candidate schema。该修订避免WS先claim吞掉Gamma SDK decode、binding、identity/shape或closedTime integrity error。
18. exhausted fence在释放barrier前由同一actor原子完成WS terminal或pending关闭+summary enqueue；post-fence事件不能再产生第二terminal metadata。

## 2026-07-21：草案 terminal、持久化与 raw authority 复审修订

后续三路只读复审发现并修订以下会影响实现唯一性的事项：

1. `post_terminal_claim_observation`不再与基础`valid_nonterminal`分类并列猜测：transport/missed/decode/binding/integrity先固定；只有integrity-clean decoded response才由既有WS claim覆盖为post-terminal observation。
2. 每个SDK-decoded `market_resolved`先进入既有`polymarket_market` raw artifact；无效WS fail closed且不写v3 terminal metadata，Gamma-first losing WS也只作raw corroboration。五种v3 row kind不扩张。
3. AC-06与plan改为逐项覆盖20个source failure枚举，并用组合fixture验证first-failure precedence。
4. WS-first的late Gamma attempt先在actor内buffer；drain后先应用transition并admit唯一WS terminal，再admit post attempts。UTC fence、ordered batch、writer prefix durability与restart合同共同保证“finalized post必有同批或更早finalized terminal”；孤儿post按artifact integrity error fail closed。
5. v3新增source evidence event type/channel与可还原`source_evidence_payload_json`；digest只校验实际存储payload，不再单独冒充raw evidence。Gamma只序列化pinned SDK完整decoded对象；SDK不能表达时触发SDK能力决策门，不走raw REST旁路。
6. timeline中的prefetched snapshot只记录fetch/receive/validation/payload/digest/admission，不再误写不存在的state-transition applied字段。
7. source endpoint与source response evidence分开：已发起但没有decoded response的transport/decode/binding attempt仍保留endpoint/fetch，只有missed/summary没有endpoint；event type/channel/payload/digest只在存在decoded或引用evidence时填写。
8. WS-old/Gamma-new跨日时，new-day completion可作为bounded drain消息进入actor但其row admission继续defer；旧日WS terminal先finalize后才写新日post，避免producer fence与terminal drain死锁。
9. recoverable payload的边界收窄为全部source-native response inputs；完整predicate由payload、本rowtiming/attempt fields与fixed identity共同重建，禁止把本地上下文伪装进source payload。
10. decoded stale/cache attempt-binding contradiction必须保存真实receive、payload、digest、response fields与validation时刻，predicate为null；不再错误套用response-unavailable reason。
11. `post_terminal_claim_observation`的winning asset/outcome与resolution evidence source/digest统一为null，source payload/digest与predicate保留，避免corroboration升级成第二terminal evidence。
12. restart除metadata外还检查既有finalized`polymarket_market/market_resolved` raw；matching raw可对应唯一WS/Gamma terminal，或在唯一exhausted summary存在时以严格晚于role-exit的receive作为合法post-horizon corroboration。无terminal/summary、receive不晚于exhausted退出、重复终态或terminal+summary并存均保守fail closed，不恢复pending/fallback/exhausted，也不伪造transition补写。crash fixture扩展到raw-finalize与metadata admission/finalize之间，并supersede此前“只查finalized metadata”的草案 wording。
13. 历史v2仅作read-only consumer/audit兼容，缺少v3 fixed-validation与payload/reference合同，不能合成v3、恢复pending或创建missed slot；v3启用后只能由fresh SDK discovery建立新authority。

## 2026-07-21：草案最终只读复审

按最新磁盘内容完成三路独立只读复审：

- schema/authority：Critical 0，Important 0；
- schedule/runtime/restart：Critical 0，Important 0；
- terminal predicate/terminal-once：Critical 0，Important 0。

本地结构校验同时确认：三份文件事项编号与引用一致；demand包含11个RF和13个AC；demand与changelog的用户审查门均为10项；无尾随空白或旧`受控discard`语义。此次复审和修订仍只涉及文档，没有修改Rust、测试、既有authority、数据或服务，也没有执行commit、push或远端操作。

## 待用户审查的决定

1. `E+3m` initial + 5 retries，总6次、最后 slot E+8m；
2. 使用独立 `market_runtime_role=resolution_pending` 与 `resolution_followup_exhausted`，不再新写 `unresolved_window_end_observation`；
3. active只记录、不作为 terminal gate；
4. expired-slug规则增加仅限活跃窗内已固定并验证identity的pending-resolution有界例外；process restart仍额外要求finalized durable identity；
5. metadata schema升级v3，采用RF-09的五种row kind、required/null matrix、稳定失败枚举、可还原source payload与digest/reference合同，且不回写历史v2；
6. 旧market assets保留到terminal或budget完成，并只为pending路由terminal事件；唯一actor claim后仍保持pending/assets直到drain结束，Gamma-first时queued WS仅作late corroboration，exhausted则必须被drain中的queued terminal阻止；
7. 每个slot最多一条v3 typed attempt row，作为旧transport-evidence authority的有界例外；row仍进入既有Parquet与manifest file facts但不扩张manifest schema/artifact；WS已claim时后到actor的Gamma completion仍完成integrity/predicate处理并保留该slot唯一attempt evidence，但唯一WS terminal必须与`post_terminal_claim_observation`同批或更早finalized，绝不允许post单独先finalized，且不得产生第二terminal ownership、evidence source或transition；
8. process restart只从finalized v3 metadata及既有`polymarket_market` raw artifact做best-effort未来slot恢复，历史v2不作为恢复authority；matching finalized WS raw必须对应唯一owned terminal，或严格晚于唯一exhausted role-exit而只作post-horizon corroboration，其他组合fail closed且不自动补写；无checkpoint和第7次probe；
9. closedTime future-skew allowance 固定 5 秒；
10. 批准后是否授权同步现有 demand/plan/changelog authority。

授权边界：文档批准不自动授权代码、git 发布或远端操作。

## 2026-07-22：用户逐项决定与阶段 0 authority 同步授权

用户对审查门作出明确决定：

1. 接受`E+3m` initial + 5 retries，总计6次、最后slot为`E+8m`。
2. 接受独立`market_runtime_role=resolution_pending`与`resolution_followup_exhausted`，不再新写`unresolved_window_end_observation`。
3. 接受Gamma`active`只记录、不参与terminal truth-value判定。
4. 接受仅限活跃窗内已固定并验证identity的pending-resolution expired-slug有界例外；process restart仍额外要求finalized durable identity。
5. 接受metadata schema v3、五种row kind、required/null matrix、稳定失败枚举、可还原source payload与digest/reference合同，且不回写历史v2。
6. 接受旧market assets保留、pending terminal-only routing、terminal claim后bounded drain及Gamma-first/WS-first/exhausted竞态合同。
7. 接受每slot最多一条v3 typed attempt row的窄transport-evidence authority例外、既有Parquet/manifest file-facts覆盖及WS terminal对post row的prefix durability合同。
8. 接受仅从finalized v3 metadata与既有`polymarket_market` raw artifact做best-effort未来slot恢复的restart矩阵；无checkpoint、历史v2恢复authority或第7次probe。
9. 拒绝并要求删除固定5秒`closedTime` future-skew allowance。现行predicate只要求closedTime存在、可解析且不早于E；closedTime与receive分别保存但不做future-skew上界比较。对应`closed_time_after_receive_plus_5s`枚举与fixture删除，source failure枚举由20项收敛为19项。此前草案中的5秒allowance与20项记录作为当时审查历史保留，由本节supersede。
10. 批准同步现有主authority。

批准范围只覆盖需求定稿和阶段0文档authority同步，不授权Rust/schema/test实现、构建或测试执行、commit、push、release、远端probe、部署、服务操作或数据清理。复核发现follow-up plan“预计修改边界”漏列其任务0.2.2已经明确要求的`demand/rebuild-core-demand.md`窄同步；已补齐该路径，并继续明确rebuild-core整体草案未获批准，不借本事项扩大其余范围。

## 2026-07-22：阶段 0 authority 同步完成与独立终审

- 主线程自审确认三件套事项编号与互引一致，demand包含11个RF、13个AC和10项用户决定；terminal predicate failure枚举为19项，现行demand/plan不存在固定5秒或替代future-skew上界。`git diff --check`通过，变更范围只有Markdown文档。
- 第一名`gpt-5.6-sol`、Ultra reasoning独立终审为PASS，Critical 0、Important 0、Minor 0；确认10项决定、生命周期/role分轴、terminal-once/drain、prefix durability、restart fail-closed及授权边界均一致。
- 第二名`gpt-5.6-sol`、Ultra reasoning合同扫描首轮指出source-availability顶部指针与changelog遗漏desired asset union及drain后移除说明。已补齐current + prefetched next + resolution_pending旧market assets并集、claim后保留至bounded drain以及terminal/budget完成后移除的记录；复审为PASS，Critical 0、Important 0、Minor 0。
- 阶段0现停在review-ready。未修改Rust/schema/test，未运行Cargo、live probe或远端命令，也未执行commit、push、release、部署、服务或数据操作；未获新的用户授权前不进入阶段1。

## 2026-07-22：真实数据采集验收改为本地香港VPN

- 用户指定本事项真实数据采集测试验收放在本地环境，并使用已配置的香港VPN线路。该决定supersede demand AC-12与plan任务4.2此前要求上传到AWS爱尔兰数据采集服务器执行的当前措辞；旧草案记录继续作为历史保留。
- AC-12现要求本地原生Ubuntu环境，并在测试前、运行期间每个完整五分钟market window至少一次、测试结束后保存不含凭据的香港VPN/出口线路证据，同时保留Polymarket market-channel与Gamma endpoint及连接/重试证据。线路不可确认、失效或静默回退本地直连时不得通过，也不得把网络结果误报为SDK或collector缺陷。
- 本地collector receive/latency只表示本地香港VPN路径视角，不得冒充AWS爱尔兰paper/live主机延迟。production训练collector与paper/live部署拓扑不因本次测试地点变化而改变。
- 本次只同步需求、计划和changelog；没有运行VPN、Cargo或真实source测试，没有修改Rust/schema/test，也没有执行commit、push、远端、部署、服务或数据操作。该时点的后续实际AC-12仍需单独测试执行授权；下一节记录用户随后已经给出的完整执行授权。
- 两路`gpt-5.6-sol`、Ultra reasoning独立审查最终均为PASS，Critical 0、Important 0、Minor 0。首轮范围审查指出测试前后两点线路证据不足以排除中途回退，以及“批准的本地路径”会形成未定义审批门；已修正为测试前、每个完整五分钟market window至少一次、测试结束后的无凭据线路证据，并改为执行前记录本地独立validation目录canonical path，复审无剩余问题。

## 2026-07-22：用户授权完整执行阶段1至阶段4

- 用户明确确认“批准执行”包含完整执行阶段1至阶段4：Rust/schema/test实现、本地构建与全量测试、每阶段独立审查后的commit/push，以及最终通过本地香港VPN执行AC-12。
- 执行必须严格按阶段顺序推进；阶段0文档先精确commit/push，随后每个阶段只有在自审、规定的`gpt-5.6-sol`/Ultra独立审查和验证通过并完成commit/push后才能进入下一阶段。
- production部署、服务操作和数据删除未获授权。阶段4只允许本地香港VPN真实source验证与本地隔离artifact，不上传或部署到AWS数据采集、paper或live服务器。

## 2026-07-22：阶段 1 预检、反审查纠正与用户批准继续

阶段 0 已按授权完成精确提交与推送：commit
`948941ea552a43eb6b2754188f35864c07e0b52b`，本地 HEAD、upstream 与远端
`origin/fix-rtds-silents` 已核对一致。进入阶段 1 预检时尚未修改任何 Rust、schema 或测试文件。

首轮两路只读预检正确确认以下代码事实：当前runtime使用`std::time::Instant/SystemTime`且
Tokio未启用`test-util`；production metadata仍固定v2并生成WS `new_market`、E+5 sentinel与
`fallback_close`；这些旧事件不能在缺少pending、terminal-once、bounded drain和admission时
直接改名映射为RF-09的五种v3 row。官方Polymarket Rust SDK能力充足，不存在SDK缺口。

随后按用户要求对预检结论本身进行三路独立反审查。反审查纠正了首轮把实现难点升级为
阶段硬冲突的过度判断：plan任务1.1明确要求修复前精确观察RED，阶段1审查门只要求
schema/predicate与默认验证通过；同一fake-clock目标在阶段2审查门才转为GREEN。因此阶段1
不要求pending/scheduler目标行为提前通过，也无需移动或合并阶段。

用户同意并批准按以下最小执行解释继续：

1. 阶段1保留compile-valid、可单独运行的named timeline RED及最小测试seam；默认构建与既有测试不得被破坏。阶段2使用同一测试转GREEN，不建立全仓通用clock框架。
2. 阶段1只在同一layout与现有writer中建立v3 row/schema/enums/validator、测试转换/roundtrip/version guard及19项纯predicate能力；production继续唯一写v2。`parallel v3`只表示类型/schema短期共存，不是双写或第二production path。
3. 不新增独立v3 writer factory、第二source kind、Cargo产品feature、production constructor空壳或提前的startup recovery。现有writer按schema version严格分派；完整recovery/crash matrix留在阶段3。
4. 阶段2接通pending、subscription union、terminal actor/drain、admission与scheduler后，删除旧metadata fanout和E+5 sentinel，再一次性切换production到v3；不得把旧事件硬塞进新row kind。
5. RF-09 required/null matrix、19项failure fixture、payload/digest/reference及v2/v3 non-mixing属于用户已批准的持久化合同，不是可删除的过度校验；实现使用基准fixture与table-driven mutation，不做字段组合的笛卡尔积或重复判定框架。

本节supersede进入反审查前尚未提交的“阶段1必须fake-clock GREEN、必须移动任务1.1及新增显式v3 writer factory”的临时判断。demand的RF/AC、阶段1至4授权及production/服务/删除禁止边界均未改变。

## 2026-07-22：阶段 1 实现、自审与验证

阶段1按上述最小解释完成待独立审查的实现：

- `live_runtime.rs`只增加Polymarket market-local双时钟采样与paused-time loopback fixture，未建立全仓clock abstraction。named测试`polymarket_resolution_timeline_is_absolute_red`默认忽略，显式运行会完整启动真实Gamma HTTP与官方SDK CLOB loopback collector，再只在最终时间线目标断言处RED；既有边界测试补齐E-1/E/E+1。
- `polymarket_market_metadata_layout.rs`在保留v2的同时定义68字段v3 row、五种row kind、稳定枚举和单row required/null/因果时序validator；跨row唯一reference、首次identity snapshot一致性及terminal/summary互斥没有伪装成单row校验，留给阶段2 actor/writer admission与阶段3 recovery。
- `sources/polymarket.rs`基于已固定官方Rust SDK `Market`类型增加纯Gamma terminal evaluator和19项稳定source failure precedence；`active`只记录，UMA status执行trim后ASCII大小写不敏感比较，不存在closedTime future-skew上界或raw REST/manual decode旁路。
- 既有`create_for_typed_source`按metadata schema version选择v2/v3，同一writer增加v3 batch转换与严格递增admission检查；没有第二factory、第二source kind、dual-write、manifest扩张或production v3 emission。现有production runtime继续构造并写v2。
- 合同测试覆盖五种row、68字段Arrow类型、required/null及因果时序拒绝、稳定枚举、真实payload可解析与digest重算、五row Parquet roundtrip、v2/v3 non-mixing、19项predicate与组合precedence、active true/false/null、无future-skew gate、SDK序列化重建及binding/decode失败。

主线程自审纠正了实现过程中的范围或语义偏差：恢复被误删的历史v2 fallback；取消把所有snapshot都强制等同首次identity validation的过度校验；阻止integrity failure被错误归入post-terminal；恢复RF-07要求的trim + ASCII大小写不敏感UMA判断；删除阶段1尚无production调用方的v3 logical-bytes辅助方法；补齐v2 row标签/version guard、`active=false`正例、source payload与typed fixture一致性，并避免测试中同一identity同时出现terminal和summary。Clippy随后只指出新loopback测试中的一个可折叠match分支，已做等价局部改写。

已执行验证：

1. `cargo test -p data-collect --test polymarket_metadata_layout_contract --test polymarket_contract --test parquet_writer_contract --locked`：18 + 7 + 11项全部通过。
2. `cargo test -p data-collect --lib live_runtime::tests::gamma_late_current_or_next_response_cannot_be_installed_after_window_advance --locked -- --exact`：1项通过。
3. `cargo test -p data-collect --lib live_runtime::tests::polymarket_resolution_timeline_is_absolute_red --locked -- --exact`：退出0，指定RED为1项ignored。
4. 同一named测试增加`--ignored --nocapture`显式运行：按计划退出101，collector与prefetch准备成功、无gap；实际仍只有旧E+5 sentinel，而目标要求T边界promotion/pending、E+3至E+8六个Gamma slot且无sentinel，失败点仅为最终目标时间线差异。
5. `cargo clippy -p data-collect --all-targets --locked -- -D warnings`：修正上述测试分支后退出0。
6. `cargo test -p data-collect --locked`：完整package默认测试退出0；named阶段1 RED保持默认ignored，所有既有及新增非RED测试通过。
7. `cargo fmt --all -- --check`与`git diff --check`：退出0；`Cargo.lock`未变化。

固定官方SDK类型和当前writer能力足以表达本阶段合同，未发现RF-11 SDK能力缺口。此时尚未创建阶段1 commit/push，也未进入阶段2；必须先完成规定的独立`gpt-5.6-sol`/Ultra审查及问题闭环。

## 2026-07-22：阶段 1 首轮独立审查 FAIL 与问题闭环

规定的`gpt-5.6-sol`、Ultra独立审查首轮结论为FAIL：Critical 0、Important 8、Minor 0。审查同时明确判断阶段1整体没有过度设计：大部分增量来自已批准的68字段显式合同、稳定枚举和测试矩阵，未出现第二writer/source或shadow SDK；反而若在writer validator内复制完整terminal predicate，才会形成第二authority和过度校验，因此没有采用该方案。

八项Important均已按最小边界修复：

1. context switch不再复用fence前采样的UTC；switch在processor fence/drain完成后通过market-local timeline provider重新采样applied time。首次install完成后才锚定next-discovery monotonic deadline，并恢复原production各判断点独立采样语义。新增delayed-fence跨E测试，证明fence前不读取applied time且跨E后返回expired、不安装旧discovery。
2. `polymarket_market_metadata_v3` Parquet file metadata改为`sort_order=collector_admission_order`；v2继续为`receive_order`。Binance既有admission声明和其他source既有receive声明不变，versioned constructor测试逐项核对。
3. `resolution_terminal_ws`现在要求pinned SDK必填的platform timestamp raw与UTC均存在、为正且数值一致；receive time仍独立保存，未增加future-skew gate。
4. exhausted summary新增`runtime_role_applied_at_utc >= E+8 horizon`、`last_valid <= applied <= timestamp_received <= admission`单row因果门；六个slot的最大valid receive仍留给阶段2 actor和阶段3跨row recovery，不在writer重复实现。
5. Gamma fixture改为先由官方SDK `Market`解码、再确定性完整序列化的有效payload，使用合法B256与RFC3339 closedTime；WS fixture补齐SDK必填`id/market/timestamp`并真实解码为`WsMessage::MarketResolved`。测试由唯一Gamma evaluator从保存payload、attempt timing和fixed identity重建typed predicate/winner，并覆盖binding/decode subtype；未在layout validator复制19项predicate。
6. closedTime layout与唯一Gamma evaluator共用trimmed RFC3339解析helper；pass、parse-error、before-E及raw/UTC一致性均由真实解析结果约束，仍不与receive比较future skew。
7. named paused-time RED新增旧asset unsubscribe trace，冻结“terminal或budget完成前不得退订、完成后最终退订”的合同；没有把无合同依据的精确teardown毫秒设为门。另增late identity table，覆盖receive在T前而verify在T后、applied=E-1、applied=E及E后，因果顺序成立时只有真实applied落在`[T,E)`才可active。
8. 五row fixture统一market A的首次snapshot evidence、fixed verification和local transition reference；reference按identity、source receive和digest唯一解析到一个source-bearing row。summary使用独立market B的完整T2/E2/horizon/role-exit时间线，不再只替换slug/ID。

修复后重新执行：

1. `cargo test -p data-collect --test polymarket_metadata_layout_contract --test polymarket_contract --test parquet_writer_contract --locked`：18 + 7 + 12项全部通过。
2. delayed-fence applied-time、late-identity table与E-1/E/E+1 Gamma installation三个指定lib测试分别通过。
3. `cargo test -p data-collect --lib --locked`：257项通过、0失败、named阶段1 RED 1项ignored。
4. `cargo clippy -p data-collect --all-targets --locked -- -D warnings`：退出0。
5. `cargo test -p data-collect --locked`：完整package与doc-tests退出0。
6. named RED默认仍ignored；显式`--ignored --nocapture`按计划退出101。真实Gamma HTTP和官方SDK CLOB loopback均启动成功，collector无gap、early prefetch与旧assets保留/最终退订合同已经满足；剩余差异仍是阶段2尚未实现的T promotion/E pending、E+3至E+8六个absolute Gamma slots及删除旧sentinel。
7. `cargo fmt --all -- --check`与`git diff --check`退出0，`Cargo.lock`未变化。

当前仍未创建阶段1 commit/push，也未进入阶段2；以上修复必须由同一独立审查代理复审至Critical/Important均为0后才能提交。

同一复审继续按当前磁盘检查时又发现两项Important证据缺口，均已局部闭合：

- Parquet writer虽然已按schema version声明v2 `receive_order`/v3 `collector_admission_order`，但daily manifest与embedded recovery facts原先仍只按source kind固定为`receive_order`。现抽取同一`typed_sort_order(schema_version, source_kind)`纯判定供WriterProperties和segment facts共用，finalize只解析一次schema version；新增测试分别创建v2/v3文件并核对Parquet property、embedded `DailyFileRecoveryMetadata.schema_version`和`facts.sort_order`三者一致。
- 纯evaluator已有attempt binding与SDK decode返回值测试，但v3正向subtype表缺少这两类合法持久化`integrity_error` row。现新增binding row，保留decoded endpoint/fetch/receive/validation/payload/digest/response fields且predicate/reason为null；新增decode row，只保留endpoint/fetch，source response与平台字段为null并使用对应unavailable reason。没有在writer或layout复制19项predicate。

两项新增定向测试均已通过；完整package、Clippy和格式检查将在最终复审前基于最新工作区再次运行。

## 2026-07-22：阶段 1 第二轮独立审查、RED 证据纠正与最终验证

第二轮独立审查没有接受“命令退出101”本身作为RED证据，而是继续检查panic位置、wire trace、
取样时刻和断言是否可能false-green。审查正确推翻了上一节“旧assets保留/最终退订合同已经满足”
这一过早结论：旧fixture最初只在stop之后读取累计unsubscribe，随后又只在E+5取一次快照，既会把
stop cleanup误当正常teardown，也会漏掉E+3至E+7之间的错误退订；一版修复还把E+8后最多
`80 * 250ms`虚拟等待变成了没有需求依据的teardown上界，属于过度校验。

按最小合同完成以下纠正：

1. paused-time collector仍使用现有官方SDK actor；只有测试CLOB loopback移到独立墙钟runtime线程，
   避免paused current-thread runtime令SDK wire flush与测试取样互相饥饿。零时长Tokio timer只用于
   让I/O driver获得轮询机会，没有引入production clock framework或第二transport实现。
2. CLOB phase clock在每次虚拟advance前写入目标时刻，避免advance内发生的E+8合法teardown被
   错标为上一slot。E+8后先完成non-stop processor fence，再等待server已观察到101与102；等待只用
   5秒墙钟harness防止测试挂死，不推进fake UTC，也不形成业务deadline。
3. teardown readiness不再作为setup独立assert，而是进入最终observed/expected；缺少正常teardown的
   实现也会到达最终RED断言。pre-stop完整wire trace同时断言101/102都已移除，并断言任何旧asset
   unsubscribe不得早于E+8；允许E+8之后任意由最后attempt完成和accepted-event drain决定的时刻，
   不要求`== E+8`、`<= E+8+20s`或其他固定上界。
4. trace settle改为async零时长timer轮询，不再用`std::thread::sleep`阻塞collector所在current-thread
   runtime。E+5快照只用于明确复现当前legacy early teardown，全量`< E+8`判定负责覆盖整个保留窗。

显式运行named RED现在稳定只在最终`observed == expected`断言失败：legacy observed为
`collector_ok=true`、`gaps=0`、early prefetch存在、101/102在E+8前已退订且stop前均已正常观察、
没有promotion/pending/attempt rows或Gamma follow-up请求，并仍有E+5及后续legacy sentinel；目标则为
T promotion、E pending、E+3至E+8六个absolute attempts、E+8前无退订、stop前最终退订且无sentinel。
因此该RED同时暴露阶段2尚缺的生命周期、scheduler和asset-retention行为，而不再依赖setup早退。

最终工作区验证：

1. `cargo test -p data-collect --test polymarket_metadata_layout_contract --test polymarket_contract --test parquet_writer_contract --locked`：18 + 7 + 12项通过。
2. `cargo test -p data-collect --lib live_runtime::tests::polymarket_resolution_timeline_is_absolute_red --locked -- --exact`：退出0，named RED为1项ignored。
3. 同一测试增加`--ignored --nocapture`：退出101且panic仅在最终目标断言；主线程与独立审查代理分别实跑得到相同分类。
4. `cargo test -p data-collect --lib --locked`：258项通过、0失败、1项ignored。
5. `cargo test -p data-collect --locked`：完整package、integration与doc-tests退出0。
6. `cargo clippy -p data-collect --all-targets --locked -- -D warnings`：退出0。
7. `cargo fmt --all -- --check`与`git diff --check`：退出0；`Cargo.lock`未变化。

本轮审查对“过度设计/过度校验”的结论是：68字段v3、五类row、19项failure与payload/digest/reference
来自已批准RF-09/AC，不是推测性扩张；production仍唯一写v2，未新增writer/source/shadow SDK。
审查实际删除了无依据的固定teardown上界和可能重复predicate authority，并保留单一shared sort-order
判定与唯一Gamma evaluator。最终独立复审正式PASS：Critical 0、Important 0、Minor 0；审查代理
连续3次显式运行named RED均只在最终目标断言退出101，并独立复核完整package、Clippy、fmt与
diff check通过、`Cargo.lock`未变化。阶段1已满足授权的commit/push门。

## 2026-07-22：阶段 2 RF-11 transport subtype 能力缺口与暂停

进入阶段2后，在实现`resolution_transport_error_class`的production分类前重新核验固定依赖的公开
typed error surface，确认已批准RF-09与pinned官方SDK当前能力不能原样同时成立：

1. RF-09冻结`resolution_transport_error_class=dns|connect|timeout|http_status|rate_limited|sdk_transport`，
   AC-08明确要求DNS availability负例；因此真实DNS错误不能无证据地伪装为另一稳定subtype。
2. 本仓库pinned `polymarket_client_sdk_v2 0.6.0` Gamma `Client`只公开`new(host)`，内部自行构造
   `reqwest::Client`；SDK error可typed downcast到`reqwest::Error`，也可取得typed HTTP status。
3. pinned `reqwest 0.13.4`公开提供`is_timeout()`、`is_connect()`、`is_decode()`与`status()`，但没有
   DNS-specific判定。其底层`hyper-util 0.1.20`内部确实用不同路径创建`"dns error"`和
   `"tcp connect error"`，但`ConnectError`位于private `connect::http` module、未公开重导出，且没有
   public kind/accessor；依赖错误显示文本或private实现细节分类不构成稳定typed合同。
4. 因此当前production可以稳定区分timeout、HTTP/rate-limit、decode/schema和总的connect类，却不能在
   不修改合同或SDK注入面的前提下稳定区分DNS与其他connect。使用错误字符串、raw REST、自建Gamma
   client或其他shadow transport均违反RF-11与项目SDK-first边界，未采用。

影响：阶段2的lifecycle/pending/scheduler主实现、production v3切换以及阶段2commit/push暂停；已完成的
fixed identity验证时刻传递、旧fallback shadow predicate清理和v3 codec准备改动保持未提交、未推送。
现实最小选项为：A）修改合同，将官方公开`reqwest::Error::is_connect()`覆盖的连接建立失败统一记为
`connect`，保留或删除永不由production发出的`dns`枚举另行决定；B）经用户批准，为本仓库pinned官方
SDK补充最小reqwest client/DNS resolver注入面，并用项目自有typed DNS marker分类，承担vendored SDK
API和测试维护范围；C）等待上游SDK/reqwest公开稳定DNS subtype。等待用户决定前不继续冲突相关实现。

## 2026-07-22T15:33:01+09:00：用户批准 RF-11 方案 A

用户在确认该能力缺口不是为香港VPN测试而新增后，批准采用推荐的最小方案A并恢复执行阶段2至阶段4。
随同批准接受的取舍是：以pinned SDK公开typed surface可稳定验证的粒度为准，避免只为遥测细分类修改
vendored SDK、注入resolver或新增shadow transport；香港VPN仍只用于最终AC-12真实采集验收，不改变schema。

权威合同同步调整为：

1. 从RF-09 v3稳定枚举、layout与测试删除`dns`；不产生永远无法由production可靠证明的标签。
2. `connect`精确对齐`reqwest::Error::is_connect()`，覆盖该公开判据报告的DNS/TCP/TLS/proxy连接建立失败。
3. transport first-match优先级冻结为typed HTTP 429 -> `rate_limited`、其他typed HTTP status ->
   `http_status`、`is_timeout()` -> `timeout`、`is_connect()` -> `connect`、其余SDK transport ->
   `sdk_transport`；SDK decode/schema仍走既有integrity路径。
4. AC-08改为覆盖connect/timeout/rate-limit availability、valid nonterminal与identity/schema contradiction三类
   结果边界。禁止错误字符串、private error source chain、raw REST或自建Gamma client分类DNS。

该决定缩小schema和实现范围，不要求修改pinned SDK，也不改变本地香港VPN AC-12、production部署禁令、
服务操作禁令或数据删除禁令。

## 2026-07-22 至 2026-07-23：阶段 2 production actor、scheduler 与 v3 切换

阶段2主体实现与自审正在现有单一Polymarket actor和现有typed writer内推进；阶段审查门尚未通过。
当前实现没有新增source kind、checkpoint、raw REST、manual WebSocket、manual signing或shadow SDK路径：

1. current在真实`E`进入独立`resolution_pending` role，lifecycle继续保持active；next按真实`T`
   promotion，缺失next时由同一actor异步执行exact-slug discovery。只有活跃窗内已经固定并验证的
   identity可进入pending；late install以fence后实际applied时刻判窗，`applied >= E`直接expired。
2. desired subscription变为current + next +最多2个pending的有界union，仍由既有官方SDK dynamic
   subscription actor执行additions-before-removals。pending旧market只路由`market_resolved`，每slug
   terminal-only fingerprint最多16条；terminal或exhausted drain完成后才精确移除旧assets。
3. follow-up scheduler只调用pinned官方SDK `market_by_slug`，按绝对`E+3m` initial和`E+4m..E+8m`
   五次retry执行，每slug最多一个in-flight，20秒timeout不累计漂移且没有第7次probe。attempt binding、
   source identity/shape、closedTime和唯一terminal predicate均由单一actor按RF-07/RF-11顺序判定。
4. production transport分类只使用公开typed surface：HTTP 429、其他HTTP status、timeout、connect、
   其余SDK transport；decode/schema保持integrity。v3枚举与测试同步删除无法可靠证明的`dns` subtype。
5. 所有SDK-decoded `market_resolved`先保存既有`polymarket_market` raw。WS/Gamma terminal由actor
   terminal-once仲裁；WS先claim时abort handle只发取消请求，pending仍等待JoinSet completion或明确的
   cancelled task回收，已完成Gamma response继续完成integrity/predicate并buffer该slot唯一attempt。
   Gamma先claim时queued WS只保留raw corroboration，不产生第二owner、evidence source或transition。
6. terminal/exhausted使用paused accepted-event fence：actor先drain并应用唯一role/lifecycle退出，再释放
   processor和执行unsubscribe。WS terminal与同UTC hour的buffered post-attempt先整体取得global budget和
   ingress capacity，再按terminal-first连续admission；容量失败为零条admit，避免terminal成功而post丢失。
   阶段2以内存中有界source-reference authority和整批原子admission阻止孤立transition/post进入queue；
   跨UTC partition的terminal bundle继续fail closed，finalized prefix durability、crash cuts与restart
   恢复属于已批准阶段3，不在阶段2提前另造writer/rollover框架。
7. production metadata source一次性切换为v3；历史v2仍只读。删除新产生的WS `new_market` metadata、
   E+5 sentinel和旧`fallback_close` conversion，保留同一Parquet writer、每日manifest file facts与
   artifact集合，不dual-write、不扩张manifest schema。

自审和前置独立审查实际发现并修正：

- WS claim后过早清除abort state会让已完成Gamma completion越过teardown；现在按task id明确回收，
  completion-wins与cancel-wins都有定向测试。
- terminal和buffered post原先逐条enqueue，第二条容量失败可能形成孤儿post evidence；现在同分区使用
  原子reserve-many admission，并测试容量不足时零条进入队列、正常时admission sequence严格相邻。
- `terminal_finalized`字段从不写true且pending完成后直接remove，不提供幂等或恢复authority，已删除，
  保留RF-04明确要求的16-entry compact terminal fingerprint。
- reverse-race fixture曾把old identity的fixed verification放在`E`之后，已改为活跃窗内真实时刻；
  AC-07另补initial、retry1、中间retry、retry5 terminal-stop，valid+transport与zero-valid exhausted。
- 全量测试首次暴露两个旧fixture假设：静默CLOB测试更新了window却没有同步source payload/digest时间，
  production v3正确拒绝为raw-day regression；已在同一测试运行日重建evidence。Gamma 100-cycle资源测试
  仍只预期一次成功请求，但阶段2会在current恢复后立即异步发起一次bounded next discovery；fixture改为
  明确等待并断言恰好两次成功请求，而不是放宽FD或attempt上限。

此前阶段2自验证快照：

1. `cargo test -p data-collect --lib live_runtime::tests::polymarket_ --locked -- --nocapture`：
   当时51项通过、0失败；后续新增用例已使该计数失效，最终结果须在阶段审查前重跑并更新。
2. terminal bundle、Gamma completion/cancel、initial/retry stop、mixed/zero-valid exhausted及三类
   reverse-race定向测试全部通过；真实SDK loopback同时覆盖exact unsubscribe和queued WS。
3. `cargo test -p data-collect --locked`：当时275项unit、全部integration与doc-tests退出0；后续新增
   用例已使该计数失效，最终结果须在阶段审查前重跑并更新。
4. `cargo clippy -p data-collect --all-targets --locked -- -D warnings`、`cargo fmt --all -- --check`
   与`git diff --check`退出0；`Cargo.lock`和vendored SDK均未修改。

本节只记录阶段2实现与自审证据；阶段2在规定的独立Ultra复审通过、commit并push前仍不标记完成。

## 2026-07-23：用户批准阶段 2 / 阶段 3 最小职责分界

阶段2前置独立审查发现：原计划任务2.3.3把actor buffer、writer admission、flush/finalize crash cuts
同时放进阶段2，而当前changelog与阶段3又把finalized prefix durability和restart归入阶段3；两者不能
同时作为当前authority。按方案不一致决策门暂停后，用户批准推荐的最小分界：

1. 阶段2闭合单一actor内的terminal-once、accepted-event drain、WS/Gamma竞态矩阵、terminal-first
   原子batch admission，以及最多current/next/pending所需的有界内存source-reference authority。
2. 阶段3统一闭合finalized prefix durability、raw/actor/writer/flush/finalize/UTC seal crash cuts、
   跨分区producer fence、finalized artifact integrity和process restart恢复。
3. demand第四节“只授权阶段0”改为历史首次授权表述；当前阶段1至阶段4授权仍以页首和本changelog为准。
4. 该分界不删除、不放宽RF-10、AC-09、AC-10或最终本地香港VPN AC-12；只避免阶段2提前复制阶段3
   writer durability/recovery框架。production部署、服务操作和数据删除仍未获授权。

阶段2继续执行前已核实的修复证据包括：真实post-E routing不再把旧current非terminal事件fan-out为
metadata；metadata admission拒绝未来causal fields但不恢复已删除的固定closedTime skew；local
source-reference validator按identity、origin、receive与digest完整tuple判唯一，允许同receive不同digest
的唯一命中；late `candidate_to_active` source-bearing row可成为后续E transition authority；GREEN时间线
测试已改为中性名称。过滤套件、full lib和metadata layout在该快照分别为54/54、278/278与14/14通过；
这些只是继续开发前快照，阶段2最终数量和完整验证仍须在后续改动完成后重跑。

继续开发后的阶段2终审前自验证：

1. discovery首次identity验证与本次payload validation已分开采样；同actor内相同canonical identity
   的fresh rediscovery保留首次fixed时刻，fresh row仍保存本次validation。真实clock fixture证明
   fetch start、source receive、candidate evaluation与validation completion按因果顺序取样。
2. actor只保留current + next +最多2个pending所需的4条discovery source-reference authority；
   source-bearing snapshot或late `candidate_to_active`必须先成功进入既有有序queue才登记。正常T/E
   transition在任何state mutation或admission sequence前按完整identity、origin、receive和digest
   唯一preflight；missing、digest mismatch或duplicate authority均fail closed。不扫描Parquet，
   finalized durability仍留在阶段3。
3. WS-first late Gamma由真实completion路径覆盖terminal、valid nonterminal、SDK decode、attempt
   binding、identity/shape、closedTime及transport七类；只有integrity-clean terminal/nonterminal
   改写为post observation，其余保留原分类。`recv=E`的`market_resolved`证明raw-first、early claim
   唯一转交pending、旧assets保留到drain；reverse-race fixture不再手工塞post row。
4. 历史v2使用手工ArrowWriter真实写读fixture证明仍可只读；production typed writer继续拒绝新建v2
   segment，v3-only字段没有被伪造进历史文件。
5. `cargo test -p data-collect --lib live_runtime::tests::polymarket_ --locked -- --nocapture`：
   62项通过、0失败。
6. `cargo test -p data-collect --locked`：286项unit、全部integration与doc-tests退出0；其中metadata
   layout 15项、Parquet writer 18项、Polymarket contract 6项均通过。
7. 四source production load精确复跑为28,745 native messages、42,370 rows、sustained
   4,682.7 msg/s、actual 4,477.8 msg/s，位于既有2,000至5,000 native msg/s验收带内。
8. `cargo clippy -p data-collect --all-targets --locked -- -D warnings`、
   `cargo fmt --all -- --check`与`git diff --check`退出0；`Cargo.lock`和vendored SDK均未修改。

以上仍是独立Ultra阶段终审前证据；只有审查问题闭环、复审通过并完成获授权的commit/push后，阶段2
才可标记完成。

阶段2独立Ultra终审发现并闭环以下问题：

1. schema reviewer发现metadata source-bearing row在UTC rollover gate返回`Defer`时，旧boolean helper
   会把“已暂存但尚未admit”误当成authority。实现改为内部
   `EventAdmissionOutcome::{Admitted, Deferred, Cutoff}`；只有`Admitted`返回并登记authority，
   `Deferred/Cutoff`均fail closed。定向回归证明deferred row没有collector admission、metadata
   sequence仍为0且actor authority仍为空；schema复审最终PASS，Critical/Important/Minor均为0。
2. runtime reviewer发现合格`market_resolved`若在`E-1ms`到达，原guard会在raw-first后丢弃terminal
   claim，违反RF-05且需求没有WS temporal predicate。删除该guard后，current/next的已完整校验事件
   一律进入既有最多2条`early_ws_claims`有界状态，仍只在E进入pending后完成transition；新增
   pre-E与既有recv=E回归，未增加skew allowance或新actor。
3. runtime reviewer发现同一fence最多两个ready pending顺序完成时，第二个transition会复用第一次
   fence后的单次clock sample；若第一条metadata admission遇到backpressure，其applied不再真实。
   改为每个slug紧邻`pending.remove`线性化点单独采样并更新actor clock。确定性channel-capacity-1
   测试用prefill阻塞第一条，推进fake clock后证明两条summary分别记录`t1/t2`，并只unsubscribe
   四个旧assets一次。
4. runtime reviewer继续发现exhausted summary applied与fence释放后第一条post-marker WS可能落在同一
   UTC毫秒，形成`raw.recv == summary.applied`，不满足RF-05/RF-10要求的严格post-horizon关系。
   最小修复只对实际exhausted role-exit在释放paused fence前复用既有
   `BUFFER_ACQUIRE_TIMEOUT`等待真实`timeline_now().utc_ms`严格大于最大exhausted applied；不伪造
   `+1`、不恢复固定closedTime skew，clock不前进则fail closed。测试证明clock冻结时post-marker
   WS不产生raw，真实推进到`t2+1`后才生成严格更晚的raw-only evidence，且没有第二terminal metadata。
5. 新clock barrier使两个冻结时间fixture正确暴露旧假设：reverse-race的constant clock及
   start-paused GREEN在actor采样后不再推进。fixture分别改为第二次真实采样前进1ms，以及在budget
   fence等待期间同步推进paused UTC/clob trace；两项exact回归与完整62项Polymarket过滤套件均通过。

最终阶段2验证为：Polymarket过滤62/62、完整lib 286/286、全部integration与doc-tests、
metadata layout 15/15、Parquet writer 18/18、Polymarket contract 6/6、
`cargo clippy -p data-collect --all-targets --locked -- -D warnings`、`cargo fmt --all -- --check`
及`git diff --check`全部通过。四source production load结果仍为28,745 native messages、
42,370 rows、sustained 4,682.7 msg/s、actual 4,477.8 msg/s。docs、schema与runtime终审在上述
修复后均为PASS，Critical/Important/Minor均为0；未改动`Cargo.lock`、vendored SDK或三个未跟踪
RTDS文档，也未执行production部署、服务操作、数据删除或提前执行香港VPN AC-12。

## 后续 changelog 规则

阶段1至阶段4现已获授权；后续每个阶段必须追加记录：

- 用户逐项决定与批准范围；
- 与需求/计划不一致的真实事实、影响、选项和最终决定；
- RED测试、实现diff和验证命令/结果；
- SDK能力或类型缺口；
- 独立阶段审查问题、修复和复审结果；
- commit/push仅在已授权时记录；
- 本轮已授权的本地香港VPN AC-12执行后记录完整artifact、binary/commit/sha256、时间线和限制。
