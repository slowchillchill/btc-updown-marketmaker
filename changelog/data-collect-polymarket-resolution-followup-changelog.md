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

## 后续 changelog 规则

阶段1至阶段4现已获授权；后续每个阶段必须追加记录：

- 用户逐项决定与批准范围；
- 与需求/计划不一致的真实事实、影响、选项和最终决定；
- RED测试、实现diff和验证命令/结果；
- SDK能力或类型缺口；
- 独立阶段审查问题、修复和复审结果；
- commit/push仅在已授权时记录；
- 本轮已授权的本地香港VPN AC-12执行后记录完整artifact、binary/commit/sha256、时间线和限制。
