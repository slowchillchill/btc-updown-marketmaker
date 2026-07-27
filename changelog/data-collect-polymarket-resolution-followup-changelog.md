# Polymarket 五分钟市场生命周期与结算追踪修复 Changelog

> 事项编号：`DC-REPAIR-20260721-POLYMARKET-RESOLUTION-FOLLOWUP`
>
> 对应需求：`demand/data-collect-polymarket-resolution-followup-demand.md`
>
> 对应计划：`plan/data-collect-polymarket-resolution-followup-plan.md`
>
> 状态：`PHASE_4_COMPLETE_LOCAL_HK_VPN_AC12_AC13_PASS_2026-07-23__V3_VALIDATE_FIRST_METADATA_V4_IMPLEMENTED_CURRENT__GAMMA_CLOSED_TIME_FORMAT_A_APPROVED_FIX_PENDING_2026-07-27`。阶段0至阶段3已分别完成自审、独立终审与commit/push；阶段4本地香港线路AC-12、全量AC-13与三路独立全面终审均已通过。用户于2026-07-24批准source schema v3的validate-first文档修订，并于2026-07-25批准metadata v4 claimant schema；二者已由SDK070 Stage3实施并成为当前new-write组合。2026-07-27 Attempt3暴露的Gamma顶层`closedTime`精确格式缺口已由用户批准方案A，本地最小修复、测试和发布待执行。production部署、服务操作和数据删除仍未获授权、未执行。

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
4. 该分界不删除、不放宽RF-10、AC-09、AC-10、AC-11或最终本地香港VPN AC-12；只避免阶段2提前复制阶段3
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

## 2026-07-23：阶段 3 UTC、restart/recovery 与资源收敛

阶段3按已批准的阶段2/3职责分界实现，未新增checkpoint、journal、sidecar、manifest字段、第二
Polymarket actor或shadow transport：

1. Polymarket day control携带精确UTC boundary。day fence先暂停唯一market pump，再把已接受WS消息
   和至多两个Gamma completion按真实receive time与稳定同刻顺序合并交给同一actor；旧日事实在旧日
   finalize前完成，新日事实只在barrier后admit。WS-owned terminal bundle继续使用terminal-first原子
   admission，restart scanner同时验证真实finalized artifact位置，禁止finalized post早于其唯一WS
   terminal。
2. next与rollover discovery task在day/stop fence前统一abort并join，barrier后只按现有立即重试路径
   fresh fetch，旧日异步结果不能跨日安装。
3. CLOB pump不可用的outer recovery仍按绝对E边界把current转入pending，并继续驱动E+3至E+8
   Gamma slots；Gamma terminal或第6次nonterminal/transport完成后无需等待CLOB重连即可由同一
   pending state完成terminal/exhausted落盘。无第7次probe，旧assets只在terminal或budget完成后
   从下一次desired subscription移除。
4. process startup新增只读finalized-artifact scanner。它只接受manifest file facts覆盖且hash、
   row count、schema version匹配的finalized metadata v3与`polymarket_market` raw；running文件和
   历史v2不作authority。scanner保存day/hour/part/row位置并fail closed验证fixed identity、
   source payload/digest/reference、attempt ordinal prefix、terminal/summary互斥及WS/Gamma/
   exhausted/raw完整matrix；只恢复仍有未来slot且总数不超过2的pending，已经越过而无durable
   attempt evidence的slot只生成missed语义。
5. recovered startup先恢复当前与下一五分钟window的既有raw lifecycle dedupe，再按scanner耗时后的
   真实clock重新归类elapsed slot；scanner期间跨UTC日则丢弃旧日只读结果并重新走startup loop，
   clock回退则fail closed。
6. 资源合同继续复用既有两个Gamma task上限、每pending 16-entry compact terminal dedupe与
   current/next/pending desired union；批量recovery fixture证明actor-owned pending、task和dedupe
   在terminal/exhausted后释放，没有引入全历史状态或新后台scheduler。

开发与自审中发现并修正：

- 初版recovered启动绕过既有raw dedupe restore，可能让重启后的current/next WS replay重复写入；
  现只对启动时真实current+next windows执行control-aware restore，pending仍使用自身compact terminal
  dedupe，避免扩大2M-entry lifecycle集合。
- 初版outer loop只有已有pending才发poll，CLOB在E前断线会丢失E transition；同时Gamma completion
  需等CLOB恢复才finalize。现outer loop复用既有boundary advancement与paused finalizer，并增加
  CLOB持续不可用的transition、terminal和exhausted回归。
- 初版day fence先处理全部queued WS再处理Gamma，不能表达`Gamma receive=D-1 / WS receive=D+1`
  的反向竞态。现以control携带的exact boundary合并排序，覆盖D-1/D/D+1及WS-old/Gamma-new、
  Gamma-old/WS-new。
- 初版scanner把“不相关raw”和“相关但identity/payload矛盾raw”都表示为`None`，存在错误恢复pending
  的fail-open；现使用三态分类，只有真实其他market或窗外raw被忽略，相关解码、schema、identity、
  payload或digest矛盾均终止恢复。
- 初版restart只用进程内admission sequence验证terminal/post prefix，无法证明跨文件durable顺序；
  现附加finalized artifact position，并补齐会改变恢复判断的terminal、summary、post及transition
  row语义。
- 完整测试首次发现阶段3把所有source control提前为最高优先级，导致普通测试fence抢在已ready的
  第6次Gamma completion与退订收敛之前。对比阶段2提交证明SDK退订能力无缺口；最终只延后
  `stop=false + no day boundary + no resume`的普通fence，让ready Gamma/finalization先收敛，
  day、stop及所有malformed control仍立即优先。定向时间线恢复通过，所有临时SDK/loopback诊断代码
  均已移除，vendored SDK最终无diff。
- 初版raw-day admission在WS/Gamma actor处理期间持有`Arc` read guard，day seal需等待该guard释放，
  而accepted terminal bundle又可能等待seal，形成闭环。最终只保留source-scoped detached
  `Permit/Defer` ticket；真正enqueue时重新取得短生命周期guard并核对day/generation。没有新增gate、
  checkpoint或第二writer。
- scanner初版先按`[E,E+8)`筛掉identity，因而无法检查horizon外仍属于同一identity的finalized
  terminal/post/raw矛盾；metadata reader又把缺少row-kind projection一律当作v2。最终先验证D/D-1
  全部finalized v3 identity及raw matrix，再只从仍有未来slot的子集恢复；Parquet footer显式v2才跳过，
  v3缺projection、footer缺失/重复/未知均fail closed。未增加`raw > Gamma completion`这类需求外时间门。
- quiesce初版只物化已到E的current early WS claim；current-pre-E会被clean stop误报成功，current
  物化时的`next.take()/rebuild`还会静默剪掉next-pre-E claim。day fence也可能在ordinary E
  advancement前先回复并seal旧日raw，outer recovery的biased control分支存在同一竞态。最终增加一个
  只读preflight：claim非空时只允许唯一current且真实时间已到E；connected与outer day fence均在
  drain/join后重新采样真实clock、确认`now >= boundary`、复用既有boundary advancement后再finalize。
  正常运行仍允许early claim等待E，检查只作用于clean exit/day seal，独立Ultra复审确认不是过度校验。
- AC-10 crash fixture初版虽为GREEN，但actor、metadata admission、WriterCommand与ActiveSegment切点
  只携带terminal单行，未形成真实WS-first terminal+buffered-post bundle；UTC正例还新建generation=1
  gate并剥离Inflight permit直接写Parquet，绕过真实writer rollover generation检查。独立Ultra复审
  判为2项Important；修复固定为调用真实Gamma completion与actor finalizer，并复用同一gate/close、
  deferred flush和writer permit检查，不增加production seam。orphan-post与reverse-prefix负例经复审
  继续有效。
- 补充并发运行`polymarket_`过滤集时，既有
  `polymarket_source_spec_uses_private_gamma_endpoint_and_millisecond_deadline`在高并发调度下先越过
  50ms source deadline，随后测试代码无界等待尚未被连接的本地mock server；主代理终止该重复run，
  确认无残留进程。该test exact为1/1；`cargo test -p data-collect --lib polymarket_ --locked --
  --test-threads=1`这一lib名称过滤在新增scanner回归前为130/130（其中也包含模块名带
  `polymarket_sdk_runtime`的既有RTDS测试），且权威全量套件已通过；
  因而没有把test harness调度抖动误报为collector或SDK缺陷，也没有为此扩大production改动。
- 阶段3全面代码终审发现restart scanner只投影canonical v3的40/68列，并让compact
  `MetadataRecord::validate()`承担不完整的第二套row校验；此前未投影的source event/channel、
  fetch/validation时间、Gamma response、transport与predicate字段若损坏，可能被错误用于恢复或
  terminal/exhausted判定。按用户批准的推荐方案，scanner现精确核对完整Arrow schema的字段名、
  顺序、类型与nullability，读取完整row，通过canonical
  `PolymarketMarketMetadataV3Row::from_draft_json_with_admission()`与`validate()`后才构造compact
  recovery view；discovery payload复用production candidate validator，Gamma payload复用官方SDK
  decode与唯一`evaluate_gamma_terminal_response()`。原有跨row/raw/prefix规则继续保留，未增加依赖、
  transport、checkpoint或第二套predicate。新增完整schema缺列、未投影required/null matrix、缺失
  budget horizon、discovery payload/identity矛盾及Gamma terminal predicate矛盾五项corruption
  fixture，均证明恢复fail closed。
- 后续代码与范围终审继续发现两项raw/WS完整性缺口和一项局部重复校验：raw scanner原先只用
  Parquet typed routing列筛选candidate，目标source payload可能被同时损坏的typed
  `event_type/market/slug`隐藏；WS row只校验两个typed platform timestamp字段互相一致，未核对
  persisted source payload中的真实`timestamp`；compact recovery view仍重复部分canonical
  row/schema判定。修复保持最小：scanner对每条raw同时解析payload中的
  `event_type/id/market/slug`并与typed routing取并集，再对相关row的typed/payload矛盾fail
  closed；WS source payload的`event_type`与`timestamp`必须精确支持typed字段；compact层只保留
  fixed-identity、finalized integrity、跨row attempt/reference、terminal-prefix和strict
  post-horizon规则，单row/schema/predicate继续唯一委托canonical validator与官方SDK/evaluator。
  新增两项corruption fixture分别证明typed routing不能隐藏目标payload、WS typed timestamp不能
  脱离source payload。没有新增网络parser、transport、状态机、checkpoint、依赖或第二套predicate。
- 文档终审同时发现AC-11证据未具名闭合，且旧“130/130”可能被误读为纯Polymarket套件；本节现已
  具名记录四项AC-11资源回归，并把该数字明确限定为lib名称过滤。该修订不改变需求、计划、实现范围
  或验收标准。

阶段3独立终审前自验证：

1. restart scanner为32/32；`live_runtime::tests::polymarket_`为88/88；单线程
   `cargo test -p data-collect --lib polymarket_ --locked -- --test-threads=1`这一lib名称过滤为
   137/137；AC-10 crash-cut exact为1/1。`polymarket_resolution_timeline_is_absolute_green`
   继续为1/1，并重新证明旧assets在budget完成后、stop前退订。
2. `cargo test -p data-collect --lib --tests --locked`及
   `cargo test -p data-collect --locked`均退出0；完整lib为347/347，全部integration test targets
   与doc-tests通过。
3. `cargo check -p data-collect --locked`与
   `cargo clippy -p data-collect --all-targets --locked -- -D warnings`退出0；
   `cargo fmt --all -- --check`及`git diff --check`退出0。
4. official-SDK/static scope scan只发现pinned `polymarket_client_sdk_v2::gamma::Client`与
   `MarketBySlugRequest`进入新增production路径；没有raw REST、manual WS/signing、第二actor、
   checkpoint或新增依赖。`Cargo.lock`、Cargo manifests、vendored SDK和三个未跟踪RTDS文档均未修改。
5. clean-stop/day-fence边界复审与最终AC-10 crash fixture复审均为PASS，Critical/Important/Minor
   均为0；两项复审都明确确认修复没有过度设计或过度校验。
6. AC-11具名资源回归全部通过：`polymarket_pending_union_and_terminal_dedupe_stay_within_frozen_bounds`
   证明最多2个pending、8个assets、每pending 16个fingerprints且额外状态小于1 MiB；
   `polymarket_hour_budget_and_two_task_ownership_are_hard_bounded`证明每小时最多72次请求且最多2个
   Gamma task；`polymarket_many_recovered_lifecycles_release_actor_owned_state`证明100个lifecycle后
   pending/assets/bytes归零；`gamma_outer_supervisor_survives_one_hundred_outer_cycles_with_bounded_fds`
   证明100次outer recovery后FD回归平台且波动有界。
7. 未执行production部署、服务操作、数据删除或提前执行香港VPN AC-12。

阶段3最终代码、范围/简洁性与文档/验收三路独立Ultra复审均为PASS，Critical 0、Important 0、
Minor 0。三项首轮问题已真实闭环：typed raw routing不能隐藏目标payload，WS platform timestamp
精确绑定保存payload，compact recovery层不再重复canonical单row/schema/predicate校验。复审明确
确认没有整体或局部过度设计/过度校验，没有shadow parser/authority、第二actor、checkpoint或范围
扩张。阶段3现已满足获授权的commit/push门；完成精确提交与远端分支一致性核对后才进入阶段4。

## 2026-07-23：阶段4本地全量验证与香港线路AC-12

阶段4在已推送的Phase3 commit
`a32226863428dcbd82b016538f36855240a41892` 上执行，没有修改Rust、schema、Cargo
manifest/lock、vendor或production网络路径。

### 本地全量验证

在独立 `target/data-collect-resolution-followup` 中执行并保存日志：

- `cargo test -p data-collect --locked -- --test-threads=1`：完整lib 347/347，
  全部integration targets与doc-tests通过；
- `cargo check -p data-collect --tests --locked`：退出0；
- `cargo clippy -p data-collect --locked --all-targets -- -D warnings`：退出0；
- `cargo fmt --all -- --check` 与 `git diff --check`：退出0。

精确release binary：

- SHA-256：
  `82aa27fb3f1dfdb425b9e9aeae97e4666ac9bfd352908afe95a2eee3ffdad0ca`；
- size：`29,430,920` bytes；
- Build ID：`d0f9ed9fa35564844051948ae8a904f9d9e0500d`；
- build receipt SHA-256：
  `1e9b7725f1ba64fbd4a4443a06aef64ad0d75d47f5332cbea8b79fbb92dfecfa`。

### 香港线路最小适配与预检

核实当前继承代理出口为日本、清空代理后的默认路由也不是香港；同时官方SDK WebSocket
路径不会可靠继承HTTP代理变量。因此直接运行会产生混合线路，不满足AC-12。

用户同意推荐方案后，只在隔离validation artifact内解压Ubuntu官方
`proxychains-ng 4.17-3build1`，通过绝对 `LD_PRELOAD`、
`strict_chain + proxy_dns + socks5 127.0.0.1:20810` 包装精确collector。
没有系统安装、route/firewall/TUN/service mutation，也没有改SDK或新增shadow client。

执行前正负控制：

- IPInfo、Cloudflare、IPify、IPWho一致为
  `8.210.176.1 / HK / HKG`；
- 无监听端口 `20812` 的strict negative请求退出7，syscall只到该本地端口；
- 官方SDK Gamma、market WS和RTDS smoke均取得真实业务消息，connect只到20810；
- 精确collector 45秒smoke退出0，strace记录的6/6次connect都只到20810，6类Parquet与
  manifest正常finalized；正式1800秒run的`/proc/maps`另行证明加载隔离library。

### 正式AC-12

validation root：
`/home/chillchill/validation-artifacts/data-collect-resolution-followup-ac12-20260723T121902Z`。

正式collector从 `2026-07-23T12:30:31.728772491Z` 至
`2026-07-23T13:00:33.702730918Z` 运行1802秒；collector和线路monitor退出码均为0。

线路与连接证据：

- before/running/after共31份route samples，失败0；
- 7次IPWho第三源交叉均与IPInfo/Cloudflare一致；
- 全程出口集合 `{8.210.176.1 / HK / HKG}`；
- proxy PID集合 `{3706}`，restart集合 `{0}`；
- collector进程树22次IPv4/IPv6 connect全部到 `127.0.0.1:20810`，
  非loopback为0；22条proxychains chain均成功。

数据证据：

- manifest含11个finalized Parquet，无running/tmp/partial/digesting文件、gap为0；
- 5个完整五分钟market的每个 `[T,E)` 都有5份香港线路样本；
- 其中4个完整market在E+3前取得WS terminal；启动半窗另有1个WS terminal；
- metadata共25行：snapshot 8、transition 12、WS terminal 5、attempt 0、summary 0；
- 5条metadata terminal与5条raw `market_resolved`按receive、payload digest、winner和
  fixed identity逐条唯一对应；
- 11/11 manifest file facts与磁盘bytes/hash/rows/footer/embedded fact一致；
- v3为精确68字段，v2文件为0，独立复算结果 `PASS`、`violations=[]`。

WS在各自E+3前完成，所以未发Gamma attempt是terminal-once合同的正确分支，不是漏测；
没有人为延迟terminal或制造retry/exhausted。报告分别保留WS platform timestamp、
Gamma discovery fetch/receive、本地collector receive、validation、lifecycle/runtime-role
applied与admission时间；WS terminal的Gamma fetch/closedTime为null。没有用closedTime推断
历史可见时间，也没有恢复被用户拒绝的5秒future-skew门。

真实source报告：
`data-collect/docs/polymarket-resolution-followup-ac12-validation.md`。

### 验收工具审查与收敛

首版离线检查器达到2587行，接近复制Rust validator；自审后删除并改为只验证本次artifact
必需事实的精简检查器。首轮精简版虽对本数据得到PASS，独立审查仍发现5个Important：

1. 合法transport/missed attempt payload分流与exhausted path识别不完整；
2. v2存在时可能不影响global verdict；
3. raw linkage缺少typed payload与反向唯一证明；
4. complete market缺少关键state-role/admission因果关系；
5. manifest缺少磁盘闭包、duplicate、strict relative/symlink与固定sort/compression合同。

以上问题只影响验收工具证明面，不影响真实artifact或Rust实现。后续三轮独立复审继续发现并
闭环了manifest路径/embedded fact、transition reference、raw terminal反向唯一、winner与固定
asset顺序、WS evidence和terminal/attempt/summary admission ordering等证明缺口；每次都只扩展
本次artifact必需的独立复算，不复制完整Rust required/null matrix、19项失败precedence、
restart crash或VPN authority。

最终稳定检查器SHA-256为
`f254dea3db0f6049018d5b22a2595e85ddbdb67e821503ecf7fbe395df2042b8`，
结果SHA-256为
`920f472b22d4adc9356ed22f47a68f991d18edad1370a2f665511a5fb519fc59`，
结果为PASS且`violations=[]`。第四轮复审为PASS，Critical 0、Important 0、Minor 2：
大小写精确的`Up/Down`对未来合法小写输入偏严，以及manifest未额外重复核对source/utc_day目录
语义；当前真实payload、路径和embedded facts全部一致，两项均不影响本次artifact。审查明确建议
在1542行处收敛，避免把验收器扩张为production Rust validator的shadow authority，主代理接受。

文档与范围终审发现首版artifact inventory只扫描五个主目录，虽216条均通过hash校验，却漏掉
报告引用的throwaway smoke dataset manifest和6个Parquet。该问题不影响正式AC-12数据，但
“固定artifact”表述不成立。最终清单改为扫描artifact根下除清单自身外的全部普通文件，
223/223条通过校验，SHA-256为
`d11219fe4e6e4cc14e7bec80a027e349a0daaf0934890d2c27f3a776338d94c6`。

文档终审还发现原报告把正式run的`/proc/maps`证据归到45秒smoke，并把smoke的6次connect
写成6个活动socket。现已准确拆分为：smoke strace的6/6次connect只到20810且6类Parquet
finalized；正式1800秒run的`/proc/maps`证明隔离library实际加载。

上述修复后，需求映射、文档/验收、范围/简洁性三路
`gpt-5.6-sol`、Ultra独立全面终审最终均为PASS，Critical 0、Important 0、Minor 0。
三路均确认：process-local `proxychains-ng`包装和每分钟线路/syscall证据直接服务于禁止静默
直连；1542行检查器只复算本次artifact并明确不复制完整Rust required/null matrix、19项失败
precedence、restart/crash或VPN authority，因此没有过度设计、过度校验或第二production
authority。阶段4已满足获授权的最终commit/push门；production部署、服务操作和数据删除仍未
获授权，也未执行。

### 范围边界

- 本地香港线路receive timing不代表AWS爱尔兰paper/live延迟；
- 进程级包装与全程syscall审计证明本次精确binary已知TCP路径没有静默直连，不扩大为
  任意未知binary的内核级隔离保证；
- 未访问或改变AWS主机，未执行production部署、production service操作、系统网络变更、
  系统包安装或数据删除；
- 三个无关untracked RTDS文档继续排除。

## 2026-07-24：future source schema v3取消raw-first

用户重新审查采集目标后，质疑“每个SDK-decoded `market_resolved`在terminal校验前先持久化”是否属于过度设计，并明确同意、批准修改文档。只读核实确认：

- 2026-07-21至2026-07-23的RF-05、raw late corroboration、post-horizon raw-only和raw-to-metadata restart矩阵均是当时真实批准、实现和验证的v2行为；
- 当前handler确实先写raw，再做target identity/winner校验；invalid raw-first只有synthetic test，真实AC-12为5条有效raw与5条terminal逐条匹配；
- raw-first提供的是无效/losing/late frame取证和跨artifact半完成状态的fail-closed标记，不提高typed业务值正确性，也不能恢复terminal；当前raw还是SDK解码后的项目重序列化而非wire bytes；
- 新总需求的canonical artifact只保存target-admitted typed projection，因此invalid/unmatched/post-horizon raw与该边界冲突。

用户决定只对未来`polymarket_orderbook_v3` new-write取代以下旧语义：

1. candidate routing不等于admission；必须先完成target lifecycle relevance和market/condition/nullable slug/exact assets/outcomes/winner完整校验。
2. qualified消息才写一条typed source row，并从同一个validated object派生terminal-once和WS terminal metadata source fields。
3. 完全unmatched写0条且不改变状态；target-associated contradiction在row admission、claim和transition前fail closed并写0条。既有有界gap/error/coverage只能保留稳定reason code、已定义的最小身份坐标、计数和时间，不得携带完整/截断frame、重序列化JSON、payload digest或等价可恢复字段集合。
4. Gamma-first accepted drain中的agreeing WS只写validated typed non-owning observation；矛盾WS仍fail closed；role-exit后写0条，不再保留post-horizon raw-only corroboration。
5. v3 restart的合法完成态包括claimant typed row + 唯一WS terminal、无WS row或仅有role-exit前validated non-owning observations + 唯一Gamma terminal，以及0条`market_resolved` source row + 唯一合法exhausted summary及其一致的finalized attempts；terminal/exhausted均不恢复pending或未来slot。
6. v3 restart以validated typed fields、唯一claimant admission/source reference、metadata authority和finalized manifest facts为准；typed source-only、typed+exhausted、late-after-role-exit、terminal+summary、重复终态或typed contradiction均fail closed，不补metadata。
7. 不新增rejected-row、quarantine、sidecar、第二artifact或generic evidence blob。若未来要求完整拒绝帧取证，必须另立需求并由用户决定。

历史边界不回写：

- 本changelog此前记录的raw-first设计、代码、crash tests、5 raw/5 terminal AC-12和当时审查结论继续作为2026-07-23真实历史；
- 当前v2 Rust/schema/tests在另获总计划阶段3实施授权并完成切换前保持不变；切换后v2只读兼容，不能授权v3 recovery；
- metadata v3自身的WS/Gamma source payload/digest traceability继续保留，但不等于orderbook source raw-first，也不再作为orderbook payload/digest recovery依赖；
- 本次只修改demand/plan/changelog，没有修改Rust、Cargo/vendor、schema、tests、数据或服务，也没有构建、网络probe、commit、push、release、部署、远端操作、migration或删除。

两路独立只读文档复审代理进一步发现：旧resolution计划存在重复v3执行游标；六份文档遗漏合法的exhausted-summary-only完成态；gap/error/coverage可能成为frame/JSON/digest/recovery旁路；历史第6、8项的日期及AC-12范围表述过宽。主代理据此完成修正，明确AC-12只证明5条有效raw与5条terminal逐条匹配。修正后两路最终复审均为`Critical 0 / Important 0 / Minor 0 / PASS`；复审代理未直接修改文件或运行构建/测试，修正未扩大授权。

## 2026-07-25：用户批准 metadata v4 claimant schema

用户回复“同意你的推荐”，批准`METADATA_V4_CLAIMANT_SCHEMA_APPROVED_2026-07-25`。本次决定解决未来source v3 claimant recovery不能修改既有metadata v3、也不能用digest/时间/物理行位置模糊关联的版本冲突：

1. 新增`polymarket_market_metadata_v4`，继续使用`source_kind=polymarket_market_metadata`及同一artifact、writer owner、partition和manifest mapping，不新增artifact、sidecar、digest、第二writer或dual-write。
2. metadata v4以metadata v3全部字段、顺序、类型、nullability和语义为不可变前缀，只在末尾追加nullable `ws_claimant_source_sequence_epoch_id: Utf8`与`ws_claimant_collector_admission_sequence: UInt64`。
3. 仅WS-owned `resolution_terminal_ws`两列同时必填，epoch非空且sequence>0；Gamma-owned terminal、`post_terminal_claim_observation`、snapshot、transition、summary和non-owning late WS均同时为null。
4. claimant pair必须精确指向一条finalized、manifest-verified source v3 `market_resolved` row，并核对event/market/slug/assets/outcomes/winner与receive/admission因果事实；0条、多条、partial pair、identity/winner矛盾、running/unknown/corrupt artifact均fail closed。
5. metadata v3 schema/version立即冻结，activation前仍是current metadata writer，activation后只读；它不能作为source v3 claimant authority。
6. current new-write只能在同一activation diff中由source v2 + metadata v3原子切换为source v3 + metadata v4。source v3 + metadata v3、source v2 + metadata v4、dual-write和半切换均禁止。

resolution demand与本计划只增加到SDK070总计划阶段3的映射，不复制第二套实现或审查游标。该批准只授权文档同步；未修改Rust、Cargo/vendor、schema、tests或数据，未执行构建、网络probe、commit、push、release、部署、migration、服务操作或删除。

## 2026-07-25：持续本地执行授权取代后续重复授权门

用户明确批准SDK070总事项持续推进直至完成，本事项future source v3 validate-first
与metadata v4 claimant映射所需的本地Rust/schema/test、构建、审查、commit和push
不再逐次申请。该决定只取代前两节在各自时点记录的“实施未授权”当前门，不回写
它们当时只批准文档的历史事实，也不把尚未执行的Stage3写成完成。

current source v2 + metadata v3继续有效到SDK070 Stage3原子activation门通过；
production activation/cutover、release、部署、远端操作、migration和删除仍不在
持续授权范围。

## 2026-07-27：Attempt3暴露Gamma `closedTime`真实格式缺口，用户批准方案A

- 完整training collector Attempt3在Gamma attempt ordinal 5收到顶层
  `closedTime="2026-07-27 05:25:52+00"`；同一response的`closed=true`、
  `acceptingOrders=false`、`umaResolutionStatus=resolved`和one-hot outcome均成立。
  市场结束为`2026-07-27T05:25:00Z`，该值表示`E+52s`。
- pinned SDK已把response成功解码为`Market`并将顶层`closed_time`暴露为
  `Option<String>`。项目共享helper只调用`DateTime::parse_from_rfc3339`，因此把这一
  无歧义UTC序列化误判为`closed_time_parse_error + closed_time_invalid`；既有actor按
  已批准合同先持久化attempt row再fail closed，导致Attempt3提前结束。
- 权威需求只冻结“存在、可解析且不早于E”，没有冻结RFC3339-only。一旦真实parse error
  成立，现有fatal行为正确；本次错误在于输入格式覆盖不足，不是应把integrity error降级。
- 用户批准方案A：保留RFC3339第一路径，只在其失败后把精确`+00`后缀规范化为
  `+00:00`并再次交给同一Chrono parser；持久化raw保持原样。无时区、其他未批准格式、
  非法日期及`closedTime < E`继续fail closed。不得使用nested event时间兜底、raw REST、
  第二parser authority或availability降级。
- 修复只允许修改共享helper和一个精确`polymarket_contract`回归；不修改SDK、schema、
  evaluator、adapter、fatal路径或旧Attempt3 artifact，不新增格式矩阵、100-cycle或资源
  验证。authority与Rust分离发布，修复后由RTDS专项从新clean/pushed HEAD执行完整六源
  8小时Attempt4。

## 后续 changelog 规则

阶段1至阶段4现已获授权；后续每个阶段必须追加记录：

- 用户逐项决定与批准范围；
- 与需求/计划不一致的真实事实、影响、选项和最终决定；
- RED测试、实现diff和验证命令/结果；
- SDK能力或类型缺口；
- 独立阶段审查问题、修复和复审结果；
- commit/push仅在已授权时记录；
- 本轮已授权的本地香港VPN AC-12执行后记录完整artifact、binary/commit/sha256、时间线和限制。
