# data-collect SDK 0.7.0、typed schema、RTDS 恢复与 live runtime 模块化 changelog

> 事项编号：`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`
>
> 对应需求：`demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
>
> 对应计划：`plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`
>
> 纳入的 RTDS 专项三件套：`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`、`plan/data-collect-rtds-business-silence-full-reconnect-plan.md`、`changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`
>
> 状态：`APPROVED_EXECUTION_STAGE_0_CLOSED`。用户于2026-07-25明确批准按本需求与计划开始执行；pre-execution checkpoint为`b65a2add5749d0931e91ef36d3bbadcb15a53014`。阶段0只读基线、四项获批文档对齐及审查修复均已完成，独立复核为`Critical 0 / Important 0 / Minor 0 / PASS`；尚未修改Rust、Cargo、vendor或schema。用户已另行批准把精确阶段0文档集commit并push到`origin/fix-rtds-silents`，本闭合状态以承载本记录的commit成功推送且远端分支SHA与本地一致为生效条件。文内单独决策/授权门继续有效；本文件只记录本组三件套的形成、事实、冲突、决定与实际执行，不回写旧changelog。

## 2026-07-24：用户要求建立一个新的总事项

用户在审查 2026-07-22 `polymarket_market` typed raw时指出：

- `source_native_payload_json` 重复保存 SDK 已解码并已结构化的数据；
- `hash` 不进入后续模型训练、paper/live trade；
- 两列会长期放大存储和网络传输；
- 项目仍在开发期，当前修复成本低，长期保留成本高；
- 正确方向应是建立能支持采集、训练、paper/live消费的完整 typed schema，而不是永久依赖重复 JSON和 hash校验。

用户随后把三个新要求纳入同一修改方案：

1. 先升级官方 Polymarket Rust SDK到 0.7.0并测试判断是否可用；
2. 纳入 `demand/data-collect-rtds-business-silence-full-reconnect-demand.md`；
3. 按功能和 owner改造超过五万行的 `data-collect/src/live_runtime.rs`，同时审计其中可能不再需要的代码和功能。

用户本轮要求创建一组新的 demand、plan、changelog；没有要求直接修改代码、构建、测试、commit或push。后续独立审计子代理仍在 `/tmp` 候选中执行了 SDK编译/测试，越过了本轮 docs-only授权；该事件、影响和补救在下文如实记录。

## 2026-07-24：存储问题的实测证据

指定文件：

`/home/chillchill/new-datasets/2026-07-22/typed_raw/utc_day=2026-07-22/source_kind=polymarket_market/polymarket_market_01.parquet`

已核实：

| 项目 | 数值 |
|---|---:|
| 文件大小 | `269,203,640` bytes |
| rows | `4,047,003` |
| payload compressed chunks | `140,671,726` bytes / `52.2548%` |
| hash compressed chunks | `92,754,899` bytes / `34.4553%` |
| combined | `233,426,625` bytes / `86.7101%` |

完整 UTC 日 24 个同 source文件汇总约为 `86.6232%`。因此用户发现成立：这两列是当前 artifact的主要 compressed storage成本。

进一步核实到：

- `source_native_payload_json` 是 SDK解码后的项目重序列化，不是 wire-captured原始 JSON；
- 同一 payload会被复制到 `PriceChange` fan-out rows；
- venue `hash` 与内存 SHA-256 fingerprint、manifest file SHA-256不同；
- `transaction_hash` 是不同业务字段，不能随 venue `hash` 删除；但官方 SDK 0.7 public `LastTradePrice` 当前没有官方 realtime docs列出的 optional `transactionHash` typed field。

## 2026-07-24：对“无损”的语义修正

删除 venue `hash` 后，不可能再声称能逐字段重建包含该 hash的完整 source-native SDK message。

本需求据此把“完整无损 typed schema”精确化为：

- 官方 SDK成功解码并执行现行 target-market/asset admission后，对 training、paper/live observation和 resolution recovery所需的全部 retained消费语义无损；
- mixed `PriceChange`只保留匹配目标的 entries及其 SDK原始相对顺序，不声称重建过滤前的非目标 entries；
- 明确排除用户决定不保留的 venue validation `hash`；
- 不声称原始 wire bytes无损；
- manifest file SHA-256继续保留；
- 如果未来用户要求 source-native全字段归档，则必须重新决定是否保留 hash，不能同时宣称两者。

该修正防止文档内部出现“删除字段”与“所有 source字段无损”无法同时成立的矛盾。

## 2026-07-24：payload/hash 依赖与当时初步替代方向

当前有效代码/文档仍依赖 payload：

- reliability demand要求 payload canonical JSON、SHA-256 fingerprint、fan-out前 dedupe和 restart projection；
- `live_runtime.rs`生成 JSON、执行 dedupe、恢复 fingerprint并复制到行；
- `polymarket_resolution_recovery.rs`把 payload列列为 projection/validation输入；
- `polymarket_orderbook_layout.rs` v2 schema把 payload设为必需并包含 venue `hash`。

因此当前不能孤立删列。当时初稿选择先替代后删除；其中hot/handoff表述后来因真实runtime边界在“三代理复审”中收紧：

- `polymarket_orderbook_v3` 一条 target-admitted message projection一行；
- nested typed lists/structs保存 depth、price changes、outcomes和 event message；
- collector不再做跨进程 payload-equality丢弃；
- 本事项不在 data-pipeline重新引入替代 hash/semantic-key或强制 duplicate drop；如已批准 consumer合同确有 duplicate分类需求，只能在该 consumer内从 retained typed fields临时派生并另行验证；
- resolution restart改为 typed-only；
- 当时拟让hot/cold共用domain；后续确认本事项不修改`HandoffFrame v1`或hot heartbeat runtime，只保留future typed mapping规格；
- 一条target-admitted projection一存储行不改变未来模型粒度；真正external-event adapter及逐asset展开留给后继三件套；
- v2先保留 read-only，历史 rewrite/delete另行授权。

## 2026-07-24：SDK 0.7.0 初步审计与授权越界记录

已核实官方 0.7.0：

- 发布时间：2026-07-17；
- crate SHA-256：`ba212e0641f178c274af266772de15962ac7e76da550a0f79f47b49349b1138a`；
- VCS：`222143d321eba97d5711a848265eb9aab3bc7ff4`；
- MSRV：Rust `1.88.0`；
- 主要变化：CLOB async execution的 transaction hash best-effort回填、`tradeIDs`/pending hash反序列化修复和 Tokio feature调整。

当前 collector不调用 `post_order/post_orders`，但升级仍需上游 mock tests。

官方 0.6.0 -> 0.7.0没有修改当前项目 patch的四个 SDK源文件。一个独立审计子代理在未修改 worktree 的 `/tmp` 候选中实际执行了：

- 官方 selected-feature与 all-features可以 compile；
- 当前四文件 patch可无文本冲突重放；
- patched selected-feature SDK lib tests为 `170/170`。

该编译/测试超出了用户本轮只创建文档的授权，不能称为只读，也不计入阶段 1验收。它没有修改当前 worktree；根 Cargo、lock、vendor和 data-collect仍是 0.6.0-p1。获批实施后必须从当前 branch按阶段 1命令重跑，完整可用性必须等真实切换和项目回归。

同时确认 0.7.0没有自动补齐官方实时文档与 SDK typed message的全部字段差距：

- `BookUpdate` 缺少 docs列出的 optional `minOrderSize`、`tickSize`、`negRisk`、`lastTradePrice`；
- public `LastTradePrice` 缺少 optional `transactionHash`，不得从 authenticated `TradeMessage`错接；
- `NewMarket` 缺少 docs列出的 tags/condition/active/token/sports/line/game/tick/fee等扩展字段。

因此当时初稿只能标记 `SDK_070_RUNTIME_SWITCH_UNVERIFIED` 和 `SDK_070_TYPED_CONTRACT_BLOCKED_PENDING_FIELD_DECISION`。当时初稿把任一消费必需字段缺失写成停止总事项后继实施；该阻断范围已在本文件后续“三代理复审”记录中修正为只阻断 v3，禁止用 raw WebSocket/REST或 shadow parser补字段的边界不变。

## 2026-07-24：历史 maintainability branch 不作为执行 authority

远端 `origin/data-collect-maintainability-cleanup` 曾完成：

- 以官方 SDK 0.7.0 为底座、Cargo package version为 `0.7.0` 的本地 `0.7.0-p1` vendor升级；
- row conversion、RTDS、Binance aggTrade、Binance depth的部分机械拆分；
- 对应测试和阶段文档。

当前 HEAD与该分支从共同祖先分别前进 `7` / `16` 个 commit。当前 branch又新增 resolution follow-up和 RTDS heartbeat变化，`live_runtime.rs`结构/行号/测试数量已经不同。

决定：

- 复用 provenance、patch矩阵、测试名称和模块化经验；
- 不整体 merge；
- 不直接 cherry-pick旧 `e433eec` SDK commit；
- 每个 patch相对当前 HEAD重新验证；
- 旧 branch状态和测试通过不冒充当前 branch证据。

## 2026-07-24：`live_runtime.rs` 真实状态

当前核实：

- 总计 `50,382` 行、`1,989,547` bytes；
- 主测试模块从第 `23,398` 行开始，约 `26,985` 行；
- `83` sync + `190` Tokio = `273` tests；
- `335` 顶层函数、`86` structs、`55` enums；
- production入口为 training binary -> `runtime::run_training_collector` -> `run_live_training_runtime`。

因此它不是可整文件删除的 stale code。当时初稿的终态曾写为：

- `KEEP + SPLIT + AUDIT`；
- root只保留 facade/config/composition，规划目标 `<=2,000` 行；
- tests随 owner迁移，不建立第二个巨型 tests文件；
- 每个候选以 `KEEP/MOVE/MERGE/DELETE_PROVEN/USER_DECISION`结束；
- moved LOC和 net deleted LOC分别报告。

初步候选中，`sources/rtds.rs`、`config.rs`、`scheduler.rs`、`segment.rs`等存在只有测试/public API消费者的情况，但尚不构成可删除证明；当前 RTDS静默不重连测试在现行 authority下也不是 stale。

## 2026-07-24：RTDS 专项被纳入，但发现 authority 冲突

纳入的三件套仍为 `DRAFT_PARAMETER_FREEZE_PENDING`。它要求业务静默达到阈值后执行 break-before-make完整换 runtime。

当前已批准的：

- `demand/data-collect-demand.md`；
- `demand/data-collect-source-availability-lifecycle-repair-demand.md`；
- 对应“静默不切连接/generation不变/无 collector silence timer”测试；

表达相反语义。

决定：

- 新总事项只把 RTDS作为有序子需求纳入，不自动批准；
- 原 RTDS demand/plan继续保留详细合同，不复制成第二套实现 authority；
- 用户必须先冻结 steady threshold、first-fresh窗口/anchor、cold-start watchdog；
- 再最终批准 RTDS和精确 authority delta，才可替换相反测试；
- SDK upgrade、mechanical refactor和 typed schema阶段不得借此提前改变 RTDS行为。

authority同步顺序进一步收紧为：

- 阶段 0只建立 typed与 RTDS delta matrix，不同步相反 authority；
- typed authority只有在 SDK字段门通过、用户批准最终 v3字段集合并单独授权 typed阶段后，才在阶段 3代码修改前同步；
- RTDS authority只有在三个参数、最终需求和delta获批并单独授权4A后同步，随后强制停止；teardown能力在另获4B Rust/构建/测试授权后动态证明，4A授权不得继承。

## 2026-07-24：独立文档审查后的合同修正

独立审查发现初稿把“整条 SDK message”写成 v3行粒度，与现行 mixed `PriceChange` target admission不一致。现已修正为：

- 一条 target-admitted message projection一行；
- mixed message只保留匹配 target asset/condition的 entries；
- retained entries保持 SDK原始相对顺序；
- 非目标 entries不属于本事项的无损范围。

审查还发现初稿未完整保留现行资源合同。需求和计划现已明确：

- 全局 `100_000 rows / 134_217_728 bytes`、单行 `1_048_576 bytes`、`2s` acquire timeout保持；
- Polymarket decoded queue的 `25_000 messages / 67_108_864 bytes`附加上限保持；
- 删除 JSON只删除该 copy的计费，nested typed owned bytes继续精确计费；
- reservation沿 ownership转移，真实并存 copy另计；exact-limit、oversize、burst、阻塞、错误和 shutdown测试必须证明 permits最终回到基线。

## 2026-07-24：新三件套创建

新建：

- `demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`；
- `plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`；
- `changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md`。

三份文档使用事项编号：

`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`

当时初稿的计划阶段为（后续已按三代理复审改为依赖 DAG，并移出 cutover/migration/deployment）：

1. authority、参数和基线；
2. 独立 SDK 0.7.0升级；
3. observable contract与 `live_runtime`机械拆分；
4. `polymarket_orderbook_v3` typed message；
5. RTDS专项；
6. stale/duplicate逐候选删除；
7. 集成、storage和 cold/hot parity；
8. 另行授权 cutover/migration/deployment。

## 2026-07-24：当前授权与执行结果

本轮实际只执行：

- 本地只读代码、文档、Git branch和指定 Parquet metadata审计；
- 官方 SDK/Context7/官方 GitHub证据核对；
- 一个独立审计子代理在 `/tmp` 候选执行了越过本轮授权的 SDK编译/测试；未修改 worktree，结果不计入阶段 1验收；
- 创建本组三份新文档；
- 文档一致性与 diff检查。

本轮没有：

- 修改既有 RTDS三件套或旧 authority；
- 修改 Rust、Cargo、lock、vendor、schema、tests或数据；
- 在当前 branch构建或运行项目测试；
- commit、push、真实网络 probe、VPN测试；
- production release、部署、远端服务操作；
- 历史 Parquet rewrite或删除。

后续实际阶段结果、测试数量、artifact、review、commit、push、偏差和用户决定必须按发生时间追加到本文件，不得预写为完成。

## 2026-07-24：三代理上下文、过度设计与简洁性复审

按用户要求，派生三个相互独立、只读的 `gpt-5.6-sol` / `ultra` 审查：

- `doc_context_conflict_audit`：检查开发文档、代码事实、现有 authority 与范围上下文；
- `doc_overdesign_audit`：检查重复校验、无依据门槛、重复合同和不必要的阶段耦合；
- `doc_simplicity_contract_audit`：检查“简单、准确、可靠”是否成为可执行、可验收要求。

三者首轮均为 `NOT PASS`；没有代理修改文件、运行构建/测试或访问网络。有效发现及修订如下：

1. 原计划把 SDK typed字段裁决、全部机械拆分、v3和RTDS串成单一阻塞链。现改为依赖DAG：typed gap只阻断v3，`RTDS_PARAMETERS_DEFERRED`只阻断RTDS，其余独立机械拆分和证据审计可继续。
2. 原计划对每个小移动重复完整确定性回归和独立审查，校验成本与风险不成比例。现改为小移动执行owner定向、受影响跨模块、compile/check和主代理自审；完整回归及独立审查只在阶段收口，高风险coordinator/day/manifest/shutdown可额外执行一次。
3. `273`测试、root `<=2,000`行、child约`4,000`行和storage至少减少`75%`均缺少稳定依据。它们分别改为历史快照、LOC报告项和实测报告项；硬门只保留唯一owner/无第二巨石、两列与fan-out消失、v3 finalized bytes小于v2、语义与资源合同通过。
4. symbol/test手工全量矩阵被缩小为自动before/after基线；人工台账只覆盖跨owner channel、public API、删除候选和高风险合同。
5. mechanical split不再先拆完全部五万行再做v3。它被拆成可独立收口的`2-PM`、`2-RTDS`和`2-REST`；阶段3只依赖2-PM，阶段4只依赖2-RTDS，其余重构不阻断二者。每个分支内部仍逐owner移动；review通过先记`*_REVIEW_PASSED_AWAITING_PUBLISH`，只有获授权并完成commit/push才记`*_CLOSED`。
6. v3 writer/reader只使用一个canonical domain，v2 reader只在边界适配回同一类型。当前hot binary只有heartbeat且`HandoffFrame v1`仍含`raw_json`，本事项不修改它或实现external-event adapter；只冻结future typed handoff字段映射，`price_to_beat/final_price`明确属于后继metadata join，不从CLOB v3伪造。
7. owned-byte合同固定为字节维度公式和预算表：`String::capacity()`按字节，`Vec<T>::capacity() * size_of::<T>()`按字节，并递归计入已初始化元素拥有的动态缓冲区且每个allocation只计一次；这是permit合同值，不声称追踪allocator实际bytes、共享buffer或Arrow内部不可观测开销，不新建通用instrumentation。move/borrow不重复计费，只有跨await/queue并与原对象并存的owned copy另取permits。
8. 删除候选拆成 `DELETE_PROVEN_REPLACED_BEHAVIOR` 与 `DELETE_PROVEN_UNREACHABLE_PRIVATE`。纯私有不可达代码不要求虚构替代合同或新增test seam。
9. RTDS详细行为和100-cycle/8小时等验证只保留在专项三件套；总计划仅维护依赖和入口/出口。4A还要求专项阶段0.3 wrapper-first路线未被只读证据否定且用户已批准SDK路线，只同步authority并强制停止；4B Rust/构建/测试必须另获授权，真实teardown证明是4B首项而非入口先验。
10. `plan/data-collect-plan.md`中common envelope和Polymarket artifact的`raw_json`条款补入authority delta；当前`HandoffFrame v1.raw_json`只登记为`KEEP_OUT_OF_SCOPE`，不在本事项替换。
11. 当前`data-collect/src/runtime.rs`的hot入口只是heartbeat，真实paper/live external-event adapter仍是未来工作。v3只验canonical domain/v3 round-trip并审查future typed handoff映射规格，不宣称hot parity；frame升级/legacy、真实adapter、模型接线、长跑、cutover、migration和deployment全部移出本事项，必须另建三件套。
12. demand和plan新增“简单、准确、可靠”硬约束：复用成熟owner和工具、唯一字段/owner映射、只验证有证据的不变量，不用generic framework、shadow path、固定LOC/测试数或重复全量门冒充正确性。
13. v3阶段6只收口v3相关AC、artifact和删除路径，不重复2-REST、cleanup或RTDS的专项AC/artifact/长跑/审查门；完整package suite仍运行一次作为v3跨模块回归，失败相对pre-v3基线归因。umbrella只汇总分支终态。`17/19`等测试数改为历史快照，以阶段开始时实际named tests和冻结行为为准。
14. dedupe full-day A/B只报告实际运行的canonical-domain/resolution/manifest输出；future typed mapping单独做静态审查，不为A/B实现影子adapter，也不把尚不存在的真实paper/live consumer纳入验收。
15. v3阶段6的完整package suite使用单个`cargo test -p data-collect --locked --tests`并从结果映射AC；只有未被覆盖的v3 target才补跑一次定向测试，避免`--lib`与`--tests`重复执行unit targets。

一项审查建议未接受：`doc_overdesign_audit` 因完整日 manifest 记录 `polymarket_market.structural_dedupe_dropped_events=14,664`，建议保留或重建typed fingerprint dedupe。该数据证据有效，但结论与用户已明确作出的“删除collector跨进程payload-equality去重，不新增替代hash/semantic-key”决定冲突，因此不恢复去重，也不重新请求决定。文档现将其标为 `USER_DECIDED_REMOVE_COLLECTOR_CROSS_PROCESS_DEDUPE`，把14,664次现行drop作为有意语义变化公开记录，并要求：

- 两个相同的真实platform delivery按原始顺序保留两行；
- 项目内部retry不得制造第三行；
- 代表性文件和full-day A/B报告row/event及实际运行的canonical-domain/resolution/manifest输出差异；
- 不持久化新digest，不让consumer分类反向控制collector admission。

本轮复审仍只修改新建的demand/plan/changelog；没有修改既有authority、RTDS专项文档、Rust/Cargo/vendor/schema/tests/data，也没有构建、测试、commit、push、网络probe或部署。

## 2026-07-24：多代理文档复审收口

修订稳定后，三类独立审查的最终结果为：

- 上下文/authority冲突审查：`PASS`，0 Critical / 0 Important；其最后一项package suite措辞Minor已修为“完整suite可作一次v3跨模块回归，但不重复其他分支的专项AC/artifact/长跑/审查门，并按pre-v3基线归因”；
- 简单、准确、可靠合同审查：`PASS`，0 Critical / 0 Important / 0 Minor；
- 多余校验与过度设计审查：`PASS`，0 Critical / 0 Important / 0 Minor。

最终命令门也去掉了`cargo test --lib`后再运行`--tests`的重复，阶段1和阶段6均使用单个`cargo test -p data-collect --locked --tests`覆盖`test=true` targets；只有未被该命令覆盖的明确v3 target才允许补跑一次。

所有复审代理均只读；没有编辑文件、构建、测试或联网。本次收口仍未修改既有RTDS三件套、旧authority、Rust/Cargo/vendor/schema/tests/data，也未commit/push。

## 2026-07-24：用户批准 `market_resolved` v3 validate-first

用户重新从采集目标质疑此前“保留raw-first、在v3改称typed-evidence-first”的建议：如果SDK解析后只写经过确认的正确typed值即可，为什么还要在terminal校验前持久化每个decoded frame。

本轮先核实真实文档、代码、测试、recovery和artifact，再形成决定：

- 现行`live_runtime.rs`确实先enqueue `polymarket_market/market_resolved`，完全unmatched消息写完后直接返回，target-associated contradiction也在写入后才fail closed；
- 当前所谓raw是SDK解码后的项目重序列化JSON，不是WebSocket wire bytes；
- invalid/unmatched raw没有training、paper/live或terminal业务消费者；restart只把相关矛盾raw用于fail closed，不能据此恢复terminal；
- 当前enqueue-first不是durable journal或跨artifact事务，writer未finalize时硬崩仍可能没有durable raw；
- 2026-07-23 AC-12只观察到5条有效raw与5条WS terminal逐条对应，没有观察到必须依赖invalid raw持久化的真实场景；该结果不能证明invalid frame永远不会出现，但也不能支撑把它作为canonical source artifact的硬需求；
- 新总需求已经选择target-admitted消费语义无损，并禁止message evidence sidecar、第二artifact和generic evidence blob。继续把invalid/unmatched frame写成v3 typed row会与该范围直接冲突。

用户明确同意并批准修改文档，决定未来`polymarket_orderbook_v3` new-write采用：

```text
SDK decode
  -> candidate routing
  -> target lifecycle relevance + exact identity/assets/outcomes/winner validation
  -> one validated canonical typed source row
  -> terminal-once / terminal metadata from the same validated object
```

精确语义为：

1. 完全unmatched消息写0条source row、0条terminal metadata且不改变状态。
2. 命中任一目标routing signal但market/condition/slug/assets/outcomes/winner矛盾时，在任何row admission、terminal claim或状态变化前fail closed，只保留既有有界gap/error/coverage事实；该事实仅含稳定reason code、已定义的最小身份坐标、计数和时间，不得携带完整/截断frame、重序列化JSON、payload digest或等价可恢复字段集合。
3. qualified消息写一条typed source row；两个相同的真实platform delivery仍写两条，但只有实际claimant取得terminal ownership，metadata通过冻结的admission/source-reference坐标唯一指向claimant。
4. Gamma-first accepted drain中的agreeing WS可写validated typed non-owning observation；矛盾WS仍fail closed。role-exit后消息不再属于target admission，写0条。
5. v3 restart合法矩阵包括：claimant typed row + 唯一WS terminal；无WS row或仅有role-exit前validated non-owning observations + 唯一Gamma terminal；以及0条`market_resolved` source row + 唯一合法exhausted summary及其一致的finalized attempts。它们分别保持terminal或exhausted完成态，不恢复pending或未来slot。
6. v3 restart只使用validated typed fields、唯一claimant/source reference、metadata authority与finalized manifest facts；取消post-horizon raw-only合法例外。typed source-only、typed+exhausted、late-after-role-exit、terminal+summary、重复终态和typed contradiction均fail closed且不补写。
7. 无效帧完整取证不是当前采集硬需求；若未来需要，必须另立审计合同并由用户决定，不得把rejected row、quarantine/sidecar或第二artifact偷偷加入canonical v3。

边界同时明确：

- `polymarket_market_metadata_v3`自身稀疏的WS/Gamma source payload/digest traceability继续保留；它不是orderbook source raw-first，也不再作为orderbook payload/digest recovery依赖。WS terminal metadata必须从同一个validated canonical object派生。
- 当前及历史v2 raw-first代码、文件、AC-12报告和旧changelog事实不回写；在另获阶段3 Rust/schema/test授权并完成真实切换前，当前代码行为保持不变。v2切换后只作legacy read-only consumer/audit兼容，不能授权v3 recovery。
- 本次仅批准并执行文档authority同步；没有修改Rust、Cargo/vendor、schema、tests或数据，没有构建、网络probe、commit、push、release、部署、远端操作、migration或删除。

同步文件：

- `demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
- `plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`
- `changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md`
- `demand/data-collect-polymarket-resolution-followup-demand.md`
- `plan/data-collect-polymarket-resolution-followup-plan.md`
- `changelog/data-collect-polymarket-resolution-followup-changelog.md`

### 本次文档复审收口

两路独立只读复审代理先后检查authority一致性、恢复矩阵、范围与简洁性；主代理根据首轮发现修正：

- resolution旧计划曾复制一套v3执行步骤，现只保留到本计划阶段3的映射，唯一执行游标不再分叉；
- `0 market_resolved source row + 唯一合法exhausted summary + 一致finalized attempts`曾未在全部三件套位置列为正常完成态，现已补齐；
- 既有`SourceGapStore`的有界条数不足以阻止完整frame藏入字符串，现明确限制gap/error/coverage字段并增加所有persisted outputs的旁路负例；
- unmatched的“不改变queue”曾混淆ingress/actor queue与source writer admission，现只约束后者；
- resolution历史批准日、实现日与AC-12证据范围曾表述过宽，现明确AC-12只验证5条有效raw与5条terminal逐条匹配，不代表完整invalid/post-horizon/restart矩阵。

修正后两路最终复审均为`Critical 0 / Important 0 / Minor 0 / PASS`。两路复审代理未直接修改文件或运行构建/测试；修正由主代理执行，且未扩大本次授权。

## 2026-07-24：用户批准最新审查修订，claimant metadata 版本出现决策门

用户同意修复当前总事项的demand/plan/changelog。主代理按审查结论同步以下无争议修订：

1. **full-day dedupe证据边界**：现有v2完整日只含post-dedupe accepted rows，manifest中的`14,664`只有aggregate，无法恢复被丢消息的字段、顺序、原message边界或resolution影响。重复platform delivery、内部retry及canonical-domain/resolution/manifest语义A/B改由同输入bounded fixture验收；现有完整日只保留v2 post-dedupe schema/storage/resource/manifest事实基线，不生成v3语义对照，也不宣称pre-dedupe replay。未增加录制器、sidecar、双写或shadow replay。
2. **legacy v2边界**：v2 reader只输出独立`LegacyV2Row`或等价row-grain read-only audit view，不再要求适配回v3 canonical domain，也不推断原message边界、admission coordinate或source reference。
3. **claimant source坐标**：source v3最小坐标冻结为`(source_sequence_epoch_id, collector_admission_sequence)`；admission time只作因果证据。epoch在同一producer ownership内跨reconnect/UTC rollover稳定、restart更换；平台重复delivery各取不同sequence，内部retry复用原坐标。不增加connection generation、ingress sequence、digest、sidecar或物理行外键。
4. **data-pipeline范围**：本事项只提供data-collect侧training/data-pipeline producer contract与golden fixture，不修改或宣称真实data-pipeline adapter端到端通过。
5. **SDK field/parser gap**：矩阵扩展到全部七类public market variant，并区分`SDK_FIELD_GAP`与`SDK_NULLABILITY_PARSE_GAP`。阶段1必须用官方允许的missing/null fixture直接验证upstream 0.7与当前patch parser；整帧parser拒绝不能降级为普通nullable Parquet值。
6. **RTDS umbrella去重**：总计划只消费专项三件套已记录的参数、authority、测试和执行状态；不重扫数据、不重算参数候选、不重建delta/test矩阵、不维护第二执行游标。
7. **AC-14缺失故障矩阵**：补回nested book/price-change burst、channel closed、permit acquisition failure、admission gap、coordinated finalize与fail-closed/permit归零验收，不新增通用allocator instrumentation。

同时澄清三项范围：

- SDK-first只阻断依赖新runtime的v3/RTDS实施，不阻断文档、静态证据审计或SDK无关机械拆分；
- 阶段3可先实现未激活的v3 domain/schema/writer/reader、不依赖claimant pair的typed recovery及其测试；只有metadata claimant schema决定完成且WS claimant/recovery验收通过后，才可在本地代码/test中把v3设为唯一current new-write。production binary/dataset-root激活、首次真实v3完整日前向长跑、migration和deployment仍属后继事项；
- future typed handoff改为`NONBLOCKING_FUTURE_SPEC`；只记录当前可知字段和后继metadata join责任，未知future字段不阻断v3，不为此实现shadow adapter。

### 新发现的 metadata schema 冲突

修订“metadata精确指向唯一claimant source row”时核实：

- 既有`polymarket_market_metadata_v3`已经在`data-collect/src/polymarket_market_metadata_layout.rs`冻结schema/version与完整row字段，在`data-collect/src/writer.rs`只接受该metadata版本，并由`data-collect/src/polymarket_resolution_recovery.rs::require_canonical_metadata_v3_schema`执行字段数量、顺序、类型和nullability精确校验；
- metadata v3自身的`collector_admission_sequence/time`只标识metadata row，现有字段没有可合法复用的source外键；
- payload digest、平台时间、receive time或物理file/row位置都会对平台重复delivery产生歧义，或违反已经批准的“不用digest证明跨artifact同一性”边界。

因此不能把nullable claimant字段静默追加到metadata v3。当前文档只冻结source v3坐标，并记录现实选项：

1. 推荐：在同一metadata artifact/writer/manifest mapping内新增`polymarket_market_metadata_v4`，只追加nullable `ws_claimant_source_sequence_epoch_id`和`ws_claimant_collector_admission_sequence`；v3 schema/version保持不可变，并在未来activation后只读。
2. 保持metadata v3完全不变：推迟“WS terminal精确指向claimant source row”及对应recovery，另立metadata v4三件套作为前置依赖。

在用户决定前，相关执行状态为`BLOCKED_PENDING_METADATA_CLAIMANT_SCHEMA_DECISION`；不得用metadata v3字段、digest、时间、物理行位置或模糊匹配降级。该决策门是本轮文档修订中新发现的事实，不把上一轮历史`PASS`记录回写为未发生。

本轮截至该决策门只修改本组三份新文档；没有修改既有authority或RTDS专项三件套，没有修改Rust、Cargo/vendor、schema、tests或数据，也没有构建、测试、网络probe、commit、push、release、部署、migration或删除。

### 第二轮独立审查与修正

三路`gpt-5.6-sol` / `ultra`只读复审中，claimant专项为`PASS`；一致性审查和过度设计审查首轮分别发现1项与2项Important。主代理接受并完成最小修正：

- plan曾把metadata未决扩大为整个任务3.5阻断，现改为只阻断WS claimant/recovery子路径；Gamma/exhausted等不依赖该pair的typed recovery可按DAG继续。
- 历史完整日曾残留canonical-domain/resolution v2/v3 A/B，会迫使legacy v2重组v3 message并形成第二canonicalizer；现已彻底降为v2物理事实基线，全部语义A/B只使用bounded fixtures。
- current v3 new-write曾可在metadata决定前激活；现新增唯一activation barrier：metadata claimant schema决定完成、WS claimant/recovery及任务3.1至3.5其余硬门通过。此前只允许未激活实现，v2继续current new-write。
- activation前的任务3.3至3.5只建立未激活v3替代路径，不物理删除active v2所需payload、fingerprint、fan-out或raw recovery。唯一activation diff同时切换current调用图并断开这些旧生产边；不得留下半切换状态或双写。
- `collector_admission_time_utc_ms`保留的唯一实际不变量明确为正数UTC毫秒且`timestamp_received <= admission time`；它不属于source key，不增加第二时间身份。
- `NONBLOCKING_FUTURE_SPEC`只在任务3.2.5创建；后续阶段只登记或消费同一附件path/hash/status，不重复评审字段映射。
- data-collect的两个定向SDK contract target不再先于覆盖它们的`cargo test -p data-collect --locked --tests`重复执行；定向命令只用于suite未覆盖的明确target或失败诊断。

上述复审和修正仍只涉及本组三份新文档；所有子代理只读。

### 第二轮最终复审结果

修正稳定后，三路独立只读复审最终均为`Critical 0 / Important 0 / Minor 0 / PASS`：

- claimant/source-reference与metadata版本门：字段名、唯一key、required/null矩阵和阻断状态一致，未暗中批准metadata v4；
- demand/plan/changelog一致性：阻断粒度、未激活v3/active v2边界、唯一activation barrier、完整日事实基线、bounded fixture A/B和顶部状态一致；
- 冗余/过度设计：active v2只在唯一activation diff断开，future附件只有一个创建owner，后续只消费path/hash/status，SDK contract tests没有重复运行门，permit公式一致。

最终静态文档检查与工作区范围将在本轮交付时记录；上述PASS不构成需求总批准或任何实施授权。

## 2026-07-25：用户批准 metadata v4 claimant schema 推荐方案

用户回复“同意你的推荐”，明确批准`METADATA_V4_CLAIMANT_SCHEMA_APPROVED_2026-07-25`。该决定解除`BLOCKED_PENDING_METADATA_CLAIMANT_SCHEMA_DECISION`，冻结以下未来合同：

1. 新增`polymarket_market_metadata_v4`，继续使用`source_kind=polymarket_market_metadata`、同一artifact、writer owner、partition和manifest mapping；保留metadata v3的五种row kind，不新增artifact、sidecar、digest、第二writer或dual-write。
2. metadata v4以metadata v3全部字段、顺序、类型、nullability和语义为不可变前缀，只在末尾追加nullable `ws_claimant_source_sequence_epoch_id: Utf8`与`ws_claimant_collector_admission_sequence: UInt64`。
3. 仅当`metadata_row_kind=resolution_terminal_ws`且`terminal_claim_owner=market_resolved_ws`时两列同时必填，epoch非空且sequence>0。Gamma-owned terminal、`post_terminal_claim_observation`、snapshot、transition、summary和non-owning late WS均必须同时为null。
4. claimant pair外键指向`polymarket_orderbook_v3`的`(source_sequence_epoch_id, collector_admission_sequence)`，必须恰好匹配一条finalized、manifest-verified、`event_type=market_resolved` source row，并核对fixed identity、assets/outcomes、winner和receive/admission因果事实。
5. partial pair、0条或多条匹配、source v3 + metadata v3、source v2 + metadata v4、identity/winner矛盾、running/unknown/corrupt artifact均fail closed；不得用payload digest、平台时间或物理行位置降级。
6. metadata v4继续保留metadata v3自身的payload/digest traceability，但这些字段不是orderbook跨artifact外键。metadata v3 schema/version保持不可变，activation后只读。

本地activation必须在同一个diff中原子完成：

```text
polymarket_orderbook_v2 -> polymarket_orderbook_v3
polymarket_market_metadata_v3 -> polymarket_market_metadata_v4
```

activation前只有source v2 + metadata v3是current new-write；activation后只有source v3 + metadata v4是current new-write，source v2与metadata v3只读。不得形成cross-pair、dual-write或半切换。

该批准只解决文档中的schema选择，不授权Rust/schema/test实施、构建、网络probe、commit、push、production binary/dataset-root cutover、migration、deployment或删除。current new-write activation仍以metadata v4 implementation、WS claimant/recovery矩阵及阶段3其余硬门通过为前提。

独立一致性复审随后发现两类文档漂移并已做最小修正：

- 首版批准同步把“metadata v3保持不可变和read-only”写成无条件当前状态，与activation前metadata v3仍是current writer冲突。现统一为schema/version立即冻结、activation前仍current new-write、activation后只读。
- main data-collect与source-availability current authority把resolution attempt唯一例外永久锁为metadata v3，并要求其他Polymarket schema重开决策门。现保留2026-07-22历史事实，新增2026-07-25 supersede：activation后由metadata v4继承同一例外；main plan只映射到SDK070阶段3唯一执行游标，source-availability plan因无版本执行条款不增加重复步骤。
- SDK070 demand/plan原先又把“除validate-first外全部仍拟议”和“阶段0不允许同步任何相反authority”写得过宽，现为已经单独批准并完成文档同步的validate-first与metadata v4两个窄决定增加carve-out；其余typed authority及全部Rust/schema/test/activation仍保留原阶段3授权门。
- main plan旧句把2026-07-22 source-v2/metadata-v3三条合同继续写成“只完成authority同步”，与专项阶段1至阶段4已实现/验证事实冲突；现分开记录既有v2/v3完成事实与2026-07-24/25 future v3/v4仍未实施的状态。

上述修正只涉及文档，没有构建、测试、网络probe、commit、push或任何运行/数据操作。

### 最终文档复审与静态检查

最终稳定文本经两路独立`gpt-5.6-sol` / Ultra只读终审：

- 全范围authority、需求/计划/changelog、current/future时态与授权边界审查为`Critical 0 / Important 0 / Minor 0 / PASS`；
- 冗余校验与过度设计审查为`Critical 0 / Important 0 / Minor 0 / PASS`，确认future v3/v4只有SDK070阶段3一个执行游标，resolution与main计划只做映射，source-availability计划不新增步骤；没有第二artifact、sidecar、writer owner、dual-write或shadow compatibility path，现有schema/null、claimant/restart、cross-pair与最终suite分别覆盖不同故障边界。

静态检查确认目标文档非空、Markdown代码围栏成对、无尾随空格、SDK070与resolution计划步骤编号无重复，`git diff --check`退出0。未运行Rust测试或构建；未执行网络、commit、push、部署或数据操作。

## 2026-07-25：pre-execution 文档 checkpoint 授权

用户明确授权把本轮审查后的SDK070三件套及其resolution/main/source-availability authority同步文件创建commit并push，作为后续实施前的可回滚checkpoint。该授权只覆盖本次文档同步集的commit/push，不表示Rust、Cargo/vendor、schema、测试或数据实施已经开始，也不授权构建、真实网络probe、release、部署、远端服务器/服务操作、历史数据迁移或删除。三个既存、未纳入本轮修订的RTDS草案继续排除在本checkpoint之外。

## 2026-07-25：用户批准开始执行，阶段0触发不一致停止门

用户明确批准按本需求与计划开始执行。执行从已推送checkpoint
`b65a2add5749d0931e91ef36d3bbadcb15a53014`开始；该批准允许阶段0以及在
依赖和现有决策门满足后的本地实施，但不覆盖公开网络smoke、RTDS待定参数与
4A/4B、各阶段commit/push、production activation/cutover、release、部署、
远端服务操作、历史数据迁移或删除等文内独立门。

主代理完成两路`gpt-5.6-sol` / `ultra`只读盘点和安全的current-v2本地writer
probe。阶段证据写入
`data-collect/docs/sdk070-stage0-baseline.md`。已核实：

- HEAD、upstream与远端均为`b65a2add5749d0931e91ef36d3bbadcb15a53014`；
  worktree只有三份既存未跟踪RTDS草案，继续排除在本事项改动之外。
- 当前workspace仍精确使用官方0.6.0底座的本地`0.6.0-p1`vendor；官方0.7.0
  crate SHA-256、VCS、MSRV与文件清单可复核，当前四个patch底文件在官方0.6.0
  与0.7.0之间逐字节相同。
- `live_runtime.rs`当前为50,382行，主测试模块26,985行；83个同步测试、190个
  Tokio测试，共273个名称唯一的当前快照。生产training入口、四个logical
  producer、六个artifact及paper/live heartbeat边界与需求描述一致。
- 用户指定Parquet文件为4,047,003行、269,203,640 bytes；被拟删除的payload与
  venue hash两列压缩chunk合计233,426,625 bytes，占86.7101%。同UTC日24文件
  合计93,481,048行、6,265,404,486 bytes，两列合计占86.6232%。
- 当前缓存状态下的有界全列streaming scan和现有v2 writer microbench已记录为
  报告基线，不作为v3性能硬门；benchmark只写并清理本地临时路径，未改变tracked
  文件。
- RTDS三项参数仍全部待冻结，umbrella结果保持`RTDS_PARAMETERS_DEFERRED`；
  wrapper路线保持
  `WRAPPER_FIRST_PROVISIONAL_REQUIRES_STAGE_1_CAPABILITY_GATE`，4A/4B未启动。

authority delta和真实owner盘点发现以下四项不能同时成立：

1. `demand/data-collect-reliability-repair-demand.md`第3.1节第3项先声明以
   source-availability需求第4.7节四路径矩阵为最终authority、且不再以固定数量
   为合同，却在同一句末尾仍只枚举三个路径并漏掉`src/ws/config.rs`；最终authority、
   当前vendor事实与本需求AC-01均为四路径。
2. source-availability需求AC-27要求单次串行完整
   `cargo test -p data-collect --locked -- --test-threads=1`，本计划阶段1和阶段6
   却使用默认并行且不含doctest的`--tests`；两者不是同一验收。
3. demand/plan要求`2-PM`完整且独立收口Polymarket source、discovery与resolution，
   同时把Gamma放入`2-REST`。真实代码由同一个Polymarket source owner创建和持有
   `GammaClient`，并直接用于initial/current/next discovery与resolution polling；
   当前两个分支归属无法同时满足。
4. 既有source-availability计划规定vendor SDK身份任一变化必须重跑受影响的
   tests、default/all-features all-targets Clippy和vendor fmt矩阵；0.7.0升级必然
   改变身份，而本计划阶段1只列出两组vendor tests及workspace Clippy/fmt，无法确定
   是有意缩减还是遗漏。

按方案不一致决策门，Stage1和Stage2相关修改立即停止。当前没有建立0.7.0 vendor
目录，没有修改Cargo/lock、Rust、schema、测试或数据，没有运行真实网络probe。
阶段0不能在用户裁决并完成文档对齐前通过独立阶段审查。

## 2026-07-25：用户批准四项推荐并完成最小文档对齐

用户回复“同意四项推荐”，批准阶段0停止门中的全部四项最小修复。实际同步如下：

1. reliability demand/plan的current说明统一为source-availability第4.7节已经批准的
   四路径allowlist，补入`src/ws/config.rs`；不改变任何vendor代码或patch语义。
2. SDK070阶段1和阶段6各自收口时只运行一次
   `cargo test -p data-collect --locked -- --test-threads=1`，删除`--tests`
   替代命令；现有完整suite同时覆盖unit、integration与doctest，不再额外重复同一
   target。
3. 当前由Polymarket source直接持有并用于discovery、identity和resolution polling的
   Gamma transport/helpers归入`2-PM`；`2-REST`不再笼统包含Gamma，且`2-PM`
   继续不依赖`2-REST`。
4. 0.7.0 vendor身份变化后，阶段1收口只运行一次已有`+1.88`
   core/all-features tests、default/all-features all-targets Clippy和固定nightly
   vendor fmt矩阵。workspace排除vendor，因此workspace Clippy/fmt不是该矩阵的
   替代；小步骤不重复矩阵，也不增加网络、bench或无关feature组合。

本轮修复只涉及demand、plan、changelog和阶段0证据文档；没有建立0.7.0 vendor，
没有修改Rust、Cargo/lock、schema、tests或数据，也没有运行网络probe。下一步是
独立阶段0审查；审查通过后仍须另获commit/push授权，才可把阶段0记为published
closure并进入后继阶段。

### 独立阶段0审查首轮修正

`gpt-5.6-sol` / `ultra`独立审查首轮发现三项Important：

1. demand顶部已经记录总需求获批，但第3.1/3.2节和范围排除项仍使用“总体草案尚未
   获批”的current时态。
2. plan阶段0审查门仍绝对禁止任何authority diff，与用户本轮批准的reliability
   current四路径清单修正不能同时成立。
3. 阶段0artifact记录了Parquet聚合和streaming scan结果，但缺少计划要求的可执行
   只读复现命令；另保留了没有说明拼接算法、也不是验收必需的patch摘要。

主代理已做最小修正：总需求明确为已批准执行，同时继续保留SDK typed字段、阶段3、
RTDS、网络、发布和production独立门；阶段0门只豁免本轮获批的四项文档对齐，继续
禁止未授权authority及任何Rust/Cargo/vendor/schema/data diff；artifact直接记录
PyArrow/jq/`time`只读复现命令和精确glob/column-chunk算法，不新增脚本或工具文件，
并删除不透明且非必要的patch concatenation摘要。历史授权状态和旧changelog均未
回写。当前等待同一审查者复核。

### 独立阶段0审查复核修正

同一审查者确认前三项Important均已正确修复，随后发现一项新的Important：计划只
规定新建`0.7.0-p1`和切换workspace，未规定tracked旧`0.6.0-p1`源码树终态，按
字面执行会留下两个完整SDK目录和潜在shadow fallback。

主代理核对总需求“从0.6升级到0.7”、单版本和无shadow path合同后补齐唯一终态：
新vendor provenance、四路径patch和workspace切换建立后，在同一阶段1 diff删除旧
vendor；`third_party`只保留一个tracked SDK vendor，active Cargo/code/probe旧路径
与Polymarket SDK 0.6精确版本引用为0，历史文档引用保留。pre-execution
checkpoint提供完整Git回滚，不新增备份目录或第二fallback。当前等待同一审查者
再次复核。

### 独立阶段0最终复审结果

同一`gpt-5.6-sol` / `ultra`审查者对全部修复复核后给出：

```text
Critical: 0
Important: 0
Minor: 0
Verdict: PASS
```

复核确认四项用户决定、三项首轮修正和旧vendor唯一终态准确落地；demand、plan、
changelog及阶段0artifact一致，验证命令可复现且没有多余门。当前diff精确为七份
本事项文档，三份既存RTDS草案保持排除；`git diff --check`通过，没有
Rust/Cargo/vendor/schema/data、网络或提交操作越界。

阶段0审查通过后曾更新为`STAGE_0_REVIEW_PASSED_AWAITING_PUBLISH`。
先前pre-execution checkpoint的单次commit/push授权已经完成并消耗。

## 2026-07-25：阶段0发布授权与闭合

用户另行明确批准把当前阶段0文档commit并push到
`origin/fix-rtds-silents`，并批准持续推进需求与计划。本次发布范围精确为阶段0
审查确认的七份文档；三份既存RTDS草案继续排除，不修改Rust、Cargo、vendor、
schema或数据。

阶段0记录为`STAGE_0_CLOSED`，以承载本记录的commit成功推送且远端分支SHA与
本地一致为生效条件。该授权允许发布阶段0并在验证后进入Stage1，不继承为后继
阶段的commit/push授权，也不扩大公开网络smoke、RTDS 4A/4B、production
activation/cutover、release、部署、远端服务操作、migration或删除授权。
