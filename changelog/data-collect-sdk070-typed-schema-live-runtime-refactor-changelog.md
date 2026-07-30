# data-collect SDK 0.7.0、typed schema、RTDS 恢复与 live runtime 模块化 changelog

> 生命周期：`HISTORICAL_EVIDENCE_ONLY`
>
> 最终快照：`FROZEN_AT_20260730_CONSOLIDATION`
>
> 当前执行 authority：`none`
>
> superseded_by：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 读取规则：本文仅用于审计历史事实；内部“当前、下一步、待执行、命令”均属于
> 当时检查点，不得驱动当前实现、测试、部署或远端操作。
>
> 2026-07-30：本文件保留 SDK070 核心完成和阶段历史；旧 Attempt6、schema 和
> 下一步表述已被后续代码及用户最终验收取代。
>
> 事项编号：`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`
>
> 对应需求：`demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
>
> 对应计划：`plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`
>
> 纳入的 RTDS 专项三件套：`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`、`plan/data-collect-rtds-business-silence-full-reconnect-plan.md`、`changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`
>
> 状态：`APPROVED_STANDING_EXECUTION_P_B1_CLOSED_2_PM_CLOSED_2_RTDS_CLOSED_STAGE3_CLOSED_STAGE5_CLOSED_STAGE6_CLOSED_2_REST_CLOSED_CORE_COMPLETE_RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_USER_TERMINATED_INCOMPLETE_ATTEMPT3_FAIL_INCOMPLETE_8H_GAMMA_CLOSED_TIME_A_IMPLEMENTATION_CLOSED_ATTEMPT4_FAIL_INCOMPLETE_8H_P_B1E_IMPLEMENTATION_CLOSED_ATTEMPT5_USER_TERMINATED_SCHEMA_CHANGE_INCOMPLETE_8H_REDUNDANT_TYPED_COLUMNS_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_CLOSED_ON_PUBLISH_ATTEMPT6_PENDING_AFTER_V7_V5`。其中`P-B1E`是本次获批的“nullable Decimal精确空串兼容”唯一标识，用于避免与Attempt1辅助文件污染处置的历史`E-A`重名；重复typed列阶段0已由commit `3d5f37e3f95f82adf2d01f3cd6190944867bff38`完成发布与三方ref核对，阶段1已由commit `470b7cd82d6242cdf9040918ad46993e291b828c`完成发布与三方ref核对，阶段2实现、本地门与三路独立终审均已通过，随承载本记录的commit成功push后闭合。RTDS network-error方案A的`366fed7d`/`415470e`、Gamma `closedTime`方案A的`3a367386`/`e086017f`及P-B1E的`15feb12`/`9d8e59b`均已按authority/Rust分离发布并完成三方ref核对。RTDS阶段0和阶段1已闭合；Attempt1、Attempt3、Attempt4永久为`FAIL + INCOMPLETE_8H`，Attempt2永久为`USER_TERMINATED + INCOMPLETE_8H`。P-B1E定向验证与三路独立终审均已通过；Attempt5已按用户指令停止；当前先闭合v5，再从新clean/pushed HEAD执行完整六源8小时Attempt6。总事项阶段0已以commit
> `86d14a9f4225f9b941206b9d47bcfe5a736c7789`闭合并发布，阶段1已以commit
> `b818e436a115eb61859fa281773342ee58f54f6a`闭合并发布；runtime为
> `SDK_070_RUNTIME_USABLE`；阶段1闭合时typed contract为
> `SDK_070_TYPED_CONTRACT_BLOCKED`。用户随后批准并发布独立前置`P-B1`，最终字段裁决为
> F04 `INCLUDED_OPTIONAL`、F01–F03与F05–F18 `EXCLUDED`；本地实现、最终门及
> 独立审查已通过，closure commit
> `d6a87fdaf0a751d6b7c4196aeb936b4c5de4c094`已推送且三方ref一致，typed终态为
> `SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED`。用户于2026-07-25批准继续推进，阶段2任务
> 2.1/2.2共同基线已落盘到
> `data-collect/docs/sdk070-stage2-observable-and-owner-baseline.md`并完成自审；
> `2-PM`四个小步均已完成：step 1/2/3通过实现与自审，step 4确认既有resolution
> recovery已是唯一owner且无需再重构。`2-PM`分支收口完整门与独立审查均已通过；
> closure commit `582eb154687c4c0522615753b3fc1903f2e833f3`已推送且三方ref
> 一致，故`2-PM_CLOSED`成立。共享dispatcher继续留在
> root。`2-RTDS`唯一owner小步、自审、分支收口完整/静态门及独立审查均已通过；
> closure commit `b7f83704cf2176de61573c6b11d792df73d68b53`已推送且三方ref
> 一致。Stage3入口独立审计为`PASS`；authority-sync commit
> `b569546ae1c9cfc24bcdfe4ba12ee4ce60ccc278`已push且三方ref一致，故
> `STAGE3_AUTHORITY_SYNC_CLOSED`成立。后继本地实现、测试、审查、
> commit和push不再逐次申请；只在上下文冲突或实际情况与需求/计划不一致时停止。
> RTDS完整参数、最终需求、wrapper-first SDK路线、authority矩阵及持续执行至阶段2本地香港VPN连续8小时只读probe已于2026-07-26获批；用户随后终止旧probe并将验证对象纠正为本地完整training collector全源正式落盘。Attempt3和Attempt4已经失败并固定；Gamma方案A已闭合，Attempt4暴露的nullable Decimal精确空串P-B1E已获批准。production activation/cutover、release、部署、
> 远端操作、migration和删除仍为范围外事项。
> Stage3任务3.1至3.7、S-B/M-B/W-B修复、本地门与重复独立审查均已通过；
> closure commit `7c6927f663d9c3b387874f688b10346956b9302d`已push且三方ref
> 一致，故`STAGE3_CLOSED`成立。随后执行游标进入独立cleanup阶段5；
> 阶段4的4A authority同步、自审和独立终审已通过；承载本记录的文档commit成功push且三方ref一致后进入4B capability gate。
> Stage5、Stage6与2-REST均已完成各自实现、完整门和独立终审。按用户批准的
> 方案A，本文条件式记录`2-REST_CLOSED`与核心总事项`CORE_COMPLETE`：只有承载
> 本记录的closure commit成功push且本地HEAD、upstream与远端ref一致后才生效。
> RTDS行为专项当前为`RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_USER_TERMINATED_INCOMPLETE_ATTEMPT3_FAIL_INCOMPLETE_8H_GAMMA_CLOSED_TIME_A_IMPLEMENTATION_CLOSED_ATTEMPT4_FAIL_INCOMPLETE_8H_P_B1E_IMPLEMENTATION_CLOSED_ATTEMPT5_USER_TERMINATED_SCHEMA_CHANGE_INCOMPLETE_8H_ATTEMPT6_PENDING_AFTER_V7_V5`。
> 本文件只记录本组三件套的事实、冲突、决定与执行，不回写旧changelog。

## 2026-07-28：方案 B 窄覆盖 W-B 的 metadata-v3 writer 条款

重复 typed 列删除阶段 2 的独立审查发现，2026-07-25 W-B 要求保留
production-compiled public offline source-v2/metadata-v3 writer，但 current
metadata-v5 实现已经删除 metadata-v3 writer API。两者不能同时成立，主代理按决策门
停止阶段 2 发布并请求用户裁决。

用户选择方案 B，精确裁决如下：

- W-B 的 public offline metadata-v3 writer `KEEP` 被撤销，该 API 删除；
- metadata v3 exact schema、reader、legacy validator 与精确文件解码仍保留，历史
  fixture 使用测试内显式冻结的 Arrow schema/batch；
- public offline source-v2 writer、schema 与 legacy row-grain reader不属于本次
  裁决，继续 `KEEP`；
- 2026-07-25 W-B、Stage3 和 Stage5 的旧记录仍如实表示当时发生过的合同与执行，不
  回写为未发生；当前 authority 由本节与
  `DC-REPAIR-20260728-REDUNDANT-TYPED-COLUMN-REMOVAL` 的三件套窄覆盖。

代码实际边界与方案 B 一致，因此没有为兼容性恢复第二 writer、schema adapter 或
shadow path；只同步当前需求、计划、changelog 与 cleanup ledger，并继续阶段 2
验证和独立复审。

## 2026-07-28：两个重复字段新事项接管 current new-write，Attempt5 用户终止

- 用户批准
  `DC-REPAIR-20260728-REDUNDANT-TYPED-COLUMN-REMOVAL`，分别以 aggTrade v7 和
  metadata v5 删除两个可无损派生的重复字段；历史 v5/v6、metadata v3/v4 不迁移、
  不重写。
- 本 umbrella 已完成的 source v3 + metadata v4 Stage3 事实保持不变；新事项只取代
  future current new-write 和后继本地验证。
- Attempt5 在 commit `92fb71883cad19285ae0a291e2a8ee94698133dd` 上运行
  `16233.700s` 后按用户指令停止，固定为
  `USER_TERMINATED_FOR_APPROVED_SCHEMA_CHANGE + INCOMPLETE_8H`；manifest
  顶层仍为`state=open`，仅`collection_day_coverage.state=ended_known`；
  finalized Parquet `27` 个 / `297,802,741` bytes、`.running=0`。
- 唯一下一长跑游标是在新事项 v7/v5 分阶段审查、commit、push 后，从新
  clean/pushed HEAD 与空 root 启动 Attempt6。顶部状态段中“Attempt5 为下一步”的
  旧句只保留停止前历史游标。
- 新事项阶段 2 的 metadata v5 实现、方案 B、完整本地门和三路
  `gpt-5.6-sol`/Ultra 最终复审均已通过；三路均为
  `Critical 0 / Important 0 / Minor 0`。当前为
  `STAGE2_CLOSED_ON_PUBLISH`，阶段 2 commit 成功 push 并完成三方 ref 核对后再
  进入 Attempt6。

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

## 2026-07-25：阶段0文档closure已发布

- 精确七份阶段0文档以commit
  `86d14a9f4225f9b941206b9d47bcfe5a736c7789`
  （`docs(data-collect): close SDK070 stage 0`）提交并push到
  `origin/fix-rtds-silents`。
- push后本地HEAD、`@{upstream}`与`git ls-remote --heads origin
  fix-rtds-silents`均为同一完整SHA。三份既存未跟踪RTDS草案未被stage、commit或
  push。
- 该commit是Stage1前可回滚checkpoint；它没有预授权Stage1 commit/push、公开网络
  smoke、RTDS 4A/4B、production activation/cutover、release、部署、远端操作、
  migration或删除。

## 2026-07-25：阶段1重建官方0.7.0 vendor并切换唯一依赖

- Stage1从本机Cargo registry缓存的官方
  `polymarket_client_sdk_v2-0.7.0.crate`全新解包建立
  `third_party/polymarket_client_sdk_v2-0.7.0-p1`，不是旧目录改名，也没有为取得
  源码访问公共网络。archive SHA-256为
  `ba212e0641f178c274af266772de15962ac7e76da550a0f79f47b49349b1138a`，
  package为0.7.0，发布时间为`2026-07-17T14:56:51Z`，MIT license，MSRV
  `1.88.0`，VCS为`222143d321eba97d5711a848265eb9aab3bc7ff4`；archive含132个
  文件，排序文件清单SHA-256为
  `55c093724c71c384cb485cc44bef1cb75d21123035cb8f253308b3d518e0dc74`。
- 官方0.6.0与0.7.0的四个patch底文件逐字节相同。Stage1从当前0.6 patch逐hunk
  重放到官方0.7；相对官方0.7.0的源码差异恰好是
  `src/ws/connection.rs`、`src/ws/config.rs`、
  `src/clob/ws/types/response.rs`、`src/rtds/subscription.rs`，没有第五个路径。
  connection/config/CLOB response/RTDS subscription patched文件SHA-256分别为
  `90070b2bd438909a44a82209963881b4c70f755bc671ea19b13c7ee2e2d6a223`、
  `dc68886999cfd3509c0668f403c6f8953f283f03847b5dbd6ebe6c5b72d20e2d`、
  `55163a02b9f4c58c0e861043e5b6a109164d6e661aceeb46b24d2dd460ac0073`、
  `4e2174a4dcb0e18ad5baf2914ec3270cbff5923915e6dc39ed7f04174fa10e7b`。
- 根`[patch.crates-io]`、workspace exclude、`data-collect`精确依赖、lock及两个bench
  provenance已切换到0.7.0/`0.7.0-p1`。offline `cargo tree`只解析一个本地
  `polymarket_client_sdk_v2 0.7.0`，生产features仍为
  `clob,gamma,rtds,ws`。
- 新vendor provenance、四路径patch和workspace切换建立后，旧tracked
  `third_party/polymarket_client_sdk_v2-0.6.0-p1`在同一Stage1 diff删除。
  `third_party`只保留一个tracked SDK vendor；active Cargo/code/bench中的旧路径及
  Polymarket SDK 0.6精确版本引用为0。阶段0artifact与旧changelog的历史引用有意
  保留，Git checkpoint提供回滚，不建立备份目录或shadow fallback。
- 本阶段没有修改`data-collect/src`、schema或RTDS业务逻辑。

## 2026-07-25：阶段1离线runtime验证完成

- 使用`+1.88`在offline模式运行vendor core approved-feature lib suite，
  `170/170`通过；all-features的unit/integration/doc harness全部通过，其中library
  `186/186`、doctest `49 passed / 20 ignored`。官方0.7 async execution
  `post_order`/`post_orders` mock覆盖通过，没有真实订单、钱包或认证调用。
- vendor default及all-features两组all-targets Clippy `-D warnings`均退出0；固定
  `nightly-2025-11-24` vendor fmt check退出0。
- 单次`cargo test -p data-collect --locked -- --test-threads=1`的lib
  `347/347`且全部integration/doc harness通过；其中现有patch/runtime contract为
  `17/17`与`19/19`。随后locked all-targets check、Clippy `-D warnings`、workspace
  fmt及staged/unstaged diff check均退出0。
- 全部构建/测试均为offline；任务1.5公开只读CLOB/RTDS/Gamma smoke未获授权且未
  运行，记录为`NOT_AUTHORIZED / NOT_RUN`，没有用synthetic结果冒充public smoke。
- vendor-local构建缓存完成后以精确manifest path执行`cargo clean`；Cargo报告清理
  18,463个可再生成文件、约26.1 GiB，随后确认vendor目录无`target/`。这不是源码、
  证据或用户数据删除。
- runtime终态确定为`SDK_070_RUNTIME_USABLE`。精确命令、toolchain、hash与结果写入
  `data-collect/docs/sdk070-stage1-sdk-runtime-and-typed-contract.md`。

## 2026-07-25：typed field/nullability fixture与consumer矩阵完成

- 2026-07-25访问官方current realtime data文档，并以固定SDK commit
  `222143d321eba97d5711a848265eb9aab3bc7ff4`核对typed struct/serde。临时offline
  harness对官方registry 0.7与当前patch分别调用
  `parse_if_interested(bytes, &MessageInterest::ALL)`，覆盖单object和array envelope；
  完成后已清理临时harness，worktree未留下第二parser。
- `book`、`price_change`、`last_trade_price`、`tick_size_change`、
  `best_bid_ask`、`new_market`、`market_resolved`七类variant都存在官方允许
  missing/null但SDK拒绝单object整帧的路径。相同element放入array时，官方0.7静默
  返回`OK messages=0`，当前patch传播同一parse error；因此patch只消除数组静默
  跳过，没有补typed字段或放宽nullability。
- `event_message`整体missing/null可解析；对象存在时的官方optional内部文本会失败。
  `EventMessage.id`在官方文档和SDK中均required，不是gap。
  `MarketResolved.question/slug/description`已为`Option`且outcomes接受
  missing/null，也不是gap。
- 当前consumer/批准合同审计确认，整帧parser gap在target admission之前丢失消息，
  不能通过Parquet nullable、`EXCLUDED`或`nullable_unavailable`降级。因此typed
  终态为`SDK_070_TYPED_CONTRACT_BLOCKED`，只停止v3
  schema/reader/writer/local new-write，不阻断runtime可用性或与这些字段无关的
  机械工作；禁止以raw REST/WS、manual signing、第二parser或shadow canonicalizer
  补洞。
- 仍须用户逐字段裁决的single-field gap为：book的
  `minOrderSize/tickSize/negRisk/lastTradePrice`、
  `LastTradePrice.transactionHash`、new-market扩展字段及resolved `tags`。
  当前未识别这些字段的WS-v3 executable consumer；但agent没有替用户选择要求官方
  typed支持、`EXCLUDED`或`nullable_unavailable`。即使全部批准降级，整帧parser
  blocker仍必须先由官方SDK-first路线修复才能解除v3阻断。
- future paper/live保持`NONBLOCKING_FUTURE_SPEC`，不能创造当前必需字段或阻断项。

## 2026-07-25：阶段1独立审查通过，等待发布授权

- 独立`gpt-5.6-sol`/`ultra`审查者只读复核完整cached diff、官方crate与vendor
  路径/内容、Cargo/lock/feature tree、测试证据、typed fixture/consumer矩阵及三组
  authority同步；没有编辑、构建、联网、commit、push或远端操作。
- 审查确认官方crate SHA/VCS/MSRV/license/132文件及文件清单hash一致；vendor与官方
  0.7.0路径集相同且只有四个批准源码路径内容不同；四个patched文件与Stage0版本
  逐字节一致，无第五路径、第二vendor或active 0.6引用。
- 审查确认`data-collect/src`、schema、RTDS业务逻辑及三份未跟踪RTDS草案均未进入
  diff；runtime证据足以支持`SDK_070_RUNTIME_USABLE`，无需重复测试；typed矩阵和
  `SDK_070_TYPED_CONTRACT_BLOCKED`结论准确且没有替用户裁决single-field gaps。
- 终审结果为：

```text
Critical: 0
Important: 0
Minor: 0
Verdict: PASS
```

当前游标为`STAGE_1_REVIEW_PASSED_AWAITING_PUBLISH`。Stage1未commit/push，
Stage2未开始；必须取得用户另行发布授权后才可创建Stage1 closure commit并push。

## 2026-07-25：阶段1发布授权与闭合

用户另行明确批准创建Stage1 commit并push到
`origin/fix-rtds-silents`。发布范围保持为已通过两轮状态复核的Stage1暂存集：
官方0.7.0 vendor重建、四路径patch重放、唯一workspace依赖切换、旧tracked
vendor删除、probe身份同步、Stage1证据及三组authority文档同步；三份既存未跟踪
RTDS草案继续排除。

本记录精确取代同日此前等待发布授权状态；旧段仅保留为历史事实。

Stage1记录为`STAGE_1_CLOSED`，以承载本记录的commit成功推送且远端分支SHA与
本地HEAD/upstream一致为生效条件。完成该验证后可按既有持续推进授权进入Stage2；
本次授权不继承为Stage2各分支或后继阶段的commit/push授权，也不扩大非parser
typed字段决定、RTDS参数与4A/4B、公开网络smoke、production activation/cutover、
release、部署、远端服务/数据操作、migration或删除授权。

## 2026-07-25：阶段2共同基线落盘并完成自审

- 用户批准继续推进后，以Stage1 closure commit
  `b818e436a115eb61859fa281773342ee58f54f6a`为事实锚，完成任务2.1/2.2的
  current observable与owner只读盘点，落盘
  `data-collect/docs/sdk070-stage2-observable-and-owner-baseline.md`。
- 基线冻结training唯一调用链、4个logical producer、6个artifact、current
  schema、identity/timestamp/admission、RawDayGate/rollover/writer/manifest/
  shutdown、resolution current/next/pending/terminal/restart及paper/live
  heartbeat-only边界；两个历史artifact根分别记录其真实build commit，不冒充
  SDK070 current public smoke。
- 当前自动测试集合为`593`，无重复leaf name、无`#[ignore]`；排序
  `path:name`与name-only SHA-256分别为
  `f038056715f0a130dcba90ee5168300ed5b57f962303376725ede7341dbfc221`
  和
  `1b25ed8f6e51e6a6696338e890c6e3a5ca84129abd6f140ca1ce9a03b2eea812`。
  lexical symbol集合为`2808`，`path:kind:name`与`kind:name` SHA-256分别为
  `d4a2ff3cc10c480e2278ec670f4153c7ff3583e7f7c7575ee515b16b4b853706`
  和
  `e2c6ad75c153040241bd3897338a37bb7b6c18efcded4b6d73fe3ebd86632e17`。
- 只登记8类真实跨owner边界；SDK actor、Polymarket pump和Gamma `JoinSet`
  仍归source内部，不重复建立第二message-flow台账。当前105个
  `#[cfg(test)]`中9个为test-module gate、96个为implementation hook，按真实
  文件owner记录production影响。
- 对照需求、计划、当前代码与历史artifact后未发现阻断阶段2的语义冲突。
  `SDK_070_TYPED_CONTRACT_BLOCKED`仍只阻断阶段3 v3；公开网络smoke仍是
  `NOT_AUTHORIZED / NOT_RUN`。
- 文档路径、artifact事实、测试/symbol数量与hash均已复算一致；
  `cargo test -p data-collect --locked --lib --tests -- --list`成功列出593个test，
  `git diff --check`退出0。当前游标为`STAGE_2_COMMON_BASELINE_COMPLETE`。
  共同基线没有修改
  Rust、schema、payload、fingerprint、admission、RTDS、paper/live、部署路径或
  `run_id`校验；三份既存未跟踪RTDS草案保持未修改、未暂存、未删除。
- 自审通过后只进入`2-PM` step 1：提取Polymarket runtime source owner及其直接
  Gamma discovery/identity/resolution transport、helpers和直接测试。adapter、
  writer seam、restart recovery与RTDS保持原位。

## 2026-07-25：`2-PM` step 1 Polymarket source owner完成自审

- 新增私有`data-collect/src/live_runtime/polymarket_source.rs`，机械迁入
  Polymarket source spec/control、SDK fast-drain pump、discovery/context、
  single processing actor、Gamma resolution transport及其直接helpers；root仅
  增加私有`mod polymarket_source; use polymarket_source::*;`。
- 新增私有
  `data-collect/src/live_runtime/tests/polymarket_source.rs`，从root机械迁入
  73个直接测试；root保留200个adapter/writer/recovery/coordinator/cross-owner
  测试。唯一文本适配是隔离资源测试的完整模块名；所有test属性和函数体与Stage1
  HEAD对账一致。
- `live_runtime.rs`由50,382行降至36,428行；新production/test模块分别为
  5,432/8,484行，三个文件合计50,344行，净减38行。行数变化只来自机械迁移、
  私有module scaffolding、最窄`pub(super)`及rustfmt布局，不是runtime语义门。
- Step2实施前复核发现，前一版未提交记录误用了最终`cargo fmt`前的root统计
  36,501行，因而把合计/净变化误记为50,417/+35。最终格式化后的root少73行，
  恰与迁移测试数相同；归一化production、73个测试函数与root remainder的hash、
  测试名称集合及验证结果均未变化。用户批准方案A后，本条把活动事实修正为
  36,428/50,344/-38；错误数字不再具有current authority。
- 主代理把Stage1 HEAD原owner片段与新production模块归一化后得到同一SHA-256
  `2c78da1d4a2c1bce1de218ed35680af0a1237692dcba3186f93dab992b07ff34`；
  73个测试函数归一化后同为
  `0b5f550295731a59896698280913346569296fe2fbe7f18994dadc3b5cd292db`；
  删除计划内owner/test并排除三个私有module声明后，root remainder归一化后同为
  `2ee0aa51666072cc0b3ca4c1b8785f86bcc366d761ebea8a00367318ddda24e7`。
  归一化只忽略空白、rustfmt可选尾逗号、`pub(super)`及唯一隔离测试路径变化。
- 全局测试仍为593，src/integration仍为347/246、无重复leaf name，name-only
  SHA-256仍为
  `1b25ed8f6e51e6a6696338e890c6e3a5ca84129abd6f140ca1ce9a03b2eea812`。
  lexical symbol只有两个计划内`mod polymarket_source`新增；剔除它们后的
  `kind:name` SHA-256仍为Stage2基线
  `e2c6ad75c153040241bd3897338a37bb7b6c18efcded4b6d73fe3ebd86632e17`。
- 主代理复跑owner测试73/73、跨owner测试8/8、五组Polymarket integration
  52/52；`cargo fmt --all -- --check`、
  `cargo check -p data-collect --locked --all-targets`与`git diff --check`
  全部退出0。
- `lib.rs`未修改，新模块不存在plain `pub`或`pub(crate)`；仅使用parent
  adapter和移动测试所需的最窄`pub(super)`。schema、payload、hash、fingerprint、
  admission、RTDS、paper/live、`run_id`与部署路径均未修改，三份既存未跟踪RTDS
  草案保持未修改、未暂存、未删除。
- 当前`2-PM`游标为`STEP_1_SELF_REVIEW_PASSED`。下一小步只允许step 2
  adapter/row conversion owner；writer seam、restart recovery与RTDS保持原位。

## 2026-07-25：`2-PM` step 2 event adapter/row conversion完成自审

- 新增私有`data-collect/src/live_runtime/polymarket_event_adapter.rs`，机械迁入
  38个纯adapter/row conversion items：canonical payload、target fan-out与identity
  校验、event-specific raw JSON、metadata v3构造、RawEvent到metadata/orderbook
  row转换及其直接test helpers。
- 新增私有
  `data-collect/src/live_runtime/tests/polymarket_event_adapter.rs`，机械迁入12个
  直接测试。root保留188个测试，source子模块保留73个，adapter子模块为12个；
  `polymarket_lifecycle_identity_contradictions_are_terminal`与
  `polymarket_invalid_market_resolved_is_raw_first_then_fail_closed`因跨
  adapter/source/admission边界继续留root。
- Step1后三文件合计50,344行；Step2后root/source/adapter/source-tests/
  adapter-tests分别为33,988/5,432/1,692/8,484/756行，合计50,352行。净增8行只
  来自私有module scaffolding、最窄`pub(super)`及rustfmt布局。
- 主代理逐item归一化对账38个production items，Stage1 HEAD与新模块SHA-256同为
  `0e75f87c0802b05f9cb0621fae16d1dddf9af33917697885539930169b12f393`；
  12个测试函数归一化后同为
  `ff8b15361618359a8c557a42dca48c5f55972e3821a43b090315efe61139037e`。
  归一化只忽略空白、rustfmt可选尾逗号和编译所需`pub(super)`；相关
  `#[cfg(test)]`/`#[allow]`属性另行逐项保留。
- 全局测试仍为593，src/integration仍为347/246，无重复leaf name，name-only
  SHA-256仍为
  `1b25ed8f6e51e6a6696338e890c6e3a5ca84129abd6f140ca1ce9a03b2eea812`。
  lexical symbol为2812；剔除source/adapter两组各两个私有module声明后仍为2808，
  `kind:name` SHA-256仍为
  `e2c6ad75c153040241bd3897338a37bb7b6c18efcded4b6d73fe3ebd86632e17`。
- 主代理复跑adapter 12/12、`polymarket_`受影响unit 100/100、六组
  adapter/schema/dedupe/writer integration 50/50；实现代理另复核source 73/73、
  root跨owner 13/13及Step1五组PM/SDK integration 52/52。fmt、locked
  all-targets check与diff check全部退出0。
- RawDay/admission、source lifecycle dedupe与Parquet restore、metadata
  enqueue/atomic ordering、`LiveSegmentWriter`/schema dispatch、restart
  recovery、manifest、coordinator及RTDS保持原位。`lib.rs`未修改，新模块没有
  plain`pub`或`pub(crate)`；schema、payload、hash、fingerprint、admission、
  paper/live、`run_id`和部署路径均未改变。
- 当前`2-PM`游标为`STEP_2_SELF_REVIEW_PASSED`。下一小步只允许step 3 writer
  integration seam及直接测试；restart recovery与RTDS保持原位。

## 2026-07-25：Step 3 writer owner冲突与`Writer-A`裁决

- 两路只读审计确认，当前`LiveSegmentWriter`生命周期、`typed_schema_version`
  schema mapping及`write_typed_live_event`均同时服务六类source；PM只占两个
  match arm。Rust中不存在可仅移动现有symbol、只做path/visibility适配便形成
  完整PM dispatcher owner的实现。
- 整体移动共享函数会把Binance/RTDS错误归入`2-PM`；新增
  `Option/handled`协议或在root/module重复source-kind match又会形成多余控制
  抽象。原需求中的“PM-specific writer dispatch”与计划的纯机械约束因此不能
  同时成立，主代理按方案不一致门停止实现并请求用户裁决；停止期间未编辑Rust、
  未构建、未测试、未提交或push。
- 用户批准`Writer-A`：root继续唯一持有六源source-kind dispatch与
  `logical_bytes`计算；既有私有`polymarket_event_adapter`只取得PM current
  schema version value、两个rows-to-writer leaf函数及PM独占one-row guard。
  两个root PM match arm分别直接委派对应leaf，不新增Option/handled协议、重复
  source-kind match、第三模块或第二writer abstraction。
- 测试只补one-row guard当前缺失的一个fail-closed直接回归，并继续运行既有
  adapter、shared writer、schema/Parquet及跨owner验证；不为已经覆盖的0/1-row
  正常路径重复增加测试。`Writer-A`不授权修改schema值、payload/hash、
  fingerprint、admission、RTDS、paper/live、部署、远端、commit或push。

## 2026-07-25：`2-PM` step 3 `Writer-A`完成自审

- `polymarket_event_adapter.rs`新增私有
  `POLYMARKET_ORDERBOOK_SCHEMA_VERSION`，把root schema mapping与adapter row
  context原有的两个`polymarket_orderbook_v2`同值literal归并为单一current
  value；没有改变schema字符串。metadata v3常量继续由
  `polymarket_market_metadata_layout`持有，没有复制或迁移。
- adapter新增两个最窄`pub(super)` leaf：
  `write_polymarket_orderbook_live_event`与
  `write_polymarket_metadata_v3_live_event`；原私有
  `require_at_most_one_typed_row`随leaf完整移动。root
  `write_typed_live_event`仍只计算一次`logical_bytes`并持有唯一六源match，两个
  PM arm分别直接调用对应leaf，其余四个source arm与error路径未改。
- 没有新增`Option/handled`协议、adapter内source-kind match、第三模块、第二
  writer abstraction、plain`pub`、`pub(crate)`或`lib.rs`改动。schema、payload、
  hash、fingerprint、admission、recovery、RTDS、paper/live、`run_id`与部署路径
  均未改变。
- 新增一个直接测试
  `polymarket_writer_rejects_unsplit_fanout_before_writing`：真实typed writer接收
  既有两row price-change fixture，断言原guard错误，并在finalize后确认row count
  为0；没有重复增加已有0/1-row正常路径测试。
- Step2后五文件合计50,352行；分支收口Clippy修正后root/source/adapter/
  source-tests/adapter-tests分别为33,965/5,432/1,727/8,484/782行，合计50,390
  行，净增38行，来自批准的两个leaf、一个version constant及一个直接测试。
- 自动测试集合为src 348、integration 246、合计594，无重复leaf name、无
  `#[ignore]`；source-order、排序`path:name`与name-only SHA-256分别为
  `c6067bb80a9717e7f6392a798070f27b6728d10be4b4e076a48dcdd263153766`、
  `23e3805c21c385831f905799e3de3c74a2417d6c7f8cccf94bd725a55f31faaa`与
  `a67ca19fb9c84e7b5b9a70bfef6cc8d869d53ea5277b4a0e146a96ee05161470`。
  lexical symbol为2,816：相对Step2只增加批准的两个production leaf、一个
  direct test及一个constant，原guard只是移动；`path:kind:name`与`kind:name`
  SHA-256分别为
  `dbd8cad3f5b2d02ac610becb7750d3d0afc324f343f014af79f5b186c87346d9`与
  `311b52bfd2da4c02616c62040354a2646316d01164362bb943a439cb5e77b1bb`。
- 验证通过：新guard测试1/1、adapter 13/13、`live_runtime::tests::polymarket_`
  101/101、共享writer统计测试1/1、六组相关integration 53/53、
  `cargo fmt --all -- --check`、`cargo check -p data-collect --locked
  --all-targets`、594-test harness list与`git diff --check`。
- `gpt-5.6-sol`/`ultra`独立只读审查结论为`PASS_WITH_MINOR`，Critical 0、
  Important 0；唯一Minor是文档把单个orderbook version value误写为复数，已在
  demand/plan/changelog同步修正。当前游标为
  `2-PM_STEP_3_SELF_REVIEW_PASSED`；下一小步只审计既有resolution recovery
  owner，不提前改RTDS或shared writer。

## 2026-07-25：`2-PM` step 4 recovery owner审计通过且无需移动

- 独立只读审计确认
  `data-collect/src/polymarket_resolution_recovery.rs`已经是唯一完整的resolution
  restart事实恢复owner：唯一production入口
  `restore_polymarket_resolution_recovery`、metadata/raw读取、canonical schema/
  payload/reference validation、finalized路径/排序及恢复结果类型均在同文件。
- 同文件`mod tests`持有32个直接测试。root
  `reconcile_stable_startup_recovery`只在UTC/manifest稳定门内调用恢复入口，PM
  source只把`PolymarketRestartRecovery`转换成pending运行态，不再次扫描或验证
  durable事实；adapter没有restore/recovery路径。
- root `restore_polymarket_dedupe_until_cancelled`只投影slug与
  `source_native_payload_json`并恢复`LifecycleFingerprintSets`，其输出、消费者和
  取消语义均不同，不是resolution-state shadow实现，也不应误搬入recovery owner。
- `lib.rs`中的public module与三个public item均为既存边界。checkout内没有额外
  外部消费者，但行为保持阶段不得借此收窄API。审查另观察到
  `finalized_attempt_ordinals`未被production source读取；删除它会改变public恢复
  结果合同且不属于Step 4，因此保持不动。
- 本步终态为`ALREADY_OWNED_NO_MOVE`：recovery、`lib.rs`、Rust、测试与schema均
  没有修改，Step3的594测试/2,816 lexical symbol事实不变。直接recovery测试
  32/32、startup crossing-midnight/clock-regression 2/2通过；此前PM unit
  101/101已经覆盖restart crash-cut与recovered-pending source消费者。
- 当前四个`2-PM`小步均完成，游标进入`2-PM_BRANCH_CLOSE_GATE`。下一步只运行
  一次分支完整确定性门与`gpt-5.6-sol`/`ultra`独立全面审查；通过后先记
  `2-PM_REVIEW_PASSED_AWAITING_PUBLISH`，仍不得自行commit/push。

## 2026-07-25：`2-PM`分支收口确定性门通过，等待独立审查

- 按计划只运行一次完整
  `cargo test -p data-collect --locked -- --test-threads=1`：348个unit、246个
  integration及0个doctest全部通过。
- `cargo check -p data-collect --locked --all-targets`已在最终行为代码上通过；
  `cargo clippy -p data-collect --locked --all-targets -- -D warnings`首轮发现
  机械迁移使Stage1 HEAD既有的`#[allow(clippy::too_many_arguments)]`与
  `polymarket_message_passes_dedupe`之间出现空行。最终只删除该空行，既有
  attribute与其函数保持和Stage1 HEAD一致。
- 修正后all-targets Clippy通过；`cargo fmt --all -- --check`与
  `git diff --check`也通过。该单行空白修正不生成runtime代码；
  完整suite已在同一函数体与调用图上通过，因此没有违反“完整suite只跑一次”的
  防重复门。
- 最终五文件合计50,390行；测试集合594与lexical symbol 2,816及其hash均不受
  lint-only空白修正影响。三份RTDS草案继续未修改、未暂存、未删除。
- 当前游标为`2-PM_BRANCH_CLOSE_GATES_PASSED_REVIEW_PENDING`；下一动作仅为
  `gpt-5.6-sol`/`ultra`独立全面审查，不创建commit或push。

## 2026-07-25：`2-PM`独立审查通过，等待发布授权

- `gpt-5.6-sol` / `ultra`独立全面审查首轮结论为
  `FAIL_PENDING_MINOR_FIX`：Critical 0、Important 0、Minor 1。唯一Minor是
  Stage1 HEAD既有的`#[allow(clippy::too_many_arguments)]`被误删，且plan、
  changelog与阶段2基线把它错误记录为迁移新增。
- 对照Stage1 HEAD核实后，在`polymarket_message_passes_dedupe`前恢复既有
  attribute，不恢复触发Clippy的空行；同步修正三份记录。最终五文件LOC为
  33,965/5,432/1,727/8,484/782，合计50,390。
- 最终工作树重新通过locked all-targets Clippy `-D warnings`、locked
  all-targets check、fmt check与`git diff --check`。该修复只恢复既有lint
  attribute并修正文档，不改变函数体、调用图或runtime语义，因此没有重复已经
  通过的完整suite。
- 同一独立审查完成最小复审，最终结论为`PASS`：Critical 0、Important 0、
  Minor 0；确认没有新增行为、owner、visibility、schema或授权边界问题。
- 当前游标为`2-PM_REVIEW_PASSED_AWAITING_PUBLISH`。尚未创建commit或push，
  不得写成`2-PM_CLOSED`；下一动作只能是取得单独发布授权后创建本分支closure
  commit并push到`origin/fix-rtds-silents`。

## 2026-07-25：`2-PM`发布授权与闭合

用户在独立审查与最小复审通过后，明确同意创建`2-PM` closure commit并push到
`origin/fix-rtds-silents`。发布范围严格限定为本事项三件套、阶段2共同基线、
root facade及两个PM owner模块和各自测试；三份既存未跟踪RTDS草案继续排除。

本记录精确取代同日此前等待发布授权状态；旧段仅保留为历史事实。

`2-PM`记录为`2-PM_CLOSED`，以承载本记录的commit成功推送且本地HEAD、upstream
与远端ref一致为生效条件。本次授权不继承为`2-RTDS`、`2-REST`、阶段3或后继
分支的commit/push授权，也不扩大typed字段决定、RTDS参数与4A/4B、公开网络smoke、
production activation/cutover、release、部署、远端操作、migration或删除授权。

## 2026-07-25：P-B/P-B1路线与single-field初始裁决

Stage1闭合后，用户先批准`P-B`的SDK-first修复方向及F01–F18全部
`EXCLUDED`，随后在实现前冲突审计中进一步冻结为`P-B1`：

- vendor生产源码差异继续只有既有四个路径；nullable补丁只进入既有
  `src/clob/ws/types/response.rs`；
- nullable标量/文本使用`Option`，三个nullable数组使用
  `DefaultOnNull -> empty Vec`，不得用伪默认值；
- data-collect在current-v2 canonical/persist/claim/state之前fail closed；
- 必要consumer、测试和三组authority文档可同步修改；
- raw WebSocket/REST、第二parser、shadow canonicalizer、Stage3及RTDS实现均不
  进入本分支。

实现审计确认data-collect直接消费`ConnectionManager` broadcast；
`subscribe_market_with_options`返回的filtered stream在当前collector路径会立即
drop，所以没有新增`subscription.rs`生产patch。因
`BookUpdate.timestamp`直接复制到`MidpointUpdate.timestamp`，两者在同一
`response.rs`保持`Option<i64>`，没有形成第五个vendor生产源码路径。

## 2026-07-25：F04从批量排除修正为nullable book快照字段

用户追问`book.lastTradePrice`是否因与独立`last_trade_price`重复而被排除。重新
核对官方类型、当前SDK和consumer后确认：

- 两者指向同一“最近成交价”概念，但语义不同；F04是book快照时已知状态，独立
  variant是订阅期间发生的成交事件并携带自己的size/side/fee/timestamp等属性；
- F04可能在首次订阅或重连后、尚无新成交事件时提供bootstrap值，不能据此认定为
  完全重复；但实际目标市场填充率仍未通过公开wire smoke验证；
- 先前`EXCLUDED`只由“当时未识别executable consumer + 整批范围裁决”支持，不是
  无价值或等价重复证明。

用户据此明确把F04改为`INCLUDED_OPTIONAL`并批准继续；F01–F03与F05–F18继续
`EXCLUDED`。实现只新增
`BookUpdate.last_trade_price: Option<Decimal>`：missing/null保持`None`，
present值保留到book canonical与raw-equivalent JSON，不伪造、不要求非空，也不
合成独立成交行。为保持current-v2兼容，新key只在present时插入；missing/null旧帧
的canonical字节、fingerprint、row及持久化语义不变。未来v3 book合同增加同名
nullable decimal-string字段，Stage3 Rust/schema/writer仍未获授权、未实施。

## 2026-07-25：P-B1实现完成，最终验证进行中

当前实现已经完成：

- 七类missing/null typed decode及single-object/array永久fixture；
- current-v2完整shape前置门和target-associated resolved零写入/零claim/零状态门；
- F04 SDK present/missing/null解码、vendor websocket stream适配，以及
  data-collect present无损保留和missing时不改变canonical shape的直接测试；
- F01–F03与F05–F18未加入SDK struct或consumer；没有Stage3、RTDS、公开网络或
  production操作。

F04定向结果为：vendor response fixture `1/1`、vendor websocket book
`1/1`、data-collect book canonical/raw `2/2`通过。一次data-collect定向命令误用
vendor的`+1.88` toolchain，依赖MSRV检查在编译前退出；改回workspace默认toolchain
后通过，这不是代码失败。

此前P-B1全门中还发生一次验证命令范围错误：
`cargo test --all-features --all-targets`在全部测试通过后进入官方
`clob_order_operations` criterion benchmark；benchmark使用vendor内硬编码dummy
private key并尝试连接CLOB，连接在`127.0.0.1:443`被拒绝，没有真实credential、
订单或成功public调用。该命令不再使用；正确替代为lib/tests/examples测试加
all-targets check。当前游标为`P_B1_VALIDATION_IN_PROGRESS`，须完成最终静态门、
文档同步和独立审查后才能申请发布。

## 2026-07-25：P-B1最终本地门通过，等待独立审查

- vendor `--all-features --lib --tests --examples`全部通过：library `193/193`，
  websocket integration `41/41`，其余integration harness与全部example test
  harness均为0失败；没有执行criterion benchmark；
- vendor `--all-features --doc`为`49 passed / 20 ignored`，locked all-targets
  check与Clippy `-D warnings`、固定nightly fmt均通过；
- data-collect完整串行suite为`351 unit / 246 integration / 0 doctest`，全部通过；
  locked all-targets check、Clippy `-D warnings`、workspace fmt与
  `git diff --check`均通过；
- P-B1后vendor相对官方registry 0.7.0仍只有四个production源码差异：
  `src/ws/connection.rs`、`src/ws/config.rs`、
  `src/clob/ws/types/response.rs`、`src/rtds/subscription.rs`；另有明确列出的
  `tests/websocket.rs`和`examples/clob/ws/orderbook.rs`两个non-production
  conformance差异，无第五个production路径；
- 四个production文件当前SHA-256依次为
  `90070b2bd438909a44a82209963881b4c70f755bc671ea19b13c7ee2e2d6a223`、
  `dc68886999cfd3509c0668f403c6f8953f283f03847b5dbd6ebe6c5b72d20e2d`、
  `9bd05887b9190449f75f5cda623a5f2e465c07033900fb1dabf89b72ab2b6aba`、
  `4e2174a4dcb0e18ad5baf2914ec3270cbff5923915e6dc39ed7f04174fa10e7b`；
- 三份既存未跟踪RTDS草案继续未修改、未暂存、未删除；没有Stage3、2-RTDS、
  public smoke、commit、push或远端操作。

主代理自审确认F04 missing/null不改变旧canonical shape，present值只保留为book
快照字段；current-v2 gate仍在canonical/persist/claim/state之前，F01–F03与
F05–F18仍无新增typed field。当前游标更新为
`P_B1_BRANCH_CLOSE_GATES_PASSED_REVIEW_PENDING`；下一动作仅为
`gpt-5.6-sol` / `ultra`独立全面审查。

## 2026-07-25：P-B1独立审查通过，等待发布授权

按阶段门派出的`gpt-5.6-sol` / `ultra`只读独立审查结论为`PASS`：

- Critical：0；
- Important：0；
- Minor：0；
- 无可执行问题。

审查确认：七类nullable typed修复与三个`DefaultOnNull`数组符合批准矩阵；
current-v2 gate位于canonical、持久化、claim和业务状态变化之前，且没有把
`MarketResolved`原本nullable的question/slug/description/outcomes新增为必填；
F04只作为optional book快照字段条件保留，没有合成独立成交事件；F01–F03与
F05–F18仍为`EXCLUDED`；vendor差异仍为四个production路径和两个明确的
non-production conformance路径；三份未跟踪RTDS草案仍未修改或暂存。

剩余非阻断风险为public smoke仍是`NOT_AUTHORIZED / NOT_RUN`，因此F04在真实
首次订阅和重连book中的填充率未知。当前游标更新为
`P_B1_REVIEW_PASSED_AWAITING_PUBLISH`。尚未创建commit或push；
`P_B1_CLOSED`须在用户另行授权并完成commit、push与ref核对后才能成立。

## 2026-07-25：P-B1发布闭合

用户明确批准创建并发布P-B1 closure checkpoint。发布范围只包含已审查通过的
18个tracked P-B1代码、测试和文档文件；三份既存未跟踪RTDS草案未暂存。

- commit：`d6a87fdaf0a751d6b7c4196aeb936b4c5de4c094`
  （`fix(data-collect): close sdk070 typed compatibility gate`）；
- push：`582eb154687c4c0522615753b3fc1903f2e833f3..d6a87fdaf0a751d6b7c4196aeb936b4c5de4c094`
  到`origin/fix-rtds-silents`；
- 发布后本地HEAD、upstream与远端ref均为
  `d6a87fdaf0a751d6b7c4196aeb936b4c5de4c094`；
- `P_B1_CLOSED`与`SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED`成立。

本次发布不授权Stage3、public smoke、RTDS参数或4A/4B，也不继承为`2-RTDS`
commit/push授权。

## 2026-07-25：`2-RTDS`唯一owner小步完成并通过自审

按已批准机械边界，只移动现有RTDS source owner及其九个直接测试：

- 新私有模块`data-collect/src/live_runtime/rtds_source.rs`唯一持有RTDS endpoint、
  bounded config、source spec、classify、subscription evidence、source-native
  canonical、dedupe restore、SDK runtime loop、generation accounting和emit；
- 新测试模块`data-collect/src/live_runtime/tests/rtds_source.rs`持有九个直接测试；
- root继续持有composition、shared sink/day/coordinator、writer/schema/
  `rtds_row_from_event`、跨channel config、跨四源业务静默及四源load测试；
- 没有新增business-silence timer、watchdog、refresh、第二连接owner或RTDS重连
  authority；生产source loop仍忽略freshness返回值，业务静默不会驱动网络变化。

机械一致性与小步门：

- production常量与九个代码块去除空白和必要`pub(super)`后，与
  `d6a87fd`逐项SHA-256一致；九个测试在移动时函数体逐字一致；
- 迁移前root 33,965行；最终root 32,589行、source 359行、tests 1,019行，
  合计33,967行，净增2行module wiring；
- RTDS owner定向测试`9/9`通过；
- root heartbeat config、source specs和跨四源业务静默三个affected tests
  `3/3`通过；
- locked all-targets check与`git diff --check`通过；
- 三份未跟踪RTDS草案哈希保持
  `d8caddb53c9fbe259ce2f844845426580f614de47ea8bd79c5f110576dc84148`、
  `911aa69bd6d086887f88c185f1b7dc20f6223e7d8637fc1848752966886fc1a7`、
  `50d4a405a664baec39844ac1952af7be10de6b2f75ad792f8abb57f62d36d4c1`。

主代理与production/test映射子代理自审均为`PASS`，没有遗漏、越界、语义变化或
新增public API。当前游标为`2-RTDS_BRANCH_CLOSE_GATE`；下一动作只运行一次完整
data-collect suite、Clippy、fmt、diff门与`gpt-5.6-sol`/`ultra`独立审查。

## 2026-07-25：`2-RTDS`分支收口本地门通过，等待独立审查

- 按计划只运行一次完整
  `cargo test -p data-collect --locked -- --test-threads=1`：
  `351 unit / 246 integration / 0 doctest`全部通过；
- 最终行为代码的locked all-targets check已在小步门通过；分支收口all-targets
  Clippy `-D warnings`通过；
- 首次fmt check只发现测试移出root后缩进减少引发的标准布局变化，以及原测试块
  移走后留下的三处孤立空行。运行`cargo fmt --all`后，最终fmt check与
  `git diff --check`通过；
- formatter只改变空白/换行，未改变token语义、函数调用图或测试集合，因此没有
  重复已经通过的完整suite。最终LOC为root 32,589、source 359、tests 1,019，
  合计33,967；
- 三份未跟踪RTDS草案继续保持原哈希，未修改、未暂存、未删除；没有public smoke、
  vendor suite、RTDS专项100-cycle/8小时门、4A/4B、commit、push或远端操作。

当前游标更新为`2-RTDS_BRANCH_CLOSE_GATES_PASSED_REVIEW_PENDING`；下一动作仅为
`gpt-5.6-sol`/`ultra`独立全面审查。

## 2026-07-25：`2-RTDS`独立审查通过，等待发布授权

`gpt-5.6-sol`/`ultra`只读独立全面审查结论为`PASS`：

- Critical：0；
- Important：0；
- Minor：0；
- 无可执行问题。

审查独立确认：

- 四组production owner代码规范化SHA-256在迁移前后全部一致，移除计划内移动块后
  的root remainder哈希也一致；
- 新测试模块恰好包含九个直接测试，逐测试规范化哈希与`d6a87fd`基线一致；
- production仍为单一`RtdsSdkRuntime`、初始化时一次`replace_subscription`和原
  receive loop；freshness返回值仍被生产loop忽略，没有timer、watchdog、refresh、
  第二连接owner或业务静默重连；
- root保留composition、shared owner、writer/schema/row conversion及跨源测试，
  没有重复、shadow path、过度抽象、多余校验或public API扩大；
- 需求、计划、changelog、Stage1/Stage2状态和P-B1 closure事实一致；未误写
  `2-RTDS_CLOSED`、Stage3、4A/4B、public smoke、production或远端授权；
- 三份受保护RTDS草案仍未跟踪、未暂存且哈希不变。

当前游标更新为`2-RTDS_REVIEW_PASSED_AWAITING_PUBLISH`。尚未创建commit或push；
只有取得用户单独授权并完成commit、push与本地HEAD/upstream/远端ref核对后，
`2-RTDS_CLOSED`才能成立。

## 2026-07-25：用户授予持续执行/发布授权并闭合`2-RTDS`

用户明确决定：本事项不再逐阶段申请本地实现、测试、审查、commit或push授权；
主代理持续推进直至任务完成，只有开发文档、代码语义、运行事实或用户明确描述
不能同时成立时，才停止并请求决策/额外授权。

该决定精确取代此前“每个阶段审查通过后再次申请commit/push授权”及“阶段3再次
申请本地Rust/schema/test授权”的当前门；旧段落只保留为历史事实。持续授权不替
未冻结RTDS参数作决定，也不扩大总计划第九节明确排除的production activation/
cutover、release、部署、远端操作、migration或删除范围。

`2-RTDS`记录为`2-RTDS_CLOSED`，以承载本记录的closure commit成功推送到
`origin/fix-rtds-silents`，且本地HEAD、upstream与远端ref一致为生效条件。发布
范围严格限定为本事项需求/计划/changelog、Stage1/Stage2状态同步、root私有module
接线及两个RTDS source/test新模块；三份既存未跟踪RTDS草案继续排除。

## 2026-07-25：`2-RTDS`发布闭合

- closure commit：
  `b7f83704cf2176de61573c6b11d792df73d68b53`
  （`refactor(data-collect): close 2-RTDS source owner`）；
- push：`d6a87fdaf0a751d6b7c4196aeb936b4c5de4c094..b7f83704cf2176de61573c6b11d792df73d68b53`
  到`origin/fix-rtds-silents`；
- 发布后本地HEAD、upstream与远端ref均为
  `b7f83704cf2176de61573c6b11d792df73d68b53`；
- 工作区只剩三份受保护RTDS草案，仍未跟踪、未暂存且哈希不变。

因此`2-RTDS_CLOSED`正式成立。

## 2026-07-25：Stage3入口审计与任务3.0 authority同步

独立只读入口审计结论为`PASS`。已核实：

- runtime终态为`SDK_070_RUNTIME_USABLE`；
- `2-PM_CLOSED`、`P_B1_CLOSED`与
  `SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED`均成立；
- 最终字段矩阵仍为F04 `INCLUDED_OPTIONAL`，F01–F03与F05–F18 `EXCLUDED`；
- `market_resolved` validate-first、metadata v4 claimant/原子activation及用户
  持续执行/发布授权均有效；
- Stage1 typed-blocked、旧等待授权文本及current source v2 + metadata v3是历史
  状态或Stage3工作基线，不构成入口阻断。

任务3.0 authority diff严格限定为：

- reliability demand中的canonical JSON、payload fingerprint、逐change fan-out及
  restart projection只继续约束source v2；future v3改用唯一canonical typed domain、
  一条SDK message一条nested row、无payload-equality drop或跨进程fingerprint；
- data-collect demand/plan中的通用raw evidence合同只对future
  `source_kind=polymarket_market` v3收敛为完整typed evidence；
- retention plan的venue `hash`、F05 `transaction_hash`及逐change行明确为v1/v2
  历史/current条款，v3按最终F矩阵与nested row执行；
- resolution-followup三件套只把“实施未授权”当前门同步为持续本地执行已授权，
  validate-first、metadata v4和历史v2语义不重写；
- 对应旧changelog只追加新supersede记录，不回写历史。

明确未改：其他source raw JSON、metadata v3/v4自身payload/digest、
`HandoffFrame v1.raw_json`、rebuild-core审核中草案、RTDS三份草案、Rust/Cargo/
schema/tests及production activation/cutover、release、部署、远端操作、migration
和删除。

`STAGE3_AUTHORITY_SYNC_CLOSED`以承载本记录的closure commit成功push到
`origin/fix-rtds-silents`且本地HEAD、upstream与远端ref一致为生效条件。

## 2026-07-25：用户批准方案B统一market ID canonical命名

阶段3未提交实现最初把SDK `NewMarket.id`与`MarketResolved.id`映射为
`clob_market_id`，而需求6.1仍写作`new_market.id`。两者保存同一个CLOB market
ID，但计划与changelog没有记录该rename；current-v2 canonical/raw JSON使用外部
wire名`id`，独立metadata domain使用`clob_market_id`，两种先例并存。主代理据此
触发不一致决策门并暂停schema冻结。

用户明确选择方案B并要求同值统一命名。当前决定冻结为：

- 第三方SDK struct和平台wire继续使用其不可修改的顶层`id`；
- 唯一typed入口执行一次显式`SDK id -> clob_market_id`映射；
- 项目自有v3 domain、Parquet、metadata、recovery和consumer统一使用
  `clob_market_id`，共同envelope的`market`继续表示condition ID；
- 不在v3 canonical合同中增加同值顶层`id`列、alias、双写或第二转换路径；
- nested `event_message.id`属于不同层级的event identity并保持原名；
- activation前current-v2 canonical/raw JSON继续遵循legacy外部字段名，不因本次
  命名决定提前改变生产行为，也不重写、回填或迁移历史artifact。

这项决定只消除项目canonical命名歧义，不改变字段值、target admission、metadata
authority、schema activation边界或production授权范围。需求、计划和本changelog
已同步；阶段3可继续修复独立审查发现的实现问题。

## 2026-07-25：用户裁决 recovery 数组字段统一为 `asset_ids`

阶段3 typed recovery入口审查发现一项文档/实现冲突：需求6.3第5项仍要求投影
`resolved_event_asset_ids`，但已冻结的v3 domain、Parquet schema、reader和contract
test统一使用`asset_ids`；需求6.1同时规定`market_resolved`继承`new_market`的
identity/text/arrays。主代理按不一致决策门暂停阶段3.5，撤回尚未完成的recovery
草稿并报告真实位置、语义和选项。

用户明确回复“同意”，批准推荐修正：

- `market_resolved`与`new_market`在项目v3 canonical domain、Parquet、recovery和
  consumer中统一使用`asset_ids`；
- 不新增同值的`resolved_event_asset_ids` alias、双列、第二映射或兼容写路径；
- 历史`polymarket_orderbook_v2.resolved_event_asset_ids`保持独立read-only
  consumer/audit兼容，不得合成v3 canonical row或claimant authority。

需求与计划已同步这一裁决。该修正只消除遗留v2字段名，不改变数组值、顺序、
target admission、metadata v4 claimant合同或production范围；阶段3.5恢复执行。

## 2026-07-25：Stage3任务3.1至3.6与单一activation diff完成

阶段3按已批准typed合同完成本地实现和current调用图切换：

- 新增唯一`polymarket_orderbook_v3` canonical domain、七类variant、nested Arrow
  schema、writer/reader和严格round-trip；一条target-admitted SDK message只产生
  一行，mixed `PriceChange`只保留目标entries并保持SDK原始相对顺序；
- F04仅作为nullable `book.last_trade_price`快照状态保留，不合成成交事件；
  F01–F03与F05–F18没有进入v3 schema；
- 第三方SDK/wire顶层`id`只在typed入口映射一次，项目domain、Parquet、metadata、
  recovery和consumer统一使用`clob_market_id`；`market_resolved`与`new_market`
  统一使用`asset_ids`。未增加同值alias、双列或第二转换路径；
- source owner先做target admission和`market_resolved`完整identity/assets/outcomes/
  winner校验，再取得既有row/byte预算与bounded queue ownership，并分配唯一
  epoch+sequence source key。平台重复delivery取得不同key，内部retry不取得第二key；
  epoch跨reconnect、hour和UTC day rollover稳定；
- v3 current ingress不构造或持久化canonical payload JSON、venue hash、payload
  fingerprint、fan-out row reference或raw recovery sidecar。旧source v2与metadata
  v3 writer在同一个本地activation diff中断开current new-write，保留legacy
  read-only或`cfg(test)`兼容验证；
- metadata v4严格以v3完整schema为前缀，只追加nullable WS claimant pair；只有
  WS-owned terminal同时填写两列。current restart recovery只接受source v3 +
  metadata v4，按exact epoch+sequence、identity、assets、winner和receive ordering
  校验；cross-pair、source-only、错误claimant、late/contradictory row和损坏artifact
  均fail closed；
- `data-collect/docs/sdk070-stage3-future-typed-handoff-mapping.md`登记为
  `NONBLOCKING_FUTURE_SPEC`，SHA-256为
  `9b8207c3a01b5356a1a365913d1e8368c69d686bbac36a3f39cbbb1629a31464`。
  本阶段没有实现future frame、external adapter、metadata join或paper/live接线。

这里的activation只表示本地源码的current new-write调用图已从source v2/metadata v3
原子切到source v3/metadata v4；没有构建或部署production binary，没有对运行服务器、
现有dataset、历史artifact或远端服务执行activation/cutover、migration、backfill、
release、部署或删除。

## 2026-07-25：独立语义审查发现跨日Defer会丢typed row，已修复

Stage3 activation语义审查发现：既有RawDay gate在UTC rollover返回`Defer`时，最初
v3 enqueue分支直接返回deferred结果，没有把已经通过validation的typed message交给
任何持久化owner。该路径会静默丢失下一日source row，并使`MarketResolved`无法建立
合法claimant。此事实与“已接纳message恰好一行”和“claim前必须已有source ownership”
合同冲突，因此在阶段收口前修复。

修复没有新增第二queue、JSON sidecar或通用框架，而是把既有单一deferred FIFO扩展为
mixed `DeferredIngressBatch`：

- typed Defer先取得同一全局row/byte预算，在同一FIFO内分配一次source key并保存原
  validated domain、deferred ticket/epoch和reservation，随后才返回admission receipt；
- rollover/stop flush只注入下一日permit并发送同一个owned message，不重新validation、
  重建JSON或分配第二key；source row与随后插入的metadata继续由单producer FIFO保证顺序；
- closed channel、budget timeout和queue admission失败仍不分配可用于claim的source key，
  所有error/drop路径释放reservation；
- 覆盖23:55窗口、boundary前后、stop、closed/timeout、source先于metadata以及epoch/
  sequence跨日稳定的测试均通过。

同一审查还发现sink-unusable drain仍用旧v2 fan-out估算虚构`rows`，会让一条
`PriceChange`或`MarketResolved`消息产生错误drop row计数。active drain已改为只记录
真实abandoned decoded message和receive-day事实，不再重新projection或伪造row/drop；
旧fan-out函数已无生产caller，留待阶段5物理删除。

## 2026-07-25：Stage3本地验证门通过，等待最终独立阶段审查

最终行为代码的本地验证证据：

- `cargo check -p data-collect --tests`通过；
- `cargo test -p data-collect --test polymarket_orderbook_v3_contract -- --nocapture`：
  `7/7`通过；
- current v3/v4 recovery定向测试：`7/7`通过；
- `parquet_writer_contract`、`polymarket_contract`与
  `polymarket_metadata_layout_contract`：`25/25`、`6/6`、`18/18`通过；
- Polymarket source完整串行模块：`89/89`通过；
- `cargo test -p data-collect --lib -- --test-threads=1`：`374/374`通过；
- `cargo test -p data-collect --locked -- --test-threads=1`：完整unit、
  integration与doc-test suite全部通过；
- `cargo fmt --all -- --check`与`git diff --check`通过。

编译warning只对应activation后已不可达的旧v2 raw/fan-out/fingerprint helper、空
fingerprint plumbing、legacy recovery pair和旧writer入口；语义审查确认它们没有
生产caller、不会drop或restore消息。它们按DAG登记为阶段5删除候选，本阶段不通过
`allow`或额外抽象掩盖，也不把清理扩大到仍需legacy read-only的schema/reader。

三份未跟踪RTDS草案未修改、未暂存，哈希仍为
`d8caddb53c9fbe259ce2f844845426580f614de47ea8bd79c5f110576dc84148`、
`911aa69bd6d086887f88c185f1b7dc20f6223e7d8637fc1848752966886fc1a7`、
`50d4a405a664baec39844ac1952af7be10de6b2f75ad792f8abb57f62d36d4c1`。
当前游标为`STAGE3_LOCAL_GATES_PASSED_REVIEW_PENDING`；下一步只进行最终独立
`gpt-5.6-sol`/`ultra`阶段审查，审查通过后按持续发布授权commit/push。

## 2026-07-25：Stage3最终独立审查未通过，等待三项一致性裁决

三名独立`gpt-5.6-sol`/`ultra`审查者分别检查合同、运行时和范围。运行时审查为
`PASS`并确认active v3/v4调用图、Defer FIFO、claim ordering、预算和source
sequence没有新blocker；合同审查为`NOT PASS`（Important 2），范围审查为
`NOT PASS`（Important 1）。已核实的不一致如下。

1. Stage3审查门要求独立审查`storage measurement脚本`，但同一计划把v2/v3
   bounded storage A/B脚本、指标和硬门放在Stage6任务6.2；当前仓库只有历史v2
   `source_hourly_layout_probe`，没有v3 A/B measurement脚本。不能把不存在的脚本
   记录为Stage3已审查，也不能静默提前或推迟计划任务。
2. production startup确实先持有dataset-root lock，验证prior-day manifest并
   reconcile current manifest的rows/bytes/SHA/file facts，再调用Polymarket
   recovery；但公开recovery API只接收`dataset_root + time`，随后重新扫描所有
   final-named Parquet，不消费刚验证的manifest path/facts snapshot。因而公开caller
   可绕过preflight，且preflight后新出现的final-named文件没有绑定到同一快照。
   一名运行时审查者认为production调用顺序已满足门，合同审查者认为这仍未满足
   demand 6.3.6与plan 3.5.2/3.5.4的manifest-finalized authority；不得由主代理
   擅自选择较弱或较强解释。
3. active runtime与writer worker只使用source v3 + metadata v4，没有dual-write；
   但production-compiled public `TypedParquetSegmentWriter`仍允许创建并写入source v2
   和metadata v3，集成测试和旧bench也证明该入口可用。文档写作activation后source
   v2与metadata v3“只读”，但没有明确该词只约束current runtime调用图，还是要求
   production public writer API硬拒绝。硬拒绝会改变public API运行语义；弱解释则
   需要修正文档，不能由主代理自行决定。

另有一项非阻断但必须补齐的验收记录：计划3.4.6要求逐owner预算表列出对象、上限、
reservation起点、跨await/queue存活期和释放点；当前只有公式和概括说明。此项不需
新增抽象或通用instrumentation，可在上述裁决后按代码事实补入文档。

当前游标回退为`STAGE3_REVIEW_BLOCKED_PENDING_CONSISTENCY_DECISIONS`。在用户裁决前
不修改上述合同语义、不创建Stage3 commit、不push；三份RTDS草案继续排除。

## 2026-07-25：用户裁决S-B、M-B、W-B，Stage3恢复执行

用户对上一节三项不一致分别选择：

1. **S-B**：把唯一bounded v2/v3 storage measurement probe、首次本地运行、结果报告
   和独立审查移入Stage3；Stage6只消费已发布证据的路径/hash/status，明确
   `NO_RERUN / NO_REIMPLEMENTATION / NO_SECOND_REVIEW`。
2. **M-B**：接受production dataset-root lease + startup prior-day manifest preflight
   + current manifest reconcile作为唯一manifest trust boundary。保留公开
   `restore_polymarket_resolution_recovery(dataset_root, time)`语义，不增加manifest
   snapshot参数或第二校验路径。直接模块调用不构成production manifest-authority
   证明；绕过startup或不遵守lease的外部文件突变是用户接受的剩余风险。
3. **W-B**：“activation后只读”只表示source v2/metadata v3退出current runtime
   new-write。production-compiled public offline legacy writer继续用于历史fixture、
   compatibility tests和S-B probe；它不得接回`LiveSegmentWriter`、current schema
   dispatch、recovery authority、dual-write或production canonical root。

demand/plan已同步三项裁决。Stage5台账相应区分：

- 旧current-runtime private payload/fingerprint/fan-out/raw-recovery branches继续是
  `REPLACED_BEHAVIOR`或`UNREACHABLE_PRIVATE`候选；
- public offline v2/metadata-v3 writer、legacy schema/reader和storage probe为`KEEP`。

计划3.4.6已补逐owner预算表，记录fast-drain SDK message、v3 canonical source、
metadata v4 bundle、resolution pending state以及queue/batch/writer/Arrow边界的上限、
reservation起点、跨queue/await存活期和释放点。SDK message与v3 projection短暂并存
所以分别占global reservation；`16 MiB`只作为row-group logical limit，不宣称精确计量
Arrow allocator。无active consumer的`POLYMARKET_PROCESSOR_STAGING_MAX_BYTES`未被
冒充现行预算owner。

### S-B probe与结果

新增：

- `data-collect/benches/polymarket_storage_ab_probe.rs`；
- `data-collect/Cargo.toml`中的`harness=false` bench target；
- `data-collect/docs/sdk070-stage3-storage-ab-measurement.md`。

probe只复用官方SDK typed parser、现有v2/v3 domain/writer和Parquet reader。它没有修改
production writer/schema/runtime，没有新增Criterion、第二encoder、manifest simulator、
recorder、sidecar、dual-write、shadow replay或通用benchmark framework。

固定fixture：

- 8,192 logical deliveries，七类variant均覆盖；
- mixed `PriceChange`使用同一原始SDK message和matched-assets集合，v2写13,000条
  price-change rows，v3写7,800条nested rows；
- 两条完全相同的book platform deliveries取得两个实际logical id/source sequence；
- internal retry显式引用`logical_delivery_id=1`并纳入fixture digest，writer sequence
  guard拒绝第二次写入。

简洁性专项审查最初发现duplicate/retry计数只是固定报告数字，会让没有retry输入时仍可
显示PASS。修复后duplicate key数量从实际logical-id集合计算，retry成为fixture引用并
纳入digest，v3 case实际执行同key retry且要求writer拒绝；同时删除重复的静态assertion、
scan schema、case label和throughput字段。审查者判断单文件长度本身不阻断：七类typed
fixture、v2 exhaustive offline projection、四个隔离case/scan进程、逐column统计与
CPU/RSS证据构成真实必要复杂度，不应拆成第二框架。

最终本地命令：

```bash
cargo check -p data-collect --locked --bench polymarket_storage_ab_probe
cargo bench -p data-collect --locked --bench polymarket_storage_ab_probe
```

均通过。最终fixture digest为
`25964d5b874cb202519596b0fb20e4ed7af2b6a74fc36ad21d3590a7ffe6331f`；
v2/v3 finalized bytes分别为`229,762 / 119,696`，v3减少`110,066 bytes`
（该fixture为`47.904353%`）。v2 `source_native_payload_json`/`hash` compressed
column chunks分别为`113,116 / 36,178 bytes`；v3不存在
`source_native_payload_json`、venue `hash`或`transaction_hash`。v2/v3 physical
rows为`13,392 / 8,192`，writer/footer/独立全列scan逐项一致。

scan子进程先请求`POSIX_FADV_DONTNEED`且两侧返回成功；文档只称
`cache-drop requested`，不冒充已证明物理cold-cache。CPU、RSS、吞吐、finalize和
scan均为本机报告项，不新增阈值。完整UTC日只消费Stage0既有v2事实，不重扫、不生成
full-day v3或声称pre-dedupe replay。

当前三项用户决策阻断已经解除，S-B probe本地硬门通过；下一步仍需完成三件套一致性
复核、受影响本地门和重复`gpt-5.6-sol`/`ultra`独立阶段审查。审查通过前不记录
`STAGE3_CLOSED`，不创建closure commit/push。

## 2026-07-25：Stage3重复独立审查通过，进入closure commit/push

S-B/M-B/W-B修复后的三名独立`gpt-5.6-sol`/`ultra`审查者分别覆盖typed contract、
current runtime/recovery以及scope/overdesign：

- contract审查：`PASS`，Critical/Important/Minor均为0；
- runtime审查：`PASS`，Critical/Important/Minor均为0；
- scope审查首次发现1项Important：storage报告只笼统引用正式测试，缺少计划3.7.3
  要求的精确语义证据索引。报告已补canonical `7/7`、writer `5/5`、current v3/v4
  recovery `7/7`、startup reconcile `2/2`、prior-day manifest preflight `2/2`
  及dataset-root lease `1/1`的路径、测试名、`--locked`命令与PASS结果；重复审查后
  为`PASS`，Blocker/Important/Minor均为0。

最终复核确认：

- current runtime只有source v3 + metadata v4 new-write，不存在dual-write；
- W-B public offline source-v2/metadata-v3 writer只服务历史fixture、兼容测试与
  S-B probe，没有接回current dispatch、recovery authority或production root；
- M-B调用链在dataset-root lease内先完成prior-day manifest preflight/current
  reconcile，再调用公开`(dataset_root, time)` recovery；没有新增snapshot或第二校验；
- S-B probe没有新增production encoder、shadow canonicalizer、manifest simulator或
  通用benchmark framework；
- 37项dead-code warning只对应已登记给Stage5的旧current-runtime私有路径；未用
  `allow`掩盖，也没有误删W-B legacy KEEP范围。

最终证据hash：

- storage probe：
  `369ee423a0f1409a7ab2766e35dafded139b482276ba44248466b934b775c261`；
- storage报告：
  `92a9df249dd0e4287d7538a6f9081f164b954e84e832288849d5ff9d83141fcb`；
- future typed handoff mapping：
  `9b8207c3a01b5356a1a365913d1e8368c69d686bbac36a3f39cbbb1629a31464`。

最终本地门为绿色：完整locked package suite已在行为实现冻结后通过；S-B只新增
bench/docs，随后`cargo check -p data-collect --locked --all-targets`、上述六组
正式语义定向测试、`cargo fmt --all -- --check`与`git diff --check`均通过。没有
重复运行Stage6才要求的clippy门；37项warning按DAG由Stage5删除后再执行
`-D warnings`。

三份未跟踪RTDS草案继续排除，哈希仍为
`d8caddb53c9fbe259ce2f844845426580f614de47ea8bd79c5f110576dc84148`、
`911aa69bd6d086887f88c185f1b7dc20f6223e7d8637fc1848752966886fc1a7`、
`50d4a405a664baec39844ac1952af7be10de6b2f75ad792f8abb57f62d36d4c1`。

`STAGE3_CLOSED`以承载本记录的commit成功push到
`origin/fix-rtds-silents`且本地HEAD、upstream与远端ref一致为生效条件。该closure
不授权production binary activation/cutover、release、部署、远端操作、dataset
migration或删除；下一执行游标为独立Stage5 cleanup，Stage4仍受RTDS专项自身参数和
授权门约束。

## 2026-07-26：Stage5 U01–U04对抗复审、用户裁决与实施

Stage5 C01–C08清理和本地门完成后，初次用户建议把U01/U02/U03/U04b保持
`DEFER`、U04a保持`KEEP`。用户质疑其中存在明显错误并要求派生subagents重新审查。
三路`gpt-5.6-sol`/Ultra审查与主代理production调用图复核确认：

- `pub`只为仓内integration tests提供可见性；crate无已证明外部consumer、tag、
  release或semver承诺，不能单独构成KEEP理由；
- U01旧RTDS adapter只由自身测试消费，current唯一owner已是
  `live_runtime/rtds_source.rs`、SDK runtime与source/hourly contracts；
- U03旧四role scheduler遗漏production六source中的metadata与depth snapshot，
  synthetic e2e还使用旧RTDS endpoint、per-segment manifest并把连续Binance/RTDS
  错误绑定到market slug；
- U04a把所有训练availability简化为recv time，不能表达current collector
  admission；U04b的旧命名和rows/bytes/duration文件rollover与current hourly
  writer/row-group语义冲突；
- U02静态`CrossTaskQueueConfig`从未接入production。一名审查者据此指出当前批准
  demand的逐队列五维硬约束并未由真实runtime完整实现，不能在未裁决需求前冒充
  “已替代”；其余审查者同意旧config本身不应保留。

用户随后批准四项推荐，最终authority为：

1. queue使用bounded channel、global inflight row/owned-byte硬预算、单行上限与
   bounded acquire/send wait，并沿用writer batch/periodic flush；queue duration、
   writer lag、flush latency和RSS只作真实观测，不建立第二逐队列五维配置owner；
2. 运行中只因UTC hour rollover；协调shutdown可提前finalize，同小时启动/重启
   路径碰撞只选择初始`part=N`而不是运行中切换；50,000 rows/16MiB只切Parquet
   row group；
3. legacy/无独立admission schema使用collector recv time，已批准保存独立collector
   admission字段的current schema使用collector admission time/sequence，
   paper/live使用handoff admission与本机runtime receive；
4. current/next/T-30只属于Polymarket market lifecycle，连续Binance/RTDS source
   独立运行；合法空响应、业务静默和stale不驱动gap/reconnect，真实transport failure
   由source-local owner以有界单次等待和无限外层总尝试恢复。

据此U01、U02、U03、U04a、U04b全部进入
`DELETE_PROVEN_REPLACED_BEHAVIOR`：删除四个旧源码模块、四个专属自引用integration
tests、一个旧synthetic e2e（共五个）、module exports，以及
`bounded_queue_contract`中的静态config自测；
真实bounded/backpressure测试、current RTDS owner、四producer/六source topology、
hourly writer、handoff及W-B legacy reader/writer均保留。代码删除精确为
`+0/-1,585`；残留symbol扫描为0。替代owner的7个integration targets共67项及6个
关键unit合同通过；最终locked package suite退出0（lib 351项及全部现存
integration/doc targets通过），all-target check、`clippy -D warnings`、fmt与
diff gate均通过。当前整个Stage5 tracked Rust diff为`+541/-5,381`，净删除
`4,840`行；该总值同时包含精确owned-byte正确性修复、必要current测试迁移与机械
clippy修正，不冒充纯删除量。Stage5仍须完成独立阶段审查和commit/push后才能闭合。

## 2026-07-26：Stage5首次终审FAIL、U05-A/U06-A裁决与修复

首次独立`gpt-5.6-sol`/Ultra阶段终审为`FAIL / CLOSURE_BLOCKED`，Blocker 0、
Important 4、Minor 1：

- C02/C03只删了production plumbing，遗漏仅由两个旧测试消费的public
  `LifecycleFingerprintSets`；该模型不被W-B legacy writer/reader或bounded A/B消费；
- C01遗漏cfg(test) metadata-v2 RawEvent adapter，以及正向要求`fallback_close`和
  `unresolved_window_end_observation`的旧测试；current source测试只允许sentinel为0；
- U02删掉静态config后，`BackpressureConfig/Monitor`仍在test/bench中用逐queue
  age/lag hard阈值产生`PauseIntake/ResumeIntake`；
- `health.rs`仍公开有限attempt/elapsed、`retry_exhausted`、circuit registry与
  fresh-business reconnect gate，而production只从整条trace取最后一次sleep；
- 新增overlay把启动/重启`part=N`碰撞写成运行中file switch，并把current admission
  schema写得过宽。

前两项属于既有C01/C02/C03批准范围，直接补删。用户随后批准：

1. `U05-A`：删除逐queue hard-threshold pause/resume控制子树及其test/bench依赖；
   保留真实bounded channel、global inflight row/owned-byte、单行上限、有界
   acquire/send wait、`QueueSample`纯观测DTO和`BackpressureReceipt`兼容字段；
2. `U06-A`：删除有限health/reconnect circuit shadow模型及三个专属旧合同，把
   production需要的确定性capped+jitter delay收窄到现有私有
   `live_runtime::retry_delay`；真实`SourceRecoverySupervisor`的有界单次等待、
   无限外层总attempt与business-silence非transport门保持不变。

当前代码已完成上述删除。首次终审修复后的定向回归为bounded queue 2项、
manifest 5项、Binance/RTDS dedupe 2项、Binance 2项、current v3 adapter 7项、
真实outer recovery 1项全部通过，throughput bench编译通过。首次终审修复后的完整
package suite退出0（lib 348项及全部现存integration/doc targets通过），all-target
check、`clippy -D warnings`、fmt与diff gate全部通过；lib比首次终审前少3项，正是
批准删除的旧adapter正向测试。整个Stage5 tracked Rust diff为
`+584/-7,499`，净删除`6,915`行。已向source-availability需求/计划/changelog
追加U06-A窄supersede，保留历史阶段事实但不再把已删除的`health.rs`写成current
owner。

重复独立代码/consumer终审随后发现1个Important：pending-state严格1MiB
owned-byte计算遗漏`BTreeMap`独立key allocation与`source_authorities`的`Vec`
spare capacity，新增的“each allocation once”测试也复现了相同漏算。修复后map
entry计key fixed size/capacity，authority vector计完整backing capacity并仅另加
已初始化元素动态String capacity；focused两个budget合同各1项通过。完整package
suite再次退出0（lib 348及全部integration/doc targets通过），all-target check、
`clippy -D warnings`、fmt和diff gate再次通过。最终tracked Rust diff为
`+641/-7,509`，净删除`6,868`行。修复后代码/consumer终审与独立
文档/authority终审均为`PASS / CLOSURE_UNBLOCKED`，Blocker 0 / Important 0 /
Minor 0；当前只待精确staging、commit/push与三方ref一致性。未授权production、
远端、部署、migration或数据删除。

## 2026-07-26：Stage5发布闭合并进入Stage6

Stage5 closure commit
`9e0d264058ed64cd63fbd05280f0826afaba0509`已push到
`origin/fix-rtds-silents`；本地HEAD、upstream与远端ref逐项一致，故
`STAGE5_CLOSED`成立。提交只包含已批准C/U清理、必要正确性修复、测试迁移与文档，
三份未跟踪RTDS草案未暂存、未修改。

Stage6随后只消费同一稳定代码快照的最终五道门、Stage3 storage/future附件和Stage0
完整日v2物理事实。完整package suite、all-target check、Clippy `-D warnings`、fmt与
diff均已在该稳定源码树通过，没有v3 AC需要补充focused target。

## 2026-07-26：Stage6 6.3.1冲突、方案A裁决与本地证据完成

Stage6只读AC/证据审计发现局部需求/计划冲突：

- 需求AC-06/AC-08只要求target-admitted SDK fixture、producer golden fixture和
  canonical/v3 round-trip，并明确真实adapter、长跑和首次real-v3 forward run另立
  三件套；
- 计划6.3.1却额外写了“代表性real artifact”；
- Stage3 v3 artifact是固定synthetic fixture经正式writer生成的确定性Parquet，
  不能冒充production real artifact；
- Stage0真实artifact是legacy v2且没有原message boundary，需求又禁止v2 reader重组
  v3 canonical domain，因此不能用它补写real-v3 parity。

现实选项为：A，以七类fixture验证canonical round-trip，并以Stage3另一个固定
deterministic fixture经正式writer产生的finalized v3 artifact验证物理落盘，两套
fixture不得混同；B，另建三件套执行首次真实v3 forward run。用户批准方案A。需求
语义不变，计划6.3.1已作最小修正，没有新增reader、adapter、录制器、sidecar、双写、
shadow replay或网络测试。

Stage6最终报告写入
`data-collect/docs/sdk070-stage6-v3-integration-parity.md`，并登记：

- 稳定代码快照
  `9e0d264058ed64cd63fbd05280f0826afaba0509`；
- probe/report/future mapping/Stage0 hash分别为
  `70c5f2dae2085d5c3584a49eb1311b2e54b81bcec08e52372dad019bd4e51dd4`、
  `527c10d79d95803640980d94aacccaf79605dd565a3ce32526820b3c08380ed3`、
  `9b8207c3a01b5356a1a365913d1e8368c69d686bbac36a3f39cbbb1629a31464`、
  `5a3705ed5c360ec6983fdffc7947c54a716182738e55ac75b3aaca410e844a53`；
- Stage3 v3 production Rust owner文件`+5,155/-2,388`、test Rust
  `+3,838/-313`；Git file-level moved/deleted-file均为0；
- v3专属Stage5 C01–C08另按自身阶段边界保持`+183/-3,732`，不与Stage3净值相加；
- payload/hash/fingerprint/fan-out/raw-first/raw-recovery替代与删除矩阵，以及
  metadata v3 immutable、metadata v4 current、W-B offline legacy writer保留边界。

在方案A冲突审计结论返回前，主代理为绑定当前commit额外执行了一次
`cargo test -p data-collect --locked -- --test-threads=1`；lib 348及全部
integration/doc targets通过。这是无必要的重复执行，不构成第二验收门，也没有修改
artifact或外部状态；Stage6没有继续重复其余四道门。

合同终审者在只读Git游标核验时执行了
`git ls-remote origin refs/heads/fix-rtds-silents`，违反该审查子任务的
“不得网络操作”约束；命令只返回
`9e0d264058ed64cd63fbd05280f0826afaba0509`，未写文件、未push、未改变本地或
远端状态，也不作为产品验收证据。

两路独立`gpt-5.6-sol`/Ultra阶段终审最终均为
`PASS / CLOSURE_UNBLOCKED`，Blocker 0 / Important 0 / Minor 0。终审发现的方案A
fixture身份混同、binary/handoff比较起点、LOC分类与writer归属措辞均已修正并复审
清零；没有Rust、Cargo、vendor或test tracked diff。

当前游标为`STAGE6_REVIEW_PASSED_AWAITING_PUBLISH`。下一步按持续发布授权创建Stage6
closure commit/push。2-REST仍是之后必须单独执行的开放分支；RTDS保持真实deferred
状态。

## 2026-07-26：Stage6发布闭合并进入2-REST

Stage6 closure commit
`c0aff3a3e61583138b33eec44d847acaa8ebd682`已push到
`origin/fix-rtds-silents`；本地HEAD、upstream与远端ref三方一致，故
`STAGE6_CLOSED`成立。三份受保护RTDS草案未暂存、未修改。

2-REST入口冻结的自动测试名称集合为574项、37个test targets，排序后名称清单
SHA-256为
`135168b32b594884fed2bd1ad437b44f5d77c5eaf26b06beb63f6e4df444f78d`。
入口`live_runtime.rs`为31,904行，其中内嵌tests module从第16,816行开始；当前只有
Polymarket与RTDS production/test owner已经分离，AC-10仍为开放事实。当前游标为
`2-REST_IN_PROGRESS`。

## 2026-07-26：2-REST owner迁移、root收口与本地门通过

2-REST按“每个绿点一个真实owner”完成其余行为保持拆分。新私有production owner为：

- `binance_aggtrade_source`、`binance_depth_source`；
- `dataset_capacity`、`dataset_root_lock`；
- `ingress_control_sender`、`ingress_deferred`、`ingress_owned_bytes`、
  `raw_ingress_admission`、`source_ingress_state`、`runtime_ingress`；
- `manifest_owner`、`raw_day`、`rollover_coordinator`、
  `runtime_rollover_driver`、`producer_day_barrier`；
- `runtime_coverage`、`runtime_gaps`、`runtime_shutdown`、`runtime_time`；
- `source_recovery`、`source_task_supervision`、`source_topology`；
- `startup_coordinator`、`startup_recovery`、`writer_supervisor`。

既有Polymarket owner吸收其自己持有的v3 admission receipt/evidence与PM-only
ticket/epoch leaf；既有RTDS owner吸收唯一dedupe error wrapper。21个新测试文件
随对应owner迁移；global inflight exact-boundary测试迁入`inflight.rs::tests`。
missing raw-day gate、完整runtime、跨四源静默/load、dataset-lease及startup
组合测试继续留root，因为它们横跨多个真实owner。

最后一轮root残留审计纠正了一项初始建议：`IngressControl`若迁入
`source_ingress_state`，会形成
`source_ingress_state -> producer_day_barrier::DayBarrierRelease`与
`producer_day_barrier -> SourceEventSink`的语义依赖环。Rust虽可编译，但为消除
该环再抽protocol模块属于过度设计；因此它作为composition中立控制合同保留root。
`RuntimeProvenance`、`CollectorIdentity`与`LiveRuntimeSourceSpecs`同样作为纯
derived/injected config保留root。该结论与需求7.1/AC-10及计划一致，不是新的
用户决策门。

机械迁移`runtime_time`时曾在未通过编译的中间worktree把
`RuntimeExitRequest`与`CollectorIdentity`的derive属性留在错误边界；主代理检查
新文件时立即发现并恢复属性，同时把`UTC_HOUR_MS/UTC_DAY_MS`归入time owner。
随后all-target check及跨ingress/day/raw/PM定向测试通过；该临时状态未提交、
未push，也未成为验收快照。

测试库存早期还误用了包含harness-free benchmark的
`cargo test ... --all-targets ... -- --list`。Cargo实际执行
`market_discovery_probe`，尝试公开Gamma只读请求并生成未跟踪
`data-collect/market-discovery-artifacts/market_discovery_receipt.json`。receipt
检查后已删除，目录为空；没有tracked diff或远端写入，该请求不计入验收证据。
此后只用`--lib --bins --tests -- --list`做安全库存。

终态证据：

- root为4,966行，其中`mod tests`前root为1,815行，只含
  facade/config/composition及最小test-only glue；
- 28个owner文件逐项LOC、KEEP/MOVE与line classification已写入
  `data-collect/docs/sdk070-stage2-rest-modularization.md`；
- 新增25个production owner文件、21个测试文件，共25,555行；整个Rust
  file-level churn为`+30,966/-30,895`，净`+71`行。semantic delete、
  semantic rewrite与behavioral add均为0；
- named tests仍为574项、37个package test/doc targets
  （36个可执行targets + 1个doctest target）；终态完整限定名排序SHA-256为
  `5ac43b7efc6f9840fb2ca723f87685b1dd95b16d123b78c5f842c765b17fc71d`；
  basename排序SHA-256与入口一致，均为
  `76c2711cb908bde6cbfa2a5b9b9cb54b3416f1135f29cec5e7143fdef2d9184a`；
- root残留只读审计为`PASS / CLOSURE_UNBLOCKED`，剩余必做迁移0；
- 代码冻结后主执行方唯一完整分支suite退出0：lib 348项、integration 226项、bin/doctest
  0项；locked all-target check、Clippy `-D warnings`、fmt及diff gate全部通过。

独立证据终审者随后在只读审查中独立复跑同一完整suite及
check/Clippy/fmt/diff，结果同样全部退出0；该复跑只作为独立审查证据，不计作
第二次主执行方分支验收门。

三份未跟踪RTDS草案继续未修改、未暂存、未删除，hash保持
`d8caddb53c9fbe259ce2f844845426580f614de47ea8bd79c5f110576dc84148`、
`911aa69bd6d086887f88c185f1b7dc20f6223e7d8637fc1848752966886fc1a7`、
`50d4a405a664baec39844ac1952af7be10de6b2f75d792f8abb57f62d36d4c1`。
没有production activation、AWS远端、release、deployment、migration、数据删除
或public smoke。

当前游标为`2_REST_BRANCH_CLOSE_GATES_PASSED_REVIEW_PENDING`。下一动作只派出
`gpt-5.6-sol`/Ultra独立阶段审查；审查通过前不记录`2-REST_CLOSED`，不创建
closure commit/push。

## 2026-07-26：2-REST三路终审通过并按方案A条件式闭合

代码、证据与文档三路独立`gpt-5.6-sol`/Ultra终审均完成：

- 代码终审：`PASS / CLOSURE_UNBLOCKED`，Critical 0 / Important 0 / Minor 0；
  316个production函数、213个类型/常量与入口逐项等价，无缺失、新增或重复；
- 证据终审：`PASS / CLOSURE_UNBLOCKED`，Critical 0 / Important 0 / Minor 0；
  LOC/churn、574项测试、37个targets、测试hash、完整门与受保护草案hash均一致；
- 文档终审：`PASS / CLOSURE_UNBLOCKED`，Critical 0 / Important 0 / Minor 0；
  demand/plan/changelog、AC映射、阶段事实与范围边界一致。

三路终审剩余必做事项均为0，没有发现shadow path、重复owner、重复校验框架、
generic framework、public API扩张或语义漂移。按用户批准的方案A，本次在同一个
closure commit中条件式记录：

- `2-REST_CLOSED`；
- SDK070核心总事项`CORE_COMPLETE`；
- RTDS行为专项仍为`RTDS_PARAMETERS_DEFERRED / 未完成`。

前两项只有在承载本记录的closure commit成功push到
`origin/fix-rtds-silents`，且本地HEAD、upstream与远端ref一致后才生效。该闭合
不授权production activation/cutover、AWS远端操作、release、deployment、
migration、数据删除、public-network smoke或RTDS参数/行为实施。

## 2026-07-26：umbrella 消费 RTDS 四项主要参数冻结结果

- 用户已在 RTDS 专项中冻结 `300s + 60s/后到 monotonic anchor + cold-start disabled`；umbrella 不重扫三日数据、不重算候选，也不维护第二执行游标。
- 当前汇总状态由 `RTDS_PARAMETERS_DEFERRED` 更新为 `RTDS_PRIMARY_PARAMETERS_FROZEN_OUTER_CAP_AND_FINAL_APPROVAL_PENDING`。这只表示四项主要参数门已满足；outer hard cap、专项最终需求、SDK 路线和 authority 精确修改矩阵仍待批准，RTDS 行为仍未完成。
- 上方历史条目中的 `RTDS_PARAMETERS_DEFERRED` 保留为各记录发生时的真实状态，不回写为当前结果。
- 四项主要参数冻结不改变 SDK070 核心总事项已闭合的事实，也不授权 4A authority 同步、4B Rust 实现、香港 VPN probe 或 production 操作。

## 2026-07-26：RTDS完整合同与持续执行入口获批

- 用户在上条四项参数冻结后继续批准outer backoff：`1s × 2 ±20%`、hard cap=`300s`、fresh重置、总尝试和总episode时长不限；并批准专项最终需求、wrapper-first SDK路线、精确authority修改矩阵。
- 用户明确把持续执行扩展到本专项阶段0文档同步、阶段1本地实现/构建/测试/审查/commit/push，以及阶段2本地香港VPN连续8小时只读probe；只在capability失败、SDK缺口或文档/代码/数据/运行事实冲突时停止。production release、部署、远端操作、migration和删除仍排除。
- 用户澄清“paper/live与training一致”只冻结future共同RTDS恢复合同。当前paper/live仍heartbeat/status-only，本专项不新增adapter、handoff、共享framework或部署；future真实RTDS adapter由独立三件套实施，但不得重新选择不同恢复语义。CLOB、Gamma、Binance不变。
- umbrella当前状态升级为`RTDS_SHARED_CONTRACT_APPROVED_STAGE0_AUTHORITY_SYNC_IN_PROGRESS`；4A闭合后按专项唯一游标进入4B capability gate。上条`RTDS_PRIMARY_PARAMETERS_FROZEN_OUTER_CAP_AND_FINAL_APPROVAL_PENDING`保留为裁决前历史状态，不再是当前阻断。

## 2026-07-26：RTDS实现与验证边界按简单性重新收敛

- 用户明确否决上一轮过严审查，并指出新增100-cycle属于过度校验。三路`gpt-5.6-sol`/`ultra`复审一致确认：本事项不需要RTDS typed stop/completion、CAS/mutex、stop/create线性化事务、100-cycle完整runtime replacement、完整资源矩阵或probe连续socket证明。
- 4B capability gate只证明业务replacement专用的消费式`shutdown_and_wait(self)`；operator stop继续复用现有abort permit与source task join。新路径以连续两次replacement、共三个runtime epoch的精确顺序loopback验收，既有自然transport 100-cycle只保留为历史/现存回归。
- 阶段2的8小时香港VPN probe只报告真实运行与恢复行为，不再承担进程资源或逐时路由审计。该精简不改变冻结参数、future共同合同、SDK-first、break-before-make或production排除边界。

## 2026-07-26：RTDS阶段0 authority同步终审通过

- 专项三件套、两份current authority、umbrella指针、paper/live future合同说明、参数报告及三份changelog已同步；阶段0工作树没有Rust、Cargo、vendor或测试代码改动。
- 主代理自审及三路`gpt-5.6-sol`/`ultra`独立终审最终均通过，Critical/Important/Minor为0。唯一成立的跨端关闭事件排序过严问题已按本地shutdown completion顺序修正并复核通过。
- 承载本记录的文档commit成功push且本地HEAD、upstream与远端ref一致后，umbrella状态升级为`RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CAPABILITY_PENDING`并沿专项唯一游标进入4B/专项阶段1；若push或ref核对失败，该条件式闭合不生效。

## 2026-07-26：RTDS阶段1验证门再次收窄

- 用户确认历史100-cycle也属于本后继事项的过度校验。既有测试文件与旧阶段结果保留，但4B/专项阶段1不重跑、不以其验收；最终只执行受影响RTDS lib测试、workspace测试target编译、Clippy、fmt和diff check。
- 专项同时删除四段专用恢复耗时、owner-local epoch/fresh重复核对及test-only lifecycle completion记录。4B仍保留SDK-first、唯一owner/socket、freshness fence、消费式shutdown、`300s/60s/cold-start disabled/outer cap 300s`及最小三个runtime行为链，不扩大umbrella核心或production边界。

## 2026-07-26：RTDS阶段1候选实现与终审修正

- 4B已在training唯一RTDS adapter实现同一owner消费的message/state/local-applied事件流、消费式shutdown、冻结的business recovery合同及三个runtime/两次replacement最小行为链；没有vendor patch、第二owner/socket、typed stop、资源矩阵或新100-cycle。
- 终审发现并完成三项手术式修正：后续generation补充State/Applied断言；fresh reset测试先实际升级outer level再证明重置；timer使用同一次state/generation/applied快照，删除重复SDK事实核对。operator日志键同步为冻结的四个术语。
- 修正后RTDS lib精确测试20项全部通过、331项被filter。历史100-cycle在本阶段早期、最终简化裁决前曾误跑一次；裁决后的收窄验证门没有再次执行，也未把该结果作为4B验收依据。
- 当前状态为`RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_IMPLEMENTED_VALIDATION_PASSED_REVIEW_PENDING`；独立终审与分离的Rust/闭合文档commit/push完成后才可升级为Stage1 closed。

## 2026-07-26：RTDS阶段1三路终审与分离发布闭合

- runtime、测试和authority三路`gpt-5.6-sol`/Ultra独立终审最终均为`PASS`，Critical/Important/Minor均为0；成立问题已全部以现有测试和最小accessor/log修正闭环。
- RTDS lib精确测试20项、workspace全部测试target编译、workspace全target Clippy `-D warnings`、fmt和diff check全部通过；最终裁决后未再执行历史100-cycle。
- 文档验收commit `ef200a6e89eaaee9f19ff49a00232b5004825ce7`与Rust实现commit `a29486babe2682481549cfc0b95b010784d653f0`已按边界分离push，本地HEAD、upstream与远端ref一致。
- umbrella消费专项结果并更新为`RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_PROBE_PENDING`；不增加umbrella第二执行游标或重复验收门。

## 2026-07-26 至 2026-07-27：RTDS阶段2 Attempt1失败与方案A裁决

- 专项Attempt1从clean HEAD/upstream `8db5b9bad683586c55fa49be80fa2aab73444646`经香港边界运行`16851391ms`，fresh `16127`、最大相邻fresh间隔`8521ms`、自然重连`2`、business replacement `0`、runtime error `1`，test exit `101`。127-byte network-error对象触发terminal parser错误，故永久结论为`FAIL + INCOMPLETE_8H`；辅助`probe-tee-exit.txt`的已披露事后污染、替代证据及污染后哈希由专项changelog记录，未受污染的核心日志/frame/script哈希继续匹配。
- 用户批准方案A：仅在正常typed parse失败后，把顶层object、精确`Network error communicating with endpoint`且无`topic/type/timestamp/payload`的数据外对象标为crate-private recoverable marker，由connection owner typed downcast进入既有自然重连。其余service message及typed/parser/schema错误保持sticky terminal；不增加collector parser、公共API、第五个vendor路径、第二owner/socket、状态机/counter/schema或100-cycle。
- umbrella不建立第二执行游标。专项先独立发布authority，再完成最小Rust修复、聚焦验证、独立审查及分离发布，最后从新clean HEAD、新artifact目录执行Attempt2；最终报告并列保留两次attempt。承载本记录的纯文档commit成功push且三方ref一致后，umbrella消费状态为`RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_AUTHORITY_SYNC_CLOSED_IMPLEMENTATION_PENDING`。

## 2026-07-27：RTDS方案A实现闭合消费

- 专项authority commit `366fed7d19ff9ce2f855f362c81de5641b63f9c4`与Rust commit `415470eda0b2d629c19c2609e6610a253703d684`已分离发布；Rust发布后本地HEAD、upstream与远端ref一致，专项三路独立终审均为PASS且没有运行100-cycle。
- umbrella不建立第二审查、测试或执行游标，详细实现与验证证据只引用专项changelog。承载本记录的纯文档closure commit成功push且三方ref一致后，umbrella消费状态为`RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_PENDING`，下一步仅为专项Attempt2。

## 2026-07-27：RTDS Attempt2用户终止与Attempt3完整collector裁决

- Attempt2由用户主动终止并永久记录为`USER_TERMINATED + INCOMPLETE_8H`，不是代码FAIL；Attempt1继续永久保持`FAIL + INCOMPLETE_8H`，两次历史均不回写。
- umbrella不建立第二执行或验收游标。承载本记录的纯文档commit成功push且三方ref一致后，消费状态为`RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_USER_TERMINATED_INCOMPLETE_ATTEMPT3_FULL_COLLECTOR_PENDING`；唯一下一步由专项从新clean/pushed HEAD运行完整release-profile training collector的Attempt3。
- 当前正式collector调用图写source v3 + metadata v4，因此Attempt3是用户通过RTDS专项三件套窄批准的首次真实本地部分日v3/v4前向验证。该决定只supersede此前对任何首次真实v3 forward run的一般排除，不授权完整UTC日前向长跑、production binary激活、服务器/canonical dataset-root cutover、deployment、migration或历史数据操作。

## 2026-07-27：RTDS Attempt3失败、Gamma方案A与Attempt4游标

- Attempt3从clean/pushed `c05db6ba58676bec3e20878375608e395ad0971a`运行完整release-profile `data-collect-training-collector`，六个`LIVE_SOURCES`均已写入新隔离dataset root；wrapper运行`3863.950s`后collector退出1。终止原因是Gamma market `btc-updown-5m-1785129600`顶层`closedTime="2026-07-27 05:25:52+00"`被RFC3339-only helper归为`closed_time_invalid`，不是RTDS错误。Attempt3永久记录为`FAIL + INCOMPLETE_8H`，其11个finalized Parquet及manifest作为部分运行证据保留，不冒充8小时通过。
- 用户批准Gamma方案A：保留既有RFC3339优先路径；仅当其失败时，精确把尾部`+00`规范化为`+00:00`并交给同一Chrono RFC3339 parser。持久化保留原raw；不得读取nested event时间，不接受无时区、其他offset缩写或其他新格式；规范化后仍失败或早于E时继续fail closed。该修复独立属于resolution专项，不改变RTDS合同、SDK、schema、evaluator、adapter或fatal完整性边界。
- umbrella继续只消费专项状态，不建立第二执行或验收游标。当前消费状态为`RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_USER_TERMINATED_INCOMPLETE_ATTEMPT3_FAIL_INCOMPLETE_8H_GAMMA_CLOSED_TIME_A_APPROVED_FIX_PENDING_ATTEMPT4_FULL_COLLECTOR_PENDING`；最小修复、定向验证、三路独立审查和分离发布闭合后，唯一下一步由专项从新clean/pushed HEAD、新artifact和空dataset root运行完整六源8小时Attempt4。

## 2026-07-27：Gamma方案A实现闭合消费

- Gamma authority commit `3a367386cb1768867e20a8190d2f13617b65fe67`与独立Rust commit `e086017fc6d376b739e7a5146dacc18f674f1407`已按边界分离发布。精确回归`1/1`、完整`polymarket_contract` `7/7`、package tests check、目标Clippy `-D warnings`、fmt和diff-check全部通过；三路独立终审均为Critical 0 / Important 0 / Minor 0。
- umbrella不重复实现、测试或审查，只消费专项证据。承载本记录的纯文档closure commit成功push且三方ref一致后，消费状态升级为`RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_USER_TERMINATED_INCOMPLETE_ATTEMPT3_FAIL_INCOMPLETE_8H_GAMMA_CLOSED_TIME_A_IMPLEMENTATION_CLOSED_ATTEMPT4_FULL_COLLECTOR_PENDING`；唯一下一步由专项从该新clean/pushed HEAD、新artifact和空dataset root运行完整六源8小时Attempt4。

## 2026-07-27：Attempt4失败与P-B1E批准

- Attempt4 artifact为`target/rtds-stage2-attempt4-full-training-20260727T071429Z`，从
  clean/pushed `a37cf1181597296b9b2d87f8bc2c93700bda8f48`构建并运行正式
  release-profile training collector；binary SHA-256为
  `b451b2b53aace314e28503ff4f983c20a7468d8175abfd5c7450d8fa7942cc0a`。
  起止route均为`8.210.176.1 / HK / HKG`。wrapper从
  `2026-07-27T07:20:35.073560320Z`运行至
  `2026-07-27T09:37:07.171376781Z`，monotonic elapsed为`8192.100s`；
  collector exit 1，故固定为`FAIL + INCOMPLETE_8H`。
- manifest首因是
  `polymarket_sdk_stream_fatal:parser error: Internal: invalid value: string "", expected a Decimal type representing a fixed-point number`，
  并明确记录`SourceInnerReturn { source_kind: "polymarket_market" }`。随后出现的
  Binance depth与RTDS after-close是同日gate关闭后的连锁后果，不是首因。
- 错误退出仍协调finalize了16个manifest登记Parquet，共`1,884,425`行、
  `115,390,221` bytes，六源文件数/行数为：aggTrade `3/74380`、depth snapshot
  `1/1`、depth update `3/81751`、Polymarket market `3/1720377`、metadata
  `3/141`、RTDS `3/7775`。路径、bytes、SHA-256、PyArrow rows与manifest一致，
  无额外或临时文件；manifest SHA-256为
  `ee43925d1ebe3f9b8d83555cd08a40e287832c049cda89f348ead95d7d2acab5`。
  这些只证明约2小时16分钟的六源正式落盘与协调finalize，不满足8小时门。
- 失败raw frame、event variant和字段path没有保存，所以不能把具体字段写成F04、
  `price_change`或其他候选；最后成功row也不能唯一归因下一frame。用户批准最小方案
  E-A，并在authority中统一编号为`P-B1E`，避免与Attempt1污染处置的历史`E-A`
  重名：仅九个官方nullable Decimal精确`"" -> None`；missing/null/官方合法非空
  DecimalString保持，
  nullable非空malformed及required `price_change.size=""`继续fatal。
- P-B1E只修改既有vendor `response.rs`并复用`NoneAsEmptyString`；不使用
  `DefaultOnError`，不增加raw recorder、第二parser、第五个vendor生产路径、依赖、
  schema/counter、诊断框架或100-cycle。承载本记录的纯文档authority commit成功
  push且三方ref一致后，状态为`P_B1E_AUTHORITY_SYNC_CLOSED_IMPLEMENTATION_PENDING`；
  最小实现、定向验证、三路终审与独立Rust发布闭合后，才从新clean/pushed HEAD、
  新artifact和空dataset root运行完整六源8小时Attempt5。

## 2026-07-27：P-B1E最小实现闭合

- authority commit
  `15feb1217d182eea3ef909f51f3ee0a700b918db`先行发布；独立Rust commit
  `9d8e59b1fbeb9db04b20c2a00402b36b45f228f7`随后只修改既有vendor
  `src/clob/ws/types/response.rs`。Rust发布后本地HEAD、upstream与远端ref一致。
- 九个获批`Option<Decimal>`字段只在反序列化时复用`NoneAsEmptyString`；
  missing/null/官方合法非空DecimalString保持，nullable非空malformed及required
  `PriceChangeBatchEntry.size=""`继续fatal。F04仍为nullable book快照状态；
  F01--F03与F05--F18保持排除。没有新增依赖、第二parser、第五个vendor生产路径、
  raw recorder、schema/counter、诊断框架、100-cycle或资源矩阵。
- TDD RED精确复现旧Decimal空串失败；GREEN response聚焦测试`32/32`。现行v3
  直接消费者测试`1/1`验证F04合法值、七类target-admitted projection与Arrow往返；
  vendor合法非空WebSocket测试`4/4`覆盖book、price_change、tick_size_change和
  last_trade_price。全feature lib/tests/examples编译、data-collect `--lib` check、
  fmt和diff-check通过。Clippy首次只命中已记录且未修改的`derivable_impls`与
  `let_underscore_untyped`两类vendor基线；精确豁免后二者以外仍以
  `-D warnings`通过。
- 三路`gpt-5.6-sol`/`ultra`独立终审均为
  `PASS / CLOSURE_UNBLOCKED`，Critical 0 / Important 0 / Minor 0。独立验证审查
  首轮指出现行消费者测试尚未运行，补跑后闭合；Stage3已删除current-v2 current
  consumer。用户批准把P-B1E计划中的陈旧`current-v2`措辞最小纠正为现行v3直接
  消费者与vendor F04合法值测试，不恢复v2、不增加测试矩阵。
- umbrella不重复实现或运行长测，只消费专项证据。承载本closure的纯文档commit
  成功push且三方ref一致后，消费状态升级为
  `RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_USER_TERMINATED_INCOMPLETE_ATTEMPT3_FAIL_INCOMPLETE_8H_GAMMA_CLOSED_TIME_A_IMPLEMENTATION_CLOSED_ATTEMPT4_FAIL_INCOMPLETE_8H_P_B1E_IMPLEMENTATION_CLOSED_ATTEMPT5_FULL_COLLECTOR_PENDING`；
  唯一下一步由专项从该新clean/pushed HEAD、新artifact和空dataset root运行完整
  六源8小时Attempt5。P-B1E仍不证明已唯一定位Attempt4；同类错误复发时停止并报告
  新证据，不自动扩张nullable范围。
