# data-collect SDK 0.7.0、typed schema、RTDS 恢复与 live runtime 模块化执行计划

> 事项编号：`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`
>
> 对应需求：`demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
>
> 对应 changelog：`changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md`
>
> 纳入的 RTDS 专项三件套：`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`、`plan/data-collect-rtds-business-silence-full-reconnect-plan.md`、`changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`
>
> 状态：`DRAFT_AWAITING_REMAINING_USER_REVIEW_AND_AUTHORITY_DECISIONS`。用户已于2026-07-24批准 `market_resolved` 的v3 new-write采用validate-first，并批准按后续审查结论修复本组三件套；又于2026-07-25批准`METADATA_V4_CLAIMANT_SCHEMA_APPROVED_2026-07-25`：新增`polymarket_market_metadata_v4`，复用同一artifact/writer/manifest mapping并只追加两个nullable WS claimant字段；metadata v3 schema/version立即冻结，activation前仍是current metadata writer，activation后转为read-only。版本决策blocker已解除，但这些授权只覆盖文档同步，未授权任何Rust/schema/test实施；metadata v4实现及WS claimant/recovery验收完成前不得原子激活source v3 + metadata v4 current new-write。用户现已单独授权把本轮审查后的文档同步集创建并push为pre-execution checkpoint；该授权不启动任何实施阶段。其余总计划决定及所有实施阶段仍等待用户逐项或逐阶段授权；既往授权越界事实只在对应changelog保留且不计入实施验收。

## 一、执行原则与成功标准

执行采用依赖 DAG，而不是把独立工作错误串成单一阻塞链：

```text
共同基线 / authority delta matrix
  ├─ SDK 0.7 runtime验证
  │    ├─ 2-PM分支收口 + typed字段决定 -> v3 -> v3集成验证
  │    └─ 2-RTDS分支收口 + RTDS参数/authority批准 -> RTDS专项
  └─ 2-REST其余行为保持模块拆分与删除证据审计
```

typed字段缺口只阻断 v3；`RTDS_PARAMETERS_DEFERRED` 只阻断 RTDS。SDK runtime不可用才阻断依赖该 runtime的 v3/RTDS，但不阻断文档、静态证据审计或不依赖 SDK变更的机械拆分。

执行必须同时满足：

- **简单**：复用官方SDK和现有source/schema/writer/budget/manifest owner边界；Stage3只在唯一owner建立最小v3 canonical domain type，不创建generic framework、第二canonicalizer、shadow path、通用allocator tracker或无第二消费者的抽象。
- **准确**：保持唯一字段/owner映射、数组顺序、nullability、decimal文本、时间、admission和 handoff语义；冲突时停止对应工作。
- **可靠**：只验证已证实的不变量，并在唯一 trust boundary做一次；不以重复全量测试、固定 LOC、固定测试数量或无依据的性能比例代替正确性。

不能把 SDK 升级、文件移动、schema 变化、RTDS 行为和死代码删除混入同一个 diff。每个小型机械绿点只需：

1. 先满足对应需求和 AC；
2. 运行 owner定向测试、受影响跨模块测试及 compile/check；
3. 主代理检查 diff、测试证据、authority和范围；
4. 同步 plan/changelog的真实 cursor与偏差。

完整确定性回归和 `gpt-5.6-sol`、`ultra` 独立审查只在阶段收口执行；coordinator/day/manifest/shutdown等高风险迁移可额外执行一次完整门。修复 Critical/Important 后复审；只有阶段另行获得 commit/push授权时才提交。

若发现文档、代码、数据或 SDK 事实冲突，执行需求第十节停止条件，不自行调和。

## 二、阶段 0：authority、参数和可复核基线

### 任务 0.1：审核并批准总三件套

- 步骤 0.1.1：用户审核“消费语义无损而非 source-native 全字段/原始 wire 无损”的定义。
  -> 验证：venue `hash` 已是 `USER_DECIDED_EXCLUDE`，不再次请求确认；结果不得宣传为完整 source message重建。
- 步骤 0.1.2：核对用户已决定把 RTDS 专项作为受控子需求纳入，但未自动批准其参数和相反行为。
  -> 验证：不再次请求纳入决定；demand/plan/changelog 均引用原 RTDS 三件套且状态一致。
- 步骤 0.1.3：冻结每阶段授权边界。
  -> 验证：文档批准、Rust 修改、构建/测试、commit/push、网络 probe、cutover、部署和删除分别列出。
- 步骤 0.1.4：登记用户已批准的 `market_resolved` v3 validate-first决定，不再次请求同一语义。
  -> 验证：决定只约束未来v3 new-write；当前`polymarket_orderbook_v2` source代码、Rust/schema/tests和整个总需求均未因此自动获批。
- 步骤 0.1.5：登记`METADATA_V4_CLAIMANT_SCHEMA_APPROVED_2026-07-25`，不再次请求metadata版本选择。
  -> 验证：只冻结metadata v4 schema合同与未来activation pairing；不把文档批准写成Rust/schema/test实施、current调用图切换或production cutover授权。

### 任务 0.2：建立精确 authority delta matrix

- 步骤 0.2.1：逐条列出 `source_native_payload_json`、fingerprint recovery、venue `hash`、raw JSON/equivalent evidence和 resolution raw projection的旧条款。
  -> 验证：每项包含旧路径/锚点、新语义、保留合同、被取代范围和测试；至少覆盖reliability/data-collect demand、`plan/data-collect-plan.md`、retention plan及resolution follow-up三件套，并把当前`HandoffFrame v1.raw_json`明确标为`KEEP_OUT_OF_SCOPE`。`market_resolved` raw-first、Gamma-first raw-only corroboration和post-horizon raw-only restart矩阵已由用户批准改为v3 validate-first，矩阵必须标记`USER_DECIDED_V3_VALIDATE_FIRST`而不是待决。
- 步骤 0.2.2：只引用并消费RTDS专项三件套已经记录的business-silence authority delta、参数状态和测试入口/出口。
  -> 验证：umbrella不重扫数据、不重算参数候选、不重建delta/test矩阵，也不维护第二执行游标；专项路径、状态和阻断原因可精确解析。
- 步骤 0.2.3：除已经单独批准并完成文档同步的`market_resolved` v3 validate-first与metadata v4 claimant两个窄决定外，阶段0只建立并评审矩阵，不同步其他相反authority；旧changelog不回写。
  -> 验证：上述两个carve-out不得重新置为待决，也不得冒充Rust/schema/test实施；其余typed authority只允许在SDK字段门通过、用户批准v3字段集合并单独授权阶段3后同步。RTDS authority只在参数/需求/delta获批并单独授权4A后同步，随即停止，4B Rust/构建/测试必须另获授权。

### 任务 0.3：消费 RTDS 专项状态，但不重复执行

- 步骤 0.3.1：读取RTDS专项计划/变更记录中的`steady_silence_threshold`、`first_fresh_observation_window`及anchor、cold-start watchdog三项状态。
  -> 验证：只记录`APPROVED`、`PENDING`或专项已经给出的真实阻断，不在umbrella计算或提出第二组参数。
- 步骤 0.3.2：把专项状态映射为本计划的单一入口结果。
  -> 验证：三项未全部冻结时只记录`RTDS_PARAMETERS_DEFERRED`并停止4A/4B；专项获批后沿用其唯一执行游标，不复制任务、测试或长跑。

### 任务 0.4：记录当前代码与数据基线

- 步骤 0.4.1：保存 HEAD、worktree status、Rust toolchain、Cargo tree、SDK vendor provenance和现有 patch文件清单。
  -> 验证：结果写入 changelog和阶段 artifact。
- 步骤 0.4.2：重新统计当前 `live_runtime.rs` 总行数、测试起点、tests、types/functions、first-party LOC与调用链。
  -> 验证：不沿用历史 remote branch的 37k/218-test快照。
- 步骤 0.4.3：保存用户文件与完整UTC日的column-chunk、row count、schema/storage/resource/manifest事实及读取/finalize性能基线。
  -> 验证：计算脚本、版本、输入路径和输出 artifact可复核。
- 步骤 0.4.4：把性能指标分成硬门与报告项。
  -> 验证：硬门仅为被删列/fan-out确实消失、v3 finalized bytes小于v2、语义与资源边界通过；实际减少比例、CPU、RSS、finalize和scan为报告项，不能预设百分比。

### 阶段 0 审查门

- demand、plan、changelog 事项编号与引用一致；
- authority冲突没有被草案状态掩盖；
- 没有 authority、Rust/Cargo/vendor/schema/data diff；
- `git diff --check` 通过；
- 独立审查通过后，只在用户授权时 commit/push。

## 三、阶段 1：独立升级官方 SDK 0.7.0

### 任务 1.1：重建官方 vendor provenance

- 步骤 1.1.1：从 crates.io取得 `polymarket_client_sdk_v2-0.7.0.crate`。
  -> 验证：SHA-256 精确为 `ba212e0641f178c274af266772de15962ac7e76da550a0f79f47b49349b1138a`。
- 步骤 1.1.2：核对 package metadata、MIT license、MSRV `1.88.0`、VCS `222143d321eba97d5711a848265eb9aab3bc7ff4` 和官方文件清单。
  -> 验证：crate内容与 provenance artifact一致；不存在的 tag/release object不写入证据。
- 步骤 1.1.3：以官方 crate全新建立“以官方 0.7.0 为底座的本地 `third_party/polymarket_client_sdk_v2-0.7.0-p1` vendor”。
  -> 验证：Cargo package version仍为 `0.7.0`；不是旧目录改名，unpatched tree与 crate逐文件一致。

### 任务 1.2：逐 hunk 重放当前四文件 patch

- 步骤 1.2.1：相对当前 HEAD 导出四路径 patch和需求/测试映射。
  -> 验证：connection、config、CLOB response和RTDS subscription每个 hunk都有 owner。
- 步骤 1.2.2：在官方 0.7.0上重放，不使用旧 `e433eec` commit直接 cherry-pick。
  -> 验证：相对官方 crate的源码差异恰好四个批准路径。
- 步骤 1.2.3：若出现第五个路径、语义冲突或上游行为变化，停止并报告。
  -> 验证：未经新决定不扩 patch。

### 任务 1.3：更新 workspace依赖

- 步骤 1.3.1：更新 workspace `exclude`、`[patch.crates-io]`、data-collect精确版本和 bench provenance引用。
  -> 验证：本地路径为 `0.7.0-p1` vendor，Cargo package/version requirement为精确 `0.7.0`。
- 步骤 1.3.2：更新 `Cargo.lock`，检查 feature和 transitive dependency变化。
  -> 验证：`cargo tree -p data-collect --locked -e features -i polymarket_client_sdk_v2` 只有 0.7.0。
- 步骤 1.3.3：确认本阶段不修改 `data-collect/src`、schema或 RTDS业务逻辑。
  -> 验证：diff path gate。

### 任务 1.4：SDK 与项目可用性测试

按从窄到宽顺序运行：

```bash
cargo test --manifest-path \
  third_party/polymarket_client_sdk_v2-0.7.0-p1/Cargo.toml \
  --locked --lib --features clob,gamma,rtds,ws

cargo test --manifest-path \
  third_party/polymarket_client_sdk_v2-0.7.0-p1/Cargo.toml \
  --locked --all-features

cargo test -p data-collect --locked --tests
cargo check -p data-collect --locked --all-targets
cargo clippy -p data-collect --locked --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

不使用 `cargo test --all-targets` 运行可能访问真实网络、写 artifact或 `harness=false` 的 bench。bench只做 compile check；真实 smoke另受任务 1.5约束。

- 步骤 1.4.1：证明 upstream async execution相关 mock tests未被 vendor破坏。
  -> 验证：不调用真实订单或钱包。
- 步骤 1.4.2：从单次`cargo test -p data-collect --locked --tests`结果映射阶段开始时实际发现的named patch/runtime contract tests；`17/19`只作为2026-07-24快照。只有该suite未覆盖明确target，或失败诊断需要时，才补跑对应定向命令。
  -> 验证：sticky terminal、all-or-error、generation-bound send、heartbeat、EOF、timeout、manager drop和100-cycle等冻结行为均有结果，不以固定测试数量或重复运行同一target过门。
- 步骤 1.4.3：完整 deterministic suite通过。
  -> 验证：记录实际测试名称集合、命令、状态、时长和失败摘要；只有长期保留的独立 artifact才需要 SHA-256。

### 任务 1.5：公开只读能力 smoke（单独授权）

- 步骤 1.5.1：只订阅 public CLOB/RTDS并执行 Gamma public read。
  -> 验证：无认证 user channel、无 signer、无订单、无资金。
- 步骤 1.5.2：记录 SDK variant、连接/关闭和 parse结果。
  -> 验证：smoke失败不以 synthetic测试掩盖。

### 任务 1.6：SDK字段能力矩阵

- 步骤 1.6.1：从需求2.3已知矩阵开始，逐variant对照官方realtime docs字段/nullability、0.7.0 typed struct与serde parser、现行target admission、当前v2 fields和当前consumer/既有已批准合同。
  -> 验证：每项标记`PRESENT / EXPLICITLY_EXCLUDED_HASH / SDK_FIELD_GAP / SDK_NULLABILITY_PARSE_GAP / NOT_APPLICABLE`，并固定官方页面访问日期与SDK commit。
- 步骤 1.6.2：为`book`、`price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask`、`new_market`和`market_resolved`构造完全符合官方文档的missing/null fixture，分别直接运行官方upstream 0.7与当前四文件patch parser；同时核对需求2.3列出的缺失typed字段。
  -> 验证：字段齐全fixture通过不能掩盖官方合法missing/null帧被拒绝；parser拒绝按variant记录为`SDK_NULLABILITY_PARSE_GAP`，不得降级为普通nullable Parquet值。不把venue `hash`与transaction hash混淆，不从authenticated `TradeMessage`错接public market字段。
- 步骤 1.6.3：由当前consumer/既有已批准合同矩阵识别每个field gap是否必需；所有非必需field gap的“排除或`nullable_unavailable`”必须由用户逐项批准。future paper/live静态附件不创造必需字段。
  -> 验证：agent不自行降级字段；任何官方合法target相关帧的typed parser拒绝直接阻断v3，不能用`nullable_unavailable`掩盖；字段处置与v3拟定schema逐项对应。
- 步骤 1.6.4：分别确定 runtime与 typed-contract终态。
  -> 验证：runtime只能为 `SDK_070_RUNTIME_USABLE / SDK_070_RUNTIME_NOT_USABLE`；typed contract只能为 `SDK_070_TYPED_CONTRACT_SUFFICIENT / SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED / SDK_070_TYPED_CONTRACT_BLOCKED`。

### 阶段 1 审查与提交门

- 独立审查 SDK provenance、四文件 diff、feature tree和测试证据；
- `SDK_070_RUNTIME_USABLE` 后可继续相应机械拆分；typed contract为 `SUFFICIENT` 或用户逐项批准的 `NONBLOCKING_GAPS_APPROVED` 后才可进入 v3；
- runtime不可用时停止依赖它的 v3/RTDS；typed contract为 `SDK_070_TYPED_CONTRACT_BLOCKED`时只停止v3，不得用 raw/shadow路径补洞；
- 通过后更新三件套；
- 只有用户授权才 commit/push；
- schema/refactor/RTDS diff必须为零。

## 四、阶段 2：共同合同基线与三个独立拆分分支

### 任务 2.1：observable contract

- 步骤 2.1.1：冻结 training entrypoint、4 logical producers、6 artifacts、source identity、timestamp/admission和 current schema。
  -> 验证：每项有代码 symbol、fixture和代表性 artifact。
- 步骤 2.1.2：冻结 UTC day/hour、RawDayGate、rollover、writer finalize、manifest durable ack和 shutdown顺序。
  -> 验证：状态转换表覆盖 deadline、operator stop、panic、queue/writer/manifest failure和 reply loss。
- 步骤 2.1.3：冻结 resolution current/next/resolution_pending、terminal-once、slot和 restart结果。
  -> 验证：对应专项 AC映射完整。
- 步骤 2.1.4：冻结 paper/live binary、role、handoff、startup receipt和 consumer boundary。
  -> 验证：明确当前hot binary只有heartbeat、`HandoffFrame v1`仍含`raw_json`且本事项不修改；不把future external-event adapter写成已实现。

### 任务 2.2：有界 symbol、message-flow 与测试基线

- 步骤 2.2.1：工具自动记录当前 symbol和测试名称集合；只对跨 owner channel、public API、删除候选及高风险 coordinator/day/manifest/shutdown项人工分配 owner。
  -> 验证：`273`只作为2026-07-24历史快照；阶段以实际 before/after名称集合对账，不为每项测试重复记录fixture/命令。
- 步骤 2.2.2：只对跨 owner task message记录 sender、receiver、数据/控制语义和 durable effect。
  -> 验证：没有只靠名称猜 owner。
- 步骤 2.2.3：为当前 implementation区 `#[cfg(test)]` hooks记录生产影响。
  -> 验证：不为了移动测试扩大 public API。

### 分支 2-PM：v3所需最小 Polymarket owner

按依赖逐个移动且每步只移动一个owner：

1. Polymarket source owner及其直接测试；
2. Polymarket adapter/row conversion owner及其直接测试；
3. Polymarket-specific writer integration seam及其直接测试；
4. Polymarket resolution recovery owner及其直接测试。

内部小步只做编译所需path/visibility适配，运行owner定向、受影响跨模块测试及compile/check并由主代理自审；不得改变schema、payload/hash/fingerprint或admission。分支收口运行一次完整确定性门和`gpt-5.6-sol`/`ultra`独立审查，先记`2-PM_REVIEW_PASSED_AWAITING_PUBLISH`；只有获授权并完成对应commit/push后才记`2-PM_CLOSED`。它是阶段3机械前置，不依赖2-RTDS或2-REST。

### 分支 2-RTDS：专项所需最小 RTDS owner

只移动RTDS source owner及其直接测试，保持当前“业务静默不重连”authority和行为；内部按小步门验证，分支收口运行一次完整回归和独立审查后先记`2-RTDS_REVIEW_PASSED_AWAITING_PUBLISH`，只有获授权并完成commit/push后才记`2-RTDS_CLOSED`。它是阶段4机械前置，不依赖2-PM或2-REST。

### 分支 2-REST：其余行为保持模块化

其余Binance aggTrade/depth、Gamma、shared writer supervisor、manifest、raw day/epoch/rollover、source supervision、runtime coordinator和root composition按收益/风险逐owner迁移。每个小步只移动一个owner及其直接测试，运行定向/受影响测试、compile/check和自审；coordinator/day/manifest/shutdown可额外运行一次完整门。

分支最终验收：

- root只保留facade/config/composition，owner唯一且依赖方向清楚；
- tests随owner分布，不形成横跨多个无关owner的单体测试模块；
- 报告各模块及root的moved/deleted/rewritten/added/net LOC，但不设固定LOC门；
- 分支收口运行一次完整确定性门和独立审查。

审查通过先记`2-REST_REVIEW_PASSED_AWAITING_PUBLISH`，获授权并完成commit/push后才记`2-REST_CLOSED`。`2-REST`不阻断阶段3或阶段4。三个分支均禁止test consolidation、死代码清理、generic source/actor/plugin framework和直接套用历史remote branch patch；发现语义漂移时回退对应小步，不用新测试接受漂移。

## 五、阶段 3：`polymarket_orderbook_v3` typed message

### 任务 3.0：同步 typed authority

- 步骤 3.0.1：确认阶段 1 runtime门通过、`2-PM_CLOSED`、typed-contract终态允许继续、用户已批准最终 v3字段矩阵并单独授权阶段 3。
  -> 验证：任何已知gap未获逐项决定或任一必需字段缺失时停止，不修改authority或代码；metadata v4版本合同已经批准，不再作为待决项，但其Rust/schema/test实施仍须包含在阶段3单独授权内。
- 步骤 3.0.2：复核已经单独同步的`market_resolved` v3 validate-first与metadata v4 claimant/原子activation文档条款，再只同步需求3.2所列其余payload/fingerprint/v2 raw projection现行authority，保留旧changelog。
  -> 验证：不得把已批准的validate-first或metadata v4版本合同重新置为待决，也不得把其文档同步冒充Rust/schema/test实施；其余authority diff逐条对应届时已批准delta matrix，不修改RTDS相反authority或`HandoffFrame v1.raw_json`合同，不混入Rust/Cargo/schema。
- 步骤 3.0.3：主代理审查 authority diff并按单独授权提交；阶段收口再做独立审查。
  -> 验证：代码修改开始前，当前 authority、最终字段矩阵和本阶段 AC一致。

### 任务 3.1：测试先行冻结 v3 schema

- 步骤 3.1.1：复用阶段1已经由官方文档约束的missing/null parser fixtures，并为七类可被SDK typed API接纳的public market variant建立target-admitted golden fixtures。
  -> 验证：parser gap不会被v3测试伪装成nullable row；可接纳fixture覆盖空/多元素数组、decimal文本、event message、terminal fields，以及含目标/非目标entries的mixed `PriceChange`。
- 步骤 3.1.2：写失败测试，要求 v3 schema没有 `source_native_payload_json` / venue `hash`，并使用 nested Arrow types。
  -> 验证：旧 writer应失败或只写 v2，测试不先改实现。
- 步骤 3.1.3：写 `PriceChange` 一条 target-admitted projection一行、list顺序和 round-trip测试。
  -> 验证：多 change fixture不产生多行；只保留通过现行 target asset/condition admission的 entries，并保持这些 entries的 SDK原始相对顺序。
- 步骤 3.1.4：写 unknown persisted event discriminator、损坏schema、缺列和nullable contradiction的fail-closed测试；对当前不可构造的SDK unknown typed variant只审查wildcard fail-closed分支并测试官方parser error传播，等官方SDK提供可构造variant后再加fixture。
  -> 验证：不为测试创建shadow enum/parser，不产生空evidence row。
- 步骤 3.1.5：写平台重复 delivery与内部 retry的区分fixture。
  -> 验证：两个完全相同的连续 platform delivery按原顺序产生两行，内部 retry不产生第三行；不引入新 fingerprint/hash。
- 步骤 3.1.6：为 `market_resolved` 写validate-first失败测试，明确candidate routing不等于admission。
  -> 验证：qualified current/next/resolution-pending消息完整校验后写一条row；完全unmatched消息写0条且不产生source writer admission/sequence、metadata、claim、transition或pending变化；命中任一目标routing signal但market id、condition id、nullable slug、asset count/duplicate/set、outcomes mapping、winning asset或winning outcome任一矛盾时，在任何row/metadata admission、claim或状态变化前fail closed，source row和terminal metadata均为0。其既有gap/error/coverage输出只能包含稳定reason code、已定义的最小身份坐标、计数和时间，不包含完整/截断source frame、项目重序列化JSON、payload digest或足以恢复消息的字段集合。后续qualified消息仍可按冻结的source fail-closed边界处理，不把unmatched误报为integrity error。
- 步骤 3.1.7：写Gamma-first与platform duplicate矩阵。
  -> 验证：accepted drain中的一条或多条合格late WS各写一条validated typed observation但不产生WS terminal、第二owner或第二transition；target-associated contradiction仍在row admission前fail closed；role-exit后的late WS写0条。两条相同qualified platform delivery产生两条不同source key的row，内部retry不产生第三条；metadata v4 terminal row通过claimant pair唯一指向实际claimant。
- 步骤 3.1.8：冻结source admission coordinate与已批准metadata v4 schema/null matrix。
  -> 验证：v3 source schema包含非空epoch、正数sequence和正数UTC毫秒admission time，唯一source key只由epoch+sequence组成；metadata v4以metadata v3全部字段、顺序、类型、nullability和语义为不可变前缀，只在末尾追加nullable `ws_claimant_source_sequence_epoch_id: Utf8`与`ws_claimant_collector_admission_sequence: UInt64`。metadata v3 schema/version立即冻结，activation前仍是current metadata writer，activation后只读；不得复用digest、时间或物理行位置。

### 任务 3.2：建立单一 domain typed message

- 步骤 3.2.1：用最小 enum + variant structs表达七类消息和共同envelope；envelope增加`source_sequence_epoch_id`、`collector_admission_sequence`和`collector_admission_time_utc_ms`。
  -> 验证：epoch在同一producer ownership内跨reconnect/UTC rollover稳定、restart更换；sequence在epoch内严格递增但允许空洞且不得回绕；admission time为正数UTC毫秒并验证`timestamp_received <= collector_admission_time_utc_ms`，不属于唯一key。不新增connection-generation、generic validation framework或JSON blob。
- 步骤 3.2.2：source owner先执行现行 target-market/asset admission；对 `market_resolved`，先把宽松candidate routing收敛到完整fixed identity/assets/outcomes/winner validation，再从通过准入的SDK message构造一次target-admitted domain projection。
  -> 验证：unmatched不写row且不改变状态；target-associated contradiction在row/claim/transition前fail closed；只有qualified消息产生domain。删除 typed -> canonical JSON -> normalized JSON -> typed往返的生产依赖，不建立第二validator。
- 步骤 3.2.3：完整target validation、对应row/byte permit和bounded source-writer queue ownership全部成立后，为每个domain message恰好分配一次Polymarket source coordinate，再从同一个validated object派生source row、terminal-once输入与metadata v4 WS claimant pair。
  -> 验证：unmatched、contradiction、permit failure、channel closed和queue admission失败均不产生可持久化source key后继续claim；两条相同platform delivery取得不同key，内部retry复用原key且只写一行；reconnect/day rollover不重置epoch或sequence，producer restart使用新epoch。Gamma-first只允许accepted drain中的validated non-owning observation，role-exit后不再admit；不重新解析payload JSON或新增quarantine/sidecar。
- 步骤 3.2.4：v3 cold writer/reader消费同一canonical domain；v2 legacy reader只输出独立`LegacyV2Row`或等价row-grain read-only audit view。
  -> 验证：v2不重建v3 message边界、canonical domain、admission coordinate或source reference，不成为第二sort/explode/canonicalize owner；不修改`HandoffFrame v1`，不实现hot external-event adapter。
- 步骤 3.2.5：记录`NONBLOCKING_FUTURE_SPEC`字段映射附件，不实现转换代码。
  -> 验证：只记录当前可知retained字段及后继metadata join负责的nullable decimal-string `price_to_beat/final_price`；未知future字段不构成v3失败，future frame version、legacy compatibility和唯一explode/sort owner留给后继三件套。

### 任务 3.3：实现 v3 writer/reader

- 步骤 3.3.1：在 schema owner中实现 event discriminator和 nested list/struct builders。
  -> 验证：实际 Parquet footer字段顺序、类型、nullability和 metadata匹配 golden。
- 步骤 3.3.2：新增的v3 writer只写v3，但在任务3.5.6前不替换current v2 writer；v2 reader始终保持legacy read-only。
  -> 验证：v3 writer拒绝unknown/legacy写入，reader兼容真实v2 fixture；未过activation barrier时生产调用图仍只激活v2 writer。
- 步骤 3.3.3：保持 ZSTD、hour/day partition、sort order、atomic rename和 manifest file facts。
  -> 验证：finalized row/bytes/SHA/schema逐项一致。
- 步骤 3.3.4：实现但暂不激活v3 writer；按collector admission order写入并验证同epoch内sequence严格递增，active v2 writer继续current new-write，v2 reader保持legacy read-only/receive-order。
  -> 验证：重复或倒退sequence、0 sequence、空epoch、非正admission time或`timestamp_received > admission time`均fail closed；不要求gapless，不增加文件级reference或sidecar。metadata v4实现与任务3.5验收完成前，current new-write调用图不得切到v3。

### 任务 3.4：准备替代 payload fingerprint 和 fan-out

- 步骤 3.4.1：让未激活v3路径不构造或持久化Polymarket canonical payload，并按 `size_of::<DomainMessage>() + Σ String::capacity() + Σ (Vec<T>::capacity() * size_of::<T>())`计算permit合同值，递归计入Vec已初始化元素拥有的动态缓冲区且每个allocation只计一次；active v2路径在任务3.5.6前保持不变。
  -> 验证：不得把Vec元素数当字节；未激活v3路径无`source_native_payload_json`，不把合同值声称为allocator实际bytes或覆盖共享buffer/Arrow内部开销，不创建通用追踪框架，v2 current writer与legacy reader均未被误删。
- 步骤 3.4.2：在未激活v3路径实现无跨进程fingerprint recovery、无fan-out前payload-equality drop的语义，并把旧active v2依赖登记为任务3.5.6同一activation diff中的待断开路径。
  -> 验证：v3 restart不投影payload，不创建sidecar/digest列；activation前v2生产行为不漂移。`14,664`只记录为现行full-day manifest中不可恢复的aggregate，不声称重放被丢消息或精确恢复row/event差异。
- 步骤 3.4.3：在未激活v3路径把一条domain message作为一个admission unit进入queue/writer。
  -> 验证：内部retry不双写，平台重复消息不被v3 collector语义静默丢弃；active v2行为在barrier前不变。
- 步骤 3.4.4：让v3 consumer/recovery不依赖旧fingerprint，并建立data-collect侧training/data-pipeline producer golden compatibility fixture；不修改真实data-pipeline adapter，不新增替代hash、semantic-key或强制duplicate drop。
  -> 验证：producer fixture只由canonical domain/v3 reader提供当前下游合同字段且不读取被删列；不能宣称真实data-pipeline端到端通过。active v2 consumer/recovery只在任务3.5.6切换时断开；future consumer若要求duplicate分类，另立合同，不能反向控制collector admission。
- 步骤 3.4.5：保留全局 `100_000 rows / 134_217_728 bytes`、单行 `1_048_576 bytes`、`2s` acquire timeout和 Polymarket decoded queue `25_000 messages / 67_108_864 bytes`上限。
  -> 验证：一条target-admitted projection取得一行和按冻结公式计算的contract-owned-byte permits；decoded queue附加上限不替代全局预算。
- 步骤 3.4.6：建立预算表，逐 owner记录对象、上限、reservation起点、跨queue/await存活期和释放点；move/borrow不重复计费，只有与原对象并存且跨await/queue的owned copy另取permit。
  -> 验证：复用现有Arrow batch/writer bound；若无可证明边界则停止报告，不创建通用instrumentation。exact-limit、oversize、nested book/price-change burst、channel closed、queue/writer阻塞、permit acquisition failure、admission gap、coordinated finalize和shutdown测试证明失败路径fail closed且permits最终回到基线，RSS只报告。
- 步骤 3.4.7：证明未激活v3路径不依赖专用raw-first handler、invalid/unmatched raw retention、Gamma-first raw-only或post-horizon raw-only，并把对应active v2生产边登记到任务3.5.6的待断开清单；不删除历史`polymarket_orderbook_v2` source reader或`polymarket_market_metadata_v3`自身的source traceability。
  -> 验证：v3调用图只存在candidate routing ->完整validation -> canonical domain -> typed row/terminal输入这一条路径；没有rejected-row schema、quarantine/sidecar、第二artifact或payload-digest recovery。静态schema/调用图和负例证明gap/error/coverage不能携带完整/截断frame、重序列化JSON、digest或等价可恢复字段集合。activation前active v2行为不变；历史v2 fixture仍可读，metadata WS/Gamma payload/digest仍只按其自身合同验证。

### 任务 3.5：未激活 typed-only resolution recovery、metadata v4 与 activation

- 步骤 3.5.1：在未激活v3 recovery路径中以typed event/market/slug/winner/outcome/assets/admission/schema取代raw projection，并实现已批准的metadata v4 overlay；active v2 raw recovery与metadata v3 current writer在任务3.5.6前保持不变。
  -> 验证：v3 recovery源码和测试不解析payload JSON。metadata v4复用同一artifact/writer/manifest mapping和五种row kind，以metadata v3完整schema为不可变前缀，只追加nullable claimant pair。仅WS-owned `resolution_terminal_ws`两列同时必填且epoch非空、sequence>0；Gamma-owned terminal、post-terminal observation、snapshot、transition、summary及non-owning late WS均同时为null。metadata v3保持不可变；不能以payload digest或模糊匹配替代，也不得提前断开active v2 recovery。
- 步骤 3.5.2：让未激活v3 recovery用validated typed contradiction、manifest finalization和metadata authority恢复。
  -> 验证：以下合法：claimant typed row + 唯一matching WS terminal；多条agreeing platform duplicates + 唯一claimant coordinate；无WS row + 唯一Gamma terminal；role-exit前accepted的agreeing typed corroboration + 唯一Gamma terminal；0条`market_resolved` source row + 唯一合法exhausted summary及与counters/status一致的finalized attempts。terminal态不重放或产生第二owner；exhausted态不恢复pending或未来slot。
- 步骤 3.5.3：写source-only、exhausted、role-exit和损坏artifact负例。
  -> 验证：validated `market_resolved` row但无terminal/summary、WS terminal无唯一claimant row、typed row + exhausted、row receive晚于role-exit、terminal+summary、重复terminal/summary、typed identity/winner/admission contradiction、running Parquet、unknown schema、缺列或损坏文件均fail closed；不恢复pending、不发Gamma、不写exhausted、不伪造transition或补metadata。
- 步骤 3.5.4：在validation前、validation后但queue admission前、typed row running、typed row finalized但terminal未finalized、terminal running/finalized、manifest finalize和UTC seal切点执行crash fixture。
  -> 验证：running artifact不作为durable fact；source-only finalized prefix按上一步fail closed；不存在post-horizon raw-only合法例外；只有finalized verified pending identity且仍有未来slot时best-effort恢复未来slot，过去slot只记missed且E+8后无第7次probe。
- 步骤 3.5.5：真实 v2/v3 mixed dataset compatibility test。
  -> 验证：activation前合法current pair只有source v2 + metadata v3，activation后合法current pair只有source v3 + metadata v4；source v3 + metadata v3、source v2 + metadata v4、dual-write、running/unknown/corrupt组合均fail closed。`polymarket_orderbook_v2`按独立legacy row view读取且raw-first只作历史audit兼容；metadata v2保持只读，metadata v3保持既有schema且只在activation后转为只读，它们都不能成为source v3 claimant authority或重建v3 canonical domain。
- 步骤 3.5.6：执行唯一current new-write activation barrier。
  -> 验证：只有metadata v4实现、WS claimant/recovery矩阵及任务3.1至3.5其余硬门通过后，同一个本地activation diff才同时把`polymarket_orderbook_v2 -> v3`与`polymarket_market_metadata_v3 -> v4`两条current new-write路径切换，并断开任务3.4登记的active v2 payload/fingerprint/fan-out/raw-recovery生产边；切换后source v2与metadata v3均只读。任一条件未满足时只保留未激活v3/v4实现和原source v2 + metadata v3路径，不关闭v3分支、不双写、不宣称cutover，也不改变production binary/dataset root。

### 任务 3.6：canonical round-trip 与 future handoff mapping

- 步骤 3.6.1：同一七类fixture分别保留内存canonical domain并执行v3 round-trip。
  -> 验证：全部当前消费retained值、ordering和timestamps一致；multi-change保持原始相对顺序；data-collect侧training/data-pipeline producer golden fixture不依赖被删字段。
- 步骤 3.6.2：登记任务3.2.5产生的`NONBLOCKING_FUTURE_SPEC`附件路径、内容hash和状态，不重复评审字段映射。
  -> 验证：附件已记录当前可知retained字段及后继metadata join的`price_to_beat/final_price`责任；不构造`HandoffFrame v1`、不宣称hot runtime parity。未知future字段不阻断v3，真实adapter、frame升级/legacy、模型接线、长跑和部署另立三件套。

### 阶段 3 审查与提交门

独立审查 source v3/metadata v4 schema、consumer、resolution、legacy compatibility和 storage measurement脚本。SDK字段能力矩阵存在blocking gap时不允许用JSON补洞；metadata v4 implementation/null matrix/claimant recovery未通过时，不得把current new-write activation标为通过。未激活小绿点可按DAG独立评审，但只有通过且另获授权后才可commit/push。

## 六、阶段 4：RTDS business-silence 完整换 runtime

本阶段只负责依赖、authority同步和进入/退出状态；详细行为、测试、长跑和性能合同不在此重复，唯一执行 authority为：

`plan/data-collect-rtds-business-silence-full-reconnect-plan.md`

参数未冻结时记录`RTDS_PARAMETERS_DEFERRED`，不阻断v3或其他拆分。阶段4必须保持子计划的两次独立授权和强制停止点：

### 4A：authority同步并停止

进入条件为三个参数、专项最终需求及精确authority delta已批准，专项阶段0.3 wrapper-first路线未被只读证据否定且用户已批准该SDK路线，并明确授权RTDS阶段0 authority同步。只同步相反authority，完成自审和独立审查后强制停止；一次4A授权不得覆盖Rust修改、构建或测试。

### 4B：另获授权后的capability与实现

进入条件为`SDK_070_RUNTIME_USABLE`、`2-RTDS_CLOSED`、4A已完成，以及用户另行明确授权RTDS阶段1 Rust修改和本地构建/测试。第一项工作才是按子计划动态证明官方SDK + 当前最小wrapper能完成真实teardown；该证明不是4A或4B入口的先验条件。证明失败则停止4B，不创建新runtime、不旁路raw WebSocket；证明通过后其余工作完全沿用子计划。

### 阶段 4 审查与提交门

严格按RTDS专项各阶段review/stop/commit/push门；4A与4B授权不得继承或合并。本总计划只核对依赖状态，且authority、Rust和`polymarket_orderbook_v3`/其他source schema不得混入同一diff或commit。

## 七、阶段 5：stale/redundant 代码与过度校验删除

### 任务 5.1：建立候选台账

- 步骤 5.1.1：从 symbol矩阵筛出只计算后丢弃、重复 owner、test-only public path、legacy adapter和旧 JSON/fingerprint路径。
  -> 验证：每项记录当前消费者和预计终态。
- 步骤 5.1.2：检查 binaries/public API/features/benches/scripts/docs/history。
  -> 验证：只有测试引用不能直接判删。
- 步骤 5.1.3：用户决定 `USER_DECISION` 项。
  -> 验证：不擅自删除 public module或 legacy兼容。

### 任务 5.2：逐候选或同构小簇处理

- 步骤 5.2.1：先分类为 `REPLACED_BEHAVIOR` 或 `UNREACHABLE_PRIVATE`。
  -> 验证：前者需要唯一替代合同和定向回归；后者只需跨features/targets/public API/scripts/legacy reader的无消费者证明，不为删除它新增test seam、抽象或虚构替代合同。
- 步骤 5.2.2：隔离删除或合并一个候选/小簇。
  -> 验证：diff每行可追溯到已批准候选。
- 步骤 5.2.3：运行受影响 targeted（仅替代行为）、package compile/check/clippy和diff gate；阶段收口运行一次deterministic suite。
  -> 验证：schema/manifest/row/gap/counter/shutdown无意外变化。
- 步骤 5.2.4：更新终态和 LOC。
  -> 验证：只有两类 `DELETE_PROVEN_*` 计入净删除。

### 阶段 5 审查与提交门

独立审查每项 consumer proof和删除范围；不得借 cleanup删除 paper/live、历史 reader或未获批产品功能。

## 八、阶段 6：v3集成、storage 与范围内 parity

### 任务 6.1：完整本地确定性门

```bash
cargo test -p data-collect --locked --tests
cargo check -p data-collect --locked --all-targets
cargo clippy -p data-collect --locked --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

- 步骤 6.1.1：只运行上述完整package deterministic/check命令一次，并把其中v3 schema/reader/writer/resolution/SDK/manifest/day rollover结果映射到v3 AC。
  -> 验证：完整suite可执行其他模块的现有测试，但只作为一次v3跨模块回归；不重复其专项AC/artifact/长跑/独立审查门，也不重复已被suite覆盖的定向测试。失败须相对pre-v3基线归因：v3 diff引入的失败阻断v3，既存或并发冲突则停止并报告。
- 步骤 6.1.2：只有某个v3 AC对应target未被上述命令覆盖时，才补跑一次该定向target并记录原因。
  -> 验证：不运行有真实网络副作用的bench，不为同一测试重复建门。

### 任务 6.2：bounded fixture A/B 与完整 UTC 日事实基线

- 步骤 6.2.1：用确定性bounded fixture从同一target-admitted logical message projection序列分别生成v2 baseline和v3 candidate；fixture另行覆盖两个相同platform deliveries与一次内部retry。
  -> 验证：输入顺序、compression、row-group和硬件环境记录；duplicate fixture证明两条平台delivery保留两行且内部retry不产生第三行。
- 步骤 6.2.2：比较 finalized bytes、column chunks、rows/messages、CPU、peak RSS、finalize latency、scan throughput。
  -> 验证：硬门为被删列/fan-out消失、v3 finalized bytes小于v2、资源/语义通过；实际减少比例、CPU、RSS、finalize和scan为报告项。
- 步骤 6.2.3：保存现有完整UTC日post-dedupe v2 artifact的schema/storage/resource/manifest事实基线。
  -> 验证：不生成v3 full-day candidate，不做canonical-domain/resolution语义对照；manifest中的`14,664`仅作为不可恢复aggregate披露，不声称pre-dedupe replay、精确row/event差异或首次真实v3前向长跑。不得为补齐输入新增录制器、sidecar、双写或shadow replay。

### 任务 6.3：canonical domain / future mapping 与产品边界

- 步骤 6.3.1：七类target-admitted fixture和代表性real artifact做canonical domain/v3 round-trip parity。
  -> 验证：当前消费字段field-for-field报告；mixed `PriceChange`只比较retained entries且保持SDK原始相对顺序；data-collect侧training/data-pipeline producer golden fixture通过，但不宣称真实data-pipeline adapter端到端通过。
- 步骤 6.3.2：消费阶段3登记的`NONBLOCKING_FUTURE_SPEC`附件路径/hash/status，并只检查现有binary边界没有变化。
  -> 验证：不重新评审同一字段映射；未知future字段记为后继附件缺口而不阻断v3。`HandoffFrame v1`和hot heartbeat行为无diff，不宣称runtime parity。
- 步骤 6.3.3：官方missing/null parser fixture、parser error传播、损坏Parquet、独立legacy v2 row view、v3及restart负例。
  -> 验证：覆盖任务3.1.6至3.1.8与3.5的qualified/unmatched/target-associated contradiction/Gamma-first/platform duplicate/restart/crash、metadata v4 prefix/null matrix及source/metadata cross-pair矩阵；全部fail closed或按冻结legacy语义读取，不把v2适配为v3 canonical domain，不为不可构造的unknown SDK enum创建shadow test path。

### 任务 6.4：v3最终报告

- 步骤 6.4.1：报告v3 schema/domain/writer/reader/recovery相关moved/deleted/rewritten/added/net LOC和artifact差异。
  -> 验证：只统计v3范围，不重复验收2-REST root结构。
- 步骤 6.4.2：报告payload/fingerprint/fan-out旧路径的替代与删除证据。
  -> 验证：同时逐项报告raw-first handler、invalid/unmatched persistence、Gamma-first raw-only、post-horizon raw-only和raw-to-metadata recovery矩阵的替代/删除证据；明确metadata v3 schema不可变且在activation后与历史orderbook v2一起只读保留，metadata v4继承v3 traceability并只追加claimant pair。只覆盖v3/v4必需删除，不重复汇总阶段5的全局cleanup台账。
- 步骤 6.4.3：报告v3与原计划偏差。
  -> 验证：事实、影响、选项和用户决定写入 changelog。

### 阶段 6 审查与提交门

独立终审需求AC-01至AC-09、AC-13至AC-15中与v3相关的diff、测试和artifact；AC-10/11由2-REST/cleanup分支收口，AC-12由RTDS子计划收口，本阶段不重复其专项验收门。通过且获授权后创建v3 closure commit/push；仍不授权deployment、旧数据删除或历史rewrite。

## 九、范围外后继事项

新数据cutover、历史迁移/删除、production release/deployment，以及真实paper/live外部adapter、模型接线和长跑，均须另建对应 demand/plan/changelog并获得授权；本计划不包含其实现任务。

## 十、计划完成定义

本总计划采用分支终态汇总，不设一个会让独立分支互相重跑或阻断的总回归门：

- SDK分支记录`RUNTIME_USABLE/NOT_USABLE`和独立typed-contract终态；
- 2-PM/v3分支只有在两列/fan-out删除、metadata v4、当前消费语义、resolution、canonical round-trip、storage和资源门通过并发布后才记完成；`NONBLOCKING_FUTURE_SPEC`只作为附件状态汇报，不成为完成硬门。typed-contract或metadata v4 implementation/validation阻断时，只记对应v3/v4子路径阻断；
- 2-RTDS与2-REST分别按自身review/publish状态结项；root模块化只在2-REST验收；
- cleanup分支独立维护两类删除台账和证据；
- RTDS行为按子计划记`COMPLETE`或`RTDS_PARAMETERS_DEFERRED/其他真实阻断`，deferred不反向阻断v3；
- umbrella只汇总各分支状态、authority和changelog一致性，不重复各分支专项验收/长跑/审查门。SDK/v3/三个机械分支及cleanup均完成时可记核心总事项完成；RTDS若deferred必须单独标明未完成；
- metadata v4实现与WS claimant/recovery验收完成后，本地代码/test中的source v3 + metadata v4原子current new-write替换才属于阶段3完成项；未满足时只可保留未激活实现。scope外production binary/dataset-root cutover、paper/live接线、deployment、migration、首次真实v3完整日前向长跑和历史删除不得冒充任何分支完成项。
