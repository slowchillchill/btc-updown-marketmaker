# data-collect SDK 0.7.0、typed schema、RTDS 恢复与 live runtime 模块化执行计划

> 事项编号：`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`
>
> 对应需求：`demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
>
> 对应 changelog：`changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md`
>
> 纳入的 RTDS 专项三件套：`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`、`plan/data-collect-rtds-business-silence-full-reconnect-plan.md`、`changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`
>
> 状态：`APPROVED_STANDING_EXECUTION_P_B1_CLOSED_2_PM_CLOSED_2_RTDS_CLOSED_STAGE3_CLOSED_STAGE5_CLOSED_STAGE6_CLOSED_2_REST_CLOSED_CORE_COMPLETE_RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_USER_TERMINATED_INCOMPLETE_ATTEMPT3_FAIL_INCOMPLETE_8H_GAMMA_CLOSED_TIME_A_IMPLEMENTATION_CLOSED_ATTEMPT4_FULL_COLLECTOR_PENDING`。RTDS的该状态仅在承载本记录的纯文档closure commit成功push且三方ref一致后生效；RTDS network-error方案A的`366fed7d`/`415470e`与Gamma `closedTime`方案A的`3a367386`/`e086017f`均已按authority/Rust分离发布并完成三方ref核对。RTDS阶段0和阶段1已闭合；Attempt1永久为`FAIL + INCOMPLETE_8H`，Attempt2永久为`USER_TERMINATED + INCOMPLETE_8H`，Attempt3永久为`FAIL + INCOMPLETE_8H`。Gamma方案A的目标测试、check、Clippy、fmt、diff-check与三路终审均已通过；唯一下一步为完整六源8小时Attempt4。总事项阶段0已以commit
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
> 一致，故`2-RTDS_CLOSED`成立。Stage3入口独立审计为`PASS`；任务3.0 authority
> sync commit `b569546ae1c9cfc24bcdfe4ba12ee4ce60ccc278`已push且三方ref
> 一致，故`STAGE3_AUTHORITY_SYNC_CLOSED`成立。用户明确批准持续推进本
> 事项直至完成；后继本地实现、测试、审查、commit和push不再逐次申请。只有上下文
> 冲突或实际情况与需求/计划不一致时才停止。RTDS完整参数、最终需求、wrapper-first
> SDK路线、authority矩阵及持续执行至阶段2本地香港VPN连续8小时验证均已获批；旧RTDS-only只读probe拓扑已由用户纠正为完整training collector全源正式落盘；Attempt3失败事实与Gamma方案A已经专项记录，修复后Attempt4沿用同一拓扑；
> production activation/cutover、release、部署、远端操作、migration和删除仍为
> 第九节范围外事项。
> Stage3任务3.1至3.7、S-B/M-B/W-B修复、本地门与重复独立审查均已通过；
> closure commit `7c6927f663d9c3b387874f688b10346956b9302d`已push且三方ref
> 一致，故`STAGE3_CLOSED`成立。随后执行游标进入独立cleanup阶段5；
> RTDS阶段4的4A authority同步、自审和独立终审已通过；承载本记录的文档commit成功push且三方ref一致后按顺序进入4B capability gate。
> 2026-07-26用户已批准Stage5四项authority修正并解除U01–U04b决策门：
> queue使用真实bounded channel/global row+owned-byte/bounded-wait owner，
> 文件采用hourly-only rollover，因果时间区分legacy recv/current admission与
> paper/live handoff admission + runtime receive，T-30只属于Polymarket lifecycle。
> 五项旧public/test-only模型统一按`DELETE_PROVEN_REPLACED_BEHAVIOR`原子处理；
> Stage5仍须完成文档同步、完整门、独立阶段审查、commit/push后才能闭合。
> 首次独立终审发现C01/C02/C03两项实施遗漏，并发现逐queue hard pause/resume与
> 有限health/reconnect circuit两套新shadow authority。用户随后批准
> `U05-A + U06-A`，允许按现有真实owner删除这两套public/test-only模型；修复后必须
> 重算LOC/测试证据、重跑完整门并重复独立审查，不能沿用首次FAIL前的closure结论。
> 当前修复后完整门与两路独立`gpt-5.6-sol`/Ultra终审均已通过，最终为
> Blocker 0 / Important 0 / Minor 0；closure commit
> `9e0d264058ed64cd63fbd05280f0826afaba0509`已push且本地HEAD、upstream与远端ref
> 一致，故`STAGE5_CLOSED`成立。Stage6本地证据、最终报告及两路独立
> `gpt-5.6-sol`/Ultra终审已经完成；终审均为
> `PASS / CLOSURE_UNBLOCKED`，Blocker 0 / Important 0 / Minor 0。2-REST仍是
> Stage6后必须单独执行的开放分支。Stage6 closure commit
> `c0aff3a3e61583138b33eec44d847acaa8ebd682`已push且三方ref一致，故
> `STAGE6_CLOSED`成立。2-REST owner迁移、root残留审计、测试库存对账与唯一完整
> 确定性门已经通过；代码、证据与文档三路`gpt-5.6-sol`/Ultra独立终审均为
> `PASS / CLOSURE_UNBLOCKED`，Critical 0 / Important 0 / Minor 0。按用户批准的
> 方案A，本文条件式记录`2-REST_CLOSED`与核心总事项`CORE_COMPLETE`：只有承载
> 本记录的closure commit成功push且本地HEAD、upstream与远端ref一致后才生效。
> RTDS行为专项当前为`RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_USER_TERMINATED_INCOMPLETE_ATTEMPT3_FAIL_INCOMPLETE_8H_GAMMA_CLOSED_TIME_A_IMPLEMENTATION_CLOSED_ATTEMPT4_FULL_COLLECTOR_PENDING`；该状态来自专项的分离发布及三次既有attempt证据，按第十节不另设
> umbrella独立执行游标或总回归门；专项下一步仅为从承载本closure的新clean/pushed HEAD、新artifact和空dataset root，以香港线路执行完整六源8小时Attempt4。

## 一、执行原则与成功标准

执行采用依赖 DAG，而不是把独立工作错误串成单一阻塞链：

```text
共同基线 / authority delta matrix
  ├─ SDK 0.7 runtime验证
  │    ├─ 2-PM分支收口 + P-B1闭合 -> v3 -> v3集成验证
  │    └─ 2-RTDS分支收口 + RTDS参数/authority批准 -> RTDS专项
  └─ 2-REST其余行为保持模块拆分与删除证据审计
```

typed字段缺口只阻断 v3；RTDS 4A已完成authority同步和终审，文档commit成功push且三方ref一致后进入4B，只有真实capability失败或新冲突才阻断后续实现。SDK runtime不可用才阻断依赖该 runtime的 v3/RTDS，但不阻断文档、静态证据审计或不依赖 SDK变更的机械拆分。

执行必须同时满足：

- **简单**：复用官方SDK和现有source/schema/writer/budget/manifest owner边界；Stage3只在唯一owner建立最小v3 canonical domain type，不创建generic framework、第二canonicalizer、shadow path、通用allocator tracker或无第二消费者的抽象。
- **准确**：保持唯一字段/owner映射、数组顺序、nullability、decimal文本、时间、admission和 handoff语义；冲突时停止对应工作。
- **可靠**：只验证已证实的不变量，并在唯一 trust boundary做一次；不以重复全量测试、固定 LOC、固定测试数量或无依据的性能比例代替正确性。

不能把 SDK 升级、文件移动、schema 变化、RTDS 行为和死代码删除混入同一个 diff。每个小型机械绿点只需：

1. 先满足对应需求和 AC；
2. 运行 owner定向测试、受影响跨模块测试及 compile/check；
3. 主代理检查 diff、测试证据、authority和范围；
4. 同步 plan/changelog的真实 cursor与偏差。

完整确定性回归和 `gpt-5.6-sol`、`ultra` 独立审查只在阶段收口执行；coordinator/day/manifest/shutdown等高风险迁移可额外执行一次完整门。修复 Critical/Important 后复审；审查通过后使用用户2026-07-25持续执行/发布授权直接commit/push并核对ref，不再重复询问。只有上下文冲突或实际情况与需求/计划不一致时停止请求决策。

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
  -> 验证：该窄决定在2026-07-24只约束未来v3 new-write，当时没有自动批准当前`polymarket_orderbook_v2` source代码、Rust/schema/tests或整个总需求；总需求已由用户于2026-07-25另行批准开始执行，但实现和activation仍走各自入口门。
- 步骤 0.1.5：登记`METADATA_V4_CLAIMANT_SCHEMA_APPROVED_2026-07-25`，不再次请求metadata版本选择。
  -> 验证：该窄决定本身只冻结metadata v4 schema合同与未来activation pairing；尚未发生的Rust/schema/test实施与current调用图切换不得写成已完成，production cutover仍未获授权；阶段3本地Rust/schema/test实施已由2026-07-25持续执行授权覆盖。

### 任务 0.2：建立精确 authority delta matrix

- 步骤 0.2.1：逐条列出 `source_native_payload_json`、fingerprint recovery、venue `hash`、raw JSON/equivalent evidence和 resolution raw projection的旧条款。
  -> 验证：每项包含旧路径/锚点、新语义、保留合同、被取代范围和测试；至少覆盖reliability/data-collect demand、`plan/data-collect-plan.md`、retention plan及resolution follow-up三件套，并把当前`HandoffFrame v1.raw_json`明确标为`KEEP_OUT_OF_SCOPE`。`market_resolved` raw-first、Gamma-first raw-only corroboration和post-horizon raw-only restart矩阵已由用户批准改为v3 validate-first，矩阵必须标记`USER_DECIDED_V3_VALIDATE_FIRST`而不是待决。
- 步骤 0.2.2：只引用并消费RTDS专项三件套已经记录的business-silence authority delta、参数状态和测试入口/出口。
  -> 验证：umbrella不重扫数据、不重算参数候选、不重建delta/test矩阵，也不维护第二执行游标；专项路径、状态和阻断原因可精确解析。
- 步骤 0.2.3：除已经单独批准并完成文档同步的`market_resolved` v3 validate-first与metadata v4 claimant两个窄决定，以及本轮用户批准的reliability current四路径清单修正外，阶段0只建立并评审矩阵，不同步其他相反authority；旧changelog不回写。
  -> 验证：四路径修正只恢复source-availability第4.7节已经批准且当前代码实际使用的allowlist，不改变typed或RTDS行为authority。上述两个carve-out不得重新置为待决，也不得冒充Rust/schema/test实施；其余typed authority只允许在SDK字段门通过、用户批准v3字段集合且2026-07-25持续执行授权有效后同步。RTDS完整入口已获裁决，按专项单一游标同步；4A闭合后进入4B，capability失败或新冲突始终停止。

### 任务 0.3：消费 RTDS 专项状态，但不重复执行

- 步骤 0.3.1：读取RTDS专项计划/变更记录中的`steady_silence_threshold`、`first_fresh_observation_window`及anchor、cold-start watchdog、outer backoff状态。
  -> 验证：只消费专项已冻结的`300s/60s/后到monotonic/cold-start disabled/outer 300s cap`，不在umbrella计算或提出第二组参数。
- 步骤 0.3.2：把专项状态映射为本计划的单一入口结果。
  -> 验证：全部入口已满足；承载阶段0记录的文档commit成功push且三方ref一致后，记录`RTDS_SHARED_CONTRACT_STAGE0_CLOSED_STAGE1_CAPABILITY_PENDING`并沿用专项唯一执行游标；不复制任务、测试或长跑。

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
- authority冲突没有被顶部状态掩盖；
- 除用户批准的四项文档对齐外，没有未授权authority diff，且没有Rust/Cargo/vendor/schema/data diff；
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
- 步骤 1.3.3：只有新vendor provenance、四路径patch和workspace依赖切换均已建立后，删除tracked旧`third_party/polymarket_client_sdk_v2-0.6.0-p1`源码树。
  -> 验证：`third_party`中tracked Polymarket SDK vendor目录恰好只有`0.7.0-p1`；旧目录不存在，active Cargo/code/probe中的Polymarket SDK旧路径和其0.6精确版本引用为0，历史文档记录保留。删除与新增属于同一阶段1 diff，可由pre-execution checkpoint回滚，不保留第二vendor或shadow fallback。
- 步骤 1.3.4：确认本阶段不修改 `data-collect/src`、schema或 RTDS业务逻辑。
  -> 验证：diff path gate。

### 任务 1.4：SDK 与项目可用性测试

按从窄到宽顺序运行：

```bash
cargo +1.88 test --manifest-path \
  third_party/polymarket_client_sdk_v2-0.7.0-p1/Cargo.toml \
  --locked --lib --features clob,gamma,rtds,ws

cargo +1.88 test --manifest-path \
  third_party/polymarket_client_sdk_v2-0.7.0-p1/Cargo.toml \
  --locked --all-features

cargo +1.88 clippy --manifest-path \
  third_party/polymarket_client_sdk_v2-0.7.0-p1/Cargo.toml \
  --locked --all-targets -- -D warnings

cargo +1.88 clippy --manifest-path \
  third_party/polymarket_client_sdk_v2-0.7.0-p1/Cargo.toml \
  --locked --all-features --all-targets -- -D warnings

cargo +nightly-2025-11-24 fmt --manifest-path \
  third_party/polymarket_client_sdk_v2-0.7.0-p1/Cargo.toml \
  --all -- --check

cargo test -p data-collect --locked -- --test-threads=1
cargo check -p data-collect --locked --all-targets
cargo clippy -p data-collect --locked --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

vendor身份由0.6.0变为0.7.0，因此上述vendor tests、default/all-features
all-targets Clippy和固定nightly fmt各只在阶段1收口运行一次；root workspace排除
vendor，workspace Clippy/fmt不能替代该矩阵。不得在每个小步骤重复该矩阵。不使用
`cargo test --all-targets`运行可能访问真实网络、写artifact或`harness=false`的
bench；bench只做compile check，真实smoke另受任务1.5约束。

- 步骤 1.4.1：证明 upstream async execution相关 mock tests未被 vendor破坏。
  -> 验证：不调用真实订单或钱包。
- 步骤 1.4.2：从单次`cargo test -p data-collect --locked -- --test-threads=1`结果映射阶段开始时实际发现的named patch/runtime contract tests；`17/19`只作为2026-07-24快照。只有该suite未覆盖明确target，或失败诊断需要时，才补跑对应定向命令。
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

### 阶段 1 当前执行记录（2026-07-25）

- 任务1.1--1.4已在真实Stage1 worktree完成；官方crate provenance、132文件清单、
  MIT、MSRV、VCS、唯一vendor、四路径相对官方diff、精确0.7.0依赖与feature tree
  均已核验。
- 阶段1规定的vendor core/all-features tests、两组all-targets Clippy、固定nightly
  fmt及data-collect单次串行完整suite、check/Clippy/fmt/diff均离线通过；
  `SDK_070_RUNTIME_USABLE`。
- 任务1.5是单独授权的公开只读smoke；当前为`NOT_AUTHORIZED / NOT_RUN`，不把它
  伪写成runtime离线门的失败或完成。
- 任务1.6已用官方文档missing/null object/array fixture分别核对官方0.7与当前patch，
  并完成当前consumer/既有批准合同矩阵。七类variant均存在admission前整帧parser
  gap，typed终态为`SDK_070_TYPED_CONTRACT_BLOCKED`；只停止v3，不用raw/shadow
  路线补洞。
- `data-collect/src`、schema、RTDS业务逻辑和Stage2改动为零。完整证据见
  `data-collect/docs/sdk070-stage1-sdk-runtime-and-typed-contract.md`。
- 独立`gpt-5.6-sol`/`ultra`阶段审查已以
  `Critical 0 / Important 0 / Minor 0 / PASS`通过；当前游标为
  `STAGE_1_CLOSED`，以承载闭合记录的commit成功推送且远端分支SHA与本地
  HEAD/upstream一致为生效条件；完成该验证后进入下一阶段。

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

### 阶段 2 共同基线当前执行记录（2026-07-25）

- 任务 2.1/2.2 的 current observable、owner、测试、symbol、跨 owner
  message-flow 与 `#[cfg(test)]` seam 已按 Stage1 closure commit
  `b818e436a115eb61859fa281773342ee58f54f6a` 落盘到
  `data-collect/docs/sdk070-stage2-observable-and-owner-baseline.md`。
- 文档路径、artifact事实、测试/symbol数量与hash、测试清单harness及
  `git diff --check`均已复核；当前游标为
  `STAGE_2_COMMON_BASELINE_COMPLETE`，任务2.1/2.2共同基线记为完成。
- 本步没有修改 Rust、schema、payload、fingerprint、admission、RTDS、
  paper/live、部署路径或 `run_id` 校验；三份既存未跟踪 RTDS 草案保持未修改、
  未暂存、未删除。
- `2-PM` step 1已完成：新增私有`live_runtime/polymarket_source.rs`与私有
  `live_runtime/tests/polymarket_source.rs`，分别承载source owner及73个直接测试；
  root只增加私有module/use与测试子模块声明，`lib.rs`未修改。主代理归一化对账
  production、测试函数与root remainder均与Stage1 HEAD一致；owner测试73/73、
  跨owner测试8/8、五组integration 52/52、fmt、locked all-targets check与
  `git diff --check`全部通过。
- `2-PM` step 2已完成：新增私有
  `live_runtime/polymarket_event_adapter.rs`与同名私有测试子模块，机械迁入38个
  纯adapter/row conversion items与12个直接测试。RawDay/admission、dedupe、
  writer dispatch、restart recovery及cross-owner测试保持root原位；`lib.rs`
  未修改。
- 主代理逐item/逐测试归一化对账均与Stage1 HEAD一致；全局测试仍为593，
  owner/跨owner/integration矩阵、fmt、locked all-targets check与diff check全部
  通过。当前`2-PM`游标为`STEP_2_SELF_REVIEW_PASSED`；下一小步只有step 3 writer
  integration seam及其直接测试。restart recovery和RTDS保持原位。
- Step 3只读审计确认没有可仅靠path/visibility适配完整迁移的PM dispatcher
  symbol：`LiveSegmentWriter`、schema mapping与typed write dispatch均同时服务
  六类source。用户批准`Writer-A`作为本小步唯一例外：root保留唯一source-kind
  match与统一logical-byte accounting；既有PM adapter只增加两个leaf写入函数、
  当前orderbook schema version value及随leaf移动的one-row guard，不新增
  handled协议、重复路由、第二writer abstraction或无关测试。
- `Writer-A`已按批准范围实现并自审：root只把两个PM write arm替换为直接leaf
  委派，adapter取得一个current orderbook schema value、两个leaf及原guard；
  metadata v3常量仍留原layout owner。新增一个guard直接测试后全局测试为594，
  PM unit 101/101、六组integration 53/53、fmt、locked all-targets check、
  test-list与diff check均通过；独立只读审查的唯一Minor复数措辞已修正。当前
  `2-PM`游标为`STEP_3_SELF_REVIEW_PASSED`，下一步只审计step 4既有recovery owner。
- Step 4只读审计结论为`ALREADY_OWNED_NO_MOVE`：
  `polymarket_resolution_recovery.rs`已唯一持有restore入口、持久化事实scan/
  validation、恢复结果类型与32个直接测试；root stable startup只调用入口，PM
  source只消费结果。lifecycle fingerprint restore是不同owner，不是shadow
  resolution recovery。recovery与`lib.rs`均不改；直接测试32/32、startup边界
  2/2及既有PM cross-owner门通过。当前游标进入`2-PM_BRANCH_CLOSE_GATE`。
- 分支收口只运行一次完整串行suite，348 unit、246 integration与0 doctest通过；
  final behavior code的check、Clippy、fmt与diff gate通过。Clippy首轮发现并删除
  Stage1 HEAD既有lint attribute与其函数之间因迁移产生的空行；attribute保持不变，
  函数体与调用图未变，不重复suite。独立全面审查首轮只发现attribute被误删及
  文档叙述错误这一项Minor；恢复既有attribute、修正文档并复跑最终静态门后，最小
  复审以Critical/Important/Minor均为0通过。closure commit
  `582eb154687c4c0522615753b3fc1903f2e833f3`已推送且三方ref一致，故
  `2-PM_CLOSED`成立。
- Stage1 typed blocker继续只阻断阶段3 v3；不把它扩大为阶段2机械拆分 blocker。

### 分支 2-PM：v3所需最小 Polymarket owner

按依赖逐个移动且每步只移动一个owner：

1. Polymarket source owner、该source直接持有的Gamma discovery/identity/resolution transport与helpers及其直接测试；
2. Polymarket adapter/row conversion owner及其直接测试；
3. PM current orderbook schema version value与两个rows-to-writer leaf实现；六源共享
   source-kind/schema/write dispatcher留在root，只补现有one-row guard缺失的
   最小直接回归测试；
4. Polymarket resolution recovery owner及其直接测试。

除用户已明确批准的step 3 `Writer-A` extract-method外，内部小步只做编译所需path/visibility适配。`Writer-A`也只允许两个PM leaf函数、一个current schema value与既有one-row guard迁移；root保留唯一共享dispatch和统一logical-byte accounting。运行owner定向、受影响跨模块测试及compile/check并由主代理自审；不得改变schema、payload/hash/fingerprint或admission。分支收口运行一次完整确定性门和`gpt-5.6-sol`/`ultra`独立审查，先记`2-PM_REVIEW_PASSED_AWAITING_PUBLISH`；只有获授权并完成对应commit/push后才记`2-PM_CLOSED`。它是阶段3机械前置，不依赖2-RTDS或2-REST。

### 分支 2-RTDS：专项所需最小 RTDS owner

该已闭合分支只移动RTDS source owner及其直接测试，并在当时保持“业务静默不重连”authority和行为；内部按小步门验证，分支收口运行一次完整回归和独立审查后，使用持续发布授权创建closure commit/push并核对ref，随后记`2-RTDS_CLOSED`。它是阶段4机械前置，不依赖2-PM或2-REST；后续行为变更只以阶段4纳入的RTDS专项为准。

当前执行事实：

- production owner已机械迁入私有`live_runtime/rtds_source.rs`：RTDS endpoint与
  bounded config、source spec、classify、subscription evidence、source-native
  canonical、dedupe restore、SDK runtime loop、generation accounting与emit；
- 九个直接测试已机械迁入`live_runtime/tests/rtds_source.rs`；root继续保留
  composition、共享sink/day/coordinator、writer/schema/row conversion、跨channel
  config、跨四源静默和四源load测试；
- 迁移前root为33,965行；最终root 32,589行、source 359行、直接测试1,019行，
  合计33,967行，净增2行module wiring，不作为固定LOC门；
- production代码块规范化后与`d6a87fd`基线逐项hash一致；九个测试在移动时函数体
  逐字一致，最终文本差异仅为标准rustfmt布局。owner定向`9/9`、三个root affected
  test `3/3`与locked all-targets check通过；
- 分支收口完整suite为`351 unit / 246 integration / 0 doctest`通过；all-targets
  Clippy `-D warnings`通过。首次fmt check只发现移动后缩进变化与三处孤立空行，
  运行标准formatter后fmt与diff门通过；不因纯格式修正重复完整suite；
- `gpt-5.6-sol`/`ultra`独立审查以
  `PASS (Critical 0 / Important 0 / Minor 0)`通过；用户随后授予持续执行/发布
  授权。当前游标记录为`2-RTDS_CLOSED`，以承载本记录的closure commit成功push且
  本地HEAD、upstream与远端ref一致为生效条件；不运行vendor suite、public smoke
  或未批准RTDS草案的专项验证。

### 分支 2-REST：其余行为保持模块化

其余Binance aggTrade/depth、shared writer supervisor、manifest、raw day/epoch/rollover、source supervision、runtime coordinator和root composition按收益/风险逐owner迁移。每个小步只移动一个owner及其直接测试，运行定向/受影响测试、compile/check和自审；coordinator/day/manifest/shutdown可额外运行一次完整门。

分支最终验收：

- root只保留facade/config/composition，owner唯一且依赖方向清楚；
- tests随owner分布，不形成横跨多个无关owner的单体测试模块；
- 报告各模块及root的moved/deleted/rewritten/added/net LOC，但不设固定LOC门；
- 分支收口运行一次完整确定性门和独立审查。

当前执行事实（2026-07-26）：

- 入口 commit为`c0aff3a3e61583138b33eec44d847acaa8ebd682`；入口
  `live_runtime.rs`为31,904行，574个named tests、37个package test/doc
  targets（36个可执行targets + 1个doctest target）；
- 其余有效行为已迁入28个私有owner；最终root为4,966行，其中测试前root
  为1,815行，只含facade/config/composition及最小test-only glue。独立残留审计为
  `PASS / CLOSURE_UNBLOCKED`，剩余必做迁移0；
- 终态仍为574个named tests、37个package test/doc targets；basename排序清单SHA-256与入口
  同为`76c2711cb908bde6cbfa2a5b9b9cb54b3416f1135f29cec5e7143fdef2d9184a`；
- Rust file-level churn为`+30,966/-30,895`，净`+71`行；semantic delete、
  rewrite和behavioral add均为0，精确owner/LOC/KEEP/MOVE及偏差记录见
  `data-collect/docs/sdk070-stage2-rest-modularization.md`；
- 主执行方唯一完整分支门已通过：lib 348项、integration 226项、0 bin/doctest，以及
  locked all-target check、Clippy `-D warnings`、fmt和diff gate；
- 当前只待`gpt-5.6-sol`/Ultra独立阶段审查；审查通过前不创建closure
  commit/push，也不记录`2-REST_CLOSED`。

审查通过后使用持续发布授权完成commit/push与ref核对并记`2-REST_CLOSED`。`2-REST`不阻断阶段3或阶段4。三个分支均禁止test consolidation、死代码清理、generic source/actor/plugin framework和直接套用历史remote branch patch；发现语义漂移时回退对应小步，不用新测试接受漂移。

## 四-A、独立前置 P-B1：SDK typed/nullability compatibility

本前置在Stage1闭合后独立执行，不回写Stage1历史结论，不属于Stage2 owner迁移，也
不等于Stage3授权。生产vendor差异路径仍只能是既有四个，其中本前置的typed修改只
落在既有`src/clob/ws/types/response.rs`；必要的vendor tests/example适配单列为
non-production conformance路径。

### 任务 PB.1：修复typed decode并冻结字段裁决

- 步骤 PB.1.1：把七类官方允许missing/null但SDK 0.7要求非空的标量/文本改为
  `Option`；三个批准数组保持`DefaultOnNull -> empty Vec`，并让直接复制
  `BookUpdate.timestamp`的`MidpointUpdate.timestamp`保持同型。
  -> 验证：missing object、null object、`[missing object]`和`[null object]`
  全部通过真实`parse_if_interested(..., MessageInterest::ALL)`返回恰好一条正确
  variant；不得增加第五个vendor生产源码路径、第二parser或伪默认值。
- 步骤 PB.1.2：按最终字段矩阵实现F04
  `BookUpdate.last_trade_price: Option<Decimal>`；F01–F03与F05–F18保持
  `EXCLUDED`。
  -> 验证：F04 missing/null为`None`，present decimal文本无损解码；它只表示book
  快照状态，不转换为独立`last_trade_price`成交事件。其余排除字段在SDK struct和
  未来v3字段矩阵中均不存在。

### 任务 PB.2：保持current-v2行为边界

- 步骤 PB.2.1：在canonical payload、fingerprint、row、terminal claim及业务状态
  变化前，对P-B1前由SDK parser强制完整、current-v2仍要求完整的shape fail closed。
  -> 验证：七类nullable负例在任何写入/claim/state变化前失败；不得把P-B1前已经
  nullable的`MarketResolved.question/slug/description/outcomes`误加为必填。
- 步骤 PB.2.2：F04有值时只把其decimal文本加入book canonical与raw-equivalent
  JSON；missing/null时省略该新key。
  -> 验证：F04 present可从持久化evidence恢复；missing/null的旧完整fixture保持
  原canonical字节、fingerprint、target admission、row与持久化语义，不合成成交行。

### 任务 PB.3：验证、独立审查与发布门

- 步骤 PB.3.1：运行vendor lib/tests/examples与doc测试、all-targets check、
  all-targets Clippy `-D warnings`、固定nightly fmt；运行data-collect完整串行suite、
  all-targets check/Clippy、workspace fmt和diff check。
  -> 验证：只记录真实命令和结果；不得用会执行联网criterion benchmark的
  `cargo test --all-targets`替代tests/examples与all-targets check。公开wire smoke
  未授权则保持`NOT_AUTHORIZED / NOT_RUN`，不得把optional typed支持写成实际填充率。
- 步骤 PB.3.2：主代理自审完整diff后，派生独立
  `gpt-5.6-sol` / `ultra`审查代码、测试、四路径边界、F矩阵、current-v2 gate及
  demand/plan/changelog/阶段证据一致性。
  -> 验证：Critical/Important必须修复并复审；无待处理问题后只记
  `P_B1_REVIEW_PASSED_AWAITING_PUBLISH`。
- 步骤 PB.3.3：本步已按当时取得的单独授权创建P-B1 closure commit并push。
  -> 验证：commit
  `d6a87fdaf0a751d6b7c4196aeb936b4c5de4c094`推送后本地HEAD、upstream与
  `origin/fix-rtds-silents`一致，故`P_B1_CLOSED`及
  `SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED`成立；它解除Stage3 typed
  前置，Stage3本地执行/发布现由2026-07-25持续执行授权覆盖。

## 五、阶段 3：`polymarket_orderbook_v3` typed message

### 任务 3.0：同步 typed authority

- 步骤 3.0.1：确认阶段 1 runtime门通过、`2-PM_CLOSED`、`P_B1_CLOSED`、typed-contract终态允许继续、用户已批准最终 v3字段矩阵，且2026-07-25持续执行授权有效。
  -> 验证：任何已知gap未获逐项决定或任一必需字段缺失时停止，不修改authority或代码；metadata v4版本合同已经批准，不再作为待决项，其Rust/schema/test实施由持续执行授权覆盖。
- 步骤 3.0.2：复核已经单独同步的`market_resolved` v3 validate-first与metadata v4 claimant/原子activation文档条款，再只同步需求3.2所列其余payload/fingerprint/v2 raw projection现行authority，保留旧changelog。
  -> 验证：不得把已批准的validate-first或metadata v4版本合同重新置为待决，也不得把其文档同步冒充Rust/schema/test实施；其余authority diff逐条对应届时已批准delta matrix，不修改RTDS相反authority或`HandoffFrame v1.raw_json`合同，不混入Rust/Cargo/schema。
- 步骤 3.0.3：主代理审查 authority diff后按持续发布授权提交；阶段收口再做独立审查。
  -> 验证：代码修改开始前，当前 authority、最终字段矩阵和本阶段 AC一致。

当前执行事实：

- 步骤3.0.1独立只读入口审计为`PASS`：`SDK_070_RUNTIME_USABLE`、
  `2-PM_CLOSED`、`P_B1_CLOSED`、
  `SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED`、最终F矩阵、
  validate-first、metadata v4/原子activation及持续执行授权全部满足；
- 步骤3.0.2只同步reliability/data-collect demand、data-collect/retention plan及
  resolution-followup三件套中的future source-v3 payload/fingerprint/v2 raw
  projection现行条款，并在对应历史changelog追加supersede记录；rebuild-core草案
  保持`KEEP_OUT_OF_SCOPE`；
- 其他source raw JSON、metadata payload/digest、`HandoffFrame v1.raw_json`、
  source v2历史、RTDS草案及production边界均未改变；
- 步骤3.0.3按持续发布授权闭合；`STAGE3_AUTHORITY_SYNC_CLOSED`以承载本记录的
  commit成功push且本地HEAD、upstream、远端ref一致为生效条件。

### 任务 3.1：测试先行冻结 v3 schema

- 步骤 3.1.1：复用P-B1长期保留的missing/null parser fixtures，并为七类可被SDK typed API接纳的public market variant建立target-admitted golden fixtures。
  -> 验证：parser gap不会被v3测试伪装成nullable row；可接纳fixture覆盖空/多元素数组、decimal文本、F04 nullable book快照状态、event message、terminal fields，以及含目标/非目标entries的mixed `PriceChange`。schema golden只包含项目canonical `clob_market_id`，不含同值顶层`id`；fixture证明SDK `NewMarket.id`/`MarketResolved.id`只在唯一入口映射一次，`market`仍为condition ID，nested `event_message.id`保持不变。
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
  -> 验证：v3 source schema包含非空epoch、正数sequence和正数UTC毫秒admission time，唯一source key只由epoch+sequence组成；metadata v4以metadata v3全部字段、顺序、类型、nullability和语义为不可变前缀，只在末尾追加nullable `ws_claimant_source_sequence_epoch_id: Utf8`与`ws_claimant_collector_admission_sequence: UInt64`。metadata v3 schema/version立即冻结，activation前仍是current metadata writer，activation后退出current runtime new-write；公开offline legacy writer只允许历史fixture、兼容测试和任务3.7 bounded A/B，不得复用digest、时间或物理行位置建立current authority。

### 任务 3.2：建立单一 domain typed message

- 步骤 3.2.1：用最小 enum + variant structs表达七类消息和共同envelope；envelope增加`source_sequence_epoch_id`、`collector_admission_sequence`和`collector_admission_time_utc_ms`。
  -> 验证：epoch在同一producer ownership内跨reconnect/UTC rollover稳定、restart更换；sequence在epoch内严格递增但允许空洞且不得回绕；admission time为正数UTC毫秒并验证`timestamp_received <= collector_admission_time_utc_ms`，不属于唯一key。不新增connection-generation、generic validation framework或JSON blob。按用户批准的方案B，第三方SDK/wire `id`只在typed入口映射为项目canonical `clob_market_id`；v3 domain、Parquet、metadata、recovery和项目consumer不再建立同值`id` alias或双路径，active-v2外部/legacy JSON保持不变。
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
  -> 验证：v3 writer拒绝unknown/legacy写入，reader兼容真实v2 fixture；未过activation barrier时生产调用图仍只激活v2 writer。W-B保留production-compiled public offline v2/metadata-v3 writer API，不要求硬拒绝legacy schema；它不能接入current runtime。
- 步骤 3.3.3：保持 ZSTD、hour/day partition、sort order、atomic rename和 manifest file facts。
  -> 验证：finalized row/bytes/SHA/schema逐项一致。
- 步骤 3.3.4：实现但暂不激活v3 writer；按collector admission order写入并验证同epoch内sequence严格递增，active v2 writer继续current new-write，v2 reader保持legacy read-only/receive-order。
  -> 验证：重复或倒退sequence、0 sequence、空epoch、非正admission time或`timestamp_received > admission time`均fail closed；不要求gapless，不增加文件级reference或sidecar。metadata v4实现与任务3.5验收完成前，current new-write调用图不得切到v3。

### 任务 3.4：准备替代 payload fingerprint 和 fan-out

- 步骤 3.4.1：让未激活v3路径不构造或持久化Polymarket canonical payload，并按 `size_of::<DomainMessage>() + Σ String::capacity() + Σ (Vec<T>::capacity() * size_of::<T>())`计算permit合同值，递归计入Vec已初始化元素拥有的动态缓冲区且每个allocation只计一次；active v2路径在任务3.5.6前保持不变。
  -> 验证：不得把Vec元素数当字节；未激活v3路径无`source_native_payload_json`，不把合同值声称为allocator实际bytes或覆盖共享buffer/Arrow内部开销，不创建通用追踪框架，v2 current writer与legacy reader均未被误删。active/legacy v2的wire-compatible canonical/raw-equivalent JSON继续使用顶层`id`；本命名决定不修改v2 bytes、fingerprint、reader或历史文件，也不授权migration/backfill。
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

  当前逐owner预算事实：

  | owner / 对象 | 上限 | reservation起点 | 跨queue/await存活期 | 释放点 |
  |---|---|---|---|---|
  | fast-drain SDK `WsMessage` / `BudgetedPolymarketMessage` | global `100,000 rows / 134,217,728 bytes`；decoded `25,000 messages / 67,108,864 bytes`；单对象`1,048,576 bytes`；共同`2s` deadline | SDK decode、market variant和owned-bytes检查后，raw-day ticket与decoded queue admission前 | 同时持有global+decoded reservation，跨decoded mpsc、fence/drain、target validation及v3 enqueue await | processor return、ignore/drop、queue closed/timeout或abort；decoded acquire失败先释放global |
  | v3 canonical source message | global `1 row + exact contract-owned bytes`；单行`1 MiB`；共同`2s` deadline | accepted ticket gate后先预留`1 MiB` byte，projection完成后shrink到exact，再用剩余deadline取得`1 row`；queue ownership成立后分配source key | 与SDK message并存时独立计费；跨Defer FIFO/source queue、`5,000` row pending batch、`8` command writer queue和writer write | writer消费完成，或任一defer/queue/writer/error/drop/shutdown析构 |
  | metadata v4 `RawEvent` bundle | 每行global `1 row + exact bytes`；bundle按总rows/bytes取得后split；单行`1 MiB`、`2s` | metadata row验证并构造`RawEvent`后、source queue前 | 可与SDK/v3 source/pending evidence并存，故独立计费；跨Defer FIFO/source queue/pending batch/writer queue/write | writer完成或失败/drop/shutdown析构 |
  | resolution pending state | pending markets `2`；source authorities `4`；总owned bytes严格小于`1,048,576` | early claim、promotion、recovery或authority mutation后由`ensure_pending_bounds`检查；不是semaphore permit | 跨actor与Gamma await | terminal/exhausted成功入队后remove、无future slot恢复删除、promotion消费early claim，或actor退出drop |
  | queue/batch/writer/Arrow结构边界 | source queue `25,000`；flush batch `5,000`；writer channel `8 commands`；row group `50,000 rows / 16,777,216 logical bytes` | 上述对象move进入各owner，不取得第二份permit | 原reservation跨各层；构造Arrow batch时输入reservation仍存活 | writer处理或command drop后释放 |

  SDK message与v3 projection在canonical构造期真实并存，因此两份global reservation不是重复计费。`16 MiB`只是row-group logical limit，不是Arrow allocator精确内存上限；RSS只报告。无active consumer的`POLYMARKET_PROCESSOR_STAGING_MAX_BYTES`不列为预算owner，留待阶段5按不可达证明处理。
- 步骤 3.4.7：证明未激活v3路径不依赖专用raw-first handler、invalid/unmatched raw retention、Gamma-first raw-only或post-horizon raw-only，并把对应active v2生产边登记到任务3.5.6的待断开清单；不删除历史`polymarket_orderbook_v2` source reader或`polymarket_market_metadata_v3`自身的source traceability。
  -> 验证：v3调用图只存在candidate routing ->完整validation -> canonical domain -> typed row/terminal输入这一条路径；没有rejected-row schema、quarantine/sidecar、第二artifact或payload-digest recovery。静态schema/调用图和负例证明gap/error/coverage不能携带完整/截断frame、重序列化JSON、digest或等价可恢复字段集合。activation前active v2行为不变；历史v2 fixture仍可读，metadata WS/Gamma payload/digest仍只按其自身合同验证。

### 任务 3.5：未激活 typed-only resolution recovery、metadata v4 与 activation

- 步骤 3.5.1：在未激活v3 recovery路径中以typed event/market/slug/winner/outcome/`asset_ids`/admission/schema取代raw projection，并实现已批准的metadata v4 overlay；active v2 raw recovery与metadata v3 current writer在任务3.5.6前保持不变。
  -> 验证：v3 recovery源码和测试不解析payload JSON；`market_resolved`与`new_market`统一使用v3 canonical `asset_ids`，不增加`resolved_event_asset_ids` alias或双列，历史v2字段只读。metadata v4复用同一artifact/writer/manifest mapping和五种row kind，以metadata v3完整schema为不可变前缀，只追加nullable claimant pair。仅WS-owned `resolution_terminal_ws`两列同时必填且epoch非空、sequence>0；Gamma-owned terminal、post-terminal observation、snapshot、transition、summary及non-owning late WS均同时为null。metadata v3保持不可变；不能以payload digest或模糊匹配替代，也不得提前断开active v2 recovery。
- 步骤 3.5.2：让未激活v3 recovery用validated typed contradiction、metadata authority，以及production startup在dataset-root lease内完成的manifest preflight/reconcile所建立的finalized facts恢复。
  -> 验证：startup/manifest owner是唯一manifest trust boundary；公开recovery API继续接收`dataset_root + time`并扫描同一锁定root的final-named Parquet，不接收snapshot、不重复manifest校验。直接模块调用只证明恢复算法，不证明production manifest authority。以下合法：claimant typed row + 唯一matching WS terminal；多条agreeing platform duplicates + 唯一claimant coordinate；无WS row + 唯一Gamma terminal；role-exit前accepted的agreeing typed corroboration + 唯一Gamma terminal；0条`market_resolved` source row + 唯一合法exhausted summary及与counters/status一致的finalized attempts。terminal态不重放或产生第二owner；exhausted态不恢复pending或未来slot。
- 步骤 3.5.3：写source-only、exhausted、role-exit和损坏artifact负例。
  -> 验证：validated `market_resolved` row但无terminal/summary、WS terminal无唯一claimant row、typed row + exhausted、row receive晚于role-exit、terminal+summary、重复terminal/summary、typed identity/winner/admission contradiction、running Parquet、unknown schema、缺列或损坏文件均fail closed；不恢复pending、不发Gamma、不写exhausted、不伪造transition或补metadata。
- 步骤 3.5.4：在validation前、validation后但queue admission前、typed row running、typed row finalized但terminal未finalized、terminal running/finalized、manifest finalize和UTC seal切点执行crash fixture。
  -> 验证：startup preflight/reconcile拒绝不一致manifest/file facts；running artifact不作为durable fact；source-only finalized prefix按上一步fail closed；recovery不另建snapshot validator。不存在post-horizon raw-only合法例外；只有finalized verified pending identity且仍有未来slot时best-effort恢复未来slot，过去slot只记missed且E+8后无第7次probe。
- 步骤 3.5.5：真实 v2/v3 mixed dataset compatibility test。
  -> 验证：activation前合法current runtime pair只有source v2 + metadata v3，activation后合法current runtime pair只有source v3 + metadata v4；source v3 + metadata v3、source v2 + metadata v4、dual-write、running/unknown/corrupt组合均fail closed。`polymarket_orderbook_v2`按独立legacy row view读取且raw-first只作历史audit兼容；metadata v2 reader保持只读，metadata v3保持既有schema且activation后退出current runtime new-write，它们都不能成为source v3 claimant authority或重建v3 canonical domain。offline legacy writer产物不得放入current canonical root冒充合法pair。
- 步骤 3.5.6：执行唯一current new-write activation barrier。
  -> 验证：只有metadata v4实现、WS claimant/recovery矩阵及任务3.1至3.5其余硬门通过后，同一个本地activation diff才同时把`polymarket_orderbook_v2 -> v3`与`polymarket_market_metadata_v3 -> v4`两条current runtime new-write路径切换，并断开任务3.4登记的active v2 payload/fingerprint/fan-out/raw-recovery生产边；切换后source v2与metadata v3退出current runtime new-write。公开offline legacy writer继续只服务历史fixture、兼容测试和任务3.7，不得接入`LiveSegmentWriter`、current dispatch、recovery authority或dual-write。任一条件未满足时只保留未激活v3/v4实现和原source v2 + metadata v3路径，不关闭v3分支、不双写、不宣称cutover，也不改变production binary/dataset root。

### 任务 3.6：canonical round-trip 与 future handoff mapping

- 步骤 3.6.1：同一七类fixture分别保留内存canonical domain并执行v3 round-trip。
  -> 验证：全部当前消费retained值、ordering和timestamps一致；multi-change保持原始相对顺序；data-collect侧training/data-pipeline producer golden fixture不依赖被删字段。
- 步骤 3.6.2：登记任务3.2.5产生的`NONBLOCKING_FUTURE_SPEC`附件路径、内容hash和状态，不重复评审字段映射。
  -> 验证：附件已记录当前可知retained字段及后继metadata join的`price_to_beat/final_price`责任；不构造`HandoffFrame v1`、不宣称hot runtime parity。未知future字段不阻断v3，真实adapter、frame升级/legacy、模型接线、长跑和部署另立三件套。

### 任务 3.7：唯一 bounded storage A/B measurement

- 步骤 3.7.1：新增唯一`harness=false` repo-local probe
  `data-collect/benches/polymarket_storage_ab_probe.rs`，复用公开v2/v3
  `TypedParquetSegmentWriter`和官方SDK typed parser；不得修改production writer、
  新增Criterion、第二encoder、manifest simulator或通用benchmark框架。
  -> 验证：同一固定fixture/digest覆盖七类variant、mixed `PriceChange`目标过滤、
  两条相同platform delivery及一次不新增admission的internal retry；两侧均为Zstd 3、
  相同row-group常量。
- 步骤 3.7.2：用隔离case进程写入v2 baseline和v3 candidate，并用独立scan进程请求
  `POSIX_FADV_DONTNEED`后执行全列扫描。
  -> 验证：报告finalized bytes/SHA、column chunks、logical messages/physical rows、
  event rows、write/finalize wall、process CPU、peak RSS、messages/s、scan throughput、
  toolchain与host硬件；缓存回收只写`advisory requested`，不得宣称物理cold-cache已证明。
- 步骤 3.7.3：执行硬门并把完整命令、fixture边界、结果和正式语义证据索引写入
  `data-collect/docs/sdk070-stage3-storage-ab-measurement.md`。
  -> 验证：v2 payload/hash列及fan-out存在，v3三项排除列和fan-out消失，
  `v3 finalized bytes < v2`，writer/footer/scan row facts一致。canonical round-trip、
  duplicate/retry、resolution和manifest authority由Stage3正式contract/startup tests
  证明，probe不复制这些owner。完整UTC日只消费Stage0已冻结v2事实，不重扫、不生成
  full-day v3。

### 阶段 3 审查与提交门

独立审查 source v3/metadata v4 schema、consumer、resolution、legacy compatibility，以及任务3.7 probe源码、运行命令和结果报告。SDK字段能力矩阵存在blocking gap时不允许用JSON补洞；metadata v4 implementation/null matrix/claimant recovery未通过时，不得把current new-write activation标为通过。未激活小绿点可按DAG独立评审，通过后按持续发布授权commit/push。

当前执行事实：

- 任务3.1至3.6的本地实现、current new-write单一activation diff和自审已经完成；
  本地current runtime source/metadata pair固定为v3/v4，不存在双写；W-B保留的
  production-compiled public offline source-v2/metadata-v3 writer只用于历史fixture、
  兼容测试和任务3.7，不得进入current runtime或recovery authority；
- `data-collect/docs/sdk070-stage3-future-typed-handoff-mapping.md`登记为
  `NONBLOCKING_FUTURE_SPEC`，SHA-256为
  `9b8207c3a01b5356a1a365913d1e8368c69d686bbac36a3f39cbbb1629a31464`；
- locked全包串行测试、374项lib测试、current v3/v4 recovery、schema/writer
  contract、fmt与diff门均通过；activation后暴露的不可达v2 helper和空
  fingerprint plumbing已登记为阶段5清理候选，不通过`allow`掩盖；
- 用户已裁决S-B/M-B/W-B：storage probe移入任务3.7；production startup
  lease + manifest preflight/reconcile满足manifest authority且recovery不新增snapshot；
  “只读”只约束current runtime new-write，offline legacy writer保留。对应文档、
  probe/report和预算表修复已经完成；本地门及三名独立
  `gpt-5.6-sol`/`ultra`重复审查均通过，期间发现的storage报告正式语义证据索引
  Important已修复并复审关闭。当前游标为`STAGE3_CLOSED`，以承载本记录的commit
  成功push及三方ref一致为生效条件。
  这里的current new-write activation只指本仓库本地代码调用图，不授权production
  binary activation/cutover、release、部署、远端操作、dataset migration或删除。

## 六、阶段 4：RTDS business-silence 完整换 runtime

本阶段只负责依赖、authority同步和进入/退出状态；详细行为、测试、长跑和性能合同不在此重复，唯一执行 authority为：

`plan/data-collect-rtds-business-silence-full-reconnect-plan.md`

完整参数、专项最终需求、wrapper-first SDK路线、精确authority修改矩阵和持续执行范围均已获批。阶段4继续遵守子计划的入口门和阶段边界：

### 4A：authority同步与阶段边界

进入条件已经满足；相反authority同步、自审、独立审查、独立文档commit/push及三方ref核对已经闭合，并冻结“当前只有training拥有真实RTDS adapter、4B只在training实现、future paper/live真实RTDS adapter必须继承共同合同”的边界。

### 4B：适用授权范围内的capability与实现

`SDK_070_RUNTIME_USABLE`、`2-RTDS_CLOSED`、4A闭合及阶段1本地授权均已满足。官方SDK + 当前最小wrapper的真实teardown capability及training实现已经完成收窄验证、三路独立终审、分离commit/push与三方ref核对。阶段2 Attempt1因精确network-error服务对象固定为`FAIL + INCOMPLETE_8H`；Attempt2固定为`USER_TERMINATED + INCOMPLETE_8H`；Attempt3从clean/pushed `c05db6ba58676bec3e20878375608e395ad0971a`运行完整六源collector并正式落盘，在`3863.950s`后因Gamma顶层`closedTime`精确格式误判固定为`FAIL + INCOMPLETE_8H`。Gamma方案A已由`3a367386`/`e086017f`完成分离发布、定向验证、三路终审和三方ref核对；本总计划只消费专项状态。当前专项唯一游标是从承载本closure的新clean/pushed HEAD、新artifact和空dataset root，以香港线路和`--max-runtime-secs 28800`运行完整四个logical producer、六个`LIVE_SOURCES`并通过既有Parquet与daily manifest正式落盘的Attempt4。

### 阶段 4 审查与提交门

严格按RTDS专项各阶段review/stop/commit/push顺序与本轮裁决后的适用授权范围执行。standing execution可以合并重复申请，但不得合并4A/4B的执行顺序、diff或commit；capability失败、SDK缺口或新冲突仍停止。本总计划只核对依赖状态，且authority、Rust和`polymarket_orderbook_v3`/其他source schema不得混入同一diff或commit。

## 七、阶段 5：stale/redundant 代码与过度校验删除

### 任务 5.1：建立候选台账

- 步骤 5.1.1：从 symbol矩阵筛出只计算后丢弃、重复 owner、test-only public path、legacy adapter和旧 JSON/fingerprint路径。
  -> 验证：每项记录当前消费者和预计终态。
- 步骤 5.1.2：检查 binaries/public API/features/benches/scripts/docs/history。
  -> 验证：只有测试引用不能直接判删；W-B已把public offline v2/metadata-v3 writer、
  legacy schema/reader和任务3.7 probe定为`KEEP`，不得与已不可达的current-runtime
  private v2 branches混为一项删除。
- 步骤 5.1.3：用户决定 `USER_DECISION` 项。
  -> 验证：不擅自删除 public module或 legacy兼容。
  -> 执行结果：2026-07-26用户批准四项推荐。`sources/rtds.rs`、
  `config.rs`、`scheduler.rs`及`segment.rs`两半均不再因`pub`或自引用测试而保留；
  它们统一进入`DELETE_PROVEN_REPLACED_BEHAVIOR`。W-B public offline
  source-v2/metadata-v3 writer、legacy schema/reader和Stage3 probe继续`KEEP`，
  不属于本次裁决。

### 任务 5.2：逐候选或同构小簇处理

- 步骤 5.2.1：先分类为 `REPLACED_BEHAVIOR` 或 `UNREACHABLE_PRIVATE`。
  -> 验证：前者需要唯一替代合同和定向回归；后者只需跨features/targets/public API/scripts/legacy reader的无消费者证明，不为删除它新增test seam、抽象或虚构替代合同。
- 步骤 5.2.2：隔离删除或合并一个候选/小簇。
  -> 验证：diff每行可追溯到已批准候选。
  -> U簇执行边界：删除旧RTDS shadow adapter/测试；删除静态queue config并只移除
  `bounded_queue_contract`中的自引用测试；删除旧scheduler、scheduler测试和
  synthetic e2e；删除整个旧segment及causal/rollover测试。对应module export一并
  删除，不修改current RTDS owner、真实bounded/inflight/backpressure-wait测试、hourly writer、
  handoff或legacy reader。
  -> 首次终审修复边界：补删C01旧metadata-v2 test-only adapter/sentinel正向测试及
  C02/C03 `LifecycleFingerprintSets`自引用模型；按用户批准`U05-A`删除逐queue
  hard-threshold pause/resume control，只保留bounded queue、真实inflight与纯观测/
  兼容DTO；按`U06-A`删除有限health/reconnect circuit旧模型，把确定性
  capped+jitter delay收窄到现有runtime helper，不修改真实source supervisor。
- 步骤 5.2.3：运行受影响 targeted（仅替代行为）、package compile/check/clippy和diff gate；阶段收口运行一次deterministic suite。
  -> 验证：schema/manifest/row/gap/counter/shutdown无意外变化。
  -> 首次终审前执行结果：替代owner的7个integration targets共67项、6个关键unit合同全部
  通过；最终`cargo test -p data-collect --locked -- --test-threads=1`退出0（lib
  351项及全部现存integration/doc targets通过）。随后all-target check、
  `clippy -D warnings`、fmt和diff gate均通过；删除符号在active
  source/tests/benches/Cargo targets中的残留为0。
  -> 首次终审修复后执行结果：bounded queue、manifest、Binance/RTDS dedupe、
  Binance source、current v3 adapter、真实outer recovery定向回归共
  `2+5+2+2+7+1`项全部通过，throughput bench编译通过。首次终审修复后的完整
  `cargo test -p data-collect --locked -- --test-threads=1`退出0（lib 348项及全部
  现存integration/doc targets通过）；all-target check、`clippy -D warnings`、fmt
  和diff gate全部通过。lib减少3项正是已批准删除的旧adapter正向测试。
  -> 重复终审修复后执行结果：pending map key与source-authority `Vec` backing
  capacity的两个focused合同各1项通过；完整package suite再次退出0（lib 348项及
  全部现存integration/doc targets通过），all-target check、`clippy -D warnings`、
  fmt和diff gate再次全部通过。
- 步骤 5.2.4：更新终态和 LOC。
  -> 验证：只有两类 `DELETE_PROVEN_*` 计入净删除。
  -> 首次终审前执行结果：U簇本身为`+0/-1,585`；整个Stage5 tracked Rust diff为
  `+541/-5,381`，净删除`4,840`行。总值包含精确owned-byte正确性修复、必要current
  测试迁移和机械clippy修正，不冒充纯删除量。
  -> 首次终审修复后执行结果：整个Stage5 tracked Rust diff为
  `+584/-7,499`，净删除`6,915`行；新增差额来自I1/I2遗漏补删及U05-A/U06-A，
  同样不把含正确性修复和测试迁移的总净值冒充纯删除量。
  -> 重复终审发现并修复pending `BTreeMap` key与source-authority `Vec` backing
  capacity漏算后，最终tracked Rust diff为`+641/-7,509`，净删除`6,868`行。

### 阶段 5 审查与提交门

独立审查每项 consumer proof和删除范围；不得借 cleanup删除 paper/live、历史 reader或未获批产品功能。

执行结果：文档/authority与代码/consumer两路`gpt-5.6-sol`/Ultra终审均为
`PASS / CLOSURE_UNBLOCKED`，Blocker 0 / Important 0 / Minor 0。代码终审首次快照
发现的pending map key与source-authority `Vec` backing owned-byte漏算已修复，
focused与完整门重跑通过，并由同一审查者复审清零。

closure commit `9e0d264058ed64cd63fbd05280f0826afaba0509`已push到
`origin/fix-rtds-silents`，本地HEAD、upstream与远端ref一致，故`STAGE5_CLOSED`
成立。

## 八、阶段 6：v3集成与范围内 parity

### 任务 6.1：完整本地确定性门

```bash
cargo test -p data-collect --locked -- --test-threads=1
cargo check -p data-collect --locked --all-targets
cargo clippy -p data-collect --locked --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

- 步骤 6.1.1：只运行上述完整package deterministic/check命令一次，并把其中v3 schema/reader/writer/resolution/SDK/manifest/day rollover结果映射到v3 AC。
  -> 验证：完整suite可执行其他模块的现有测试，但只作为一次v3跨模块回归；不重复其专项AC/artifact/长跑/独立审查门，也不重复已被suite覆盖的定向测试。失败须相对pre-v3基线归因：v3 diff引入的失败阻断v3，既存或并发冲突则停止并报告。
- 步骤 6.1.2：只有某个v3 AC对应target未被上述命令覆盖时，才补跑一次该定向target并记录原因。
  -> 验证：不运行有真实网络副作用的bench，不为同一测试重复建门。
  -> 执行结果：Stage6消费Stage5最终修复后、随后发布为
  `9e0d264058ed64cd63fbd05280f0826afaba0509`的同一稳定源码快照；完整package
  suite、all-target check、Clippy `-D warnings`、fmt与diff门均已通过，没有AC
  target缺口。Stage6入口在反重复审计结论返回前额外执行了一次package suite并通过，
  但不作为第二验收门；其余四道门没有重复执行。

### 任务 6.2：消费 Stage3 storage 与完整 UTC 日既有证据

- 步骤 6.2.1：只登记Stage3已经独立审查并发布的
  `data-collect/benches/polymarket_storage_ab_probe.rs`、
  `data-collect/docs/sdk070-stage3-storage-ab-measurement.md`路径、内容hash和
  closure状态。
  -> 验证：`NO_RERUN / NO_REIMPLEMENTATION / NO_SECOND_REVIEW`；Stage5不得删除或
  改写该证据owner。若文件hash漂移，按新diff处理而不是沿用旧结果。
- 步骤 6.2.2：消费`data-collect/docs/sdk070-stage0-baseline.md`已经冻结的完整UTC日
  post-dedupe v2 schema/storage/resource/manifest事实。
  -> 验证：不重扫完整日、不生成v3 full-day candidate、不做full-day canonical-domain/
  resolution语义对照；manifest中的`14,664`仅作不可恢复aggregate披露，不声称
  pre-dedupe replay、精确row/event差异或首次真实v3前向长跑。不得为补齐输入新增
  录制器、sidecar、双写或shadow replay。

### 任务 6.3：canonical domain / future mapping 与产品边界

- 步骤 6.3.1：七类target-admitted fixture做canonical domain/v3 round-trip；
  Stage3另一个固定deterministic fixture经正式writer生成finalized v3 Parquet
  artifact。两套fixture不得混同。
  -> 验证：当前消费字段field-for-field报告；mixed `PriceChange`只比较retained
  entries且保持SDK原始相对顺序；data-collect侧training/data-pipeline producer
  golden fixture通过，但不宣称真实data-pipeline adapter端到端或production real
  artifact通过。Stage0真实v2 artifact只消费物理事实，不得重组为v3 canonical
  domain。首次真实v3 forward run另建三件套。
  -> 执行结果：用户于2026-07-26批准方案A，解除原“代表性real artifact”与需求
  fixture/scope边界的冲突；七类fixture及真实target admission owner的canonical
  round-trip通过，Stage3另一固定fixture的v3 writer/reader与确定性finalized
  artifact通过。
- 步骤 6.3.2：消费阶段3登记的`NONBLOCKING_FUTURE_SPEC`附件路径/hash/status，并只检查现有binary边界没有变化。
  -> 验证：不重新评审同一字段映射；未知future字段记为后继附件缺口而不阻断v3。`HandoffFrame v1`和hot heartbeat行为无diff，不宣称runtime parity。
- 步骤 6.3.3：官方missing/null parser fixture、parser error传播、损坏Parquet、独立legacy v2 row view、v3及restart负例。
  -> 验证：覆盖任务3.1.6至3.1.8与3.5的qualified/unmatched/target-associated contradiction/Gamma-first/platform duplicate/restart/crash、metadata v4 prefix/null matrix及source/metadata cross-pair矩阵；全部fail closed或按冻结legacy语义读取，不把v2适配为v3 canonical domain，不为不可构造的unknown SDK enum创建shadow test path。

### 任务 6.4：v3最终报告

- 步骤 6.4.1：报告v3 schema/domain/writer/reader/recovery相关moved/deleted/rewritten/added/net LOC和artifact差异。
  -> 验证：只统计v3范围，不重复验收2-REST root结构。
- 步骤 6.4.2：报告payload/fingerprint/fan-out旧路径的替代与删除证据。
  -> 验证：同时逐项报告raw-first handler、invalid/unmatched persistence、Gamma-first raw-only、post-horizon raw-only和raw-to-metadata recovery矩阵的替代/删除证据；明确metadata v3 schema不可变且在activation后与source v2一起退出current runtime new-write，legacy schema/reader和W-B offline writer保留，metadata v4继承v3 traceability并只追加claimant pair。只覆盖v3/v4必需删除，不重复汇总阶段5的全局cleanup台账。
- 步骤 6.4.3：报告v3与原计划偏差。
  -> 验证：事实、影响、选项和用户决定写入 changelog。
  -> 执行结果：最终报告已写入
  `data-collect/docs/sdk070-stage6-v3-integration-parity.md`；包含AC映射、证据hash、
  artifact差异、v3 LOC、旧路径替代/删除矩阵、方案A及重复package suite过程偏差。

### 阶段 6 审查与提交门

独立终审需求AC-01至AC-08、AC-13至AC-15中与v3相关的diff、测试和artifact；AC-09已由Stage3 task 3.7 closure，本阶段只核对证据路径/hash/status，不重跑或重复评审。AC-10/11由2-REST/cleanup分支收口，AC-12由RTDS子计划收口。通过后按持续发布授权创建v3 closure commit/push；deployment、旧数据删除或历史rewrite仍不在本计划范围内。

执行结果：两路独立`gpt-5.6-sol`/Ultra终审均为
`PASS / CLOSURE_UNBLOCKED`，Blocker 0 / Important 0 / Minor 0；终审发现的
fixture身份混同、binary边界比较起点和writer归属措辞已修正并复审清零。当前游标为
`STAGE6_REVIEW_PASSED_AWAITING_PUBLISH`。

## 九、范围外后继事项

新数据cutover、历史迁移/删除、production release/deployment，以及真实paper/live外部adapter、模型接线和除RTDS专项阶段2完整collector attempt外的长跑，均须另建对应 demand/plan/changelog并获得授权。用户批准的Attempt3按本地香港线路、独立dataset root和完整training collector拓扑发起，但在`3863.950s`失败而未完成8小时；修复后的Attempt4将在新的隔离dataset root完整运行8小时。两者都会真实写source v3 + metadata v4，但均不构成完整UTC日前向长跑、production binary激活、服务器/canonical dataset-root cutover或部署。

## 十、计划完成定义

本总计划采用分支终态汇总，不设一个会让独立分支互相重跑或阻断的总回归门：

- SDK分支记录`RUNTIME_USABLE/NOT_USABLE`；P-B1通过独立审查、另行发布并核对ref后
  才记录独立typed-contract终态
  `SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED`，否则保持真实pending/blocked
  状态；
- 2-PM/v3分支只有在两列/fan-out删除、metadata v4、当前消费语义、resolution、canonical round-trip、storage和资源门通过并发布后才记完成；`NONBLOCKING_FUTURE_SPEC`只作为附件状态汇报，不成为完成硬门。typed-contract或metadata v4 implementation/validation阻断时，只记对应v3/v4子路径阻断；
- 2-RTDS与2-REST分别按自身review/publish状态结项；root模块化只在2-REST验收；
- cleanup分支独立维护两类删除台账和证据；
- RTDS行为按子计划记`COMPLETE`、当前阶段状态或真实capability/冲突阻断，未完成状态不反向阻断v3；
- umbrella只汇总各分支状态、authority和changelog一致性，不重复各分支专项验收/长跑/审查门。SDK/v3/三个机械分支及cleanup均完成时可记核心总事项完成；RTDS处于任一未完成状态时必须单独标明；
- metadata v4实现与WS claimant/recovery验收完成后，本地代码/test中的source v3 + metadata v4原子current new-write替换才属于阶段3完成项；未满足时只可保留未激活实现。RTDS专项阶段2完整collector attempts只新增真实本地部分日v3/v4前向验证证据，不重开或冒充v3分支完成项；scope外production binary/dataset-root cutover、paper/live接线、deployment、migration、首次真实v3完整UTC日前向长跑和历史删除不得冒充任何分支完成项。
