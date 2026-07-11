# sdk-live-trade 错误记录汇总

生成日期：2026-06-28（Asia/Tokyo）。

本文件是对 `/home/chillchill/sdk-live-trade` 历史计划、changelog、repo guidance、本地官方 Polymarket 文档镜像、artifact pullback 和 git 历史清单的错误复盘汇总；它不是新的实盘执行授权，也不是当前 active plan 的替代文件。当前 sdk-live-trade 执行入口仍以源仓库 `AGENTS.md`、`lastest-command.md` 和 active plan/changelog 为准。

## 输入与基线

- 只读源仓库：`/home/chillchill/sdk-live-trade`。
- 产物目录：`/home/chillchill/btc-updown-marketmaker/mistake-log/`。
- 源仓库状态：`## sdk-live-trade...origin/sdk-live-trade`。
- 源仓库 HEAD：`e146d12b870e775cfa1c85cc89c08d20dc525aab`。
- sdk-live-trade docs Markdown：`114` 个，`40431 total` 行。
- archive Markdown：`43` 个。
- 结构化输入：`源清单.tsv`、`git历史清单.tsv`、`引用证据清单.tsv`、`错误实例台账.jsonl`、`错误族统计.tsv`。

## 统计口径

- 只有源文档或 git/artifact 证据明确记录具体错误、失败 run、失败 gate、错误假设、后续修正或 Symptom/Cause/Fix 模式时才计入。
- 同一文件同一小节重复描述同一错误只计一次；不同日期、不同 run 或不同修复周期复发分别计数。
- 一个失败 run 包含多个独立根因时，按独立错误族拆分。
- 统计数字由 `错误实例台账.jsonl` 自动聚合为 `错误族统计.tsv`，主文档不手工改数。

## 高频错误族总览

| 排名 | count_group | 分类 | 次数 | mistake_ids |
| --- | --- | --- | ---: | --- |
| 1 | `cancel_ttl_matched_cancel_normalization` | `order_lifecycle_and_cancel_semantics` | 6 | `M0023,M0024,M0025,M0026,M0027,M0028` |
| 2 | `artifact_writer_schema_and_persistence_failure` | `artifact_audit_schema_and_persistence` | 5 | `M0011,M0012,M0013,M0014,M0015` |
| 3 | `fail_close_severity_too_broad` | `risk_operator_and_fail_closed_severity` | 4 | `M0040,M0041,M0042,M0043` |
| 4 | `ledger_vs_snapshot_authority` | `ledger_reconciliation_and_account_authority` | 4 | `M0034,M0035,M0036,M0037` |
| 5 | `no_live_smoke_or_stdout_overclaim` | `truth_surface_confusion` | 4 | `M0004,M0005,M0006,M0007` |
| 6 | `platform_sellable_balance_maturity_and_sell_reject` | `platform_rule_or_hidden_rule_misunderstanding` | 4 | `M0019,M0020,M0021,M0022` |
| 7 | `checkpoint_or_plan_cursor_confusion` | `plan_authority_and_cursor` | 3 | `M0001,M0002,M0003` |
| 8 | `late_user_channel_synthetic_alias` | `order_lifecycle_and_cancel_semantics` | 3 | `M0031,M0032,M0033` |
| 9 | `model_contract_and_account_state_staleness` | `model_contract_and_strategy_semantics` | 3 | `M0045,M0046,M0047` |
| 10 | `platform_post_order_response_semantics` | `platform_rule_or_hidden_rule_misunderstanding` | 3 | `M0016,M0017,M0018` |
| 11 | `review_docs_sync_and_review_gate` | `review_and_process_failure` | 3 | `M0053,M0054,M0055` |
| 12 | `sdk_first_boundary_and_shadow_implementation` | `sdk_first_boundary_and_shadow_implementation` | 3 | `M0050,M0051,M0052` |
| 13 | `validator_or_artifact_gate_overclaim` | `truth_surface_confusion` | 3 | `M0008,M0009,M0010` |
| 14 | `deployment_runtime_boundary_confusion` | `deployment_and_runtime_boundary` | 2 | `M0048,M0049` |
| 15 | `generic_not_found_vs_exact_matched_cancel` | `order_lifecycle_and_cancel_semantics` | 2 | `M0029,M0030` |
| 16 | `validator_second_business_interpreter` | `ledger_reconciliation_and_account_authority` | 2 | `M0038,M0039` |
| 17 | `startup_metadata_and_tick_normalization` | `platform_rule_or_hidden_rule_misunderstanding` | 1 | `M0044` |

## 计数校验表

| count_group | count | mistake_ids | 分类 |
| --- | ---: | --- | --- |
| `cancel_ttl_matched_cancel_normalization` | 6 | `M0023,M0024,M0025,M0026,M0027,M0028` | `order_lifecycle_and_cancel_semantics` |
| `artifact_writer_schema_and_persistence_failure` | 5 | `M0011,M0012,M0013,M0014,M0015` | `artifact_audit_schema_and_persistence` |
| `fail_close_severity_too_broad` | 4 | `M0040,M0041,M0042,M0043` | `risk_operator_and_fail_closed_severity` |
| `ledger_vs_snapshot_authority` | 4 | `M0034,M0035,M0036,M0037` | `ledger_reconciliation_and_account_authority` |
| `no_live_smoke_or_stdout_overclaim` | 4 | `M0004,M0005,M0006,M0007` | `truth_surface_confusion` |
| `platform_sellable_balance_maturity_and_sell_reject` | 4 | `M0019,M0020,M0021,M0022` | `platform_rule_or_hidden_rule_misunderstanding` |
| `checkpoint_or_plan_cursor_confusion` | 3 | `M0001,M0002,M0003` | `plan_authority_and_cursor` |
| `late_user_channel_synthetic_alias` | 3 | `M0031,M0032,M0033` | `order_lifecycle_and_cancel_semantics` |
| `model_contract_and_account_state_staleness` | 3 | `M0045,M0046,M0047` | `model_contract_and_strategy_semantics` |
| `platform_post_order_response_semantics` | 3 | `M0016,M0017,M0018` | `platform_rule_or_hidden_rule_misunderstanding` |
| `review_docs_sync_and_review_gate` | 3 | `M0053,M0054,M0055` | `review_and_process_failure` |
| `sdk_first_boundary_and_shadow_implementation` | 3 | `M0050,M0051,M0052` | `sdk_first_boundary_and_shadow_implementation` |
| `validator_or_artifact_gate_overclaim` | 3 | `M0008,M0009,M0010` | `truth_surface_confusion` |
| `deployment_runtime_boundary_confusion` | 2 | `M0048,M0049` | `deployment_and_runtime_boundary` |
| `generic_not_found_vs_exact_matched_cancel` | 2 | `M0029,M0030` | `order_lifecycle_and_cancel_semantics` |
| `validator_second_business_interpreter` | 2 | `M0038,M0039` | `ledger_reconciliation_and_account_authority` |
| `startup_metadata_and_tick_normalization` | 1 | `M0044` | `platform_rule_or_hidden_rule_misunderstanding` |

## 错误族明细

## M01. TTL、model、final cancel 与 matched-cancel 归一化缺失
- **count_group：** cancel_ttl_matched_cancel_normalization
- **次数：** 6
- **分类：** order_lifecycle_and_cancel_semantics
- **典型症状：** ttl_cancel_unresolved、ack_missing、matched-cancel、late user-channel 多轮复发；TTL cancel、model cancel、final cancel-all、user-channel 和 CLOB evidence 没有统一 typed event 合同；本地 lifecycle 已 terminal 后跳过 matched-cancel synthetic evidence，导致 TTL ack 审计漏记
- **错误假设：** 把 exact matched-cancel 后的 dust remainder 当作还需 mint shares 的 remaining fill；把不同入口的订单状态分别局部解释；把两个 authority 入口各自写 ack 当成安全冗余
- **当前正确规则：** cancel 网络入口收敛到 SDK cancel_orders batch，terminal evidence 统一裁决；lifecycle/user-channel 写出 authoritative ack 后必须移除 pending TTL state；后续 SDK completion 只保留审计；prior cumulative fill 后剩余量小于 venue dust 时只生成 Terminal/Filled evidence，不再 ledger apply
- **为什么会复发：** SDK response、user-channel、CLOB evidence、runtime synthetic evidence 曾由多个入口各自解释，导致 ack 缺失、重复 ack 或 dust 误入账。
- **mistake_ids：** M0023,M0024,M0025,M0026,M0027,M0028
- **证据：** M0023 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L21-L33，ref REF007；M0024 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L37-L65，ref REF006；M0025 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-18-r5-no-approval-three-market-runtime-authoritative-plan.md:L439-L462，ref REF004；M0026 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-18-r5-no-approval-three-market-runtime-authoritative-plan.md:L503-L516，ref REF007；M0027 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-13-r5-local-ledger-authoritative-trading-plan-compact.md:L78-L88，ref REF004；M0028 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L190-L198，ref REF006
- **后续防错检查：** 所有订单 evidence 先进入 NormalizedOrderEvent，再进入 lifecycle arbitration、ledger 和 validator。
- **类似工作前必须验证：** 覆盖 exact matched-cancel、TTL ack、overlap cancel、final cancel-all、venue dust 和 late terminal 去重。

## M02. artifact schema 与失败证据持久化缺口
- **count_group：** artifact_writer_schema_and_persistence_failure
- **次数：** 5
- **分类：** artifact_audit_schema_and_persistence
- **典型症状：** clean_sell_accepted=true 的 row 带 reject/error 字段，final writer 前退出且没有可信 gate report；final writer validation failure 曾直接 ? 返回，导致 gate report/fail trace 缺失；probe/sample/buy evidence 曾只在 run 结束或 fail trace 写出时落盘
- **错误假设：** 把 JSONL/summary 可用当作 CSV schema 对齐；把 blank error_message 视为业务 clean，但未清洗 canonical row；把 final writer 当作唯一持久化点
- **当前正确规则：** clean accepted canonical probe row 不得携带 reject_class、error_message 或 reject-derived fields；hot-path audit row 只保留摘要、digest 和 reference；完整材料由 durable snapshot/artifact 承载；live path 应持续 append/flush forensic stream，streaming evidence 只作 forensic review，不降低 canonical final validator
- **为什么会复发：** final writer 曾是证据唯一落盘点，schema 校验失败会让已完成样本不可审计；审计 row 也曾携带过大可增长材料。
- **mistake_ids：** M0011,M0012,M0013,M0014,M0015
- **证据：** M0011 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-25-platform-sellable-maturity-probe-plan.md:L392-L417，ref REF019；M0012 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-25-platform-sellable-maturity-probe-plan.md:L463-L474；M0013 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L193-L224；M0014 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L53-L95，ref REF013；M0015 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L111-L115
- **后续防错检查：** 新 writer 改动必须验证 canonical schema、stream forensic、fail-closed report/trace 和 bounded audit row。
- **类似工作前必须验证：** 强制失败 writer 测试、CSV/JSONL 字段对齐、create-new/no-overwrite、fail trace 包含已收集 probes/samples。

## M03. fail-close severity 过宽
- **count_group：** fail_close_severity_too_broad
- **次数：** 4
- **分类：** risk_operator_and_fail_closed_severity
- **典型症状：** 单个 CadenceGap、post-run warning、validator string 曾进入 top-level fail_closed_reasons；no-live rehearsal 因 CadenceGap fail closed，根因是 hard source-health block 后 producer cadence 状态未 reset；SELL reject 曾因重复次数、存在时长、sum of matched orders 诊断字段被升级为 stop/fail
- **错误假设：** 把 source 中断恢复后的 cadence gap 当作交易暴露硬失败；把诊断字段和频次阈值当作跨 tick 风控状态；把运行层抖动和策略合法性/账户安全混为一谈
- **当前正确规则：** SELL reject ambiguous evidence 进入 quarantine/audit-review；只有 credential/scope/schema/audit writer 等硬边界 fail closed；normalized event 必须携带 runtime_gate_policy；writer/source jitter/CadenceGap 无 active exposure 时不 hard fail-close；warning/audit-only/cleanup-only 与 hard_fail_closed 必须按结构化 severity 合同分层
- **为什么会复发：** 字符串 reason、source jitter、CadenceGap、post-run warning 和 validator error 曾混入 top-level hard gate。
- **mistake_ids：** M0040,M0041,M0042,M0043
- **证据：** M0040 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L98-L115；M0041 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-13-r5-local-ledger-authoritative-trading-plan-compact.md:L113-L127；M0042 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L171-L188，ref REF003；M0043 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L190-L205
- **后续防错检查：** 每个 normalized event 明确 runtime_gate_policy，warning/audit-only/cleanup-only/hard_fail_closed 分层。
- **类似工作前必须验证：** 无 active exposure 时 writer backpressure/source jitter/CadenceGap 不直接 top-level hard fail-close。

## M04. 本地 ledger 与平台 snapshot/reconciliation 权威混用
- **count_group：** ledger_vs_snapshot_authority
- **次数：** 4
- **分类：** ledger_reconciliation_and_account_authority
- **典型症状：** trade_history_filled_size_mismatch、Data API position mismatch、final snapshot unsafe 多轮复发；platform account snapshot、Data API positions/value、balance-allowance 曾被误用于下一 tick 账户向量；post-live snapshot Data API positions/value 与 reconciliation allows_new_orders=false 保持为阻断证据
- **错误假设：** 把 final snapshot/reconciliation 与热路径本地账本权威混用；把 run 内 clean accept 或 open_order_count=0 当作账户 exposure 已完全安全；把单个平台查询或 post_order response 当作订单/账户最终真相
- **当前正确规则：** NAV 由本地 ledger + 当前可见 orderbook 保守估值；post/user-channel/trade/CLOB evidence 并发仲裁；R5 hot path 以本地 lifecycle + local ledger 为权威；final platform/Data/CLOB snapshot 不覆盖本地账本；reconciliation verdict 必须分 open_order_safe、position_value_safe、current_exposure_safe、trade_history 和 audit_complete
- **为什么会复发：** Data API positions/value、CLOB final query、startup/final snapshot 和本地 ledger 在不同阶段有不同权威层级。
- **mistake_ids：** M0034,M0035,M0036,M0037
- **证据：** M0034 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L79-L97，ref REF009；M0035 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L98-L102，ref REF009；M0036 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L109-L135，ref REF010；M0037 AGENTS.md:L144-L149，ref REF011
- **后续防错检查：** 热路径模型输入、NAV 和订单状态以本地 lifecycle/ledger/orderbook/SDK-user-channel 事件为准。
- **类似工作前必须验证：** reconciliation verdict 拆分 open_order_safe、position_value_safe、current_exposure_safe、trade_history 和 audit_complete。

## M05. no-live、smoke 与 stdout 过度背书
- **count_group：** no_live_smoke_or_stdout_overclaim
- **次数：** 4
- **分类：** truth_surface_confusion
- **典型症状：** no-live gate 不采样、不发 BUY/SELL、不产生 valid sample；artifact smoke 只验证 1 条 valid sample 保存，不代表正式 30/30；stdout 显示 sample-0030 start，但没有 gate report/JSONL/CSV/fail trace
- **错误假设：** 把 no-live 作为真实订单状态交错的充分覆盖；把 no-live 通过当作真实订单路径闭合；把 smoke mode 当作正式 probe 目标降级
- **当前正确规则：** no-live 只验证环境、凭据、配置、SDK post/cancel not attempted、writer 路径和 gate report；smoke 结果必须显式标记 artifact_smoke=true，不能冒充正式 30/30 dataset；没有可信 gate report 和持久化样本时，只能作为失败路径证据
- **为什么会复发：** no-live、artifact smoke 和 stdout 都是必要证据，但只覆盖局部路径；真实订单事件异步组合必须靠 live artifact 链路复核。
- **mistake_ids：** M0004,M0005,M0006,M0007
- **证据：** M0004 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L226-L231；M0005 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L150-L160，ref REF017；M0006 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-25-platform-sellable-maturity-probe-plan.md:L392-L417，ref REF019；M0007 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L161-L174，ref REF007
- **后续防错检查：** 任何通过结论必须写明覆盖的是 no-live、artifact smoke、stdout 进度还是正式 live gate。
- **类似工作前必须验证：** 检查 run report、gate report、samples、fail trace、SDK post/cancel 计数与真实资金授权边界。

## M06. 平台可卖余额成熟与 SELL reject 语义误判
- **count_group：** platform_sellable_balance_maturity_and_sell_reject
- **次数：** 4
- **分类：** platform_rule_or_hidden_rule_misunderstanding
- **典型症状：** 本地 ledger 已入账 5 shares，但平台 sellable balance 尚未成熟，SELL 被拒；SELL not enough balance / allowance 曾被升级为 stop_new_orders 或 hard fail-close；SELL reject 的 ambiguous evidence 曾默认进入 hard boundary
- **错误假设：** 把单笔 SELL reject 当作账户不可用或跨 tick suppression 依据；把平台余额查询当成热路径账户权威；把平台即时 sellable deficit 反向覆盖本地 ledger shares
- **当前正确规则：** R5 下一 tick account_state 以本地 lifecycle、ledger、orderbook 与 SDK/user-channel 事件为准；SELL reject 只有 credential/BUY collateral/scope/schema/audit writer 等硬边界才 fail closed；blank-id SELL balance/allowance reject 只写 warning/audit-only；不重试、不 reserve、不入账、不阻断下一 tick
- **为什么会复发：** user-channel/local ledger 可先于平台 sellable balance 成熟；把单笔 SELL reject 反向覆盖 ledger 会引出 suppression 和 fail-close。
- **mistake_ids：** M0019,M0020,M0021,M0022
- **证据：** M0019 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L64-L71，ref REF003；M0020 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L132-L158，ref REF003；M0021 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L171-L188，ref REF003；M0022 AGENTS.md:L137-L138，ref REF009
- **后续防错检查：** SELL balance/allowance reject 按单笔 warning/audit-only 处理，不 reserve、不入账、不跨 tick 写 cap/state。
- **类似工作前必须验证：** 确认 BUY collateral/credential/scope/schema/audit writer 才保持硬边界，SELL reject 只写 bounded diagnostic。

## M07. 权威计划与执行光标混淆
- **count_group：** checkpoint_or_plan_cursor_confusion
- **次数：** 3
- **分类：** plan_authority_and_cursor
- **典型症状：** 文档记录 completed checkpoint，但 open Phase/Task 仍未关闭；旧计划顶部保留历史执行光标和下一步，但已被后续 active plan 覆盖；30 个 valid samples 已写出但正式 gate 仍因 CSV fail-closed，不得继续真实资金 probe
- **错误假设：** 把 checkpoint 完成当作整个 phase 或计划完成；把归档文档的历史停点当作当前执行入口；把样本数量达标当作 gate/计划通过
- **当前正确规则：** checkpoint 完成只关闭当前任务；当前 cursor 仍以 open Phase/Task 和 active authority 为准；归档计划只作历史证据；当前执行顺序以 AGENTS/lastest-command 指向的 active plan/changelog 为准；正式 gate 的状态、post-live reconciliation 和 active plan 当前边界共同决定是否能继续
- **为什么会复发：** 历史计划保留了当时的执行光标，checkpoint、阶段、计划和当前 active authority 同时出现，容易把局部完成或旧停点当作当前状态。
- **mistake_ids：** M0001,M0002,M0003
- **证据：** M0001 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-13-r5-local-ledger-phase2-task6-model-input-continuity.md:L75-L80；M0002 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-13-r5-local-ledger-authoritative-trading-plan-compact.md:L19-L61；M0003 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L137-L144，ref REF013
- **后续防错检查：** 先读 AGENTS.md、lastest-command.md 与 active plan/changelog 顶部状态；把 checkpoint complete、phase complete 和 whole-plan complete 分开记录。
- **类似工作前必须验证：** 确认 active authority 文件名、当前禁止项、下一步边界、源仓库 HEAD 和 docs/changelog 是否同步。

## M08. late user-channel 与 synthetic fill alias 缺口
- **count_group：** late_user_channel_synthetic_alias
- **次数：** 3
- **分类：** order_lifecycle_and_cancel_semantics
- **典型症状：** late user-channel real trade 到达时，已被 synthetic remaining fill 覆盖，却被 exact alias 规则误判为冲突；same order partial real trade + exact matched-cancel synthetic close 后，late real trade 触发 matched_post_real_trade_conflict；validator 过去可能重新推导 covered-by-synthetic 决策
- **错误假设：** 只接受 synthetic fill 与 late real trade 完全逐字段等价；把 late cumulative evidence 当作新的不兼容成交；把 validator 当第二套 alias 判定器
- **当前正确规则：** covered-by-synthetic 范围内的 late real trade 应归一化为 AliasExistingFill/audit-only，不重复 ledger apply；validator 只验证 normalized alias/raw digest/reference 完整性，不重新推导业务决定；同一订单 synthetic remaining segment 覆盖范围内只审计 alias，不重复入账
- **为什么会复发：** synthetic remaining fill 闭合订单后，late real trade 是累计迟到 evidence，不一定是新冲突；旧规则只接受逐字段完全等价。
- **mistake_ids：** M0031,M0032,M0033
- **证据：** M0031 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L66-L78，ref REF008；M0032 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-13-r5-local-ledger-authoritative-trading-plan-compact.md:L44-L57，ref REF008；M0033 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L74-L77，ref REF008
- **后续防错检查：** covered-by-synthetic 范围内 late real trade 归一化为 AliasExistingFill/audit-only，不重复 ledger apply。
- **类似工作前必须验证：** 检查 size/cash/fee/asset/side/order/trade id/dust guard，并保留 raw digest/reference。

## M09. 模型契约与 account_state 时序混用
- **count_group：** model_contract_and_account_state_staleness
- **次数：** 3
- **分类：** model_contract_and_strategy_semantics
- **典型症状：** feature frame 曾使用调用方 stale value 或在 lifecycle evidence 应用前生成 readiness；旧 Ready 在 lifecycle evidence 应用后仍进入第二次 submit，导致旧 feature window 与新 account_state 混用；当前模型契约明确 n_features=160、seq_len=120、alpha/account_state 顺序和 SizeHead/Flip 语义
- **错误假设：** 把模型可见账户值当作可由调用方或旧 frame 注入；硬编码或猜测模型输入维度、输入名、action 语义；认为同 tick Ready 可以跨 ledger 更新继续使用
- **当前正确规则：** feature/account_state 必须从本地 ledger、当前 bids 和 source lifecycle evidence 后刷新派生；启动或热加载必须校验契约，policy 推理必须使用固定 features/alpha/account_state shape 与顺序；有 lifecycle evidence 且缺 refresh hook 时当前 tick 应降级 FeatureStale/no-order；真实 live source 必须 refresh readiness
- **为什么会复发：** feature frame、account_state 和 lifecycle evidence 在同 tick 的刷新顺序不一致，会让 policy 看到旧特征和新账户。
- **mistake_ids：** M0045,M0046,M0047
- **证据：** M0045 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-13-r5-local-ledger-phase2-task6-model-input-continuity.md:L14-L27；M0046 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-13-r5-local-ledger-phase2-task6-model-input-continuity.md:L39-L53；M0047 AGENTS.md:L140-L146
- **后续防错检查：** 模型输入维度、输入名、alpha/account_state 顺序必须从契约校验；lifecycle apply 后刷新 readiness。
- **类似工作前必须验证：** 覆盖 fill 后下一 ready tick、缺 refresh hook 降级 FeatureStale/no-order 和 shape/order hash 校验。

## M10. POST /orders per-order response 语义误判
- **count_group：** platform_post_order_response_semantics
- **次数：** 3
- **分类：** platform_rule_or_hidden_rule_misunderstanding
- **典型症状：** POST /orders batch item success=true 但 orderID 为空、UNKNOWN 且带 not enough balance / allowance；blank platform id / UNKNOWN / error-only response 曾进入 accepted path；BUY submit not accepted 需要有限 retry，但并非所有 reject 都可 retry
- **错误假设：** 只按 success=true 判定 SdkAccepted；把 BUY reject 泛化为可重试瞬态错误；把 SDK transport 成功与订单 accepted 混为一谈
- **当前正确规则：** accepted classifier 必须同时绑定 official item success、非空 id、非 UNKNOWN status、无拒单错误和 scope 可对应；post_orders item 只有非空 platform order id、无拒单错误且状态可解释才可进入 accepted hot path；仅 exact MARKET_NOT_READY、HTTP 425/429/500 could not insert order 等无落单证据 transient 可有限 retry；其他直接 fail closed with evidence
- **为什么会复发：** SDK/HTTP 成功与平台 accepted 是不同层；batch item 可带 per-order error、空 orderID 或 UNKNOWN status。
- **mistake_ids：** M0016,M0017,M0018
- **证据：** M0016 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L60-L79，ref REF005；M0017 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L92-L130，ref REF005；M0018 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L30-L38，ref REF012
- **后续防错检查：** adapter 只允许非空 platform order id、无拒单错误、状态可解释且 scope 可对应的 item 进入 SdkAccepted。
- **类似工作前必须验证：** 对 success=true blank id、UNKNOWN、error_message、post-only/restricted/transient retry 全部有正负例。

## M11. review、文档同步与复审门禁遗漏
- **count_group：** review_docs_sync_and_review_gate
- **次数：** 3
- **分类：** review_and_process_failure
- **典型症状：** 当前规则要求对同事/AI/review 反馈独立验证，不能直接照单全收；docs/status review 发现 lastest-command、active plan 顶部摘要和 changelog 历史段落有 stale current-status wording；计划明确方案落盘和修复后都要派生 GPT-5.5/xhigh 只读 subagents 审查
- **错误假设：** 只修代码/测试，不同步当前状态文档；大步骤完成后只靠自审推进到下一执行门；把 review 输出原样当事实或修复清单
- **当前正确规则：** 主 agent 必须验证反馈正确性，错误反馈要拒绝，无法完全验证的部分要标明边界；复杂/长步骤必须按计划执行只读复审，review 发现 actionable issue 先修复再继续；实盘路径修改必须同步 active plan/changelog/lastest-command，review 后修正文档 stale wording
- **为什么会复发：** review 输出、实现状态和 handoff 文档可能彼此漂移；不验证 review 或不同步文档会让下一轮用错入口。
- **mistake_ids：** M0053,M0054,M0055
- **证据：** M0053 AGENTS.md:L82-L88；M0054 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-25-platform-sellable-maturity-probe-plan.md:L545-L555；M0055 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L162-L165
- **后续防错检查：** 大步骤按计划做 GPT-5.5/xhigh 只读复审；实盘路径改动同步 active plan/changelog/lastest-command。
- **类似工作前必须验证：** 主 agent 独立判断 review 结论，记录采纳/拒绝依据，并复查 docs/status wording。

## M12. SDK-first 边界与影子实现风险
- **count_group：** sdk_first_boundary_and_shadow_implementation
- **次数：** 3
- **分类：** sdk_first_boundary_and_shadow_implementation
- **典型症状：** 当前规则禁止未经用户同意改用 raw REST、manual signing、自建 SDK 旁路或 shadow implementation；BUY retry修复明确不授权扩大 wallet/model/market scope、SDK fallback、raw REST/manual signing 或 shadow implementation；artifact persistence 修复仍明确不引入 raw REST/manual signing/shadow implementation
- **错误假设：** 为绕过 BUY reject 或平台限制临时扩大 scope/旁路 SDK；平台 SDK 能力缺口时自行绕过官方 Rust SDK；把 observability 缺口扩展成交易路径重写
- **当前正确规则：** artifact smoke/streaming 只能增强证据，不改变 SDK-backed 交易语义；retry 规则只能在既定 scope 和 SDK-first path 内收敛；必须先说明缺口、影响和选项，经用户同意后才能改变 SDK-first 路径
- **为什么会复发：** 平台缺口和实盘压力容易诱发 raw REST、manual signing 或平行 adapter，但这会绕过已建立的审计和认证边界。
- **mistake_ids：** M0050,M0051,M0052
- **证据：** M0050 AGENTS.md:L35-L39，ref REF001；M0051 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L30-L38，ref REF002；M0052 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L150-L161，ref REF001
- **后续防错检查：** 任何 SDK 能力缺口先列明影响和可选方案，经用户批准后才能改变路径。
- **类似工作前必须验证：** 代码 diff 中不得出现 raw REST/manual signing/自建 SDK 旁路；scope 不扩大 wallet/model/market。

## M13. validator 或局部 artifact 被当作整体 gate
- **count_group：** validator_or_artifact_gate_overclaim
- **次数：** 3
- **分类：** truth_surface_confusion
- **典型症状：** JSONL/stream/summary 中 valid_sample_count=30，但整体 status=FailClosed；CLI summary 与 gate report 明确 status=FailClosed，唯一原因是 CSV 字段数不一致；post-live snapshot open_order_count=0，但 Data API positions 仍使 allows_new_orders=false
- **错误假设：** 把 open orders 清零当作账户/新订单安全完全恢复；把 samples JSONL/summary 可复核当作正式 CSV/schema gate 已通过；把局部产物有效当作整体 gate passed
- **当前正确规则：** CSV/JSONL/summary schema 必须全部对齐；final writer validation error 仍使 gate fail-closed；open-order safety、position exposure、trade-history availability 和 audit_complete 必须分层判断；正式 gate 必须同时满足 final writer/schema/reconciliation 当前边界
- **为什么会复发：** JSONL/summary、open-order check、validator 局部通过都可能与 final writer、CSV schema 或 post-live reconciliation 不一致。
- **mistake_ids：** M0008,M0009,M0010
- **证据：** M0008 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L20-L28，ref REF013；M0009 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L53-L95，ref REF013；M0010 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L109-L144，ref REF009
- **后续防错检查：** 把 gate status、valid_sample_count、writer status、post-live account/reconciliation 分列，不用单个数字覆盖整体结论。
- **类似工作前必须验证：** 读取 gate report、summary、samples、reconciliation report 的 sha 和字段，不从终端摘要推断。

## M14. 部署与运行边界混淆
- **count_group：** deployment_runtime_boundary_confusion
- **次数：** 2
- **分类：** deployment_and_runtime_boundary
- **典型症状：** AWS Ireland sdk-live-trade 服务器只有约 2 vCPU/1.9GiB RAM，无 swap，不是 Rust 构建机；回归清单明确远端 AWS live 服务器只运行 binary，不作为 Rust 构建机
- **错误假设：** 把生产运行机和开发构建机混用；把远端 runtime 服务器当作 cargo build/test/check/fmt 机器
- **当前正确规则：** release binary 必须本地构建并记录 commit/sha/path，再作为运行产物部署到 /home/ubuntu/sdk-live-trade；每次 live 前确认 binary-only deploy、runtime root 和 artifact pullback，不在服务器编译源码
- **为什么会复发：** AWS live 服务器是低配 runtime，不是构建机；混用源码、binary、model artifact 和钱包 env 会破坏可追溯性。
- **mistake_ids：** M0048,M0049
- **证据：** M0048 AGENTS.md:L52-L61；M0049 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L190-L205
- **后续防错检查：** 本地 build、记录 commit/sha/path，binary-only deploy 到固定 runtime root；服务器不跑 cargo。
- **类似工作前必须验证：** 确认 dirty state、binary sha256、config/model sha、runtime root、pullback artifact 和 rollback 材料。

## M15. generic not-found 与 exact matched-cancel 混同
- **count_group：** generic_not_found_vs_exact_matched_cancel
- **次数：** 2
- **分类：** order_lifecycle_and_cancel_semantics
- **典型症状：** 早期收敛曾把 exact 与 generic matched-cancel 并列处理；当前规则显式禁止 generic not-found 直接入账或写 stop_new_orders/no_order/cleanup_only/top-level hard fail-close
- **错误假设：** 把 generic already-canceled-or-matched 当作成交证明；把 order not found 类字符串当作统一终态
- **当前正确规则：** generic order not found 只能作为 order-local retryable warning，等待 accepted cancel/user-channel/CLOB/exact matched-cancel 关闭；generic/plain not-found 只作为 order-local retryable warning；exact matched-cancel 依赖本地材料裁决
- **为什么会复发：** 相似错误字符串曾被合并处理，但 only exact matched-cancel 可在本地材料充分时参与 filled/terminal 裁决。
- **mistake_ids：** M0029,M0030
- **证据：** M0029 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L54-L61，ref REF006；M0030 AGENTS.md:L137-L138，ref REF006
- **后续防错检查：** generic/plain not-found 只能是 order-local retryable warning，不得入账或写 stop/no-order/cleanup/hard gate。
- **类似工作前必须验证：** 正负例同时覆盖 exact matched-cancel、generic already-canceled-or-matched、plain not-found 和 retry scope。

## M16. artifact validator 成为第二套业务解释器
- **count_group：** validator_second_business_interpreter
- **次数：** 2
- **分类：** ledger_reconciliation_and_account_authority
- **典型症状：** validator 曾重新解释 TTL、matched-cancel、covered-by-synthetic、CLOB terminal、SELL sign 等业务语义；artifact validator 曾把 audit-only/no-ledger-apply duplicate ack 一律当 hard duplicate-id
- **错误假设：** 把 post-run validator 建成第二套业务解释器；用 row duplicate key 代替业务 ledger-impacting 冲突判定
- **当前正确规则：** validator 只验证 runtime 写出的 normalized rows、raw digest、reference integrity 和 artifact consistency；validator 必须区分 audit-only duplicate、ledger-impacting delta 和不可解释冲突
- **为什么会复发：** validator 若重建 TTL、matched-cancel、covered-by-synthetic 或 SELL sign 语义，就会和 runtime/lifecycle 分叉。
- **mistake_ids：** M0038,M0039
- **证据：** M0038 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L129-L144，ref REF007；M0039 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L190-L201，ref REF007
- **后续防错检查：** validator 只验证 runtime 已写出的 normalized rows、digest、reference integrity、side/sign/reservation contract。
- **类似工作前必须验证：** 禁止 source-specific rebuilder 重新做业务裁决；旧 helper 只能作为 bounded consistency scan。

## M17. startup metadata 原始值比较缺少 normalization
- **count_group：** startup_metadata_and_tick_normalization
- **次数：** 1
- **分类：** platform_rule_or_hidden_rule_misunderstanding
- **典型症状：** M1 startup TickSizeMismatch 源于 Gamma/CLOB decimal 字符串格式差异缺少 raw/normalized evidence
- **错误假设：** 按原始字符串比较 tick size/min order size
- **当前正确规则：** startup context 应 decimal-normalized 比较，同时写 raw value 与 normalized value
- **为什么会复发：** Gamma/CLOB 原始 decimal 字符串格式不同但数值等价，缺 raw/normalized evidence 会放大定位成本。
- **mistake_ids：** M0044
- **证据：** M0044 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L116-L127，ref REF023
- **后续防错检查：** tick size、min order size 等 startup metadata 同时保留 raw value 和 normalized decimal 比较结果。
- **类似工作前必须验证：** artifact 中能复核两侧 raw/normalized value；真实 mismatch 仍 fail closed。

## 平台与 artifact 引用索引

| reference_id | kind | path | line_ref | sha256 | required_by_mistake_ids |
| --- | --- | --- | --- | --- | --- |
| `REF001` | `official_doc` | `polymarket-docs/current/api-reference/clients-sdks.md` | `L5-L10` | `` | `M0050,M0052` |
| `REF002` | `official_doc` | `polymarket-docs/current/trading/overview.md` | `L31-L38` | `` | `M0051` |
| `REF003` | `official_doc` | `polymarket-docs/current/concepts/order-lifecycle.md` | `L54-L61` | `` | `M0019,M0020,M0021,M0042` |
| `REF004` | `official_doc` | `polymarket-docs/current/concepts/order-lifecycle.md` | `L120-L125` | `` | `M0025,M0027` |
| `REF005` | `api_spec` | `polymarket-docs/api-spec/clob-openapi.yaml` | `L346-L399` | `` | `M0016,M0017` |
| `REF006` | `api_spec` | `polymarket-docs/api-spec/clob-openapi.yaml` | `L5166-L5185` | `` | `M0024,M0028,M0029,M0030` |
| `REF007` | `asyncapi` | `polymarket-docs/asyncapi/asyncapi-user.json` | `L16-L39` | `` | `M0007,M0023,M0026,M0038,M0039` |
| `REF008` | `asyncapi` | `polymarket-docs/asyncapi/asyncapi-user.json` | `L437-L520` | `` | `M0031,M0032,M0033` |
| `REF009` | `api_spec` | `polymarket-docs/api-spec/data-api-openapi.yaml` | `L341-L363` | `` | `M0010,M0022,M0034,M0035` |
| `REF010` | `api_spec` | `polymarket-docs/api-spec/data-api-openapi.yaml` | `L412-L487` | `` | `M0036` |
| `REF011` | `official_doc` | `polymarket-docs/current/concepts/positions-tokens.md` | `L81-L87` | `` | `M0037` |
| `REF012` | `official_doc` | `polymarket-docs/current/trading/matching-engine.md` | `L146-L199` | `` | `M0018` |
| `REF013` | `artifact_report` | `poly-arb-ai/rust/sdk-live-trade/artifacts/live-validation/pullback/platform-sellable-maturity-live-20260626T133250Z/platform_sellable_maturity_gate_report.json` | `` | `d2a9427f0cf10b9649224c172d16838ea793f6bb8bb18109402e5877daff40a9` | `M0003,M0008,M0009,M0014` |
| `REF014` | `artifact_report` | `poly-arb-ai/rust/sdk-live-trade/artifacts/live-validation/pullback/platform-sellable-maturity-live-20260626T133250Z/platform_sellable_maturity_fail_closed_trace.json` | `` | `e74fd13533679dd70b28b1e53df833bb8ded557ba90706ed9c6bfc0a0b0a7f55` | `M0008,M0009,M0014` |
| `REF015` | `artifact_report` | `poly-arb-ai/rust/sdk-live-trade/artifacts/live-validation/pullback/platform-sellable-maturity-live-20260626T133250Z/platform_sellable_maturity_summary.json` | `` | `f697182c4537b14b4de871995d86257df6ca8bdf6d1c506624939b73d44f5acf` | `M0008,M0009,M0014` |
| `REF016` | `artifact_report` | `poly-arb-ai/rust/sdk-live-trade/artifacts/live-validation/pullback/platform-sellable-maturity-live-20260626T133250Z/platform_sellable_maturity_samples.jsonl` | `` | `9f581d1e0a1951af413f987c3e2a9caf03cf6227f5367f08ad1868a381060375` | `M0008,M0009,M0014` |
| `REF017` | `artifact_report` | `poly-arb-ai/rust/sdk-live-trade/artifacts/live-validation/pullback/platform-sellable-maturity-artifact-smoke-20260626T132728Z/platform_sellable_maturity_gate_report.json` | `` | `b5902438cfeba5469c173f2ddf5cabc8d17cf249926d2e54137924ec4c1e0adf` | `M0005` |
| `REF018` | `artifact_report` | `poly-arb-ai/rust/sdk-live-trade/artifacts/live-validation/pullback/post-live-account-snapshot-20260626T145200Z/reconciliation_report.json` | `` | `b8427657ae369811e8575f1e6479f469dd525b7c510137b83785dd4a1702fd79` | `M0010,M0036` |
| `REF019` | `artifact_manifest` | `poly-arb-ai/rust/sdk-live-trade/artifacts/live-validation/pullback/platform-sellable-maturity-live-20260626T092500Z/artifact_bundle_manifest.tsv` | `` | `900515a01002eb7c2815c8ace68c98d9fb9a988f99ffbb8c223eccecd824d51c` | `M0006,M0011` |
| `REF020` | `artifact_manifest` | `poly-arb-ai/rust/sdk-live-trade/artifacts/remote-r5-runs/r5-three-market-live-funds-20260619T092850Z/artifact_bundle_manifest.tsv` | `` | `7041a4d0a905d543e77be82a68ae51449e8b4a505aa3a6ed6dbf8b2c6ddfa3a2` | `M0031,M0032` |
| `REF021` | `artifact_manifest` | `poly-arb-ai/rust/sdk-live-trade/artifacts/remote-r5-runs/r5-three-market-live-funds-20260621T150800Z/artifact_bundle_manifest.tsv` | `` | `11ae90e48a6d293419a53f021d04bec97e3da62d33db4ab51fa15ddac104fcda` | `M0016,M0019,M0020,M0021` |
| `REF022` | `official_doc` | `polymarket-docs/current/trading/matching-engine.md` | `L31-L50` | `` | `M0018` |
| `REF023` | `official_doc` | `polymarket-docs/current/trading/orderbook.md` | `L91-L105` | `` | `M0044` |

## 后续开发/重构前检查清单

- 先确认 active plan/changelog、lastest-command 与 AGENTS.md 当前指针一致，再使用归档文档。
- 把 checkpoint、phase、whole-plan、formal gate、no-live、artifact smoke、stdout 进度分开写结论。
- 涉及 Polymarket 语义前先查本地官方文档镜像和 api/asyncapi spec，并写入引用 ID。
- post_orders accepted 只认非空 platform order id、无拒单错误、状态可解释和 scope 可对应。
- cancel、TTL、user-channel、CLOB 和 synthetic evidence 先归一化，再入 lifecycle、ledger、validator。
- Data API/CLOB/platform snapshot 只按计划限定的 startup/recovery/reconciliation/audit 用途使用，不覆盖热路径 ledger。
- writer/schema 改动必须覆盖 canonical writer、stream forensic、fail trace、bounded audit row 和 create-new/no-overwrite。
- warning/audit-only/cleanup-only/hard_fail_closed 必须使用结构化 severity，不靠字符串 reason 直接升级。
- 模型输入改动必须校验 feature_order、shape、alpha 顺序、account_state 顺序和 lifecycle apply 后 refresh。
- 部署前确认本地 build、binary sha、config/model sha、runtime root、pullback artifact；远端 live 服务器不跑 cargo。
- review 反馈先独立验证；复杂阶段按计划派生 GPT-5.5/xhigh 只读复审，docs/status stale wording 同步修正。

## 附录：结构化产物

- `源清单.tsv`：159 条源文件记录，覆盖 active/supporting/historical/archive 源。
- `git历史清单.tsv`：1341 条 git history 记录。
- `引用证据清单.tsv`：23 条官方文档、api spec、asyncapi、artifact report/manifest 引用。
- `错误实例台账.jsonl`：55 条错误实例记录。
- `错误族统计.tsv`：17 个错误族聚合。
