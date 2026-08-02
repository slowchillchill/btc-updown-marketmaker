# sdk-live-trade 错误记录汇总

生成日期：2026-06-28（亚洲/东京时区，`Asia/Tokyo`）。

本文件是对 `/home/chillchill/sdk-live-trade` 历史计划、变更记录、仓库指南、本地官方 Polymarket 文档镜像、拉回产物和 Git 历史清单的错误复盘汇总；它不是新的实盘执行授权，也不是当前活跃计划的替代文件。当前 `sdk-live-trade` 执行入口仍以源仓库 `AGENTS.md`、`lastest-command.md` 和活跃计划与变更记录为准。

## 输入与基线

- 只读源仓库：`/home/chillchill/sdk-live-trade`。
- 产物目录：`/home/chillchill/btc-updown-marketmaker/mistake-log/`。
- 源仓库状态：`## sdk-live-trade...origin/sdk-live-trade`。
- 源仓库 HEAD：`e146d12b870e775cfa1c85cc89c08d20dc525aab`。
- `sdk-live-trade` 文档：`114` 个 Markdown 文件，`40431 total` 行。
- 归档文档：`43` 个 Markdown 文件。
- 结构化输入：`源清单.tsv`、`git历史清单.tsv`、`引用证据清单.tsv`、`错误实例台账.jsonl`、`错误族统计.tsv`。

## 统计口径

- 只有源文档或 Git/产物证据明确记录具体错误、失败运行、失败门禁、错误假设、后续修正，或“症状/原因/修复”模式时才计入。
- 同一文件同一小节重复描述同一错误只计一次；不同日期、不同运行或不同修复周期的复发分别计数。
- 一次失败运行包含多个独立根因时，按独立错误族拆分。
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

## M01. TTL、模型撤单、最终撤单与 `matched-cancel` 归一化缺失
- **count_group：** `cancel_ttl_matched_cancel_normalization`
- **次数：** 6
- **分类：** `order_lifecycle_and_cancel_semantics`
- **典型症状：** `ttl_cancel_unresolved`、`ack_missing`、`matched-cancel` 和迟到用户通道多轮复发；TTL 撤单、模型撤单、最终全部撤单、用户通道和 CLOB 证据没有统一的类型化事件合同；本地生命周期已进入终态后跳过 `matched-cancel` 合成证据，导致 TTL 确认审计漏记。
- **错误假设：** 把 `exact matched-cancel`（精确命中 `matched-cancel` 分类）后的尘埃剩余量当作仍需生成的额外成交，并再次应用到持仓和账本；把不同入口的订单状态分别局部解释；把两个权威入口各自写入确认当成安全冗余。
- **当前正确规则：** 撤单网络入口收敛到 SDK `cancel_orders` 批处理，终态证据统一裁决；生命周期或用户通道写出权威确认后，必须移除待处理 TTL 状态；后续 SDK 完成事件只保留审计；先前累计成交后剩余量小于平台尘埃阈值时，只生成 `Terminal`（终态）/`Filled`（已成交）证据，不再应用于账本。
- **为什么会复发：** SDK 响应、用户通道、CLOB 证据和运行时合成证据曾由多个入口各自解释，导致确认缺失、重复确认或尘埃剩余量误入账。
- **mistake_ids：** M0023,M0024,M0025,M0026,M0027,M0028
- **证据：** M0023 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L21-L33，ref REF007；M0024 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L37-L65，ref REF006；M0025 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-18-r5-no-approval-three-market-runtime-authoritative-plan.md:L439-L462，ref REF004；M0026 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-18-r5-no-approval-three-market-runtime-authoritative-plan.md:L503-L516，ref REF007；M0027 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-13-r5-local-ledger-authoritative-trading-plan-compact.md:L78-L88，ref REF004；M0028 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L190-L198，ref REF006
- **后续防错检查：** 所有订单证据先进入 `NormalizedOrderEvent`，再进入生命周期仲裁、账本和校验器。
- **类似工作前必须验证：** 覆盖 `exact matched-cancel`（精确命中 `matched-cancel` 分类）、TTL 确认、重叠撤单、最终全部撤单、平台尘埃阈值和迟到终态事件去重。

## M02. 产物数据模式与失败证据持久化缺口
- **count_group：** `artifact_writer_schema_and_persistence_failure`
- **次数：** 5
- **分类：** `artifact_audit_schema_and_persistence`
- **典型症状：** `clean_sell_accepted=true` 的行携带拒绝或错误字段，在最终写入器之前退出，且没有可信门禁报告；最终写入器校验失败曾直接通过 `?` 返回，导致门禁报告和失败轨迹缺失；探测、样本和买入证据曾只在运行结束或写出失败轨迹时落盘。
- **错误假设：** 把 JSONL 或摘要可用当作 CSV 数据模式对齐；把空白 `error_message` 视为业务干净，但未清洗规范行；把最终写入器当作唯一持久化点。
- **当前正确规则：** 干净接受的规范探测行不得携带 `reject_class`、`error_message` 或拒绝派生字段；热路径审计行只保留摘要、摘要哈希和引用；完整材料由持久快照或产物承载；实盘路径应持续追加并刷新取证流，流式证据只用于取证复核，不降低规范最终校验器标准。
- **为什么会复发：** 最终写入器曾是证据唯一落盘点，数据模式校验失败会让已完成样本不可审计；审计行也曾携带过大的可增长材料。
- **mistake_ids：** M0011,M0012,M0013,M0014,M0015
- **证据：** M0011 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-25-platform-sellable-maturity-probe-plan.md:L392-L417，ref REF019；M0012 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-25-platform-sellable-maturity-probe-plan.md:L463-L474；M0013 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L193-L224；M0014 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L53-L95，ref REF013；M0015 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L111-L115
- **后续防错检查：** 新写入器改动必须验证规范数据模式、流式取证、`fail-closed report/trace`（失败关闭报告与轨迹），以及有界审计行。
- **类似工作前必须验证：** 强制写入器失败测试、CSV/JSONL 字段对齐、新建且不覆盖，以及失败轨迹包含已收集探测和样本。

## M03. `fail-close`（故障时拒绝）的严重级别过宽
- **count_group：** `fail_close_severity_too_broad`
- **次数：** 4
- **分类：** `risk_operator_and_fail_closed_severity`
- **典型症状：** 单个 `CadenceGap`、运行后警告和校验器字符串曾进入顶层 `fail_closed_reasons`；`no-live` 演练因 `CadenceGap` 而触发 `fail-closed`（故障时拒绝），根因是数据源健康状态硬阻断后，生产者节奏状态未重置；卖出拒绝曾因重复次数、存在时长和匹配订单总量诊断字段而被升级为停止或失败。
- **错误假设：** 把数据源中断恢复后的节奏缺口当作交易暴露硬失败；把诊断字段和频次阈值当作跨后续决策周期持续生效的风控状态；把运行层抖动和策略合法性或账户安全混为一谈。
- **当前正确规则：** 卖出拒绝的含糊证据进入 `quarantine/audit-review`（隔离与审计复核）；只有凭据、范围、数据模式和审计写入器等硬边界才触发 `fail-closed`（故障时拒绝）；归一化事件必须携带 `runtime_gate_policy`；写入器或数据源抖动、`CadenceGap` 在没有活跃暴露时不得触发 `hard fail-close`（硬失败关闭）；`warning`（警告）、`audit-only`（仅审计）、`cleanup-only`（仅清理）和 `hard_fail_closed` 必须按结构化严重级别合同分层。
- **为什么会复发：** 原因字符串、数据源抖动、`CadenceGap`、运行后警告和校验器错误曾混入顶层强制门禁。
- **mistake_ids：** M0040,M0041,M0042,M0043
- **证据：** M0040 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L98-L115；M0041 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-13-r5-local-ledger-authoritative-trading-plan-compact.md:L113-L127；M0042 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L171-L188，ref REF003；M0043 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L190-L205
- **后续防错检查：** 每个归一化事件明确 `runtime_gate_policy`，并对 `warning`（警告）、`audit-only`（仅审计）、`cleanup-only`（仅清理）和 `hard_fail_closed` 分层。
- **类似工作前必须验证：** 没有活跃暴露时，写入器背压、数据源抖动和 `CadenceGap` 不直接触发顶层 `hard fail-close`（硬失败关闭）。

## M04. 本地账本与平台快照和对账权威混用
- **count_group：** `ledger_vs_snapshot_authority`
- **次数：** 4
- **分类：** `ledger_reconciliation_and_account_authority`
- **典型症状：** `trade_history_filled_size_mismatch`、Data API 头寸不匹配和最终快照不安全多轮复发；平台账户快照、Data API 头寸或价值、余额或授权额度曾被误用于下一决策周期的账户向量；实盘后快照中的 Data API 头寸或价值与对账 `allows_new_orders=false` 保持为阻断证据。
- **错误假设：** 把最终快照或对账与热路径本地账本权威混用；把运行内干净接受或 `open_order_count=0` 当作账户暴露已完全安全；把单个平台查询或 `post_order` 响应当作订单或账户最终真相。
- **当前正确规则：** NAV 由本地账本加当前可见订单簿保守估值；提交、用户通道、成交和 CLOB 证据并发仲裁；R5 热路径以本地生命周期和本地账本为权威；最终平台/Data/CLOB 快照不覆盖本地账本；对账结论必须分为 `open_order_safe`、`position_value_safe`、`current_exposure_safe`、`trade_history` 和 `audit_complete`。
- **为什么会复发：** Data API 头寸或价值、CLOB 最终查询、启动或最终快照和本地账本在不同阶段有不同权威层级。
- **mistake_ids：** M0034,M0035,M0036,M0037
- **证据：** M0034 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L79-L97，ref REF009；M0035 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L98-L102，ref REF009；M0036 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L109-L135，ref REF010；M0037 AGENTS.md:L144-L149，ref REF011
- **后续防错检查：** 热路径模型输入、NAV 和订单状态以本地生命周期、账本、订单簿和 SDK 用户通道事件为准。
- **类似工作前必须验证：** 对账结论拆分为 `open_order_safe`、`position_value_safe`、`current_exposure_safe`、`trade_history` 和 `audit_complete`。

## M05. `no-live`（不提交真实订单）、冒烟测试与标准输出过度背书
- **count_group：** `no_live_smoke_or_stdout_overclaim`
- **次数：** 4
- **分类：** `truth_surface_confusion`
- **典型症状：** `no-live` 门禁不采样、不发送买入或卖出、不产生有效样本；产物冒烟测试只验证 1 条有效样本保存，不代表正式 30/30；标准输出显示 `sample-0030 start`，但没有门禁报告、JSONL、CSV 或失败轨迹。
- **错误假设：** 把 `no-live` 模式作为真实订单状态交错的充分覆盖；把 `no-live` 通过当作真实订单路径闭合；把冒烟模式当作正式探测目标降级。
- **当前正确规则：** `no-live` 模式只验证环境、凭据、配置、SDK 未尝试提交或撤单、写入器路径和门禁报告；冒烟结果必须显式标记 `artifact_smoke=true`，不能冒充正式 30/30 数据集；没有可信门禁报告和持久化样本时，只能作为失败路径证据。
- **为什么会复发：** `no-live`、产物冒烟测试和标准输出都是必要证据，但只覆盖局部路径；真实订单事件的异步组合必须依靠实盘产物链路复核。
- **mistake_ids：** M0004,M0005,M0006,M0007
- **证据：** M0004 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L226-L231；M0005 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L150-L160，ref REF017；M0006 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-25-platform-sellable-maturity-probe-plan.md:L392-L417，ref REF019；M0007 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L161-L174，ref REF007
- **后续防错检查：** 任何通过结论必须写明覆盖的是 `no-live`、产物冒烟测试、标准输出进度，还是正式实盘门禁。
- **类似工作前必须验证：** 检查运行报告、门禁报告、样本、失败轨迹、SDK 提交或撤单计数与真实资金授权边界。

## M06. 平台可卖余额成熟与卖出拒绝语义误判
- **count_group：** `platform_sellable_balance_maturity_and_sell_reject`
- **次数：** 4
- **分类：** `platform_rule_or_hidden_rule_misunderstanding`
- **典型症状：** 本地账本已入账 5 份，但平台可卖余额尚未成熟，卖出被拒；精确错误 `SELL not enough balance / allowance` 曾被升级为 `stop_new_orders` 或 `hard fail-closed`（硬失败关闭）；卖出拒绝的含糊证据曾默认进入硬边界。
- **错误假设：** 把单笔卖出拒绝当作账户不可用，或形成跨后续决策周期持续生效的抑制状态的依据；把平台余额查询当成热路径账户权威；把平台即时可卖缺口反向覆盖本地账本份额。
- **当前正确规则：** R5 下一决策周期的 `account_state` 以本地生命周期、账本、订单簿和 SDK 用户通道事件为准；卖出拒绝只有凭据、买入抵押品、范围、数据模式和审计写入器等硬边界才触发 `fail-closed`（故障时拒绝）；空白编号的卖出余额或授权额度拒绝只写 `warning`（警告）或 `audit-only`（仅审计）；不重试、不预留、不入账、不阻断下一决策周期。
- **为什么会复发：** 用户通道和本地账本可先于平台可卖余额成熟；用单笔卖出拒绝反向覆盖账本，会引出 `suppression`（抑制）与 `fail-closed`（故障时拒绝）。
- **mistake_ids：** M0019,M0020,M0021,M0022
- **证据：** M0019 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L64-L71，ref REF003；M0020 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L132-L158，ref REF003；M0021 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L171-L188，ref REF003；M0022 AGENTS.md:L137-L138，ref REF009
- **后续防错检查：** 卖出余额或授权额度拒绝按单笔 `warning`（警告）或 `audit-only`（仅审计）处理，不预留、不入账，也不写入跨后续决策周期持续生效的上限或状态。
- **类似工作前必须验证：** 确认仅买入抵押品、凭据、范围、数据模式和审计写入器保持硬边界，卖出拒绝只写有界诊断。

## M07. 权威计划与执行光标混淆
- **`count_group`：** `checkpoint_or_plan_cursor_confusion`
- **次数：** 3
- **分类：** `plan_authority_and_cursor`
- **典型症状：** 文档记录检查点已完成，但尚未关闭的阶段或任务仍然存在；旧计划顶部保留历史执行光标和下一步，但已被后续活跃计划取代；30 个有效样本已写出，但正式门禁仍因 CSV 校验失败而关闭，不得继续真实资金探测。
- **错误假设：** 把检查点完成当作整个阶段或计划完成；把归档文档的历史停点当作当前执行入口；把样本数量达标当作门禁或计划通过。
- **当前正确规则：** 检查点完成只关闭当前任务；当前执行光标仍以尚未关闭的阶段或任务和活跃权威文档为准；归档计划只作历史证据；当前执行顺序以 `AGENTS.md`、`lastest-command.md` 指向的活跃计划和变更记录为准；正式门禁状态、实盘后对账结果和活跃计划当前边界共同决定是否能继续。
- **为什么会复发：** 历史计划保留了当时的执行光标，检查点、阶段、计划和当前活跃权威文档同时出现，容易把局部完成或旧停点误当作当前状态。
- **mistake_ids：** M0001,M0002,M0003
- **证据：** M0001 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-13-r5-local-ledger-phase2-task6-model-input-continuity.md:L75-L80；M0002 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-13-r5-local-ledger-authoritative-trading-plan-compact.md:L19-L61；M0003 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L137-L144，ref REF013
- **后续防错检查：** 先读 `AGENTS.md`、`lastest-command.md` 与活跃计划和变更记录的顶部状态；把检查点完成、阶段完成和整个计划完成分开记录。
- **类似工作前必须验证：** 确认活跃权威文档文件名、当前禁止项、下一步边界、源仓库 `HEAD`，以及文档与变更记录是否同步。

## M08. 用户通道迟到消息与合成成交等价归并（`alias`）缺口
- **`count_group`：** `late_user_channel_synthetic_alias`
- **次数：** 3
- **分类：** `order_lifecycle_and_cancel_semantics`
- **典型症状：** 用户通道的真实成交消息迟到时，其成交量已经被合成的剩余成交覆盖（`covered-by-synthetic`），却被严格逐字段等价的 `alias` 规则误判为冲突；同一订单先发生部分真实成交，再由 `exact matched-cancel`（精确命中 `matched-cancel` 分类）合成闭合后，迟到的真实成交触发 `matched_post_real_trade_conflict`；校验器过去可能重新推导 `covered-by-synthetic` 决定。
- **错误假设：** 只接受合成成交与迟到的真实成交在每个字段上完全等价；把迟到的累计证据当作新的不兼容成交；把校验器当作第二套 `alias` 判定器。
- **当前正确规则：** 在 `covered-by-synthetic` 覆盖范围内，迟到的真实成交应归一化为 `AliasExistingFill` 并仅用于审计，不重复应用到账本；校验器只验证 `alias` 归一化结果、原始数据摘要哈希（`raw digest`）和引用完整性，不重新推导业务决定；同一订单的合成剩余成交段覆盖范围内只审计 `alias`，不重复入账。
- **为什么会复发：** 合成的剩余成交闭合订单后，迟到的真实成交属于迟到的累计证据，不一定代表新冲突；旧规则只接受逐字段完全等价。
- **mistake_ids：** M0031,M0032,M0033
- **证据：** M0031 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L66-L78，ref REF008；M0032 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-13-r5-local-ledger-authoritative-trading-plan-compact.md:L44-L57，ref REF008；M0033 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L74-L77，ref REF008
- **后续防错检查：** `covered-by-synthetic` 覆盖范围内的迟到真实成交归一化为 `AliasExistingFill` 并仅用于审计，不重复应用到账本。
- **类似工作前必须验证：** 检查 `size`、`cash`、`fee`、`asset`、`side`、`order`（订单标识）、`trade id`（成交标识）和 `dust guard`（尘埃量防护），并保留原始数据摘要哈希与引用（`raw digest/reference`）。

## M09. 模型契约与 `account_state` 时序混用
- **`count_group`：** `model_contract_and_account_state_staleness`
- **次数：** 3
- **分类：** `model_contract_and_strategy_semantics`
- **典型症状：** 特征帧曾使用调用方传入的陈旧值，或在应用生命周期证据前生成就绪状态；旧的 `Ready` 在应用生命周期证据后仍进入第二次提交，导致旧特征窗口与新的 `account_state` 混用；当前模型契约明确规定 `n_features=160`、`seq_len=120`、`alpha` 与 `account_state` 的顺序，以及 `SizeHead` 与 `Flip` 的语义。
- **错误假设：** 认为模型可见的账户值可由调用方或旧帧注入；硬编码或猜测模型输入维度、输入名和动作语义；认为同一决策周期的 `Ready` 可跨账本更新继续使用。
- **当前正确规则：** 特征与 `account_state` 必须根据本地账本、当前买单以及来源生命周期证据刷新后派生；启动或热加载时必须校验契约，策略推理必须使用固定的 `features`、`alpha`、`account_state` 形状与顺序；存在生命周期证据但缺少刷新钩子时，当前决策周期应降级为 `FeatureStale/no-order`；真实实盘来源必须刷新就绪状态。
- **为什么会复发：** 特征帧、`account_state` 和生命周期证据在同一决策周期内的刷新顺序不一致，会让策略同时看到旧特征和新账户状态。
- **mistake_ids：** M0045,M0046,M0047
- **证据：** M0045 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-13-r5-local-ledger-phase2-task6-model-input-continuity.md:L14-L27；M0046 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-13-r5-local-ledger-phase2-task6-model-input-continuity.md:L39-L53；M0047 AGENTS.md:L140-L146
- **后续防错检查：** 模型输入维度、输入名、`alpha` 与 `account_state` 顺序必须按契约校验；应用生命周期证据后刷新就绪状态。
- **类似工作前必须验证：** 覆盖成交后的下一就绪决策周期、缺少刷新钩子时降级为 `FeatureStale/no-order`，以及形状与顺序哈希（`shape/order hash`）校验。

## M10. `POST /orders` 的逐订单响应语义误判
- **`count_group`：** `platform_post_order_response_semantics`
- **次数：** 3
- **分类：** `platform_rule_or_hidden_rule_misunderstanding`
- **典型症状：** `POST /orders` 批次中的单项响应虽然为 `success=true`，但 `orderID` 为空、状态为 `UNKNOWN`，且带有 `not enough balance / allowance` 错误；空平台标识、`UNKNOWN` 状态或仅含错误的响应曾进入接受路径；买单提交未被接受时需要有限重试，但并非所有拒绝都可重试。
- **错误假设：** 只按 `success=true` 判定 `SdkAccepted`；把买单拒绝泛化为可重试的瞬态错误；把 SDK 传输成功与订单被平台接受混为一谈。
- **当前正确规则：** 接受分类器必须同时要求官方单项响应成功、标识非空、状态不是 `UNKNOWN`、不存在拒单错误且能对应既定范围；`post_orders` 单项只有具备非空平台订单标识、无拒单错误且状态可解释时，才可进入接受热路径；仅对严格匹配 `MARKET_NOT_READY`，或 HTTP 425/429/500 且包含 `could not insert order` 等无落单证据的瞬态错误做有限重试；其他情况直接携带证据触发 `fail-closed`（故障时拒绝）。
- **为什么会复发：** SDK 或 HTTP 成功与订单被平台接受属于不同层级；批次单项可能带逐订单错误、空 `orderID` 或 `UNKNOWN` 状态。
- **mistake_ids：** M0016,M0017,M0018
- **证据：** M0016 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L60-L79，ref REF005；M0017 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L92-L130，ref REF005；M0018 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L30-L38，ref REF012
- **后续防错检查：** 适配器只允许平台订单标识非空、无拒单错误、状态可解释且能对应既定范围的单项进入 `SdkAccepted`。
- **类似工作前必须验证：** 为 `success=true` 但标识为空、`UNKNOWN`、`error_message`、`post-only`、`restricted` 和瞬态错误重试分别准备正反例。

## M11. 审查、文档同步与复审门禁遗漏
- **`count_group`：** `review_docs_sync_and_review_gate`
- **次数：** 3
- **分类：** `review_and_process_failure`
- **典型症状：** 当前规则要求独立验证同事、AI 或审查反馈，不能直接照单全收；文档与状态审查发现 `lastest-command.md`、活跃计划顶部摘要和变更记录历史段落存在陈旧的当前状态措辞；计划明确要求在方案落盘和修复后都派生 GPT-5.5、`xhigh` 的只读子代理进行审查。
- **错误假设：** 只修代码或测试，不同步当前状态文档；完成大步骤后只靠自审推进到下一执行门；把审查输出原样当作事实或修复清单。
- **当前正确规则：** 主代理必须验证反馈的正确性，拒绝错误反馈，并标明无法完全验证部分的边界；复杂或耗时较长的步骤必须按计划执行只读复审，审查发现可执行问题时先修复再继续；实盘路径修改必须同步活跃计划、变更记录和 `lastest-command.md`，审查后修正文档中的陈旧措辞。
- **为什么会复发：** 审查输出、实现状态和交接文档可能彼此漂移；不验证审查结果或不同步文档，会让下一轮使用错误入口。
- **mistake_ids：** M0053,M0054,M0055
- **证据：** M0053 AGENTS.md:L82-L88；M0054 poly-arb-ai/sdk-live-trade-docs/changelog/2026-06-25-platform-sellable-maturity-probe-plan.md:L545-L555；M0055 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L162-L165
- **后续防错检查：** 大步骤按计划执行 GPT-5.5、`xhigh` 的只读复审；实盘路径改动同步活跃计划、变更记录和 `lastest-command.md`。
- **类似工作前必须验证：** 主代理独立判断审查结论，记录采纳或拒绝依据，并复查文档与状态措辞。

## M12. SDK 优先边界与影子实现风险
- **`count_group`：** `sdk_first_boundary_and_shadow_implementation`
- **次数：** 3
- **分类：** `sdk_first_boundary_and_shadow_implementation`
- **典型症状：** 当前规则禁止未经用户同意改用原始 REST、手动签名、自建 SDK 旁路或影子实现；买单重试修复明确不授权扩大钱包、模型或市场范围，也不授权 SDK 后备路径、原始 REST、手动签名或影子实现；产物持久化修复仍明确不引入原始 REST、手动签名或影子实现。
- **错误假设：** 为绕过买单拒绝或平台限制而临时扩大范围或绕过 SDK；平台 SDK 存在能力缺口时自行绕过官方 Rust SDK；把可观测性缺口扩展为交易路径重写。
- **当前正确规则：** 产物冒烟或流式写入只能增强证据，不改变由 SDK 支持的交易语义；重试规则只能在既定范围和 SDK 优先路径内收敛；必须先说明缺口、影响和选项，经用户同意后才能改变 SDK 优先路径。
- **为什么会复发：** 平台能力缺口和实盘压力容易诱发原始 REST、手动签名或平行适配器，但这些做法会绕过已建立的审计和认证边界。
- **mistake_ids：** M0050,M0051,M0052
- **证据：** M0050 AGENTS.md:L35-L39，ref REF001；M0051 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L30-L38，ref REF002；M0052 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L150-L161，ref REF001
- **后续防错检查：** 任何 SDK 能力缺口都先列明影响和可选方案，经用户批准后才能改变路径。
- **类似工作前必须验证：** 代码差异中不得出现原始 REST、手动签名或自建 SDK 旁路；范围不得扩大到其他钱包、模型或市场。

## M13. 校验器或局部产物被当作整体门禁
- **`count_group`：** `validator_or_artifact_gate_overclaim`
- **次数：** 3
- **分类：** `truth_surface_confusion`
- **典型症状：** JSONL、流式产物和摘要中的 `valid_sample_count=30`，但整体 `status=FailClosed`；命令行摘要与门禁报告明确为 `status=FailClosed`，唯一原因是 CSV 字段数不一致；实盘后快照显示 `open_order_count=0`，但 Data API 返回的持仓仍使 `allows_new_orders=false`。
- **错误假设：** 把未结订单清零当作账户和新订单安全完全恢复；把样本 JSONL 与摘要可复核当作正式 CSV 和结构门禁已通过；把局部产物有效当作整体门禁通过。
- **当前正确规则：** CSV、JSONL 和摘要的 `schema`（数据模式）必须全部对齐；最终写入器校验错误仍使门禁保持 `fail-closed`（故障时拒绝）状态；未结订单安全、持仓敞口、成交历史可用性和 `audit_complete` 必须分层判断；正式门禁必须同时满足最终写入器、`schema`（数据模式）和对账的当前边界。
- **为什么会复发：** JSONL 与摘要、未结订单检查或校验器局部通过，都可能与最终写入器、CSV 结构或实盘后对账结果不一致。
- **mistake_ids：** M0008,M0009,M0010
- **证据：** M0008 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L20-L28，ref REF013；M0009 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L53-L95，ref REF013；M0010 poly-arb-ai/sdk-live-trade-docs/plan/2026-06-25-platform-sellable-maturity-probe-plan.md:L109-L144，ref REF009
- **后续防错检查：** 分别记录门禁状态、`valid_sample_count`、写入器状态、实盘后账户状态与对账结果，不用单个数字覆盖整体结论。
- **类似工作前必须验证：** 读取门禁报告、摘要、样本和对账报告的 SHA 与字段，不从终端摘要推断。

## M14. 部署与运行边界混淆
- **`count_group`：** `deployment_runtime_boundary_confusion`
- **次数：** 2
- **分类：** `deployment_and_runtime_boundary`
- **典型症状：** AWS 爱尔兰的 sdk-live-trade 服务器只有约 2 vCPU、1.9 GiB 内存且没有交换空间，不是 Rust 构建机；回归清单明确远端 AWS 实盘服务器只运行二进制文件，不作为 Rust 构建机。
- **错误假设：** 混用生产运行机和开发构建机；把远端运行服务器当作执行 `cargo build`、`cargo test`、`cargo check` 或 `cargo fmt` 的机器。
- **当前正确规则：** 发布版二进制文件必须在本地构建并记录提交、SHA 和路径，再作为运行产物部署到 `/home/ubuntu/sdk-live-trade`；每次实盘运行前确认仅部署二进制文件、运行根目录和产物回收路径，不在服务器编译源码。
- **为什么会复发：** AWS 实盘服务器是低配置运行机，不是构建机；混用源码、二进制文件、模型产物和钱包环境变量文件会破坏可追溯性。
- **mistake_ids：** M0048,M0049
- **证据：** M0048 AGENTS.md:L52-L61；M0049 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L190-L205
- **后续防错检查：** 在本地构建并记录提交、SHA 和路径，仅将二进制文件部署到固定运行根目录；服务器不运行 Cargo。
- **类似工作前必须验证：** 确认工作树修改状态、二进制文件 SHA-256、配置与模型 SHA、运行根目录、回收产物和回滚材料。

## M15. `generic not-found`（通用未找到错误）与 `exact matched-cancel` 混同
- **`count_group`：** `generic_not_found_vs_exact_matched_cancel`
- **次数：** 2
- **分类：** `order_lifecycle_and_cancel_semantics`
- **典型症状：** 早期收敛曾把 `exact matched-cancel` 与 `generic matched-cancel` 并列处理；当前规则显式禁止 `generic not-found`（通用未找到错误）直接入账，或写入 `stop_new_orders`、`no_order`、`cleanup_only` 和顶层 `hard fail-close`（硬失败关闭）状态。
- **错误假设：** 把 `generic already-canceled-or-matched`（通用的已撤销或已匹配响应）当作成交证明；把 `order not found` 类字符串当作统一终态。
- **当前正确规则：** 通用的 `order not found` 只能作为订单局部、可重试的警告，等待已接受的撤单响应、用户通道、CLOB 或 `exact matched-cancel` 证据关闭订单；`generic/plain not-found`（通用或普通未找到错误）只作为订单局部、可重试的警告；`exact matched-cancel` 依赖本地材料裁决。
- **为什么会复发：** 相似错误字符串曾被合并处理，但只有 `exact matched-cancel` 可在本地材料充分时参与成交或终态裁决。
- **mistake_ids：** M0029,M0030
- **证据：** M0029 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L54-L61，ref REF006；M0030 AGENTS.md:L137-L138，ref REF006
- **后续防错检查：** `generic/plain not-found`（通用或普通未找到错误）只能是订单局部、可重试的警告，不得入账，也不得写入 `stop/no-order/cleanup/hard gate`（停止发单、无订单、仅清理或硬门禁）状态。
- **类似工作前必须验证：** 正反例同时覆盖 `exact matched-cancel`、`generic already-canceled-or-matched`（通用的已撤销或已匹配响应）、`plain not-found`（普通未找到错误）和重试范围。

## M16. 产物校验器成为第二套业务解释器
- **`count_group`：** `validator_second_business_interpreter`
- **次数：** 2
- **分类：** `ledger_reconciliation_and_account_authority`
- **典型症状：** 校验器曾重新解释 TTL、`matched-cancel`、`covered-by-synthetic`、CLOB 终态和卖出符号等业务语义；产物校验器曾把 `audit-only/no-ledger-apply`（仅审计、不应用到账本）的重复确认一律视为 `hard duplicate-id`（硬性重复标识错误）。
- **错误假设：** 把运行后校验器建成第二套业务解释器；用数据行重复键代替业务层对影响账本冲突的判定。
- **当前正确规则：** 校验器只验证运行时写出的归一化数据行、原始数据摘要哈希（`raw digest`）、引用完整性和产物一致性；校验器必须区分 `audit-only duplicate`（仅审计重复）、`ledger-impacting delta`（影响账本的增量）和不可解释冲突。
- **为什么会复发：** 如果校验器重建 TTL、`matched-cancel`、`covered-by-synthetic` 或卖出符号语义，就会与运行时和生命周期逻辑分叉。
- **mistake_ids：** M0038,M0039
- **证据：** M0038 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L129-L144，ref REF007；M0039 poly-arb-ai/sdk-live-trade-docs/plan/2nd-moneyrun-bugs-fix.md:L190-L201，ref REF007
- **后续防错检查：** 校验器只验证运行时已写出的归一化数据行、摘要哈希（`digest`）、引用完整性，以及方向、符号和预留（`side/sign/reservation`）契约。
- **类似工作前必须验证：** 禁止按来源定制的重建器重新做业务裁决；旧辅助程序只能用于有界一致性扫描。

## M17. 启动元数据的原始值比较缺少归一化
- **`count_group`：** `startup_metadata_and_tick_normalization`
- **次数：** 1
- **分类：** `platform_rule_or_hidden_rule_misunderstanding`
- **典型症状：** M1 启动时出现 `TickSizeMismatch`，原因是 Gamma 与 CLOB 的十进制字符串格式不同，而产物没有同时保留原始值和归一化值证据。
- **错误假设：** 按原始字符串比较 `tick size`（最小价格变动单位）和 `min order size`（最小订单量）。
- **当前正确规则：** 启动上下文应先按十进制数归一化再比较，同时写出 `raw value`（原始值）与 `normalized value`（归一化值）。
- **为什么会复发：** Gamma 与 CLOB 的原始十进制字符串格式不同但数值等价，缺少 `raw value`（原始值）与 `normalized value`（归一化值）证据会增加定位成本。
- **mistake_ids：** M0044
- **证据：** M0044 poly-arb-ai/sdk-live-trade-docs/changelog/反复问题汇总.md:L116-L127，ref REF023
- **后续防错检查：** `tick size`（最小价格变动单位）、`min order size`（最小订单量）等启动元数据同时保留 `raw value`（原始值）和归一化后的十进制比较结果。
- **类似工作前必须验证：** 产物中能复核两侧的 `raw value`（原始值）与 `normalized value`（归一化值）；真实不一致仍须 `fail-closed`（故障时拒绝）。

## 平台与产物引用索引

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

- 先确认活跃计划与变更记录、`lastest-command.md` 和 `AGENTS.md` 的当前指针一致，再使用归档文档。
- 分别记录检查点、阶段、整个计划、正式门禁、`no-live`（不提交真实订单）、产物冒烟和标准输出进度的结论。
- 涉及 Polymarket 语义前，先查本地官方文档镜像以及 API 和 AsyncAPI 规范，并写入引用标识。
- `post_orders` 的接受判定只认可非空平台订单标识、无拒单错误、状态可解释且能对应既定范围的响应。
- 撤单、TTL、用户通道、CLOB 和合成证据必须先归一化，再进入生命周期、账本和校验器。
- Data API、CLOB 和平台快照只按计划限定的启动、恢复、对账和审计用途使用，不覆盖热路径账本。
- 写入器或 `schema`（数据模式）改动必须覆盖规范写入器、流式取证、失败轨迹、有界审计行，以及新建且不覆盖既有文件的约束。
- `warning`、`audit-only`、`cleanup-only` 和 `hard_fail_closed` 必须使用结构化严重性级别，不得依据字符串原因直接升级。
- 模型输入改动必须校验 `feature_order`、形状、`alpha` 顺序、`account_state` 顺序，以及应用生命周期证据后的刷新行为。
- 部署前确认本地构建结果、二进制文件 SHA、配置与模型 SHA、运行根目录和回收产物；远端实盘服务器不运行 Cargo。
- 先独立验证审查反馈；复杂阶段按计划派生 GPT-5.5、`xhigh` 的只读复审，并同步修正文档和状态中的陈旧措辞。

## 附录：结构化产物

- `源清单.tsv`：159 条源文件记录，覆盖活跃、辅助、历史和归档来源，对应类别为 `active/supporting/historical/archive`。
- `git历史清单.tsv`：1341 条 Git 历史记录。
- `引用证据清单.tsv`：23 条官方文档、API 规范、AsyncAPI、产物报告或清单引用。
- `错误实例台账.jsonl`：55 条错误实例记录。
- `错误族统计.tsv`：17 个错误族聚合。
