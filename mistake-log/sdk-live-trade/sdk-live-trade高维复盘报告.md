# sdk-live-trade 高维复盘报告

生成日期：2026-06-28（Asia/Tokyo）。

本报告写于完整读取以下产物之后：`源清单.tsv`、`git历史清单.tsv`、`引用证据清单.tsv`、`错误实例台账.jsonl`、`错误族统计.tsv`、`sdk-live-trade错误记录汇总.md`。它是复盘材料，不是实盘执行授权，也不替代 `/home/chillchill/sdk-live-trade` 的 active plan/changelog。

## 输入完整性

- `源清单.tsv`：159 条源记录，覆盖 active/supporting/historical/archive 文档。
- `git历史清单.tsv`：1341 条 git 历史记录，用于防止只看当前正文而漏掉修复周期。
- `引用证据清单.tsv`：23 条官方文档、API spec、AsyncAPI 和 artifact 引用。
- `错误实例台账.jsonl`：55 条错误实例，分为 17 个 count_group。
- `错误族统计.tsv`：17 个错误族，全部在主汇总文档有明细块和计数校验表。
- `sdk-live-trade错误记录汇总.md`：主汇总文档，包含基线、统计口径、错误族明细和开发前检查清单。

## 高维结论

1. 最大问题不是单个 Polymarket API 细节，而是“权威层级”长期混杂：active plan 与历史停点、no-live 与 live-funds、local ledger 与平台 snapshot、runtime 决策与 validator 复核、artifact stream 与 final writer 都曾互相替代。
2. 订单事件语义是复发核心。`order_lifecycle_and_cancel_semantics` 占 11 条，最高频错误族是 `cancel_ttl_matched_cancel_normalization`。真实资金路径里，SDK response、user-channel、CLOB evidence 和 synthetic evidence 必须先归一化，不能由入口各自解释。
3. 平台规则误解通常不是文档没读，而是把 transport 成功、per-order success、平台可卖成熟、Data API positions 和交易热路径账户状态放在同一层解释。POST /orders accepted、SELL reject、tick metadata 都需要官方文档与本地 artifact 双证据。
4. 证据系统必须先于结论。`artifact_audit_schema_and_persistence` 与 `truth_surface_confusion` 合计 12 条，说明 stdout、局部 JSONL、smoke、validator 或 open-order check 都不能单独证明正式 gate 通过。
5. 复审和文档同步不是形式。多次修复的剩余风险来自 stale current-status wording、review 反馈未验证或 plan/changelog/lastest-command 未同步，最终会让下一轮用错执行入口。

## 错误结构

| 分类 | 条数 | 解释 |
| --- | ---: | --- |
| `order_lifecycle_and_cancel_semantics` | 11 | 真实订单事件异步交错，必须以 normalized event 合同收敛。 |
| `platform_rule_or_hidden_rule_misunderstanding` | 8 | 官方 API/spec 与平台行为需要逐项绑定到 adapter 和 gate。 |
| `truth_surface_confusion` | 7 | 局部通过、stdout、smoke 或 no-live 被过度外推。 |
| `ledger_reconciliation_and_account_authority` | 6 | 本地账本、平台 snapshot、Data API、CLOB query 的权威边界混杂。 |
| `artifact_audit_schema_and_persistence` | 5 | schema、writer、stream、fail trace 和 audit row 的证据完整性不足。 |
| `risk_operator_and_fail_closed_severity` | 4 | warning/audit-only/cleanup-only 与 hard gate 分层不足。 |
| `plan_authority_and_cursor` | 3 | active authority、历史停点和 checkpoint 状态混用。 |
| `model_contract_and_strategy_semantics` | 3 | 模型输入契约、account_state 与 lifecycle 时序混用。 |
| `sdk_first_boundary_and_shadow_implementation` | 3 | 实盘压力下需要保持 SDK-first，不引入未授权旁路。 |
| `review_and_process_failure` | 3 | review 结论、文档状态和执行门禁需要独立验证。 |
| `deployment_and_runtime_boundary` | 2 | 本地 build 与远端 runtime、binary/model/wallet/artifact 边界必须分清。 |

## 复发机制

- **表面成功过早外推。** valid sample、no-live、smoke、stdout、open-order check、局部 validator 都是单层证据。只要缺 gate report、fail trace、post-live account/reconciliation 或 final schema，就不能升级为正式成功。
- **入口驱动而不是事件合同驱动。** SDK actor、TTL、model cancel、final cancel-all、user-channel、CLOB query 曾各自解释订单状态。正确模型是所有入口先写 normalized event，再让 lifecycle/ledger/risk/validator 消费同一个 typed contract。
- **平台账户视图与本地交易账本混用。** Data API positions/value、platform sellable balance 和 CLOB snapshots 是审计与 reconciliation 输入，不是下一 tick account_state 的覆盖源。
- **validator 越权。** validator 一旦重新解释业务语义，就会与 runtime 分叉；它只能验证 runtime 写出的 normalized fact、digest、reference 与 artifact consistency。
- **文档状态漂移。** 历史计划保留执行光标，review 修复后若 lastest-command 和 active plan/changelog 不同步，下一轮容易复用过期边界。

## 防错模型

1. **权威定位层。** 先确定 active plan/changelog、lastest-command、HEAD、dirty state、当前禁令，再读取归档证据。
2. **平台语义层。** 对任何 Polymarket 规则，用本地官方 docs/api-spec/asyncapi 的 reference_id 绑定实现或复盘结论。
3. **事件归一化层。** 所有订单 evidence 写入 NormalizedOrderEvent，保留 raw digest/reference，避免入口专属 helper 绕过裁决。
4. **账本权威层。** 热路径只消费 local lifecycle/ledger/current orderbook/SDK-user-channel evidence；平台 snapshot 只在限定阶段参与 reconciliation。
5. **证据持久化层。** streaming forensic 保障运行中证据，canonical writer 保障正式 schema；writer failure 必须留下 gate report/fail trace。
6. **severity 层。** 每个 event 带 runtime_gate_policy；warning、audit-only、cleanup-only、hard fail-closed 不能靠字符串混判。
7. **review 层。** 主 agent 对 review 结论独立验证，复杂阶段按计划做 GPT-5.5/xhigh 只读复审，修复后再更新执行入口文档。

## Top 错误族优先级

| 优先级 | count_group | 次数 | 处理重点 |
| --- | --- | ---: | --- |
| 1 | `cancel_ttl_matched_cancel_normalization` | 6 | 优先保持 normalized event、TTL ack、matched-cancel、venue dust、batch cancel 的正负例。 |
| 2 | `artifact_writer_schema_and_persistence_failure` | 5 | 优先验证 final writer failure 后仍有 gate report、fail trace 和 stream evidence。 |
| 3 | `fail_close_severity_too_broad` | 4 | 优先检查 warning/audit-only/cleanup-only/hard gate 是否被字符串 reason 混判。 |
| 4 | `ledger_vs_snapshot_authority` | 4 | 优先检查 Data API/CLOB/platform snapshot 是否越权覆盖 local ledger 或 account_state。 |
| 5 | `no_live_smoke_or_stdout_overclaim` | 4 | 优先在结论中标明证据覆盖层级，防止把 smoke/no-live/stdout 外推为 live success。 |
| 6 | `platform_sellable_balance_maturity_and_sell_reject` | 4 | 优先检查 SELL reject 只影响该笔 submit，不生成跨 tick suppression/state。 |

## 后续 sdk-live-trade 开发前必须检查

- active plan/changelog 与 lastest-command 是否都指向同一当前执行边界。
- 本轮结论是否明确区分 checkpoint、phase、whole-plan、formal gate、no-live、artifact smoke、stdout。
- 涉及平台行为的判断是否有 `引用证据清单.tsv` 中的官方 docs/spec/AsyncAPI reference_id。
- POST /orders accepted 判定是否同时要求非空 order id、无拒单错误、状态可解释和 scope 可对应。
- TTL/model/final cancel、user-channel、CLOB、synthetic evidence 是否都先进入 normalized event。
- generic not-found 是否保持 order-local retryable warning，未进入 ledger、stop_new_orders、no_order 或 hard gate。
- local ledger 是否仍是热路径 account_state/NAV 权威，Data API/CLOB/platform snapshot 是否仅作限定用途。
- artifact writer 是否覆盖 canonical schema、stream forensic、fail trace、bounded audit row 和 create-new/no-overwrite。
- severity 是否按 runtime_gate_policy 分层，而不是靠 error string 或 validator string 升级。
- 模型输入是否校验 feature_order、shape、alpha 顺序、account_state 顺序，并在 lifecycle apply 后 refresh readiness。
- 部署是否保持本地 build、binary-only remote runtime、模型 artifact 与钱包 env 分离，并记录 sha256。
- review 反馈是否由主 agent 验证后采纳或拒绝，复杂阶段是否完成 GPT-5.5/xhigh 只读复审。

## 最小落地建议

- 把 `错误实例台账.jsonl` 中的 `count_group` 作为后续 review checklist 的稳定 key，不在每次事故后重命名。
- 新增或修改 live path 前，至少选择相关 Top 6 错误族做 targeted negative/positive test 或 artifact replay。
- 对每个真实资金 run 的结论使用固定模板：授权边界、binary/config/model sha、SDK post/cancel/fill/cancel_ack、gate status、writer status、open-order status、account/reconciliation status、下一步禁令。
- 任何“通过”都要写清楚通过的证据层；任何“不能继续”都要写清楚阻断的 gate、artifact path 和 sha。
