# sdk-live-trade 高维复盘报告

生成日期：2026-06-28（亚洲/东京时区，`Asia/Tokyo`）。

本报告写于完整读取以下产物之后：`源清单.tsv`、`git历史清单.tsv`、`引用证据清单.tsv`、`错误实例台账.jsonl`、`错误族统计.tsv`、`sdk-live-trade错误记录汇总.md`。它是复盘材料，不是实盘执行授权，也不替代 `/home/chillchill/sdk-live-trade` 的活跃计划与变更记录。

## 输入完整性

- `源清单.tsv`：159 条源记录，覆盖活跃、辅助、历史和归档文档，对应类别为 `active/supporting/historical/archive`。
- `git历史清单.tsv`：1341 条 Git 历史记录，用于防止只看当前正文而漏掉修复周期。
- `引用证据清单.tsv`：23 条官方文档、API 规范、AsyncAPI 和产物引用。
- `错误实例台账.jsonl`：55 条错误实例，分为 17 个 `count_group`。
- `错误族统计.tsv`：17 个错误族，全部在主汇总文档有明细块和计数校验表。
- `sdk-live-trade错误记录汇总.md`：主汇总文档，包含基线、统计口径、错误族明细和开发前检查清单。

## 高维结论

1. 最大问题不是单个 Polymarket API 细节，而是“权威层级”长期混杂：活跃计划与历史停点、`no-live`（不提交真实订单的演练）与真实资金运行（`live-funds`）、本地账本与平台快照、运行时决策与校验器复核、流式产物与最终写入器都曾互相替代。
2. 订单事件语义是复发核心。`order_lifecycle_and_cancel_semantics` 占 11 条，最高频错误族是 `cancel_ttl_matched_cancel_normalization`。在真实资金路径中，SDK 响应、用户通道、CLOB 证据和合成证据必须先归一化，不能由各入口分别解释。
3. 平台规则误解通常不是因为没读文档，而是把传输成功、逐订单响应成功、平台可卖余额成熟、Data API 持仓和交易热路径账户状态放在同一层解释。`POST /orders` 的接受判定、卖单拒绝和 `tick metadata`（最小价格变动元数据）都需要官方文档与本地产物双重证据。
4. 证据系统必须先于结论。`artifact_audit_schema_and_persistence` 与 `truth_surface_confusion` 合计 12 条，说明标准输出、局部 JSONL、冒烟结果、校验器或未结订单检查（`open-order check`）都不能单独证明正式门禁通过。
5. 复审和文档同步不是形式。多次修复后的剩余风险来自陈旧的当前状态措辞、未经验证的审查反馈，或计划、变更记录和 `lastest-command.md` 未同步；这些问题最终会让下一轮使用错误的执行入口。

## 错误结构

| 分类 | 条数 | 解释 |
| --- | ---: | --- |
| `order_lifecycle_and_cancel_semantics` | 11 | 真实订单事件异步交错，必须以归一化事件合同收敛。 |
| `platform_rule_or_hidden_rule_misunderstanding` | 8 | 官方 API 规范与平台行为需要逐项绑定到适配器和门禁。 |
| `truth_surface_confusion` | 7 | 局部通过、标准输出、冒烟结果或 `no-live` 状态被过度外推。 |
| `ledger_reconciliation_and_account_authority` | 6 | 本地账本、平台快照、Data API 和 CLOB 查询的权威边界混杂。 |
| `artifact_audit_schema_and_persistence` | 5 | `schema`（数据模式）、写入器、流式证据、失败轨迹和审计行的证据完整性不足。 |
| `risk_operator_and_fail_closed_severity` | 4 | `warning`（警告）、`audit-only`（仅审计）、`cleanup-only`（仅清理）与 `hard gate`（硬门禁）的分层不足。 |
| `plan_authority_and_cursor` | 3 | 活跃权威文档、历史停点和检查点状态混用。 |
| `model_contract_and_strategy_semantics` | 3 | 模型输入契约、`account_state` 与生命周期时序混用。 |
| `sdk_first_boundary_and_shadow_implementation` | 3 | 实盘压力下需要保持 SDK 优先，不引入未授权旁路。 |
| `review_and_process_failure` | 3 | 审查结论、文档状态和执行门禁需要独立验证。 |
| `deployment_and_runtime_boundary` | 2 | 本地构建与远端运行，以及二进制文件、模型、钱包和产物边界必须分清。 |

## 复发机制

- **表面成功被过早外推。** 有效样本、`no-live`、冒烟结果、标准输出、未结订单检查（`open-order check`）和局部校验器结果都只是单层证据。只要缺少门禁报告、失败轨迹、实盘后账户与对账结果或最终 `schema`（数据模式），就不能升级为正式成功。
- **由入口驱动，而不是由事件合同驱动。** `SDK actor`（SDK 异步 actor）、TTL、`model cancel`（模型撤单）、`final cancel-all`（最终全量撤单）、用户通道和 CLOB 查询曾分别解释订单状态。正确模型是所有入口先写入 `normalized event`（归一化事件），再让生命周期、账本、风险和校验器消费同一个类型化事件合同（`typed contract`）。
- **平台账户视图与本地交易账本混用。** Data API 的持仓与价值、平台可卖余额和 CLOB 快照是审计与对账输入，不是下一决策周期 `account_state` 的覆盖来源。
- **校验器越权。** 校验器一旦重新解释业务语义，就会与运行时分叉；它只能验证运行时写出的归一化事实、摘要哈希（`digest`）、引用和产物一致性。
- **文档状态漂移。** 历史计划保留执行光标；审查修复后，如果 `lastest-command.md` 与活跃计划和变更记录不同步，下一轮容易复用过期边界。

## 防错模型

1. **权威定位层。** 先确定活跃计划与变更记录、`lastest-command.md`、`HEAD`、工作树修改状态和当前禁令，再读取归档证据。
2. **平台语义层。** 对任何 Polymarket 规则，用本地官方文档、API 规范和 AsyncAPI 的 `reference_id` 绑定实现或复盘结论。
3. **事件归一化层。** 所有订单证据写入 `NormalizedOrderEvent`，保留 `raw digest/reference`（原始数据摘要哈希与引用），避免入口专属辅助程序绕过裁决。
4. **账本权威层。** 热路径只消费本地生命周期、账本、当前订单簿以及 SDK 用户通道（`SDK-user-channel`）证据；平台快照只在限定阶段参与对账。
5. **证据持久化层。** 流式取证保障运行中证据，规范写入器保障正式 `schema`（数据模式）；写入器失败时必须留下门禁报告和失败轨迹。
6. **严重性分层。** 每个事件携带 `runtime_gate_policy`；`warning`（警告）、`audit-only`（仅审计）、`cleanup-only`（仅清理）和 `hard fail-closed`（硬失败关闭）不能依靠字符串混判。
7. **审查层。** 主代理独立验证审查结论，复杂阶段按计划执行 GPT-5.5、`xhigh` 的只读复审，修复后再更新执行入口文档。

## 最高优先级错误族

| 优先级 | `count_group` | 次数 | 处理重点 |
| --- | --- | ---: | --- |
| 1 | `cancel_ttl_matched_cancel_normalization` | 6 | 优先保留 `normalized event`（归一化事件）、TTL `ack`（确认）、`matched-cancel`（撤单时订单已撮合的证据分类）、平台尘埃量（`venue dust`）和 `batch cancel`（批量撤单）的正反例。 |
| 2 | `artifact_writer_schema_and_persistence_failure` | 5 | 优先验证最终写入器失败后仍有门禁报告、失败轨迹和流式证据。 |
| 3 | `fail_close_severity_too_broad` | 4 | 优先检查 `warning`（警告）、`audit-only`（仅审计）、`cleanup-only`（仅清理）和 `hard gate`（硬门禁）是否被字符串原因混判。 |
| 4 | `ledger_vs_snapshot_authority` | 4 | 优先检查 Data API、CLOB 或平台快照是否越权覆盖本地账本或 `account_state`。 |
| 5 | `no_live_smoke_or_stdout_overclaim` | 4 | 优先在结论中标明证据覆盖层级，防止把冒烟结果、`no-live` 状态或标准输出外推为实盘成功。 |
| 6 | `platform_sellable_balance_maturity_and_sell_reject` | 4 | 优先检查卖单拒绝只影响该笔提交，不得形成跨后续决策周期持续生效的 `suppression/state`（抑制状态）。 |

## 后续进行 sdk-live-trade 开发前必须检查

- 活跃计划与变更记录和 `lastest-command.md` 是否都指向同一当前执行边界。
- 本轮结论是否明确区分检查点、阶段、整个计划、正式门禁、`no-live`（不提交真实订单）、产物冒烟和标准输出。
- 涉及平台行为的判断是否有 `引用证据清单.tsv` 中官方文档、规范或 AsyncAPI 的 `reference_id`。
- `POST /orders` 的接受判定是否同时要求订单标识非空、无拒单错误、状态可解释且能对应既定范围。
- TTL、模型撤单、最终撤单、用户通道、CLOB 和合成证据是否都先进入 `normalized event`（归一化事件）。
- `generic not-found`（通用未找到错误）是否保持为 `order-local retryable warning`（订单局部、可重试的警告），且未进入账本、`stop_new_orders`、`no_order` 或 `hard gate`（硬门禁）。
- 本地账本是否仍是热路径 `account_state` 与 NAV 的权威来源，Data API、CLOB 和平台快照是否仅用于限定用途。
- 产物写入器是否覆盖规范 `schema`（数据模式）、流式取证、失败轨迹、有界审计行，以及新建且不覆盖既有文件的约束。
- 严重性是否按 `runtime_gate_policy` 分层，而不是依据错误字符串或校验器字符串升级。
- 模型输入是否校验 `feature_order`、形状、`alpha` 顺序和 `account_state` 顺序，并在应用生命周期证据后刷新就绪状态。
- 部署是否保持本地构建、远端运行机仅部署二进制文件、模型产物与钱包环境变量文件分离，并记录 SHA-256。
- 审查反馈是否由主代理验证后采纳或拒绝，复杂阶段是否完成 GPT-5.5、`xhigh` 的只读复审。

## 最小落地建议

- 把 `错误实例台账.jsonl` 中的 `count_group` 作为后续审查检查清单的稳定键，不在每次事故后重命名。
- 新增或修改实盘路径前，至少选择相关的前 6 个错误族执行针对性的正反例测试或产物回放。
- 对每次真实资金运行的结论使用固定模板：授权边界，`binary/config/model sha`（二进制文件、配置和模型的 SHA），`SDK post/cancel/fill/cancel_ack`（SDK 提交、撤单、成交与撤单确认），`gate status`（门禁状态），`writer status`（写入器状态），`open-order status`（未结订单状态），`account/reconciliation status`（账户与对账状态），以及下一步禁令。
- 任何“通过”都要写清楚通过的证据层；任何“不能继续”都要写清楚造成阻断的 `gate`（门禁）、`artifact path`（产物路径）和 `sha`（摘要哈希）。
