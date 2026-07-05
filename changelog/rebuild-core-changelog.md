# rebuild-core changelog

> 对应总需求文档：`/home/chillchill/btc-updown-marketmaker/demand/rebuild-core-demand.md`

> 对应总计划文档：`/home/chillchill/btc-updown-marketmaker/plan/rebuild-core-plan.md`

## 2026-07-03

### 初始总计划

- 根据 `demand/rebuild-core-demand.md` 和 `mistake-log/2026-07-03-重构前错误汇总与优先级.md` 创建总计划 `plan/rebuild-core-plan.md`。
- 总计划定位为宏观架构、阶段、gate 和后续模块拆分顺序，不授权代码执行、部署、paper、small-live、正式 live 或真实资金操作。
- 总计划明确历史采集数据处理先于新的“数据采集 -> 数据处理”工作流，避免绕过三四个月历史数据。

### 调研依据

- 只读参考本地旧路径：`/home/chillchill/sdk-live-trade`、`/home/chillchill/live-trade`、`/home/chillchill/2-paper-trade`、`/home/chillchill/V2`、`/home/chillchill/2-model-train`、`/home/chillchill/poly-arb-2`。
- 只读参考官方资料：Polymarket API/Trading/Auth/WebSocket/Rate Limits/Matching Engine、Polymarket Rust SDK、Binance Spot WebSocket/REST/rate limits。
- 已按用户决策删除非官方参考项目清单；核心需求和总计划只保留官方资料作为协议、SDK、API 和平台规则约束，不保留外部社区项目、开源交易框架或外部 bot repo 作为参考来源。

### 审查修复

- 子代理需求覆盖审查：PASS，无修复项。
- 子代理 mistake-log 覆盖审查提出 verdict、路径、5090 validator、active market 输入、`asset_ids` miss 和 `market_resolved` 覆盖问题；已在总计划中修复。
- 子代理架构审查提出路径表、gate 清单、artifact contract 适用范围和 Phase 1/模块 1 边界问题；已在总计划中修复。
- 子代理路径/授权/部署审查发现 `AGENTS.md` 仍残留旧路径口径；已同步为当前口径。
- 本地一致性检查发现总需求 verdict 仍残留旧口径；已同步为 `PASS / WARNING / NO_SUBMIT / CANCEL_ONLY / FAIL_CLOSED`，并保留 coverage incomplete / quarantine 作为原因标签和执行权限动作。

### 当前路径口径

- 当前文档落盘仓库：`/home/chillchill/btc-updown-marketmaker`。
- 后续开发、代码编写与本地构建路径：`/home/chillchill/btc-updown-marketmaker`。
- 各远端服务器源码/工作目录统一写作 `~/btc-updown-marketmaker`，不得把服务器上 `/home/ubuntu/btc-updown-marketmaker` 误作本地路径。
- 交易服务器上的训练模型产物与交易程序二进制文件目录：`/home/ubuntu/live-trade`。
- wallet 文件使用 `./wallets/.env.wallet1`；本地源路径为 `/home/chillchill/btc-updown-marketmaker/wallets/.env.wallet1`，`./wallets` 作为 gitignored 钱包 secret/env 部署材料上传或复制到交易服务器，远端实际落点必须由部署 receipt 记录且不得通过 Git 同步。
- 这些路径口径必须在后续模块需求、计划和 changelog 中保持一致；若 AGENTS、demand、plan、changelog 出现不一致，先停下来同步，不进入代码开发。

### 模型决策与 live-trade 桥梁边界修复

- 将“交易决策和风控完全由模型实现，live-trade 只是模型与平台之间的桥梁”提升为核心需求和总计划边界。
- 在总需求中补充 ModelIntentBridgeGate、模型 trading/risk intent、平台反馈回传和禁止 live-trade 改写模型交易/风控 intent 的验收标准。
- 在总计划中调整宏观架构为 `Model Runtime -> Model Intent Stream -> Permission / Hard-Boundary Gate -> Intent Bridge / Execution Adapter -> Platform Feedback Normalizer`。
- 新增 `model-intent-bridge` 模块拆分项，放在执行链路和 live 部署模块之前，防止后续模块内置第二套策略或风控。
- 明确硬边界 gate、operator、ledger 和 execution adapter 只能阻断、降级、撤单、回滚或回传原因，不得生成替代交易决策。
- 根据子代理审查收紧语义：operator cancel 只能是用户/操作员安全审计动作；gate 的 NO_SUBMIT/CANCEL_ONLY 只能是权限/硬边界 verdict，不能作为 live-trade 生成的策略性动作；ModelIntentBridgeGate 核心边界不允许 WARNING 豁免。

## 2026-07-05

### ledger、CASH、NAV 与 PnL 口径澄清

- 根据用户审查，确认总需求中 `ledger`、`CASH`、`NAV`、`PnL` 和 `wallet allowance` 口径容易混淆。
- 在总需求中明确：未限定的 `PnL` 指程序启动后的跨市场累计 PnL；单笔交易 PnL 和单个市场 PnL 必须显式标为 per-order 或 per-market。
- 在总需求中明确：`CASH` 指当前可用于交易的余额，必须由本地热路径 ledger 在决策时刻派生；不得把 `wallet allowance` 当作 `CASH`、`NAV` 或 `PnL` 字段。
- 在总需求中明确：`NAV` 是 ledger 在决策时刻计算的账户级净值估值，不是 PnL，也不是单笔订单或单个市场的独立状态源。
- 同步总计划中的账本权威分层、模型输入覆盖、Phase 4 验证和 small-live 检查项，将 `wallet allowance` 账户字段改为 `CASH`，并把 SDK allowance 限定为 approval/preflight 证据。

### NAV 保留与 PnL 派生口径二次修正

- 根据用户继续审查，确认 `NAV` 应保留为 ledger 派生的账户级净值状态，`PnL` 不应与 `NAV` 并列为独立账本权威状态。
- 在总需求中收紧为：未限定的 `PnL` 指由当前 `NAV`、程序启动 baseline NAV 和运行期间外部资金流调整项派生的程序启动后跨市场累计 PnL；只有启动 baseline 仅含 CASH 且无历史持仓、历史挂单、外部充值/提现时，才可简化为 `current NAV - startup CASH`。
- 同步总计划中的账本权威分层、Phase 4 验证和训练 gate，避免把 `PnL` 写成与 `NAV` 并列的权威状态。
