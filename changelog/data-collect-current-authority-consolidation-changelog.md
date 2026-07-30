# data-collect 当前权威与历史隔离整理 Changelog

> 事项编号：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 对应需求：`demand/data-collect-current-authority-consolidation-demand.md`
>
> 对应计划：`plan/data-collect-current-authority-consolidation-plan.md`
>
> 状态：`PHASE_2_COMPLETE_PHASE_3_PENDING`

## 2026-07-30：用户批准范围与执行顺序

- 用户确认接受“当前权威层 + 隔离历史层 + 权威校验门”的文档治理方向。
- 用户明确要求先关闭 `ROLLOVER_PENDING`：2026-07-28 事故事项已经验收，UTC day
  rollover 和完整健康 UTC 日不再作为完成阻断门。
- 该决策只移除阻断门，不把尚未记录的 rollover/full-day 运行证据改写为已经发生。
- 用户决定 paper/live 真实外部市场源 adapter 延后到训练模块完成后再返回实施；当前
  heartbeat/status 能力保持不变，该延期不阻断本轮 `data-collect` 收尾。
- 第二步必须全面审查所有 `data-collect` 相关文档，以当前代码、测试和最新决策修正
  上下文冲突；历史事实保留，但旧状态不能继续冒充当前 authority。
- 上述两步完成并分别通过独立阶段审查后，才实施当前权威层、Git 历史冻结、活动搜索
  面隔离和自动一致性检查。

## 2026-07-30：阶段 1 实施与自审

- 2026-07-28 事故三件套顶部状态统一为
  `CLOSED_BY_USER_ACCEPTANCE_20260730`。
- 事故需求取消 UTC day rollover 与完整健康 UTC 日的 completion gate；事故计划把
  6.3/6.4 标记为由用户移除的原计划任务；事故 changelog 保留当时 pending 事实，并
  追加用户最终验收章节。
- 主需求、主计划、主 changelog、README、acceptance matrix 和 paper/live 接入说明
  已同步 `DEFERRED_UNTIL_TRAINING_MODULE_COMPLETE` 排期边界。
- 未修改 Rust、Cargo、schema、测试或远端运行状态。
- `git diff --check` 通过。
- 本地合同测试通过：
  - `deploy_readme_contract`：5 passed；
  - `hot_path_handoff_contract`：16 passed；
  - `paper_live_boundary_contract`：4 passed。
- 自审确认没有把未取得的 rollover/full-day 证据写成已发生，也没有改变 UTC fence
  实现合同或 future paper/live 公共技术合同。当前等待独立阶段审查。

## 2026-07-30：阶段 1 首轮独立审查修复

- `gpt-5.6-sol/ultra` 首轮独立审查结论为 `FAIL`，发现一个 Important：
  事故 changelog 更早的历史检查点仍有无限定的“待执行、下一步、当前仍须”语气，
  单独切片时可能继续冒充当前游标。
- 审查另指出一项职责措辞风险：主 changelog 把 paper/live runtime 自有的
  runtime receive、消费背压和 append-only log 混写为 future `data-collect`
  adapter 必须继承的合同。
- 已做最小修复：
  - 对所有被指出的旧检查点增加“截至当时、历史、后续已完成或已取代”限定；
  - 把 future `data-collect` adapter 的 handoff/共同 RTDS 合同，与 paper/live
    runtime 自有的 receive、背压处理和 append-only log 责任分开。
- 未改变任何历史时间、运行数据、SHA、测试结果或代码语义。当前等待同一审查者复审。

## 2026-07-30：阶段 1 独立复审通过

- 同一 `gpt-5.6-sol/ultra` 审查者复核最新 diff，结论为 `PASS`：
  Critical 0、Important 0、Minor 0。
- 审查确认事故三件套、历史检查点限定、paper/live 延期和职责边界均与用户决策及
  当前代码能力一致。
- 审查复跑 `deploy_readme_contract`、`hot_path_handoff_contract` 和
  `paper_live_boundary_contract`，共 25 项全部通过；该结果只覆盖阶段 1 文档边界，
  不冒充 workspace 全量测试通过。
- 阶段 1 完成，创建并 push 本阶段 commit 后进入阶段 2。

## 2026-07-30：阶段 2 全量文档审计与修正

- 阶段开始时审计范围为 `60` 份相关 Markdown：data-collect 主文档与本次治理
  triplet `53` 份，直接约束或提供历史输入的关联文档 `7` 份；新增本审计报告后，
  阶段 2 工作树范围为 `61` 份。
- 最终处置为 current/active `12` 份、historical `45` 份、evidence-only input
  `4` 份；逐文件映射和最终状态记录在
  `data-collect/docs/data-collect-document-authority-audit-20260730.md`。
- `43` 份旧三件套、旧独立计划和阶段报告已增加
  `HISTORICAL_EVIDENCE_ONLY / authority none` 首屏；额外发现并把
  `推荐开发顺序.md` 与 `5分钟持续加载市场数据的硬件消耗预估.md` 作为第 `44/45`
  份历史文件；旧 `tofix.md`、冷路径 blocker、SELL/cadence 和部署估算不再产生
  当前游标。
- 主 demand/plan/changelog 已增加 current 关闭摘要；旧 Attempt6 不再是游标，
  current schema 固定为 orderbook v3、metadata v5、depth snapshot v3、depth
  update v4、aggTrade v7 和 RTDS v4。
- 主 plan Phase 8 整体及危险子任务已就地标明历史、禁止重新执行；3 秒 RTDS、
  自动 retention/delete、retention admin、metadata v4、release alias 和远端删除
  指令均不再拥有 authority。
- `demand/rebuild-core-demand.md` 仍是审核中的跨模块草案，未整份历史化；只修正
  data-collect 当前语义为 aggTrade v7、training RTDS 300s/60s 窄例外、当前只有
  training 真实 source，paper/live 真实 adapter 为训练完成后的 future 工作。
- training 运维说明改为 current master triplet；旧部署只作历史证据。
- paper/live 文档明确 future data-collect adapter 与 paper/live runtime 的职责：
  前者负责真实 source、handoff sink 和共同 RTDS，后者负责本机 receive、consumer
  backpressure 和自身 append-only log。
- acceptance matrix 已完整重写为 current-only 验收合同，不再混入旧 Phase、旧 SDK
  gap、Attempt 或远端长跑游标。
- 未修改 Rust、Cargo、schema、远端服务或数据。

## 2026-07-30：阶段 2 测试限制

- 当前 lib 回归在精确跳过
  `four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second`
  后通过 `386 passed / 0 failed / 1 ignored / 1 filtered`。
- 13 个 SDK、schema、binary、部署和 manifest integration targets 合计
  `134 passed / 0 failed`。
- 被跳过的 named test 把运行当日 12:00 UTC 固定为 receive time；UTC 12:00 前会被
  admission clock 判为 future 并超时。这是已知 wall-clock fixture 问题，不冒充
  production runtime 失败，也不得把排除后的结果写成无条件 full-suite PASS。
- `git diff --check` 通过后进入独立阶段审查；审查结论尚未记录。

## 2026-07-30：阶段 2 自审与测试兼容修复

- 首轮 13-target integration run 在
  `training_deploy_readme_describes_single_binary_single_unit_flow` 失败：当前入口已
  切到 master triplet，但既有 contract test 仍要求旧 direct-deployment 三件套路径
  可追溯。
- 已把三条旧路径补回部署 README 的“历史部署来源”，同时明确它们不是当前重部署
  authority；没有恢复旧部署、删除或远端操作权限。
- 修复后 13 个 integration targets 全部通过，合计
  `134 passed / 0 failed`。
- lib regression 按 acceptance matrix 精确排除唯一 wall-clock fixture 后通过：
  `386 passed / 0 failed / 1 ignored / 1 filtered`。
- `cargo fmt --all -- --check`、`cargo check -p data-collect --locked --all-targets`、
  `cargo clippy -p data-collect --locked --all-targets -- -D warnings` 和
  `git diff --check` 均通过。
- 自动检查确认恰有 `45` 份 Markdown 标记
  `HISTORICAL_EVIDENCE_ONLY`，且每份均包含
  `当前执行 authority：none`。

## 2026-07-30：阶段 2 独立审查修复

- `gpt-5.6-sol/ultra` 独立审查首先发现，43 份旧事项与阶段报告的统一 banner 缺少
  `最终快照` 和 `superseded_by`；已补齐冻结状态、治理后继事项和统一读取规则。
- 审查发现 current training 部署说明深层仍保留可被局部摘取的旧冷启动、上传、
  preflight 和 `systemctl --user enable --now` 流程。已把该段压缩为
  “历史首次部署流程（禁止直接复用）”，删除可执行命令，并明确启动验证只适用于
  另行批准后的只读观察，本文不授权停服或改变远端状态。
- 随后复测揭示 `data-collect/tests/deploy_readme_contract.rs` 仍强制要求上述旧命令。
  已仅更新该文档契约测试：改为要求历史可追溯、禁止直接复用和另行批准边界，并反向
  拒绝旧部署标题、精确 systemctl 命令和“preflight 通过后”执行语气。未修改 Rust
  runtime、Cargo、schema 或交易逻辑。
- 4 份跨模块 mistake-log evidence input 已补齐最终快照、authority none 和
  `execution_superseded_by`；它们不是 data-collect 状态沿革长文，阶段 3 继续保留在
  `mistake-log/`，但由 banner、current 路由和自动检查阻止其取得执行 authority。

## 2026-07-30：阶段 2 独立复审通过

- 同一 `gpt-5.6-sol/ultra` 审查者复核最新工作树，结论为 `PASS`：
  Critical 0、Important 0、Minor 0。
- 审查者独立复现 `61 = 12 current/active + 45 historical + 4 evidence-only`；
  45 份历史文档和 4 份 evidence input 的最终快照、authority 与替代关系字段完整。
- 审查者复跑 13 个 integration targets，结果为 `134 passed / 0 failed`；同时确认
  `cargo fmt --all -- --check` 与 `git diff --check` 通过。
- lib 回归仍按已记录的精确 named-test 排除口径为
  `386 passed / 0 failed / 1 ignored / 1 filtered`，未把动态 UTC 夹具限制隐瞒成
  无条件 full-suite PASS。
- 阶段 2 完成；本阶段 commit/push 后才能进入阶段 3 的 tag 冻结、current 层建立与
  活动历史隔离。
