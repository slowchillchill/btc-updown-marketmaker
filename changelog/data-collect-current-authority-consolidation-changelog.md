# data-collect 当前权威与历史隔离整理 Changelog

> 事项编号：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 生命周期：`GOVERNANCE_RECORD`
>
> 当前执行 authority：`none`
>
> superseded_by：`data-collect/docs/CURRENT.md`
>
> 对应需求：`demand/data-collect-current-authority-consolidation-demand.md`
>
> 对应计划：`plan/data-collect-current-authority-consolidation-plan.md`
>
> 状态：`COMPLETE`

## 用户决策

- 2026-07-28 事故已最终验收；真实 UTC rollover/full healthy day 不再阻断完成，
  同时不声明未记录的运行证据已经发生。
- paper/live 真实 external-source adapter 不属于 `model-train`，保持延期；等下一
  模块 `model-train`（模型训练模块，不是 `data-collect-training-collector`）完成
  之后，只有在用户决定返回 `data-collect` 时才另立三件套。
- 旧状态以 current 代码、测试和最新决策修正；历史事实保留但不再驱动执行。

## 阶段 1

- 同步事故关闭与 paper/live 延期边界。
- 首轮独立审查发现旧检查点和职责措辞仍可能被局部误读；修复后复审
  `Critical 0 / Important 0 / Minor 0`。
- 25 项定向文档合同测试通过。
- commit `e2de3c2b42c83253f4fca5ad0483d695c338d6ed` 已 push。

## 阶段 2

- 审计 61 份相关 Markdown：12 current/active、45 historical、4 evidence-only。
- 以 SDK 0.7.0 + 唯一 patch、六个 current schema、RTDS 300/60、50GB exit 75、
  fixed identity/resolution/rollover 和 paper/live heartbeat 能力修正活动口径。
- 历史文档补齐 final snapshot、authority none、supersession 与读取规则。
- 更新旧部署文档契约测试，拒绝已失效的部署命令，不修改 runtime。
- 13 个 integration targets 为 `134 passed / 0 failed`；lib regression 精确排除
  wall-clock fixture 后为 `386 passed / 0 failed / 1 ignored / 1 filtered`。
- 独立复审 `Critical 0 / Important 0 / Minor 0`。
- commit `4ed31ad6abe7237d92cc99a4857979d8ae26d373` 已 push。

## 阶段 3 实施

- annotated tag `data-collect-history-pre-current-authority-20260730` 已推送：
  - tag object：`7f9a7b09cc7936cdd3c38b7d9953878985614626`
  - peeled commit：`4ed31ad6abe7237d92cc99a4857979d8ae26d373`
- `data-collect/docs/archive-manifest.tsv` 记录 45 个历史对象和 11 个阶段 2 snapshot，
  共 56 个 path/blob。
- 独立审查发现 11 个 snapshot 旧 blob 不都含历史 banner，不能沿用 historical
  首屏读取规则；现已拆分协议：historical 必须核对同 blob banner，snapshot 由
  manifest `kind=snapshot`、活动同路径和 CURRENT owner 判为 authority none，并
  由自动 guard 固定 snapshot 替代集合与规则文本。
- 新增唯一 `data-collect/docs/CURRENT.md` 与六个 `docs/current/` 主题 owner。
- 主 demand/plan/changelog 已收敛为 current 合同；README 和四个旧入口压缩为
  route-only。
- 45 份历史长文已从活动树移除；四份 mistake-log evidence input 仍保留，
  execution superseded by CURRENT。
- 根 AGENTS 增加 current/archive 路由；rebuild-core 草案明确不是 current
  data-collect authority。
- `document_authority_contract` 验证唯一 current 图、tag/path/blob、历史隔离与五类
  临时负例；`deploy_readme_contract` 已改读 current operations/interfaces/limits。
- 阶段 3 独立审查首先发现 README 仍复制规范、paper/live 延期可能与 training
  collector 混淆，以及“五类负例”被误记为四类；README 已改为纯 route-only，
  延期已明确为“不属于 model-train，model-train 完成后仍须用户决定返回
  data-collect”，负例计数已修正。
- 技术准确性专项审查继续发现并收紧：hot 默认 7 天/region/config 语义、
  run-id receipt confinement 尚未 fail-closed、heartbeat component test 不等于
  完整 binary E2E、75/76 只属于 training、startup capacity direct 75 与运行期
  coordinated 75 的区别、configured capacity override、RTDS
  `×2 / ±20% / 300s / 无独立 exhaustion`、resolution late-ignore 与 contradiction
  fail-closed 边界，以及 `0.7.0-p1` 四文件 patch surface 与 SDK transport-control
  unbounded channel 例外。
- 上述审查只修正文档与契约测试，并将九字段 Decimal 边界加入常规 SDK patch
  integration target；未修改 Rust runtime、schema 或远端状态。
- 最终独立审查发现 vendored SDK 单元测试名把 optional `PriceChangeBatchEntry.size`
  误称为 required；仅重命名测试以匹配真实类型与 allowlist 语义，测试体和生产实现
  均未改变。

## 当前验证状态

- 15 个 current integration targets：`159 passed / 0 failed`。
- 其中 document authority 正向与临时负例：`4 passed / 0 failed`。
- lib regression 精确排除 named wall-clock fixture 后：
  `386 passed / 0 failed / 1 ignored / 1 filtered`。
- `cargo fmt --all -- --check`、`cargo check -p data-collect --locked --all-targets`、
  `cargo clippy -p data-collect --locked --all-targets -- -D warnings` 和
  `git diff --check` 通过。
- 技术准确性复审与阶段 3 最终 `gpt-5.6-sol/ultra` 独立审查均为
  `Critical 0 / Important 0 / Minor 0`，结论 `PASS`。
