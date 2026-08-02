# data-collect 当前权威与历史隔离整理变更日志

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

- 2026-07-28 事故已最终验收；真实 UTC 跨日切换和完整健康日不再阻断完成，
  同时不声明未记录的运行证据已经发生。
- 模拟盘和实盘的真实外部数据源适配器不属于 `model-train`，保持延期；等下一
  模块 `model-train`（模型训练模块，不是 `data-collect-training-collector`）完成
  之后，只有在用户决定返回 `data-collect` 时才另立三件套。
- 旧状态以当前代码、测试和最新决策修正；历史事实保留但不再驱动执行。

## 阶段 1

- 同步事故关闭与模拟盘、实盘延期边界。
- 首轮独立审查发现旧检查点和职责措辞仍可能被局部误读；修复后复审结果为
  `Critical 0 / Important 0 / Minor 0`。
- 25 项定向文档合同测试通过。
- 提交 `e2de3c2b42c83253f4fca5ad0483d695c338d6ed` 已推送。

## 阶段 2

- 审计 61 份相关 Markdown：12 份当前或活动文档、45 份历史文档、4 份仅作证据的文档。
- 以 SDK 0.7.0 加唯一补丁、六个当前数据模式、RTDS 300/60、50GB 退出码 75、
  固定身份标识、市场判定与跨日切换，以及模拟盘和实盘的心跳能力修正活动口径。
- 历史文档补齐最终快照、无执行权威、替代关系与读取规则。
- 更新旧部署文档契约测试，拒绝已失效的部署命令，不修改运行时。
- 13 个集成测试目标为 `134 passed / 0 failed`；库回归测试精确排除挂钟时间夹具后为
  `386 passed / 0 failed / 1 ignored / 1 filtered`。
- 独立复审 `Critical 0 / Important 0 / Minor 0`。
- 提交 `4ed31ad6abe7237d92cc99a4857979d8ae26d373` 已推送。

## 阶段 3 实施

- 带注释标签（`annotated tag`）`data-collect-history-pre-current-authority-20260730` 已推送：
  - 标签对象（`tag object`）：`7f9a7b09cc7936cdd3c38b7d9953878985614626`
  - 解引用提交（`peeled commit`）：`4ed31ad6abe7237d92cc99a4857979d8ae26d373`
- `data-collect/docs/archive-manifest.tsv` 记录 45 条 `kind=historical` 和 11 条阶段 2
  `kind=snapshot`，共 56 组路径与 blob。
- 独立审查发现 11 个旧 `snapshot` blob 不都含历史首屏标记，不能沿用 `historical`
  blob 的首屏读取规则；现已拆分协议：`historical` 必须核对同一 blob 的首屏标记，
  `snapshot` 由清单中的 `kind=snapshot`、活动树同路径文件和 `CURRENT` 主题归属文档
  判为无执行权威，并由自动检查固定 `snapshot` 替代集合与规则文本。
- 新增唯一 `data-collect/docs/CURRENT.md` 与六个 `docs/current/` 主题归属文档。
- 主需求、计划和变更日志已收敛为当前合同；README 和四个旧入口压缩为仅路由页面。
- 45 份历史长文已从活动树移除；四份错误记录证据输入仍保留，其执行权威已由
  CURRENT 替代。
- 根 AGENTS 增加当前与归档路由；rebuild-core 草案明确不是 data-collect 当前权威。
- `document_authority_contract` 验证唯一当前权威图、标签、路径、blob、历史隔离与
  五类临时负例；`deploy_readme_contract` 已改读当前操作、接口与限制文档。
- 阶段 3 独立审查首先发现 README 仍复制规范、模拟盘和实盘延期可能与训练采集器
  混淆，以及“五类负例”被误记为四类；README 已改为纯路由页面，延期已明确为
  “不属于 `model-train`，`model-train` 完成后仍须用户决定返回 data-collect”，
  负例计数已修正。
- 技术准确性专项审查继续发现并收紧：热路径程序默认运行 7 天、区域与配置语义；
  运行标识回执路径尚未实现失败即关闭；心跳组件测试不等于完整二进制端到端测试；
  75/76 只属于训练；启动容量直接退出码 75 与运行期协调退出码 75 的区别；配置容量
  覆盖；RTDS 的 `×2 / ±20% / 300s / 无独立耗尽状态`；市场判定迟到事件忽略与矛盾时
  失败即关闭的边界；
  以及 `0.7.0-p1` 四文件补丁范围与 SDK 传输控制无界通道例外。
- 上述审查只修正文档与契约测试，并将九字段 Decimal 边界加入常规 SDK 补丁集成
  测试目标；未修改 Rust 运行时、数据模式或远端状态。
- 最终独立审查发现仓库内置 SDK 副本的单元测试名把可选字段 `PriceChangeBatchEntry.size`
  误称为必填字段；仅重命名测试以匹配真实类型与允许列表语义，测试体和生产实现
  均未改变。

## 当前验证状态

- 15 个当前集成测试目标：`159 passed / 0 failed`。
- 其中文档权威正向与临时负例：`4 passed / 0 failed`。
- 库回归测试精确排除具名挂钟时间夹具后：
  `386 passed / 0 failed / 1 ignored / 1 filtered`。
- `cargo fmt --all -- --check`、`cargo check -p data-collect --locked --all-targets`、
  `cargo clippy -p data-collect --locked --all-targets -- -D warnings` 和
  `git diff --check` 通过。
- 技术准确性复审与阶段 3 最终 `gpt-5.6-sol/ultra` 独立审查均为
  `Critical 0 / Important 0 / Minor 0`，结论 `PASS`。
