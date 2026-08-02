# data-collect 当前权威入口

> 生命周期：`CURRENT_AUTHORITY_ENTRY`
>
> 当前执行 authority：`self`
>
> 当前需求：`demand/data-collect-demand.md`
>
> 当前计划：`plan/data-collect-plan.md`
>
> 当前 changelog：`changelog/data-collect-changelog.md`
>
> 历史读取入口：`data-collect/docs/archive-manifest.tsv`
>
> 执行授权：`none`

这是所有 `data-collect` 需求、代码、测试、运行和部署问题的强制首读入口。
它只路由当前事实，不授权修改代码、部署、启停服务、替换二进制文件、删除数据或改变
远端状态。

## 当前结论

- `data-collect` 当前模块已经完成并由用户验收。
- 仓库构建训练、模拟盘热路径和实盘热路径三个二进制程序；当前只有训练二进制程序
  连接真实 Polymarket、RTDS 和 Binance 数据源。
- 模拟盘和实盘二进制程序当前只提供启动回执、Unix 套接字和心跳、状态；
  真实外部市场数据源适配器不属于 `model-train`，继续保持延期。用户确认的当前跨模块
  主线为 `data-collect(training) -> sim-system -> data-pipeline(training) -> model-train`。
  必须先完成 `model-train`（模型训练模块，不是 `data-collect-training-collector`）；
  之后仍只有在用户决定返回 `data-collect` 时，才另立三件套开发。
- 上述模拟盘和实盘完整二进制程序的心跳循环尚无端到端测试，热路径运行标识的回执
  路径限制也尚未失败即关闭；精确测试强度与风险分别由验收、限制归属文档记录，不得
  从“代码已有”推导为生产验收。
- 2026-07-28 事故已由用户最终验收。真实 UTC 日切换和完整健康 UTC 日运行证据
  不作已取得声明，也不再是模块完成阻断门。
- 当前没有 `data-collect` Rust 实现、生产部署或远端操作游标；新工作必须另立活动
  需求、计划和变更记录。

## 当前文档归属

| 主题 | 唯一当前归属文档 |
| --- | --- |
| 模块行为、SDK、schema、运行时不变量 | `data-collect/docs/current/contracts.md` |
| 二进制程序、CLI、数据源、产物和交接接口 | `data-collect/docs/current/interfaces.md` |
| 构建、路径、退出码、运行与部署权限边界 | `data-collect/docs/current/operations.md` |
| 验收矩阵、测试命令与最新结果 | `data-collect/docs/current/acceptance.md` |
| 未实现、不声明和外部条件限制 | `data-collect/docs/current/known-limitations.md` |
| 文档范围、归档处置与一致性结论 | `data-collect/docs/current/document-authority-audit.md` |

`data-collect/README.md` 和旧文档原路径只提供入口兼容，不拥有独立执行权威。
同一主题不得在多个当前文件中发展第二套规范。

## 证据优先级与冲突门

当前判断必须同时核对：

1. 本入口及其指定的主题归属文档；
2. 当前主需求、计划和变更记录；
3. 当前 Cargo、代码与测试；
4. 用户最新明确决策。

它们相互冲突，或与真实产物、环境、运行结果冲突时，必须停止相关判断和执行，
列出精确位置与语义并交由用户决定。不得按文件日期、篇幅、搜索排名或单个片段自行
选择执行依据。

## 历史读取规则

45 份历史沿革长文已从活动工作树移除。完整内容冻结在带注释标签
`data-collect-history-pre-current-authority-20260730`，解引用提交为
`4ed31ad6abe7237d92cc99a4857979d8ae26d373`；逐文件原路径和 blob 记录在
`data-collect/docs/archive-manifest.tsv`。

历史只能回答“当时发生了什么、为何这样决策”。需要时必须按清单指定的提交和路径
显式读取；历史中的“当前、下一步、待执行、READY、PENDING、命令”均无当前执行权威。
读取后必须回到本入口、当前代码和测试复核，禁止为了搜索方便把
历史长文恢复进活动工作树。

清单的两类记录必须按不同协议读取：

- `historical` 行是已移出活动树的历史长文；其带标签 blob 内有
  `HISTORICAL_EVIDENCE_ONLY` 首屏标记。局部读取时必须同时读取同一 blob 的
  首屏生命周期、最终快照和替代关系，并核对本 `CURRENT` 的对应主题归属文档。
- `snapshot` 行是仍有活动路径、但内容已被当前、路由或治理层替换前的
  冻结快照。旧 blob 不保证含生命周期首屏标记；清单中的 `kind=snapshot`
  本身即声明该 blob 的当前执行权威为 `none`、已被活动当前层替代。读取
  前必须先核对该清单行、活动树同路径文件和本 `CURRENT` 的对应主题归属文档，
  不得用旧 blob 自身的状态词或命令判断当前状态。

不得只读取搜索命中的中段。缺少所属类别对应的上述核对项时，该片段只能作为未
采用线索，不能形成实现、测试、部署或状态结论。

四份跨模块 `mistake-log` 错误记录继续保留为 `EVIDENCE_ONLY_INPUT`，其
`当前执行 authority` 为 `none`；它们只能提供待重新验证的问题线索。

## 明确禁止恢复的旧路线

不得从历史恢复旧 SDK 或 schema、旧 RTDS 3 秒刷新、自动保留或删除、发布别名、
旧部署命令或旧尝试游标。当前技术与权限边界只以本入口指定的主题归属文档、代码、
测试和新批准事项为准。
