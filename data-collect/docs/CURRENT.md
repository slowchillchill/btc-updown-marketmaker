# data-collect 当前路由入口

> 生命周期：`CURRENT_AUTHORITY_ENTRY`
>
> 当前路由 authority：`self`
>
> 当前需求：`demand/data-collect-demand.md`
>
> 当前规范计划：`plan/data-collect-plan.md`
>
> 当前 changelog：`changelog/data-collect-changelog.md`
>
> 历史读取入口：`data-collect/docs/archive-manifest.tsv`
>
> 执行授权：`none`

这是所有 `data-collect` 需求、代码、测试、运行和部署问题的强制首读路由，不直接规定业务或
技术语义。`plan/data-collect-plan.md` 是唯一当前规范来源；需求只描述宏观业务目标和验收，
changelog 只保存当时事实与取代关系，`data-collect/docs/current/` 只提供由计划派生的操作、
接口、验收和限制参考。本入口不授权任何代码、数据、部署或远端状态变化。

## 当前状态路由

- 主模块已完成并由用户验收，当前没有 Rust 实现、生产部署或远端操作游标。
- 已完成的模型训练消费边界修正由
  `plan/data-collect-model-train-consumer-boundary-plan.md` 规范；该计划执行授权为 `none`。
- 当前获批的后续开发事项是 `plan/data-pipeline-training-plan.md`，它不重开
  `data-collect`。模拟盘和实盘真实适配器仍须在用户决定返回本模块后另立需求与计划。

## 当前派生参考

| 主题 | 当前参考文档 |
| --- | --- |
| 合同和运行时不变量 | `data-collect/docs/current/contracts.md` |
| 二进制程序、CLI、数据源、产物和交接接口 | `data-collect/docs/current/interfaces.md` |
| 构建、运行与部署权限边界 | `data-collect/docs/current/operations.md` |
| 当前验收边界与测试命令 | `data-collect/docs/current/acceptance.md` |
| 未实现、不声明和外部条件限制 | `data-collect/docs/current/known-limitations.md` |

以上参考不得新增或改变计划中的语义。`data-collect/README.md` 和旧文档原路径只提供入口
兼容，不拥有独立执行权威。

## 历史读取规则

45 份历史沿革长文已从活动工作树移除，冻结在带注释标签
`data-collect-history-pre-current-authority-20260730`，解引用提交为
`4ed31ad6abe7237d92cc99a4857979d8ae26d373`；逐文件路径和 blob 记录在
`data-collect/docs/archive-manifest.tsv`。

归档只用于回答“当时发生了什么”，不得从其中的当前、下一步、状态词或命令推导动作，也
不得为搜索方便恢复到活动树。读取 `historical` 或 `snapshot` 对象时，必须按归档清单核对
类别、生命周期和替代关系，再回到当前计划、代码、数据与测试复核。

## 冲突与权限门

当前计划、真实代码、数据、产物、运行结果或用户最新决定不能同时成立时，必须停止相关
判断和执行，列出精确位置、语义和影响交由用户决定。changelog 的旧事实不参与当前语义
裁决。部署、启停服务、替换二进制文件、删除数据、推送或其他外部状态变化仍需具体授权。
