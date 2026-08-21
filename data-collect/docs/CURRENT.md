# data-collect 当前路由入口

> 生命周期：`CURRENT_AUTHORITY_ENTRY`
>
> 当前路由 authority：`self`
>
> 当前任务事实源：`https://github.com/qiuqiuaiweb3/btc-updown-marketmaker/issues/21`
>
> 外部磁盘保护事项：`https://github.com/qiuqiuaiweb3/btc-updown-marketmaker/issues/24`
>
> 历史读取入口：`data-collect/docs/archive-manifest.tsv`
>
> 执行授权：`none`

这是所有 `data-collect` 代码、测试、运行和部署问题的技术路由入口，不直接规定任务目标、范围、
状态或游标。GitHub 模块 Issue #21 是该模块的任务事实源；`data-collect/docs/current/` 只保存需要
随代码版本维护的操作、接口、验收和限制参考。本入口不授权任何代码、数据、部署或远端状态变化。

## 当前任务路由

- 模块状态、需求、计划、事项目录和执行游标只读取 GitHub Issue #21；本文件不复制任务状态。
- 专项合同与证据分别读取 GitHub Issue #22、#23、#24。
- 后续开发必须创建新的 GitHub Issue；不得从本文件、归档清单或 Git 历史取得执行授权。
- 当前后续模块 `data-pipeline(training)` 的任务事实源为 GitHub Issue #25，它不重开
  `data-collect`。模拟盘和实盘真实适配器仍须由用户另行批准。

## 当前派生参考

| 主题 | 当前参考文档 |
| --- | --- |
| 合同和运行时不变量 | `data-collect/docs/current/contracts.md` |
| 二进制程序、CLI、数据源、产物和交接接口 | `data-collect/docs/current/interfaces.md` |
| 构建、运行与部署权限边界 | `data-collect/docs/current/operations.md` |
| 当前验收边界与测试命令 | `data-collect/docs/current/acceptance.md` |
| 未实现、不声明和外部条件限制 | `data-collect/docs/current/known-limitations.md` |

以上参考不得新增或改变 GitHub Issue 中的任务语义。`data-collect/README.md` 和旧文档原路径只提供入口
兼容，不拥有独立执行权威。

## 历史读取规则

45 份历史沿革长文已从活动工作树移除，冻结在带注释标签
`data-collect-history-pre-current-authority-20260730`，解引用提交为
`4ed31ad6abe7237d92cc99a4857979d8ae26d373`；逐文件路径和 blob 记录在
`data-collect/docs/archive-manifest.tsv`。

归档只用于回答“当时发生了什么”，不得从其中的当前、下一步、状态词或命令推导动作，也
不得为搜索方便恢复到活动树。读取 `historical` 或 `snapshot` 对象时，必须按归档清单核对
类别、生命周期和替代关系，再回到当前适用 Issue、代码、数据与测试复核。

## 冲突与权限门

当前适用 Issue、真实代码、数据、产物、运行结果或用户最新决定不能同时成立时，必须停止相关
判断和执行，列出精确位置、语义和影响交由用户决定。Git 历史中的旧事实不参与当前语义
裁决。部署、启停服务、替换二进制文件、删除数据、推送或其他外部状态变化仍需具体授权。
