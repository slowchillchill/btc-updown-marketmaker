# data-collect 当前路由入口

> 生命周期：`CURRENT_AUTHORITY_ENTRY`
>
> 当前路由 authority：`self`
>
> 当前需求：`demand/data-collect-external-disk-guard-demand.md`
>
> 当前规范计划：`plan/data-collect-external-disk-guard-plan.md`
>
> 当前 changelog：`changelog/data-collect-external-disk-guard-changelog.md`
>
> 历史读取入口：`data-collect/docs/archive-manifest.tsv`
>
> 执行授权：`阶段 2 / 任务 2.5：提交推送并部署启用 UTC 12:00 timer`

这是所有 `data-collect` 需求、代码、测试、运行和部署问题的强制首读路由，不直接规定业务或
技术语义。`plan/data-collect-external-disk-guard-plan.md` 是当前活动事项的唯一规范来源；
`plan/data-collect-plan.md` 继续拥有尚未改变的合同和已部署事实。需求只描述宏观业务目标和
验收，changelog 只保存当时事实与取代关系，`data-collect/docs/current/` 只提供由计划派生的
操作、接口、验收和限制参考。本入口不授权任何代码、数据、部署或远端状态变化。

## 当前状态路由

- 主模块既有事项已完成并由用户验收；本事项阶段 1、任务 2.1、任务 2.2 和任务 2.3 已完成。
  任务 2.4 已正常停止旧服务、永久删除当前 UTC 日目录并安装四个冻结产物；首次候选启动被
  错误的 `NRestarts` 验收门主动停止。第二次启动在 180 秒内保持候选进程稳定且没有重启，但
  尚未完成启动恢复，随后按获批验收门停止。取消该硬时限后，同一候选已完成启动恢复和就绪后
  182 秒稳定写入验收，当前长期运行。
- 事项 `DATA-COLLECT-EXTERNAL-DISK-GUARD-20260807` 的需求和计划已经获得用户批准。当前获批
  范围只有两个目标：根文件系统达到 30GB 时由每日外部任务删除最早一个历史日，
  同时只移除采集器运行期间每 60 秒的容量扫描；修复 `real_source_alignment_probe` 原 bench
  事故。启动时 50GB 检查、容量参数和退出码 `75` 保留，不新增清理协调锁，清理脚本不控制
  采集服务且执行后只验收服务仍为 `active`。路由为
  `demand/data-collect-external-disk-guard-demand.md`、
  `plan/data-collect-external-disk-guard-plan.md` 和
  `changelog/data-collect-external-disk-guard-changelog.md`。当前主需求已经完成窄范围同步；
  阶段 0 的批准后文档同步、最终独立复审、提交和首次分支推送已经完成；阶段 1 两个目标的
  实现、完整验证、独立阶段复审、提交和推送也已经完成。阶段 2 的发布候选从干净且已推送的
  提交隔离构建并完成生产换版；新清理产物已经安装，timer 为 `disabled/inactive` 且清理脚本
  未运行。当前日清单为候选身份、`open/active`，已有 6 个运行中 Parquet；采集服务为 `active/running`、
  `MainPID=1315799`、`NRestarts=0`。生产事实文档同步和两路独立审查已经通过。用户已授权把
  timer 调度更新为每日 UTC `12:00:00` 并首次启用，并已选择先复审、提交和推送当前 10 份
  文档与 1 份 timer 源文件，再部署这个 timer 单文件候选。服务器在部署前仍为原调度且
  `disabled/inactive`；手动运行清理服务、其他产物重装、再次手动删除和 PR 均未获授权。
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
