# RTDS-PONG-A 原路径重置部署计划

> 生命周期：`HISTORICAL_EVIDENCE_ONLY`
>
> 最终快照：`FROZEN_AT_20260730_CONSOLIDATION`
>
> 当前执行 authority：`none`
>
> superseded_by：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 读取规则：本文仅用于审计历史事实；内部“当前、下一步、待执行、命令”均属于
> 当时检查点，不得驱动当前实现、测试、部署或远端操作。
>
> 2026-07-30：本计划只记录 2026-07-19 部署过程，已被后续部署闭环取代，任何远端
> 命令都不得从本文件重新获得执行权。
>
> 事项编号：`DC-DEPLOY-20260719-RTDS-PONG-A-RESET`
>
> 对应需求：`demand/data-collect-rtds-pong-a-reset-deployment-demand.md`
>
> 对应 changelog：`changelog/data-collect-rtds-pong-a-reset-deployment-changelog.md`
>
> 状态：`COMPLETE`

## 阶段 1：冻结范围与只读 preflight

- 核对 RTDS-PONG-A commit、测试、worktree 与远端旧 service/binary/unit/current-day manifest。
- 核对本地 2026-07-18 closed 副本全部 manifest SHA；盘点精确删除目标，不触碰 closed 历史日。
- 验证：需求、计划、changelog 同一事项编号；本地 71/71 SHA 匹配；远端删除目标是旧 identity 的唯一 current-day 目录。

## 阶段 2：隔离 production 构建与 staging

- 从 `1d95823614ceed78288ce34cb95fbf23e58ef64d` 创建 clean detached worktree，设置同值 `DATA_COLLECT_GIT_COMMIT`，使用独立 target 执行 locked release build。
- 验证本地 SHA、Build ID、ELF/loader、最高 GLIBC、`--help`、`--version`；以 exclusive staging 上传并在远端重复兼容性验证。
- 验证：远端 GLIBC 2.39 可满足 binary；正式 binary 尚未替换，旧 PID 仍运行到计划停服点。

## 阶段 3：协调停服、精确删除与原子替换

- 执行 `systemctl --user stop data-collect-training-collector.service`，等待协调 finalize 与 cgroup 完全退出。
- 复核 MainPID=0、无 holder、lease 可取得、目标 owner/type/non-symlink/non-mount、manifest 仍为旧 identity；记录删除前 bytes/files。
- 只删除 `utc_day=2026-07-19`，复核历史 closed 日仍存在；随后 sync staging、原子替换正式 binary、同步 destination 目录并复核 SHA/owner/mode。
- 验证：删除目标不存在、staging 不残留、正式 binary 为新 SHA，unit 内容未变化。

## 阶段 4：立即启动与运行验收

- 启动原 unit；重复采样 service、PID、NRestarts、journal、manifest、逐 source 文件与资源。
- RTDS 至少取得新 update 和文件增长证据；观察时间必须跨过多个旧 15 秒 timeout，确认 PID、generation相关 reconnect counter与 subscription 不出现旧循环症状。
- 验证：满足对应需求第四节，不把 socket、空 running 文件或 PING 本身冒充业务数据。

## 阶段 5：证据收口

- 把构建、删除、安装、启动与验收的精确事实写入对应 changelog；同步旧部署 authority 与 RTDS-PONG-A 文档指针。
- 运行文档合同、fmt/diff 检查并由独立代理审查实际部署结果；成立问题修复后提交并 push。
- 验证：实现 commit 与部署证据区分清楚，工作树只包含本事项文档变更，远端新程序继续运行。

## 执行结果

阶段 1 至阶段 5 已完成；新程序保持原 unit、原 binary 路径和原 dataset root 运行，未增加 `ver2`、第二服务或第二数据根。UTC 11:00 自然小时封口已取得 RTDS finalized Parquet 行数、update 计数、SHA 与 manifest source counter 证据；最终独立终审与文档门均通过，本计划随收口commit发布到当前远端分支。
