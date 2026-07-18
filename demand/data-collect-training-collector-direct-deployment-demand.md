# 训练数据采集器直接长期部署需求

> 事项编号：`DC-DEPLOY-20260718-DIRECT-LONG-RUN`
>
> 对应计划：`plan/data-collect-training-collector-direct-deployment-plan.md`
>
> 对应 changelog：`changelog/data-collect-training-collector-direct-deployment-changelog.md`
>
> 状态：`APPROVED_EXECUTION_IN_PROGRESS`

## 一、用户批准与目标

用户于 2026-07-18 批准把已经完成阶段 8 的训练 collector 直接部署到数据采集服务器并开始长期运行：

- 可以在任意 UTC 时刻启动，不等待 UTC 00:00；
- 第一次正式启动即使用仓库唯一的 `Restart=on-failure` unit；
- 不使用 `Restart=no`、独立 gate unit、独立数据根、15 分钟 gate 或 26 小时放行门；
- 部署命令和operator不删除、移动、隔离、回写或伪造任何既有训练数据；唯一例外是启动后的现有 runtime 按 state-aware recovery 合同关闭历史 open manifest；
- 普通网络不稳定、服务端静默、一个或多个 source 暂时没有数据不是部署失败，也不得因此结束 collector；
- 部署后持续运行，由用户长期观察异常退出、数据落盘和服务器资源。

本次正式实现 commit 固定为 `c6b8bac3c5d3ce7ea2fa689bd431cf1a8b7070de`。本事项只授权训练数据采集服务器 `ubuntu@3.255.156.183` 上的 production release 构建产物上传、正式 binary 原子替换、现有 user service 启用和启动，以及只读运行验证；不授权 paper/live、交易、模型、钱包或历史数据清理操作。

## 二、部署合同

1. release binary 必须在本地仓库的 clean detached worktree 中从固定实现 commit 执行 locked release build，记录完整 commit、binary SHA-256、ELF/loader 和实际 GLIBC version requirement。远端禁止编译。
2. 正式路径只保留一个训练 binary 和仓库已有的一个 user unit。上传 staging 只是同目录下带 SHA 的瞬时文件，不得形成第二个 service、第二个 collector 或第二个数据根。
3. 在替换 binary 前必须只读确认：旧 service 无 PID/cgroup、dataset lease 无占用、本次首次冷启动的当前UTC日无任何collector产物（目录、manifest、finalized Parquet或瞬时文件）、目标路径及父路径不是 symlink、unit 仍为仓库权威内容。本次不扩张为current-day状态兼容矩阵。
4. 若当前 UTC 日已出现任何collector产物，必须原样保留并停止部署；退出码 75/76 与 `RestartPreventExitStatus=75 76` 继续作为本地容量/完整性边界，不得为了自动恢复而删除。
5. 本地产物实际要求的最高 GLIBC 版本必须不高于远端 2.39；否则在上传和替换前停止，不得到远端试编译或碰运气。
6. staging SHA、ELF、loader、`ldd`、`--help` 和 `--version` 通过后，才可在同一文件系统原子替换固定 binary；rename后必须同步destination所在目录，再复核正式SHA/owner/mode，最后执行 `systemctl --user enable --now data-collect-training-collector.service`。
7. 正式 service 从启动起保持 `Restart=on-failure`。同 identity 的进程级故障可以由 systemd 恢复；单个 source 的普通 availability 故障仍必须由同一 PID 内 source owner 持续恢复。观察到 restart 时记录事实，不把“当前又 active”冒充从未重启。

## 三、验收标准

### 3.1 启动即时验收

- service 为 `active/running`，MainPID 大于零，`/proc/<pid>/exe` SHA 与部署 SHA 相同，unit/ExecStart 与仓库权威源一致；
- 当日 manifest 为顶层 v3、`state=open`，build commit、binary SHA、config digest、collector host ID与region完整identity正确，嵌套 collection-day coverage v1 为 `active`；若未发生同日restart，首次启动必须有`first_started_utc_ms == last_started_utc_ms`且`reactivated_within_day=false`；若已发生同identity restart，则允许`true`，但first必须保持、last更新并与`NRestarts`、PID和journal事实一致；
- 已知2026-07-15 legacy open manifest必须由runtime中性关闭为`reconciled_legacy_coverage_unknown`，coverage仍为unknown且不得伪造unclean gap；
- 六个 source counter 存在。启动时零行合法；逐 source 只按真实 `.parquet.running` 或 finalized Parquet 报告正向出数证据；
- journal 无 panic、writer/manifest/identity/queue ownership 等本地完整性 terminal；普通 source 静默、stale 或外部恢复日志不使整体启动失败；
- 重复采样 MainPID、`NRestarts`、CPU、RSS、线程、FD、socket、`/proc/<pid>/io` 和磁盘余量，排除启动即失控。

### 3.2 持续运行验收

- 跨下一 UTC 小时后，至少对实际已 finalized 的 source 核对 Parquet 可读性、行数、SHA、manifest file facts；静默 source 不要求生成空文件；
- 跨下一 UTC 日后，旧日完成既定关闭、新日为 `open + active`；若无真实进程故障，MainPID 保持不变；
- 长期持续记录 PID/`NRestarts`、journal、逐 source 数据增长、小时/日 rollover、RSS/FD/线程/磁盘趋势。约 24 小时 soak 只能证明观察期间通过，不能冒充每种 reconnect 分支都被真实触发；
- 从日内启动得到的首日是部分 UTC 日，必须如实保留，不得冒充完整 00:00--24:00 UTC collection day。

部署完成的即时验收与长期观察是两个时间尺度；不等待长期窗口才允许启动，也不因某一路暂时无数据否定 collector/runtime 已成功启动。
