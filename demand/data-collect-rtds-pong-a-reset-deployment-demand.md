# RTDS-PONG-A 原路径重置部署需求

> 事项编号：`DC-DEPLOY-20260719-RTDS-PONG-A-RESET`
>
> 对应计划：`plan/data-collect-rtds-pong-a-reset-deployment-plan.md`
>
> 对应 changelog：`changelog/data-collect-rtds-pong-a-reset-deployment-changelog.md`
>
> 状态：`COMPLETE_DEPLOYED_AND_VALIDATED_20260719T110131Z`

## 一、用户批准与目标

用户于 2026-07-19 明确批准停止数据采集服务器上的旧训练 collector、删除为本次同日切换造成 identity 冲突的旧数据产物、原路径原 unit 替换为已完成的 `RTDS-PONG-A` 程序并立即启动；不等待 UTC 11:01，不增加 `ver2` 路径，也不保留第二个 collector、第二个 service 或第二个数据根。

本次实现 commit 固定为 `1d95823614ceed78288ce34cb95fbf23e58ef64d`。本事项只授权 `ubuntu@3.255.156.183` 上训练 collector 的本地隔离 release 构建、上传、旧 service 协调停止、精确旧 current-day 产物删除、正式 binary 原子替换、原 unit 启动与只读验收；不授权删除 closed 历史日、paper/live、模型、钱包或交易操作。

## 二、数据与删除边界

1. 本地 `/home/chillchill/new-datasets/2026-07-18/typed_raw/utc_day=2026-07-18` 必须保持不变；其 manifest 必须为 `closed + utc_day_barrier`，71 个 manifest file SHA-256 必须全部匹配后才允许远端删除。
2. 远端保留 `utc_day=2026-07-15` 与 `utc_day=2026-07-18` closed 日，不删除整个 `training` 或 `typed_raw` 根。
3. 唯一数据删除目标为停服时真实 current UTC 日目录：`/home/ubuntu/btc-updown-marketmaker/datasets/training/typed_raw/utc_day=2026-07-19`。它包含旧 build 从 UTC 00:00 到停服的产物；删除后远端不可恢复，且新 manifest 不得把这段时间伪装为已覆盖。
4. 删除前必须确认旧 service 已完成协调停止、MainPID/cgroup/数据根 lease 和全部文件 holder 已退出；目标及逐级父路径必须是预期 owner 的真实目录、非 symlink、非 mount/descendant mount，目标 manifest identity 必须仍为旧 commit `c6b8bac3c5d3ce7ea2fa689bd431cf1a8b7070de` 与旧 binary SHA `469c75af7e682bf59ce0106a8ee6d729f1f5184da16fb24ad3eae9f0b7479f78`。
5. 删除必须精确作用于上述 current-day 目录；发现路径、identity、日期、holder、mount 或 owner/type 不匹配时停止，不扩大删除范围。

## 三、构建与替换合同

1. 新 binary 必须从已 push 的固定 commit 在本地 clean detached worktree 中执行 locked release build，并记录完整 commit、SHA-256、Build ID、ELF/loader 与最高 GLIBC requirement；远端禁止编译。
2. 最高 GLIBC requirement 必须不高于远端 `2.39`。上传使用正式 binary 同目录下带 SHA 的唯一 staging 文件，先验证远端 SHA、ELF、loader、`ldd`、`--help` 与 `--version`。
3. 旧 service 停止前不得替换正式 binary；停服与删除门通过后，staging 必须在同一 filesystem 原子替换固定 binary并同步目录。
4. 仓库唯一 `Restart=on-failure` unit、ExecStart、dataset root 和数据布局保持不变。启动新程序后，同日目录由新 identity 从空状态创建；不实现 same-day identity takeover，也不混写旧 manifest。

## 四、验收标准

- service 为 `enabled + active/running`，MainPID 大于零，`NRestarts` 不持续增加，`/proc/<pid>/exe` 与正式 binary SHA 均等于新部署 SHA；
- 新 `utc_day=2026-07-19/manifest.json` 为 v3、`state=open`，build commit 为 `1d95823614ceed78288ce34cb95fbf23e58ef64d`，binary SHA、config digest、host、region 完整且 coverage `first_started_utc_ms` 为真实 current-day activation 时刻；unit 启动时刻另行记录，不混为同一事件；
- 新 current-day 目录不存在旧小时文件或旧 identity，远端 2026-07-15 与 2026-07-18 closed 日保持不变；
- RTDS 出现新程序启动后的真实 update 落盘正向证据；短时观察中不再因缺少文本 PONG 产生约 16 秒 generation/reconnect 循环；
- CLOB、Binance 与 metadata source 继续按真实情况落盘，journal 无 panic、manifest identity、writer、lease 或 queue terminal；
- 记录删除目标、删除前大小/文件数、旧/new binary SHA、启动时间、PID、manifest identity、初始 source counter 与资源证据。
