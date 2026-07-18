# 训练数据采集器直接长期部署 Changelog

> 事项编号：`DC-DEPLOY-20260718-DIRECT-LONG-RUN`
>
> 对应需求：`demand/data-collect-training-collector-direct-deployment-demand.md`
>
> 对应计划：`plan/data-collect-training-collector-direct-deployment-plan.md`

## 2026-07-18：用户取代旧 M8-A 部署编排

- 用户明确批准在数据采集服务器空闲、多日没有 current-day manifest 的冷启动事实下，从任意时间直接部署并长期运行。
- 用户明确否决 `Restart=no`；正式 collector 从第一次启动起使用仓库唯一 `Restart=on-failure` unit。
- 不再使用独立 gate unit、独立数据根、15 分钟 gate、26 小时放行门或等待 UTC 00:00。跨小时、跨 UTC 日和约 24 小时观察继续作为运行证据，但不阻止启动。
- 不删除、移动、隔离或改写历史数据。未来 same-day 不同 identity 升级/回滚不是本次冷启动，必须按当时真实状态另行决策。

## 2026-07-18：部署前只读事实

- 实现 commit、local HEAD、upstream 与 remote ref 一致：`c6b8bac3c5d3ce7ea2fa689bd431cf1a8b7070de`，subject 为 `test(data-collect): complete source lifecycle phase 8`。
- 数据采集服务器正式 unit 为 disabled/inactive/dead，`MainPID=0`、`NRestarts=0`；旧进程于 2026-07-15 协调停止。
- 2026-07-18 current-day manifest 不存在；2026-07-16 与 2026-07-17 目录也不存在；没有 `.running/.tmp/.partial/.digesting` 瞬时文件。
- 2026-07-15 legacy manifest 保持 `state=open`，`collection_day_coverage`字段缺失并按legacy unknown读取；本次禁止人为修改，允许新 runtime 按已实现的 prior-day state-aware recovery 合同中性关闭。
- 远端 unit SHA-256 为 `7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`，与仓库权威 unit 一致，无需替换。
- 远端旧 binary SHA-256 为 `ec910ccc008187b72ebcd3a2466623db1d5e6abcb27a24c2881a76e435047852`。
- 远端为 2 CPU、约 1.9 GiB RAM、无 swap，训练数据约 834 MB，磁盘约 54 GiB 可用；启动后必须持续观察资源，但这些事实不要求先运行临时 gate。
- 本地 glibc 为 2.43，远端为 2.39。只有实际 release binary 的最高 GLIBC requirement 不高于 2.39 才允许上传和替换；这是 ABI 兼容硬边界，不是运行时 gate。

## 待追加执行证据

- staging/原子安装、service enabled/active、启动 PID 和时间；
- 当日 manifest、逐 source 初始出数、journal 与资源采样；
- 长期跨小时、跨 UTC 日和约 24 小时观察由正在运行的 service 继续积累。

## 2026-07-18：合同收敛、独立审查与release build

- 部署README及现行authority已统一为：当前UTC日无任何collector产物的首次正式冷启动可任意时刻直接启用唯一`Restart=on-failure` unit；不使用`Restart=no`、独立gate、UTC边界等待或数据删除。future same-day不同identity takeover仍未获授权。
- `cargo fmt --all -- --check`、`git diff --check`均退出0；`cargo test -p data-collect --locked --test deploy_readme_contract`为5/5通过。测试只静态冻结当前部署authority，不等待真实跨小时或24小时。
- 文档/authority、生产部署顺序、测试/运行验收三路独立终审最终均为Critical 0、Important 0。成立反馈已修正：current-day前置收敛为无任何collector产物、legacy coverage是字段缺失、runtime recovery是operator不手工改写的唯一例外、restart后`reactivated_within_day`条件语义、完整五字段identity、rename后目录同步及GLIBC 2.39数值边界。
- 从clean detached worktree `/tmp/data-collect-build.fswWzI/src` 的固定实现commit构建，命令为`cargo build --release --locked -p data-collect --bin data-collect-training-collector`，独立target为`/tmp/data-collect-build.fswWzI/target`；build前后HEAD固定且worktree clean。
- release binary路径为`/tmp/data-collect-build.fswWzI/target/release/data-collect-training-collector`，SHA-256为`469c75af7e682bf59ce0106a8ee6d729f1f5184da16fb24ad3eae9f0b7479f78`，Build ID为`d357e0125d75c249d21d33205a8a1fe6a658d019`，x86-64 PIE，interpreter为`/lib64/ld-linux-x86-64.so.2`，`--help`/`--version`均退出0。
- 实际最高GLIBC requirement为`GLIBC_2.38`，不高于远端`GLIBC_2.39`，因此无需升级远端glibc，也无需替代构建环境。
