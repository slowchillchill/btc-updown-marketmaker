# 训练数据采集器直接长期部署 Changelog

> 事项编号：`DC-DEPLOY-20260718-DIRECT-LONG-RUN`
>
> 对应需求：`demand/data-collect-training-collector-direct-deployment-demand.md`
>
> 对应计划：`plan/data-collect-training-collector-direct-deployment-plan.md`

> 2026-07-19 RTDS-PONG-A 的当前生产重置部署改由 `changelog/data-collect-rtds-pong-a-reset-deployment-changelog.md` 记录；本文件以下内容保持 2026-07-18 首次冷启动历史事实。

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

## 2026-07-18：正式直接部署与即时验收完成

- 部署authority提交`512f427c645458ac550b0156bc8fea4b11c175d5`已push，local HEAD、upstream与`origin/after-data-plan`一致；用户无关dirty文件未进入提交。
- 本地产物原子发布到`target/release/data-collect-training-collector`后SHA仍为`469c75af7e682bf59ce0106a8ee6d729f1f5184da16fb24ad3eae9f0b7479f78`。随后以exclusive-create上传到正式binary同目录的带SHA瞬时staging文件；远端SHA、x86-64 PIE、loader、`ldd`、`--help`、`--version`及`GLIBC_2.38`全部通过。
- 最终preflight时间为`2026-07-18T10:14:08Z`：service仍`inactive/dead`、`MainPID=0`、`NRestarts=0`，dataset lease可取得，当前UTC日目录/产物不存在，unit SHA仍为`7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`，staging与destination同filesystem。
- staging完成sync后原子替换固定binary，随后同步binary及destination目录并复核owner/mode/SHA；unit内容未变。`systemctl --user enable --now data-collect-training-collector.service`于`2026-07-18T10:14:08Z`执行成功，从第一次启动即为唯一`Restart=on-failure`正式service。
- 初始`MainPID=1128932`、`NRestarts=0`、`enabled + active/running`；`/proc/1128932/exe` SHA与部署SHA一致。至`2026-07-18T10:16:04Z` PID与restart计数未变，journal只有systemd启动记录，没有panic、writer/manifest/identity/queue terminal。
- 当日manifest为`data_collect_daily_manifest_v3`、`state=open`，完整identity为build `c6b8bac3c5d3ce7ea2fa689bd431cf1a8b7070de`、上述binary SHA、config digest `028d5ecdea7094ba3467b00f97f6d61624693248447ae81d50e681b4c012732b`、host `ip-172-26-10-0`、region `eu-west-1`。coverage v1于`1784369660169`毫秒激活，`first=last`、`ended=null`、`reactivated_within_day=false`。
- 2026-07-15 legacy manifest已由runtime关闭为`reconciled_legacy_coverage_unknown`，`collection_day_coverage`字段仍缺失；既有历史gaps保留，没有被部署命令删除或回写。
- 六个source counter与六个writer `.parquet.running`均存在并由当前PID打开。`polymarket_market`文件在`10:14:35Z -> 10:15:03Z -> 10:16:04Z`从`426418 -> 2062106 -> 4647657` bytes持续增长；其余五个文件当时仍为0 bytes，因此只记录“尚无durable row正向证据”，不把打开文件或建立socket冒充已落盘行，也不因此否定runtime启动。
- 初始资源采样为RSS `55--62 MiB`、FD `21--22`、线程`9--10`，磁盘可用约`57.1 GB`；启动累计CPU从`45.3%`降至`21.6%`，包含历史数据扫描，不能冒充长期稳态结论。`/proc/io`写入持续增长，多个外部TLS/WebSocket连接已建立。
- 部署后文档/事实、生产状态与测试/验收三路独立终审均为Critical 0、Important 0，三路都判定即时验收PASS；审查期间仍为同一PID、`NRestarts=0`，没有把零字节running文件或established socket冒充typed row。
- collector现保持长期运行。跨下一UTC小时的finalized Parquet、跨下一UTC日rollover及约24小时资源/连接观察尚未自然发生，不是启动放行门，后续由用户在不中断service的情况下继续观察。
