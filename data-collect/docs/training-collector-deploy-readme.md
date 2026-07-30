# data-collect 训练采集器部署说明

> 生命周期：`CURRENT_OPERATIONS_GUIDE`
>
> 当前基线：官方 Rust SDK `0.7.0` + 仓库唯一 `0.7.0-p1` patch；current new-write
> schema 为 orderbook v3、metadata v5、depth snapshot v3、depth update v4、
> aggTrade v7、RTDS v4。三个 binary 中只有 training collector 当前连接真实外部源。
>
> 2026-07-18 首次直接部署和 2026-07-19 RTDS-PONG-A 是历史部署证据，不是当前
> 重部署授权。本文只描述当前训练运行合同；任何新部署仍须按届时另行批准的需求、
> 计划和用户授权执行。

对应需求：`demand/data-collect-demand.md`

对应计划：`plan/data-collect-plan.md`

对应 changelog：`changelog/data-collect-changelog.md`

历史部署来源（只用于审计，不是当前重部署 authority）：

- `demand/data-collect-training-collector-direct-deployment-demand.md`
- `plan/data-collect-training-collector-direct-deployment-plan.md`
- `changelog/data-collect-training-collector-direct-deployment-changelog.md`

本文件只描述数据采集服务器上的训练 cold path。paper/live hot path 是分别部署、分别运行的独立角色，见 `data-collect/docs/paper-live-hot-path-integration.md`。远端服务器禁止编译 Rust。

## 权威产物与路径

本地只构建一个训练二进制：

```text
target/release/data-collect-training-collector
```

数据采集服务器只保留一个正式二进制和一个 user unit：

```text
/home/ubuntu/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector
/home/ubuntu/.config/systemd/user/data-collect-training-collector.service
```

仓库内唯一 unit 权威源是：

```text
data-collect/deploy/systemd/data-collect-training-collector.service
```

训练部署不得使用 release bundle、二进制别名、`current` symlink、独立 gate unit、retention admin 或部署 receipt。实现 commit、binary sha256 与 unit sha256 只记录到对应 changelog。

## 本地 production 构建

production binary 必须来自已经 push 的实现 commit，不使用主 worktree 的 dirty 状态或主 target cache：

1. 从该完整 commit 创建临时 detached、clean worktree。
2. 设置 `DATA_COLLECT_GIT_COMMIT=<完整 commit>`。
3. 使用该 worktree 独立的临时 target 执行：

```bash
cargo build --release --locked -p data-collect --bin data-collect-training-collector
```

4. 构建后再次确认 HEAD 未变化且 worktree clean，计算隔离 binary 的 SHA-256。
5. 先复制到主仓库 `target/release/data-collect-training-collector.local-staging-<sha256>`，复核 SHA 后原子改名为固定产物路径。
6. 删除临时 worktree 和临时 target。

paper/live 源码 binary 仍可分别用自己的单 `--bin` 命令构建，但不进入训练部署产物。

## CLI 与 systemd 合同

正式 ExecStart 固定为：

```text
/home/ubuntu/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector \
  --dataset-root /home/ubuntu/btc-updown-marketmaker/datasets \
  --collector-region eu-west-1 \
  --run-forever \
  --max-dataset-bytes 50000000000
```

长期 unit 使用：

```text
Restart=on-failure
RestartSec=10
RestartPreventExitStatus=75 76
StartLimitIntervalSec=600
StartLimitBurst=5
KillSignal=SIGTERM
TimeoutStopSec=120
```

退出码含义：

- `0`：操作员 SIGTERM 后完成协调停止。
- `75`：`dataset_capacity_reached`；已 drain/finalize 并更新 open manifest，systemd 不重启。
- `76`：不可恢复的配置、路径、恢复或数据合同错误，systemd 不重启。
- 其他非零：运行故障；由 `Restart=on-failure` 处理。

`--help` 和 `--version` 必须在创建 Tokio runtime、解析数据根、连接网络或写文件前退出 0。

## 唯一生产数据布局

```text
/home/ubuntu/btc-updown-marketmaker/datasets/training/typed_raw/
└── utc_day=YYYY-MM-DD/
    ├── manifest.json
    └── source_kind=<source_kind>/
        ├── <source_kind>_HH.parquet
        └── <source_kind>_HH-part-N.parquet
```

正常情况下每个 source/hour 只有一个 Parquet；同一小时进程重启发生文件名碰撞时才使用 `part-N`。稀疏 snapshot source 不生成空文件。写入期间只允许同目录 `*.running` 和 `manifest.json.tmp` 瞬时存在。

每个 UTC 日只能有一个 `manifest.json`。它只记录采集事实：文件路径、行数、字节数、SHA-256、六个 source counter、紧凑 source gap、build/config/host/region identity，以及直接嵌套的单一 `data_collect_collection_day_coverage_v1`。历史 aggTrade backfill/continuity字段只读兼容；新 WS-only 生产不做 REST backfill，`backfill_rows` 始终为零。日内 SIGTERM 保持当前日 `open`；只有跨日 barrier、queue/batch drain 和全部 writer finalize ack 完成后才写 `closed`。data-pipeline 只消费 `closed` 日。

生产目录不得生成 segment/run manifest、startup/retention/resource/backpressure receipt、coverage/gap/reconnect/rollover sidecar、逐事件日志、queue samples 或 validation report。详细运行错误只进入 journald。collector 不删除 raw；`datasets/training` 达到 50,000,000,000 bytes 时协调停止并退出 75，等待独立归档运维清理。

## 历史首次部署流程（禁止直接复用）

2026-07-18/19 曾执行过一次性冷启动、带 SHA staging 上传、远端 preflight、
原子替换和 `systemctl --user enable --now`。这些步骤只证明当时部署发生过，
不是当前重新部署、替换 binary、启停 service 或删除数据的授权，也不得由本节局部
摘取命令重新执行。

完整历史事实保留在第 19--23 行列出的三件套及其后继部署事项中。任何未来部署都必须
根据届时真实 commit、binary、远端 service、manifest、数据目录和运行环境，另建并批准
对应需求、计划与 changelog；本 current guide 只提供运行合同和验收观察项。

## 启动即时验证

仅在另行批准的部署或启动完成后，才按本节进行有界、只读验证；本节本身不授权启动、
停止或替换 collector：

- service 为 `active/running`，MainPID 大于零，`/proc/<pid>/exe` SHA 与部署 SHA 相同，`NRestarts` 不持续增加；
- 当前日 manifest 为顶层 v3、`state=open`，build/binary/config/host/region identity 正确，collection-day coverage v1 为 `active`；
- 若未发生同日restart，首次启动写入真实日内时间，`first_started_utc_ms == last_started_utc_ms`且`reactivated_within_day=false`；若已经发生同identity restart，则允许`true`，但first保持、last更新并与`NRestarts`、PID和journal一致；
- 已知2026-07-15 legacy open manifest被runtime中性关闭为`reconciled_legacy_coverage_unknown`，coverage仍为unknown且不伪造unclean gap；
- 六个 source counter 存在。某 source 出现 `.parquet.running` 可作为已经收行的正向证据；启动时零行和稀疏 source 暂无文件均合法；
- journal 无 panic、writer/manifest/identity/queue ownership terminal；普通 source 静默、stale、网络恢复或暂时无数据不是整体启动失败；
- 短时间重复读取 CPU、RSS、线程、FD、socket、`/proc/<pid>/io` 和磁盘余量，只用于排除启动即失控。

`.parquet.running` 不是 finalized Parquet，不能被 data-pipeline 读取。只有自然发生小时 finalize 后，才对 finalized 文件检查 Parquet 可读性、行数、SHA 和 manifest file facts。

## 持续运行与观察

正式 service 在上述即时验证后继续长期运行：

- 跨下一 UTC 小时观察实际有数据 source 的 finalize；静默 source 不要求生成空文件；
- 跨下一 UTC 日观察旧日关闭、新日 `open + active` 和 rollover；这是持续运维观察，
  不是当前模块 completion gate，无进程级故障时 PID 应保持不变；
- 约 24 小时及更长时间观察 PID/`NRestarts`、journal、逐 source 增长、RSS/FD/线程/磁盘趋势和真实连接恢复；约24小时soak只能证明观察期间通过，不能冒充每种reconnect分支都被真实触发。

这些是运行证据，不是启动前 gate。从日内开始得到的首日是部分 UTC 日，不得冒充完整 00:00--24:00 UTC collection day。systemd 因真实进程级非零故障启动同 identity 新 PID 是批准的 `Restart=on-failure` 行为，但必须记录 restart 事实；单 source 外部故障仍由同一 PID 内 owner 持续恢复。

## 失败与未来切换边界

发现本地完整性错误、启动失败或 restart storm 时，必须保留全部真实数据、journal、
manifest 与 digest 证据，不执行失败日期清理，并按另行批准的 incident/runbook 处置；
本文件不单独授权停服或修改远端状态。退出 75/76 继续由
`RestartPreventExitStatus=75 76` 阻止自动重启。

历史冷启动不授权 future same-day 不同 identity 升级、回滚或删除历史数据。需要这些
操作时，必须基于当时 manifest/coverage/service 事实另行批准；不得把历史冷启动合同
扩展成通用 takeover。

2026-07-19 的 `RTDS-PONG-A` 同日替换是单独批准并已结束的历史事项，不是 identity
takeover；其删除范围、构建、停服、原子替换和验收只作为对应历史三件套中的审计事实，
不再具有当前 authority，也不得据此扩大为通用同日升级或删除流程。
