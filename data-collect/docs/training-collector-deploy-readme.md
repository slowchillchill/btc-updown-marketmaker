# data-collect 训练采集器部署说明

对应需求：`demand/data-collect-training-collector-direct-deployment-demand.md`

对应计划：`plan/data-collect-training-collector-direct-deployment-plan.md`

对应 changelog：`changelog/data-collect-training-collector-direct-deployment-changelog.md`

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

## 任意时刻直接部署

本流程用于多日停服、当日还没有 collector manifest 的首次正式冷启动。无需等待 UTC 00:00，也不运行 `Restart=no`、独立 gate unit、独立数据根、15 分钟 gate 或 26 小时放行门。唯一正式 unit 从第一次启动起就是 `Restart=on-failure`。

上传只使用正式 binary 同目录下带 SHA 的瞬时独占 staging 路径：

```text
/home/ubuntu/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector.upload-<sha256>
```

创建前必须对目标和逐级父目录做不跟随 symlink 的 owner/type/containment 检查；staging 路径必须不存在，并使用 exclusive-create 语义上传。远端必须复核 binary SHA、ELF、owner/mode、loader、`ldd`、最高 GLIBC requirement 不高于远端 2.39，以及无副作用的 `--help`/`--version`。

原子替换前再做一次普通只读 preflight：

1. service 仍为 inactive，MainPID/cgroup 已退出，dataset-root 独占 lease 没有占用；
2. 本次首次冷启动的当前 UTC 日目录及manifest、finalized Parquet、`.running/.tmp/.partial/.digesting`等collector产物仍全部不存在；发现任何当前日产物都原样保留并停止，不得删除后绕过；
3. unit、ExecStart、binary destination、逐级父路径、owner/type/mount 与 SHA 仍符合权威合同；
4. staging 与 destination 位于同一 filesystem。

preflight 通过后，`sync` staging、原子替换固定 binary、同步 destination 所在目录、复核正式 SHA/owner/mode，然后直接执行：

```bash
systemctl --user enable --now data-collect-training-collector.service
```

unit 内容已与仓库权威源一致时不重复替换。部署命令和operator不删除、移动、隔离或改写历史 raw、Parquet 或 manifest；唯一例外是prior legacy open manifest由启动后的runtime按state-aware recovery处理。

## 启动即时验证

启动后进行有界、只读验证，不停止 collector 等待报告：

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
- 跨下一 UTC 日观察旧日关闭、新日 `open + active` 和 rollover；无进程级故障时 PID 应保持不变；
- 约 24 小时及更长时间观察 PID/`NRestarts`、journal、逐 source 增长、RSS/FD/线程/磁盘趋势和真实连接恢复；约24小时soak只能证明观察期间通过，不能冒充每种reconnect分支都被真实触发。

这些是运行证据，不是启动前 gate。从日内开始得到的首日是部分 UTC 日，不得冒充完整 00:00--24:00 UTC collection day。systemd 因真实进程级非零故障启动同 identity 新 PID 是批准的 `Restart=on-failure` 行为，但必须记录 restart 事实；单 source 外部故障仍由同一 PID 内 owner 持续恢复。

## 失败与未来切换边界

发现本地完整性错误、启动失败或 restart storm 时，停止服务并保留全部真实数据、journal、manifest 与 digest 证据，不执行失败日期清理。退出 75/76 继续由 `RestartPreventExitStatus=75 76` 阻止自动重启。

本次冷启动不授权 future same-day 不同 identity 升级、回滚或删除历史数据。需要这些操作时，必须基于当时 manifest/coverage/service 事实另行批准；不得把当前直接冷启动合同扩展成通用 takeover。

2026-07-19 的 `RTDS-PONG-A` 同日替换是单独批准的精确 current-day 删除后冷启动，不是 identity takeover；其删除范围、构建、停服、原子替换和验收只以 `demand/data-collect-rtds-pong-a-reset-deployment-demand.md`、对应计划及 changelog 为 authority，不得据此扩大为通用同日升级或删除流程。
