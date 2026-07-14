# data-collect 训练采集器部署说明

对应需求：`demand/data-collect-reliability-repair-demand.md`

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

每个 UTC 日只能有一个 `manifest.json`。它只记录采集事实：文件路径、行数、字节数、SHA-256、逐 source 首末接收时间与写入/结构去重/backfill/lag/reconnect/drop 计数、紧凑 gap、Binance 连续性状态及 build/config/host/region identity。日内 SIGTERM 保持当前日 `open`；只有跨日 barrier、queue/batch drain 和全部 writer finalize ack 完成后才写 `closed`。data-pipeline 只消费 `closed` 日。

生产目录不得生成 segment/run manifest、startup/retention/resource/backpressure receipt、coverage/gap/reconnect/rollover sidecar、逐事件日志、queue samples 或 validation report。详细运行错误只进入 journald。collector 不删除 raw；`datasets/training` 达到 50,000,000,000 bytes 时协调停止并退出 75，等待独立归档运维清理。

## 停旧服务前的 staging 预检

首次切换前，旧服务继续运行。新 binary 与 unit 只允许存在于带 SHA 的瞬时独占 staging 路径：

```text
/home/ubuntu/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector.upload-<sha256>
/home/ubuntu/.config/systemd/user/data-collect-training-collector.staging-<sha256>.service
```

创建前必须对目标和逐级父目录做不跟随 symlink 的 owner/type/containment 检查；staging 路径必须不存在，并使用 exclusive-create 语义上传。远端必须复核 binary SHA、ELF、owner/mode、loader、`ldd`、GLIBC 兼容性以及无副作用的 `--help`/`--version`。

gate unit 必须由仓库权威 unit 机械生成，只把 `Restart=on-failure` 改为 `Restart=no`；不得维护第二份 gate 源文件。`systemd-analyze --user verify` 通过前不得替换正式 binary/unit，也不得停止旧服务。

## 破坏性切换与 gate

只有实现独立审查、production build 和全部 staging 预检均通过后，才按批准计划执行：

1. 重新核对旧 unit、ExecStart、MainPID/cgroup、`/proc/<pid>/exe`、binary SHA、目标路径 owner/type/mount/打开句柄。
2. 协调停止旧服务；再次确认所有删除目标无 mount、无打开句柄且身份与白名单一致。
3. 只删除批准白名单内的旧程序和旧数据。
4. 依次把 staging binary 和 `Restart=no` unit 原子替换到固定正式路径；任一部分替换中断都保持 inactive，并把实际 old/new digest 组合记入 changelog。
5. 在非 23 UTC 的小时边界前，以最终 ExecStart 和同一生产数据根启动 gate，跨小时运行至少 15 分钟。

gate 不使用独立数据根、不生成验证 sidecar，也不 `enable`。直接检查：

- service/journal 没有 panic、drop、不可解释 gap 或 restart；
- 已 finalized Parquet 可读，行数和 manifest 文件事实一致；
- 生产根除 Parquet、每日唯一 manifest 和允许的瞬时文件外没有其他产物；
- `*.running` 与 `manifest.json.tmp` 在协调停止后不存在；
- 当前日 manifest 保持 `open`，跨日只有满足完整 barrier/ack 合同才 `closed`。

gate 通过并完成阶段独立审查后，只把同一正式 unit 的 `Restart=no` 原子更新为仓库权威的 `Restart=on-failure`，然后 daemon-reload 与 enable；不重启正在运行的 collector。

## 长期验收

长期 unit 必须连续运行至少 26 小时，并实际覆盖一个 Binance WebSocket 连接的 24 小时平台生命周期、至少一个 UTC 日切换和至少 312 个 nominal 5 分钟时槽。任何 systemd 进程退出或 MainPID 变化都重置 26 小时窗口；Binance 连接提前重连则重置其 24 小时子窗口。

验收直接读取 closed manifest、Parquet 必要列/metadata 和 journald，不生成独立报告。发现 code、unit、数据正确性或产物合同问题时必须立即 `disable --now`，完成协调停止并按失败 build/day 身份清理；身份混合或无法证明时停止并请求用户决策。

## 回滚边界

本流程不保留服务器端旧 release bundle，因此不存在 symlink 回滚。gate 或长期版本失败时保持服务 inactive，在 changelog 记录 `failed_gate`、`failed_long_running` 或 `partial_replace` 以及 binary/unit digest 和清理结果；修复版重新完成本地验证、独立审查、已 push commit 构建和 staging 预检后再切换。不得自行恢复已批准永久删除的旧 collector 或旧数据。
