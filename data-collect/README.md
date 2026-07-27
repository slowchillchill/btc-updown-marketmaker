# data-collect

`data-collect` 是 BTC up/down 市场数据采集模块，提供三个彼此独立的二进制程序：

| 二进制 | 当前职责 | 输出根目录 |
| --- | --- | --- |
| `data-collect-training-collector` | 从 Polymarket、Polymarket RTDS 和 Binance 采集六类 typed raw 数据，按 UTC 小时写 Parquet，并维护每日 manifest | `<dataset-root>/training/typed_raw` |
| `data-collect-paper-hot` | 在 paper 服务器本机提供 Unix socket heartbeat/status handoff | `<dataset-root>/paper/data_collect_handoff` |
| `data-collect-live-hot` | 在 live 服务器本机提供 Unix socket heartbeat/status handoff | `<dataset-root>/live/data_collect_handoff` |

只有 `data-collect-training-collector` 当前连接真实外部数据源。`paper-hot` 和
`live-hot` 当前不采集 Polymarket、RTDS 或 Binance 数据，也不负责交易、下单、撤单、
钱包、模型输入或账本逻辑。

## 训练采集器

### 数据源

训练采集器固定启用以下六类数据：

| `source_kind` | 内容 |
| --- | --- |
| `polymarket_market` | Polymarket CLOB market WebSocket 当前和下一期 BTC Up/Down 5 分钟市场的盘口与市场事件 |
| `polymarket_market_metadata` | 当前和下一期市场的发现、生命周期和 resolution metadata |
| `polymarket_rtds_chainlink_btc_usd` | Polymarket RTDS Chainlink `btc/usd` 数据及 freshness 分类；重复 payload 不重复落盘 |
| `binance_depth_snapshot` | Binance BTCUSDT 深度快照 |
| `binance_depth_update` | Binance BTCUSDT `depth@100ms` 增量及 applied/not-applied stitch verdict |
| `binance_aggtrade` | Binance BTCUSDT `aggTrade` trade 或有界 invalid-input evidence |

Polymarket 接入使用官方 `polymarket_client_sdk_v2 = 0.7.0`。外部 endpoint 由程序
固定，不通过 CLI 配置。

### 数据布局

```text
<dataset-root>/training/typed_raw/
└── utc_day=YYYY-MM-DD/
    ├── manifest.json
    └── source_kind=<source_kind>/
        ├── <source_kind>_HH.parquet
        ├── <source_kind>_HH-part-N.parquet
        └── <source_kind>_HH[-part-N].parquet.running
```

- `manifest.json` 是该 UTC 日唯一的采集事实清单。
- UTC 日和小时按 collector 的 `recv_time_utc_ms` 分区，不按外部 source timestamp
  分区。
- 当前 UTC 日的 manifest 为 `open`；完成 UTC 日切换后，旧日变为 `closed`。
- `*.parquet.running` 是当前正在写入的小时分区，不能作为 finalized Parquet 消费。
- Parquet row group 未达到 `50000` 行或 `16777216` 逻辑字节时，数据可能仍在内存，
  `.running` 文件可能暂时为 `0` 字节；segment 在小时变化、UTC 日切换或协调停止时
  finalize 为可读取的 `.parquet`。
- 同一 source/hour 发生文件名碰撞时才使用 `part-N`。
- `binance_depth_snapshot` 是启动、重连或连续性修复时产生的稀疏数据，不保证每小时
  都有文件。
- 程序使用 dataset-root 独占锁；同一个 dataset root 不能同时运行两个训练采集器。

### 参数

```text
data-collect-training-collector
  --dataset-root <absolute-path>
  [--collector-region <region>]
  [--run-forever | --max-runtime-secs <seconds>]
  [--max-dataset-bytes <bytes>]
```

| 参数 | 必需 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--dataset-root` | 是 | 无 | 数据根目录，必须是绝对路径；程序会创建目录并解析为 canonical path |
| `--collector-region` | 否 | 自动解析 | 显式值不能为空；未提供时依次尝试 `COLLECTOR_REGION`、AWS IMDS region，最后使用 `unknown` |
| `--run-forever` | 否 | `false` | 持续运行，直到收到 `SIGINT`、`SIGTERM`、容量限制或运行错误 |
| `--max-runtime-secs` | 否 | `604800` | 有界运行秒数，必须大于 `0`；不能与 `--run-forever` 同时使用 |
| `--max-dataset-bytes` | 否 | `50000000000` | `<dataset-root>/training` 的容量上限，必须大于 `0`；启动时及运行期间每 60 秒检查 |
| `-h`、`--help` | 否 | 无 | 必须单独使用；无网络或文件副作用，退出码为 `0` |
| `--version` | 否 | 无 | 必须单独使用；输出 binary 版本并以 `0` 退出 |

如果既不传 `--run-forever`，也不传 `--max-runtime-secs`，程序默认运行
`604800` 秒（7 天），不是永久运行。

### 固定运行参数

以下参数由当前程序固定，不是 CLI 开关：

| 项目 | 当前值 |
| --- | --- |
| ingress batch 发送条件 | 累计 `5000` 行，或最老数据等待 `1000ms` |
| Parquet row group flush 条件 | `50000` 行或 `16777216` 逻辑字节 |
| dataset 容量复查周期 | `60s` |
| RTDS fresh 判定 | topic 为 `crypto_prices_chainlink`、type 为 `update`、symbol 为 `btc/usd`、消息和 payload timestamp 合理，且 `接收时间 - payload时间` 位于 `-5000ms..=30000ms` |
| RTDS steady silence 阈值 | 曾收到 fresh，且当前仍 connected、订阅已本地 applied 时，连续 `300s` 没有 fresh；真实断线由 SDK 恢复 |
| RTDS 新 generation 首次 fresh 观察窗口 | 曾收到 fresh 后，connected 与订阅本地 applied 的较晚时点之后 `60s` |
| RTDS cold-start watchdog | 首次 fresh 前禁用 |
| RTDS outer backoff | 从 `1s` 开始逐次翻倍，带约 `±20%` jitter，单次最多 `300s`；fresh 后重置，总尝试次数不限 |

### 退出码

| 退出码 | 含义 |
| ---: | --- |
| `0` | 有界运行结束、`SIGINT`/`SIGTERM` 协调停止完成，或 `--help`/`--version` 成功 |
| `1` | 普通运行错误或 Tokio runtime 创建失败 |
| `75` | 达到 `--max-dataset-bytes`，完成协调停止后退出 |
| `76` | 错误带有 `operator_intervention_required:` 标记，需要操作员处理；包括被标记的配置、路径、恢复或数据合同错误 |

## Paper/live hot handoff

两个 hot binary 使用相同参数：

```text
data-collect-{paper|live}-hot
  --dataset-root <path>
  --run-id <id>
  [--max-runtime-secs <seconds>]
  [--collector-region <region>]
  [--config <path>]
```

| 参数 | 必需 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--dataset-root` | 是 | 无 | 本机数据根目录；启动时创建并 canonicalize，建议使用明确的绝对路径 |
| `--run-id` | 是 | 无 | 本次运行标识，trim 后不能为空；同时用于 startup receipt 文件名 |
| `--max-runtime-secs` | 否 | `604800` | heartbeat server 最长运行秒数，必须大于 `0` |
| `--collector-region` | 否 | 自动解析 | 非空值作为显式 region；未提供或空白时自动解析 |
| `--config` | 否 | 无 | 必须指向现有普通文件；当前实现只验证并把 canonical path 纳入 config digest，不读取业务配置内容 |

hot binary 当前没有 `--run-forever`、`--help` 或 `--version` 参数。正常跑满期限时
退出 `0`，返回错误时退出 `1`。

启动后会创建：

```text
<output-root>/
├── <run-id>.startup_receipt.json
└── hot.sock
```

本机 consumer 每次连接 `hot.sock` 时，server 返回一条 newline-delimited JSON
heartbeat frame。`hot.sock` 在 server 正常结束时删除。

## 构建

本地开发或功能验证时，从 workspace 根目录执行：

```bash
DATA_COLLECT_GIT_COMMIT="$(git rev-parse HEAD)" \
  cargo build --release --locked -p data-collect \
    --bin data-collect-training-collector \
    --bin data-collect-paper-hot \
    --bin data-collect-live-hot
```

输出文件：

```text
target/release/data-collect-training-collector
target/release/data-collect-paper-hot
target/release/data-collect-live-hot
```

正式部署必须从已经 push 的 clean commit 使用隔离 target 构建，构建前后确认 commit
和 clean 状态不变，并记录 release binary 的 SHA-256。生产服务器只运行本地构建
完成的 binary，不在服务器上执行 Rust 编译。

## 启动命令

### 有界训练采集

以下示例采集 10 分钟：

```bash
./target/release/data-collect-training-collector \
  --dataset-root /absolute/path/to/datasets \
  --collector-region eu-west-1 \
  --max-runtime-secs 600 \
  --max-dataset-bytes 50000000000
```

### 长期训练采集

```bash
./target/release/data-collect-training-collector \
  --dataset-root /absolute/path/to/datasets \
  --collector-region eu-west-1 \
  --run-forever \
  --max-dataset-bytes 50000000000
```

数据采集服务器的正式 user service 使用：

```text
/home/ubuntu/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector \
  --dataset-root /home/ubuntu/btc-updown-marketmaker/datasets \
  --collector-region eu-west-1 \
  --run-forever \
  --max-dataset-bytes 50000000000
```

仓库内 unit：

```text
data-collect/deploy/systemd/data-collect-training-collector.service
```

该 unit 已安装到以下路径并完成 `daemon-reload` 后，才能通过 systemd 启动：

```text
/home/ubuntu/.config/systemd/user/data-collect-training-collector.service
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now data-collect-training-collector.service
```

查看状态和日志：

```bash
systemctl --user status data-collect-training-collector.service
journalctl --user -u data-collect-training-collector.service -f
```

### 本地启动 Paper heartbeat handoff

以下命令用于 workspace 内本地验证。仓库当前没有 paper systemd unit；服务器运行时
应调用已经部署到该服务器的 binary，而不是保留或使用本地 `target` 目录。

```bash
./target/release/data-collect-paper-hot \
  --dataset-root /absolute/path/to/datasets \
  --run-id "paper-hot-$(date -u +%Y%m%dT%H%M%SZ)"
```

### 本地启动 Live heartbeat handoff

以下命令用于 workspace 内本地验证。仓库当前没有 live systemd unit；live 服务器
只运行本地构建后部署的 binary，不在服务器上编译。

```bash
./target/release/data-collect-live-hot \
  --dataset-root /absolute/path/to/datasets \
  --run-id "live-hot-$(date -u +%Y%m%dT%H%M%SZ)"
```
