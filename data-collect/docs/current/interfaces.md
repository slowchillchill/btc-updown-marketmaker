# data-collect 当前接口与产物

> 生命周期：`CURRENT_CONTRACT`
>
> 当前执行 authority：`data-collect/docs/CURRENT.md`
>
> 当前需求：`demand/data-collect-demand.md`
>
> 当前计划：`plan/data-collect-plan.md`
>
> 当前 changelog：`changelog/data-collect-changelog.md`
>
> 历史读取入口：`data-collect/docs/archive-manifest.tsv`
>
> 执行授权：`none`

## 1. 训练程序 CLI

```text
data-collect-training-collector
  --dataset-root <absolute-path>
  [--collector-region <region>]
  [--run-forever | --max-runtime-secs <seconds>]
  [--max-dataset-bytes <bytes>]
```

- `--dataset-root` 必需且必须是绝对路径。
- 未提供区域时依次使用 `COLLECTOR_REGION`、AWS IMDS 区域、`unknown`。
- 未选择 `--run-forever` 时，`--max-runtime-secs` 默认 `604800`。
- `--max-dataset-bytes` 默认 `50000000000`。
- `--help` 和 `--version` 是无网络、无文件副作用的快速路径。

## 2. 训练数据源与产物

六个数据源及当前 schema 由 `current/contracts.md` 唯一规定。输出布局为：

```text
<dataset-root>/training/typed_raw/
└── utc_day=YYYY-MM-DD/
    ├── manifest.json
    └── source_kind=<source_kind>/
        ├── <source_kind>_HH.parquet
        ├── <source_kind>_HH-part-N.parquet
        └── <source_kind>_HH[-part-N].parquet.running
```

分区只按采集器接收时间；数据源时间戳不决定分区。快照是稀疏数据源，不要求每小时都有
文件。数据消费方只能读取最终化 Parquet 和满足自身准入合同的已关闭日期；不得读取
`.running`。

在离线训练链路中，上述直接消费方只允许是 `ConsumerKind::DataPipeline`。
历史名称 `ConsumerKind::Training` 表示下游 `model-train`，不得直接读取该目录；它只能
消费经过唯一 `data-pipeline(training)` 规范化、准入和发布的版本化输出。

## 3. 模拟盘、实盘 CLI 与当前输出

```text
data-collect-{paper|live}-hot
  --dataset-root <path>
  --run-id <id>
  [--max-runtime-secs <seconds>]
  [--collector-region <region>]
  [--config <path>]
```

- `--dataset-root` 与 `--run-id` 必需。
- 未提供 `--max-runtime-secs` 时，心跳服务端默认运行 `604800s`（7 天）后
  正常返回。
- 非空 `--collector-region` 优先；否则依次使用 `COLLECTOR_REGION`、AWS IMDS
  区域、`unknown`。
- `--config` 当前只要求能规范化为现有普通文件；规范化路径进入配置摘要，但
  文件内容不被解析为业务配置，也不进入摘要。
- `--run-id` 当前只校验去除首尾空白后非空，尚未强制为安全的单一文件名组件；回执
  路径限制见 `current/known-limitations.md`。

角色派生的交接和套接字根目录：

```text
<dataset-root>/paper/data_collect_handoff/
<dataset-root>/live/data_collect_handoff/
```

使用安全单一文件名组件作为运行标识时，程序会在对应根目录写
`<run-id>.startup_receipt.json` 并创建 `hot.sock`。消费方每次连接收到一条以换行符
分隔的 `data_collect_handoff_v1` JSON 帧；当前服务端实际发送
`frame_type=source_status`、`status=heartbeat`，正常结束时删除套接字。当前代码
没有对不可信运行标识提供回执路径失败即关闭保证。

代码中已有 `external_market_event`、数据源证据和深度证据的版本化帧类型与校验器，
但当前两个热路径二进制程序没有真实数据源生产者接线。这些类型是后续适配器的合同基础，
不是“真实市场事件流已实现”的证据。

## 4. 时间戳与职责所有权

- 训练类型化原始数据由采集器接收、准入时间和顺序确定因果边界。
- 交接通道内的 `event_seq` 不是数据源入站或采集器准入坐标。
- 后续模拟盘和实盘消费方必须在本机记录自己的
  `runtime_receive_time_utc_ms`；不得用采集主机的
  `collector_recv_time_utc_ms` 冒充本机接收时间。
- 模拟盘和实盘运行时自己拥有消费方背压处置和仅追加事件日志；`data-collect` 不写交易
  运行时日志，也不解释订单或账本。

## 5. 二进制文件输出

本地发布构建产生：

```text
target/release/data-collect-training-collector
target/release/data-collect-paper-hot
target/release/data-collect-live-hot
```

构建和远端状态变更边界只以 `current/operations.md` 为准。
