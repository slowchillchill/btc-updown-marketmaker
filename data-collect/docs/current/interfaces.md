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

## 1. Training CLI

```text
data-collect-training-collector
  --dataset-root <absolute-path>
  [--collector-region <region>]
  [--run-forever | --max-runtime-secs <seconds>]
  [--max-dataset-bytes <bytes>]
```

- `--dataset-root` 必需且必须是绝对路径。
- 未提供 region 时依次使用 `COLLECTOR_REGION`、AWS IMDS region、`unknown`。
- 未选择 `--run-forever` 时，`--max-runtime-secs` 默认 `604800`。
- `--max-dataset-bytes` 默认 `50000000000`。
- `--help` 和 `--version` 是无网络、无文件副作用的 fast path。

## 2. Training source 与 artifact

六个 source 及 current schema 由 `current/contracts.md` 唯一规定。输出布局为：

```text
<dataset-root>/training/typed_raw/
└── utc_day=YYYY-MM-DD/
    ├── manifest.json
    └── source_kind=<source_kind>/
        ├── <source_kind>_HH.parquet
        ├── <source_kind>_HH-part-N.parquet
        └── <source_kind>_HH[-part-N].parquet.running
```

分区只按 collector receive time；source timestamp 不决定分区。snapshot 是稀疏 source，
不要求每小时都有文件。data consumer 只能读取 finalized Parquet 和满足自身准入合同的
closed 日；不得读取 `.running`。

## 3. Paper/live CLI 与当前输出

```text
data-collect-{paper|live}-hot
  --dataset-root <path>
  --run-id <id>
  [--max-runtime-secs <seconds>]
  [--collector-region <region>]
  [--config <path>]
```

- `--dataset-root` 与 `--run-id` 必需。
- 未提供 `--max-runtime-secs` 时，heartbeat server 默认运行 `604800s`（7 天）后
  正常返回。
- 非空 `--collector-region` 优先；否则依次使用 `COLLECTOR_REGION`、AWS IMDS
  region、`unknown`。
- `--config` 当前只要求能 canonicalize 为现有普通文件；规范化路径进入 config
  digest，但文件内容不被解析为业务配置，也不进入 digest。
- `--run-id` 当前只校验 trim 后非空，尚未强制为安全的单一文件名组件；receipt
  confinement 限制见 `current/known-limitations.md`。

角色派生的 handoff/socket root：

```text
<dataset-root>/paper/data_collect_handoff/
<dataset-root>/live/data_collect_handoff/
```

使用安全单一文件名组件作为 run-id 时，运行会在对应 root 写
`<run-id>.startup_receipt.json` 并创建 `hot.sock`。consumer 每次连接收到一条
newline-delimited `data_collect_handoff_v1` JSON frame；当前 server 实际发送
`frame_type=source_status`、`status=heartbeat`，正常结束时删除 socket。当前代码
没有对不可信 run-id 提供 receipt 路径 fail-closed 保证。

代码中已有 `external_market_event`、source evidence 和 depth evidence 的 versioned
frame 类型与校验器，但当前两个 hot binary 没有真实 source producer wiring。
这些类型是 future adapter 的合同基础，不是“真实市场事件流已实现”的证据。

## 4. Timestamp 与职责所有权

- training typed raw 由 collector receive/admission 时间和顺序确定因果边界。
- handoff transport-local `event_seq` 不是 source ingress 或 collector admission
  coordinate。
- future paper/live consumer 必须在本机记录自己的
  `runtime_receive_time_utc_ms`；不得用采集主机的
  `collector_recv_time_utc_ms` 冒充本机接收时间。
- paper/live runtime 自己拥有 consumer backpressure 处置和 append-only event log；
  `data-collect` 不写交易 runtime 日志，也不解释订单或 ledger。

## 5. Binary 输出

本地 release build 产生：

```text
target/release/data-collect-training-collector
target/release/data-collect-paper-hot
target/release/data-collect-live-hot
```

构建和远端状态变更边界只以 `current/operations.md` 为准。
