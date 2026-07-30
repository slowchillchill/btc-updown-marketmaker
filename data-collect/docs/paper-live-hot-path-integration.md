# paper/live hot path integration

对应需求：`demand/data-collect-demand.md`

> 当前状态：`DEFERRED_UNTIL_TRAINING_MODULE_COMPLETE`
>
> 2026-07-30 用户决策：paper/live 真实外部市场源 adapter 延后到训练模块完成后，
> 再以独立 demand/plan/changelog 返回实施、测试和部署。该延期不阻断当前
> `data-collect` 模块收尾；本文件保留 future 接入合同，不表示当前已经实现。

本阶段不自动部署到 AWS paper/live 服务器。未来部署 `data-collect-paper-hot` 或 `data-collect-live-hot` 必须先获得用户单独批准。二者是分别部署在 paper/live 服务器上的独立 binary，不由训练 collector 或交易主程序兼任。live 服务器仍是运行目标，不是构建机，只能复制已构建 binary 和必要 runtime config；`data-collect-live-hot` 必须安装在 `/home/ubuntu/live-trade/data-collect/current/bin/` 下。

当前阶段只验证 frame contract、heartbeat/status server 和 fake consumer；不宣称 paper/live 二进制已在交易服务器完成真实 Polymarket/Binance/RTDS `external_market_event` 长跑验收。真实 paper/live 外部市场源 handoff 部署前，必须另行获得用户批准，并由对应 paper/live runtime 写入本机 runtime receive timestamp 和自身 append-only event log。

2026-07-26 已冻结future RTDS共同恢复合同：以后paper/live任一真实RTDS adapter接入时，必须与training使用相同合格fresh分类、`steady_silence_threshold=300s`、`first_fresh_observation_window=60s`、`Connected`与latest desired本地applied的后到monotonic anchor、首次fresh前禁用cold-start watchdog，以及`1s × 2 ±20%`、单次最长`300s`、fresh重置、总尝试不限的outer backoff。必须由唯一owner严格break-before-make，旧runtime真实完成后才能创建新runtime，任意时刻最多一个RTDS socket，每条connection只应用一次完整`btc/usd` filtered desired。

该共同合同不表示当前paper/live已经实现或验证RTDS。future adapter仍须另立demand/plan/changelog，补齐本机handoff sink、runtime receive、背压、append-only log、进程重启后`ever_received_fresh=false`、长跑、资源与部署验收；不得启动或复用training专属Parquet、manifest、dedupe-restore和`SourceEventSink`路径，也不得另选不同RTDS恢复语义。

PMXT-like hourly parquet 只属于训练 cold path：`data-collect-training-collector` 在数据采集服务器写 finalized typed raw parquet 和每日唯一 `manifest.json`。paper/live hot path 仍消费本机 realtime handoff frame，不能等待 finalized hourly parquet，也不能把 hourly parquet 当成实时唯一数据来源。paper/live 可以把 hourly parquet 用于审计或离线对照，但实时交易可见性必须来自本机 handoff receive time 和对应 runtime append-only log。

本集成说明不生成 labels、features、tick_stream、replay context 或训练切分；这些属于 `data-pipeline` / 训练链路。`data-pipeline` 只消费 `state=closed` 日期的 finalized typed raw parquet 和每日 manifest，再生成下游训练产物。coverage、gap、reconnect、drop 与 backfill 等采集事实收敛在该 manifest 内，不使用独立 sidecar 或 receipt。

## 三服务器拓扑

| 服务器 | 二进制 | CLI dataset root | effective output root | 职责 |
| --- | --- | --- | --- | --- |
| 数据采集服务器 | `data-collect-training-collector` | `~/btc-updown-marketmaker/datasets` | `~/btc-updown-marketmaker/datasets/training/typed_raw` | finalized typed raw parquet、每日唯一 manifest；达到 50GB 时协调退出 75，不删除 raw |
| paper 服务器 | `data-collect-paper-hot` | `~/btc-updown-marketmaker/datasets` | `~/btc-updown-marketmaker/datasets/paper/data_collect_handoff` | 本机 hot path handoff frame contract、heartbeat/status、future external market frame 边界 |
| live 服务器 | `data-collect-live-hot` | `~/btc-updown-marketmaker/datasets` | `~/btc-updown-marketmaker/datasets/live/data_collect_handoff` | 本机 hot path handoff frame contract、heartbeat/status、future external market frame 边界 |

## 启动命令模板

paper 服务器模板：

```bash
~/btc-updown-marketmaker/data-collect/current/bin/data-collect-paper-hot --dataset-root ~/btc-updown-marketmaker/datasets --run-id paper-hot-$(date -u +%Y%m%dT%H%M%SZ)
```

live 服务器模板：

```bash
/home/ubuntu/live-trade/data-collect/current/bin/data-collect-live-hot --dataset-root ~/btc-updown-marketmaker/datasets --run-id live-hot-$(date -u +%Y%m%dT%H%M%SZ)
```

上述命令是未来部署前的调用模板，不是本阶段已授权在 paper/live 服务器执行的命令。当前实现会启动本机 socket heartbeat/status handoff server；真实 `external_market_event` source adapter 接入必须在 paper/live 独立实施/部署事项中验收，其中RTDS adapter必须满足上方已冻结的共同恢复合同。

## local stream

默认本机 Unix socket：

```text
~/btc-updown-marketmaker/datasets/paper/data_collect_handoff/hot.sock
~/btc-updown-marketmaker/datasets/live/data_collect_handoff/hot.sock
```

frame 为 newline-delimited JSON，`frame_schema_version=data_collect_handoff_v1`。

## frame schema

`external_market_event` frame 至少包含：

```json
{
  "frame_type": "external_market_event",
  "event_seq": 1,
  "source_kind": "polymarket_market",
  "source_event_type": "book",
  "source_identifier": "btc-updown-5m-...",
  "source_timestamp": "1767225600000",
  "collector_recv_time_utc_ms": 1767225600010,
  "handoff_send_time_utc_ms": 1767225600011,
  "collector_host_id": "ip-...",
  "collector_region": "eu-west-1",
  "raw_json": "{\"event_type\":\"book\"}"
}
```

status frame 类型：

```text
source_status
source_gap
reconnect_trace
backpressure_state
heartbeat
```

paper/live runtime 连接本机 socket 后，收到每个 frame 的第一件事是记录本机 `runtime_receive_time_utc_ms`，再写入自身 append-only event log 或 Market Context Builder 输入。paper/live 以本机 `runtime_receive_time_utc_ms` 作为 causal cutoff 可见性主轴，不能用训练服务器 `recv_time_utc` 替代。

## fail closed 与 degraded

- fail closed：路径越界、socket 不在 server-local dataset root 下、frame schema 不可解析、runtime 无法记录 `runtime_receive_time_utc_ms`、source frame 顺序不可解释、send timeout/drop 破坏不可丢边界。
- degraded：source stale、source_gap、reconnect_trace、backpressure_state、短暂 queue high、单源缺口但 frame 边界仍保全。

`data-collect` 不接管交易逻辑、钱包、下单、撤单、NOE/ledger、order evidence、model input assembly、submit/cancel path 或 execution feedback。paper/live runtime 接入后必须由对应 runtime 写自身 append-only event log、runtime receive timestamp、Market Context Builder 输出、NOE/ledger 或交易证据；`data-collect` 只提供外部市场源 handoff。
