# RTDS 阶段 0 三日参数候选与误触发报告

> 事项编号：`DC-REPAIR-20260721-RTDS-BUSINESS-SILENCE-FULL-RECONNECT`
>
> 对应需求：`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`
>
> 对应计划：`plan/data-collect-rtds-business-silence-full-reconnect-plan.md`
>
> 对应 changelog：`changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`
>
> 状态：`ANALYSIS_COMPLETE_SHARED_PARAMETERS_FROZEN`
>
> 数据范围：`/home/chillchill/new-datasets` 下 `2026-07-23`、`2026-07-24`、`2026-07-25` 三个 UTC 日；全程只读。

## 1. 结论

三日 typed raw 的本地物理完整性通过，72 个 RTDS Parquet 共 219,715 行；其中 219,712 行满足当前合格实时更新合同，另有 3 行为 `stale`。排除 2026-07-24 已由 manifest 明确记录的 collector 停机区间后，fresh 接收间隔的中位数为 `0.912s`、P99.99 为 `9.118s`，短间隔簇的最长值为 `123.823s`；其后直接跳到 `524.550s`，三日共有 9 个超过 `300s` 的长静默区间，最长为 `6012.721s`。

但 typed raw 和 day manifest 不包含每个 SDK generation 的 `Connected`、latest desired `local applied` 或 40 次自然 transport reconnect 的逐事件时间戳。因此：

- 可以计算“如果当时一直满足 `Connected + local applied`，steady threshold 最多会启动几个 recovery episode”；
- 不能确认这些候选命中中有多少在真实运行时会发生；
- 不能把“后来自动恢复 fresh”直接写成误触发，也不能把“更早 teardown 一定恢复”写成收益；
- 本报告可由现有证据辨识出的确认误触发数为 `0`，确认有效恢复数也为 `0`；这不代表真实误触发率为零，候选 episode-entry 数只是上界。

基于三日数据和上述证据边界，用户已于 2026-07-26 冻结：

1. `steady_silence_threshold = 300s`；
2. `first_fresh_observation_window = 60s`，anchor 采用当前 runtime/generation 首次同时满足 `Connected + latest desired local applied` 时的 monotonic 时刻，即取两个观测事件中较晚者；
3. cold start 从未收到过合格 fresh 时，首条 fresh watchdog 保持禁用；同一 source task 一旦收到首条合格 fresh，后续 generation 才启用该窗口。

这些值及第11节 outer backoff 现在组成完整冻结参数合同。用户已另行批准authority同步、本地实现及阶段2本地香港VPN连续8小时只读probe持续执行；当前代码只有training拥有真实RTDS adapter，新恢复行为尚未实现并将在阶段1仅对training实现与验证。future paper/live真实RTDS adapter接入时必须继承同一合同。production release和部署仍是独立边界。

## 2. 输入完整性与 provenance

| UTC 日 | Manifest SHA-256 | RTDS 文件 | RTDS 行 | 字节 | fresh | stale | fresh 首条接收 | fresh 末条接收 |
|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-07-23 | `d35b433437d3a6d3102b3f72f7213254ad1756ebe7ba8aa76e5ebea77f8e7c74` | 24 | 76,528 | 3,575,890 | 76,528 | 0 | `00:00:01.838Z` | `23:59:59.867Z` |
| 2026-07-24 | `6cf647cdb247a2a65536681e985d8f81e4131645a6a76731ba8e57e29f87851d` | 25 | 79,608 | 3,830,314 | 79,605 | 3 | `00:00:02.071Z` | `23:59:59.340Z` |
| 2026-07-25 | `d02c2b4f20632cc5e5e5defcc92e3f8dcf9b1c0f90c54d77f62ea02c9fe06872` | 23 | 63,579 | 3,002,458 | 63,579 | 0 | `00:00:01.238Z` | `23:59:59.227Z` |
| 合计 | — | 72 | 219,715 | 10,408,662 | 219,712 | 3 | — | — |

72 个 manifest entry 与磁盘路径集合、实际字节数、SHA-256、`PAR1` 头尾、footer 行数、15 列 schema、file facts、实际首尾接收时间、payload 时间和 event counts 全部一致。无 missing、extra、temp、symlink 或零行 finalized RTDS 文件。

三日共同 lineage：

```text
manifest schema=data_collect_daily_manifest_v3
state=closed
closure_reason=utc_day_barrier
collection_day_coverage.state=ended_known
build_commit=1d95823614ceed78288ce34cb95fbf23e58ef64d
collector_binary_sha256=d3377da9657d235e0f6ccfc7b096418e93d4c4b187efba35fe8f63a2825b804f
collector_host_id=ip-172-26-10-0
collector_region=eu-west-1
RTDS schema=polymarket_rtds_chainlink_btc_usd_v4
sort_order=receive_order
```

三日 RTDS manifest counter 均为 `dropped_rows=0`、`lag_events=0`、`backfill_rows=0`、`structural_dedupe_dropped_events=0`。各文件按 production numeric-part，即 manifest entry 的 `utc_hour, part` 排序后读取时，`timestamp_received` 倒退次数为 `0`。2026-07-24 manifest 数组的字面顺序把 `_06-part-1` 放在 `_06` 前，不能按 JSON 数组顺序直接拼接；复核程序先排序并最终对 fresh 行全局排序。

物理完整不等于业务连续：

- 2026-07-24 有合法的 `_06-part-1.parquet`。manifest 记录 collector 于 `2026-07-24T06:46:14.033Z` 关闭，于 `06:50:44.087Z` 日内重新启动，已知停机 `270.054s`；该停机不计入业务静默候选。
- 2026-07-25 manifest 和磁盘均无 `_19.parquet`。18 时末条 fresh 为 `18:35:07.688Z`，20 时首条 fresh 为 `20:15:20.409Z`。这不是文件传输损坏，而是 typed raw 如实记录的业务可用性缺口。
- 按合格 fresh 更新统计的 5 分钟桶中，23、24、25 日分别有 `20/288`、`6/288`、`59/288` 个空桶；24 日 `11:00–11:05Z` 桶含 stale 行但无 fresh，不能写成“所有 RTDS 行为空”。

## 3. 统计合同

合格实时更新严格使用当前生产合同：

```text
1_000_000_000_000 <= message_timestamp_ms <= 9_999_999_999_999
1_000_000_000_000 <= payload_timestamp_ms <= 9_999_999_999_999
age_ms == timestamp_received_ms - payload_timestamp_ms
topic == "crypto_prices_chainlink"
message_type == "update"
symbol == "btc/usd"
-5_000 <= age_ms <= 30_000
freshness_status == "fresh"
```

三日逐行重算 `age_ms` 和 freshness 分类均与持久化字段一致。3 条非 fresh 行都是 topic/type/symbol 正确但 age 分别为 `79,086 / 79,344 / 40,558ms` 的 `stale`；没有 invalid timestamp/topic/type/symbol 行。

typed raw 已位于现有结构去重之后，三日 manifest 的结构去重丢弃计数均为 0；219,715 个 `source_native_payload_json` 全局重复数也为 0。离线分析以相邻合格行的 `timestamp_received` 差值近似运行时 fresh inter-arrival；真正实现仍必须使用 monotonic clock，不能使用 UTC 或 payload timestamp 驱动 deadline。

已知 collector 停机跨越的相邻 fresh：

```text
2026-07-24T06:46:13.445Z -> 2026-07-24T06:50:45.624Z
raw inter-arrival=272.179s
```

该相邻项整体从阈值计数和分位数中排除；频率分母只扣除 manifest 明确给出的 `270.054s` collector 停机。因此有效观察分母为：

```text
72h - 270.054s = 71.924985h
```

这是保守的离线近似：停机前末条 fresh 到 close、restart 到首条 fresh 的合计 `2.125s` 无法从同一个跨停机相邻项中无损拆分，但远低于本报告候选阈值，不影响候选命中数。

## 4. Fresh 间隔分布

排除上述已知 collector 停机相邻项后，共有 219,710 个可比较间隔；分位数使用 `numpy.quantile(method="linear")`：

| 指标 | 间隔 |
|---|---:|
| P50 | `0.912s` |
| P90 | `1.576s` |
| P95 | `1.732s` |
| P99 | `2.177s` |
| P99.9 | `3.749s` |
| P99.99 | `9.118s` |
| 最大值 | `6012.721s` |

长尾分层：

| 区间 | 数量 |
|---|---:|
| `(5s, 10s]` | 143 |
| `(10s, 15s]` | 5 |
| `(15s, 30s]` | 0 |
| `(30s, 60s]` | 2 |
| `(60s, 120s]` | 0 |
| `(120s, 180s]` | 1 |
| `(180s, 300s]` | 0 |
| `(300s, 600s]` | 1 |
| `(600s, 1200s]` | 0 |
| `(1200s, 1800s]` | 1 |
| `(1800s, 2700s]` | 0 |
| `(2700s, 3000s]` | 4 |
| `(3000s, 3600s]` | 0 |
| `(3600s, 5400s]` | 2 |
| `> 5400s` | 1 |

`123.823s` 是 300 秒以下短间隔簇的最大值，下一项直接为 `524.550s`。这支持把 `300s` 作为短间隔簇与长静默簇之间的保守分界，但不能把 `123.823s` 宣称为协议 SLA 或所有未来“正常最长间隔”；样本只有三日。

## 5. `steady_silence_threshold` 候选

候选计数规则：相邻 fresh 间隔 `Δ > T` 时，假设整个等待期间一直满足运行时前置条件，则在前一条 fresh 后 `T` 秒形成一个 steady-threshold recovery-episode 候选入口。由于缺少逐 generation state/applied 时间戳，下表是这种 episode 入口的上界，不是实际重连或 episode 内 runtime replacement 尝试次数。

| 阈值 `T` | 三日候选触发上界 | 每有效小时 | 23/24/25 日 | 解释 |
|---:|---:|---:|---|---|
| `30s` | 12 | `0.166840` | `3 / 4 / 5` | 仅是敏感性基线，安全余量过小 |
| `60s` | 10 | `0.139034` | `2 / 3 / 5` | 仍无法覆盖三日短尾最大值 |
| `120s` | 10 | `0.139034` | `2 / 3 / 5` | 会命中 `123.823s` 间隔 |
| `180s` | 9 | `0.125130` | `2 / 2 / 5` | 样本内与 300 秒命中相同，未来余量较小 |
| `300s` | 9 | `0.125130` | `2 / 2 / 5` | 冻结前推荐，现已采纳；短尾最大值的 `2.42x`，仍能覆盖全部九个长静默 |
| `600s` | 8 | `0.111227` | `2 / 1 / 5` | 冻结前保守备选，未采纳；少一次候选，8 个共同候选的 episode-entry 时点比 300 秒阈值晚 300 秒 |
| `1800s` | 7 | `0.097324` | `2 / 0 / 5` | 过度保守；首次主动 episode-entry 不早于 last fresh 后 30 分钟 |

冻结前推荐并已由用户采纳 `300s`，理由不是“300 秒以内必然正常”，而是：

- 三日短尾最大值为 `123.823s`，180 至 300 秒之间没有样本；
- 300 秒仍命中全部 9 个长静默区间；
- 600 秒只减少 1 个候选，却让其余 8 个共同候选的首次主动 episode-entry 时点统一晚 300 秒；
- 三日样本不足以支持更激进的 120/180 秒作为生产默认值。

`600s` 与 `1800s` 均未被本次参数合同采纳；未来若要改变已冻结的 `300s`，必须重新提交参数决策。

### 5.1 300 秒候选触发明细

| UTC 日 | 前一条 fresh | 下一条 fresh | 间隔 | 300 秒候选触发时刻 |
|---|---|---|---:|---|
| 2026-07-25 | `18:35:07.688Z` | `20:15:20.409Z` | `6012.721s` | `18:40:07.688Z` |
| 2026-07-25 | `11:01:43.682Z` | `12:28:56.264Z` | `5232.582s` | `11:06:43.682Z` |
| 2026-07-23 | `22:23:04.719Z` | `23:27:53.370Z` | `3888.651s` | `22:28:04.719Z` |
| 2026-07-25 | `05:41:17.609Z` | `06:28:53.584Z` | `2855.975s` | `05:46:17.609Z` |
| 2026-07-23 | `18:01:35.012Z` | `18:48:26.001Z` | `2810.989s` | `18:06:35.012Z` |
| 2026-07-25 | `13:43:36.585Z` | `14:28:56.862Z` | `2720.277s` | `13:48:36.585Z` |
| 2026-07-25 | `07:43:49.259Z` | `08:28:53.789Z` | `2704.530s` | `07:48:49.259Z` |
| 2026-07-24 | `01:32:56.486Z` | `02:00:06.738Z` | `1630.252s` | `01:37:56.486Z` |
| 2026-07-24 | `10:59:53.637Z` | `11:08:38.187Z` | `524.550s` | `11:04:53.637Z` |

三条 `stale` 分布在相邻的两个 fresh gap 中：

```text
123.823s gap（10:57:49.814Z -> 10:59:53.637Z）：
  10:59:53.086Z

524.550s gap（10:59:53.637Z -> 11:08:38.187Z）：
  11:01:17.344Z
  11:07:05.558Z
```

它们按已批准的当前 freshness 合同不重置业务 fresh deadline。

三日样本的最长静默为 `6012.721s`，与需求 2.1 已记录的历史 `6075.343s` 数量级相同，但不是同一事件。历史事件起止为：

```text
2026-07-20T00:32:36.493Z -> 2026-07-20T02:13:51.836Z
```

因此可复核的候选时刻为：

| 阈值 | 历史 6075.343 秒事件的候选时刻 |
|---:|---|
| `300s` | `2026-07-20T00:37:36.493Z` |
| `600s` | `2026-07-20T00:42:36.493Z` |
| `1800s` | `2026-07-20T01:02:36.493Z` |

这些时刻由需求内已经核实的事故起点做确定性加法得到，满足计划 0.2.1 的历史事件算术要求；本轮指定的 23–25 日 typed raw 不包含该事件，故没有用三日数据重新验证其 state/applied 前提，也没有拿它替换本次实测最大值。

## 6. 自然恢复与“误触发”边界

三日 manifest 记录的 RTDS `reconnect_events` 分别为 `14 / 13 / 13`，合计 40，即每有效小时 `0.556135` 次。这些计数只证明同一 SDK runtime 内自然 transport generation 曾推进；manifest 没有对应 UTC 时间、generation、state 或 applied 事件，不能把 40 次 reconnect 映射到上述 9 个长静默。

| 判断项 | 可确认数量 | 说明 |
|---|---:|---|
| 300 秒 steady episode-entry 上界 | 9 | 假设触发前一直 `Connected + local applied` |
| 已确认不适用的静态 gap 候选 | 1 | `272.179s` collector 停机；影响 30–180 秒 naive 扫描，已从所有统计排除 |
| 确认会在真实 runtime 触发 | 0 | 缺少逐事件 state/applied 时间 |
| 确认误触发 | 0 | 自然恢复不等于此前 teardown 是错误动作 |
| 确认由完整换 runtime 改善 | 0 | 没有 counterfactual 新 runtime 数据 |
| 最终重新出现 fresh 的长静默 | 9 | 只证明历史最终恢复，不证明恢复原因 |

因此，对 300 秒 steady-threshold episode 入口，严格的确认误触发区间是：

```text
0 <= 实际误触发 episode-entry 数 <= 实际 steady episode-entry 数 <= 9
```

该不等式不覆盖同一 episode 内由 first-fresh timeout 和 outer backoff 形成的后续 runtime replacement 尝试；typed raw 无法给出这些尝试的上界。要在未来收窄，必须由 minimal wrapper 记录 current generation 的 state、local-applied、fresh 与自然 reconnect 时刻；不能根据 day-level reconnect counter 猜测。

## 7. `first_fresh_observation_window` 候选

### 7.1 数据能证明什么

typed raw 只保存收到的业务消息，没有保存 runtime 创建、SDK `Connected` 或 latest desired `local applied` 时刻。因此三日数据不能直接计算：

```text
local-applied-to-first-fresh
```

唯一可近似观察的已知重启样本是 2026-07-24 collector 日内 reactivation：

```text
manifest last_started=06:50:44.087Z
first fresh=06:50:45.624Z
difference=1.537s
```

但 `last_started` 不是 SDK `Connected + local applied`，且只有一个样本，不能把 `1.537s` 当作窗口依据。三日普通 fresh 间隔的 P99.99 为 `9.118s`，短尾最大值为 `123.823s`，也只能作为间接风险边界，不能替代 generation 首条 fresh 分布。

### 7.2 冻结前候选与建议

| 窗口 | 取舍 |
|---:|---|
| `30s` | 激进；离线 generic inter-arrival 上界会比 60 秒多命中 `38.859s`、`42.030s` 两段 |
| `60s` | 冻结前暂推荐，现已采纳；60/90/120 秒在三日 generic inter-arrival 上界中均命中同一 10 段，60 秒会更早触发 timeout/下一主动动作 |
| `120s` | 样本内不减少候选上界，timeout/下一主动动作时点比 60 秒晚 60 秒 |
| `300s` | 保守；比 60 秒少 1 个 generic 候选，timeout/下一主动动作时点晚 240 秒 |

冻结前暂推荐并已由用户采纳 `60s`：它是三日 generic inter-arrival 敏感性分析中，不增加 60/90/120 秒共同候选集合的最短候选。该比较仍不是 generation 首条 fresh 的直接估计，因而不是外部 SLA。未来 probe 必须单独报告实际 `local-applied-to-first-fresh` 分布；若尾部接近该窗口，必须重新提交参数决策。

冻结前建议并已由用户采纳需求 5.3.1 的 anchor：

```text
anchor = max(
  monotonic_time(current runtime/generation first observed Connected),
  monotonic_time(latest desired first observed local applied in same generation)
)
```

理由：

- 仅 `Connected` 时 latest desired 可能尚未本地 applied，提前计时会把订阅 actor 延迟算成业务静默；
- 仅 `local applied` 不足以证明当前 generation 仍 connected；
- 后到事件能唯一表达二者首次同时成立；
- `local applied` 只表示 wrapper/actor 本地完成，不得命名为 wire send、server ACK 或业务订阅确认。

当前 wrapper 内部已持有 SDK state receiver、connection generation 和 applied observation，所以不需要 vendor patch 或第二连接 owner就能设计出该 anchor；但当前 source-facing surface 还不充分：`recv_event_until()` 消费 transport state 变化而不返回 `Connecting/Reconnecting`，initial generation 的 local-applied 也可能被 freshness fence 吸收。故这里只证明 anchor 语义唯一且最小 wrapper-only 扩展有现成输入，不声称当前生产 surface 已经交付。阶段 0/1 仍须按计划验证 initial/后续 generation 与竞态。

## 8. Cold-start 候选

冻结前建议并已由用户采纳：`DISABLED`。

事实：

- 三日 typed raw 没有 source task 启动到首条 fresh 所需的 state/applied 事件；
- 只有一个 collector reactivation 到首条 fresh 的间接成功样本；
- 没有“从未收到过 fresh 且单连接 stuck”的真实样本；
- 也没有“共享上游永久不产生 fresh”的反例样本。

推断：

- 在 source task 从未建立过一次合格业务流之前，无法区分单连接 stuck、上游整体静默和订阅尚未真正生效；
- 此时启用完整 runtime replacement 可能在共享上游静默时制造无证据的持续 churn。

因此，默认先由 SDK transport recovery 处理 cold start；`ever_received_fresh=false` 时不启动 business watchdog。首条合格 fresh 到达后将 `ever_received_fresh=true` 保持到同一 source task 结束，后续 generation 才使用 60 秒 first-fresh window。未来只有可控 stuck-cold-start 测试和永久上游静默测试同时通过后，才应重新审议 `ENABLED`。

## 9. 可复核命令

Manifest 与逐文件 SHA：

```bash
for day in 2026-07-23 2026-07-24 2026-07-25; do
  base="/home/chillchill/new-datasets/$day/typed_raw/utc_day=$day"
  sha256sum "$base/manifest.json"
  (
    cd "$base" &&
    jq -r '
      .files[]
      | select(.source_kind == "polymarket_rtds_chainlink_btc_usd")
      | "\(.sha256)  \(.path)"
    ' manifest.json |
    sha256sum --check --strict -
  )
done
```

Fresh 间隔和阈值计数的最小复核程序：

```bash
python3 - <<'PY'
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

days = ("2026-07-23", "2026-07-24", "2026-07-25")
root = Path("/home/chillchill/new-datasets")
qualified = []
source_payloads = set()

for day in days:
    base = root / day / "typed_raw" / f"utc_day={day}"
    manifest = json.loads((base / "manifest.json").read_text())
    entries = sorted(
        (
            item for item in manifest["files"]
            if item["source_kind"] == "polymarket_rtds_chainlink_btc_usd"
        ),
        key=lambda item: (item["utc_hour"], item["part"]),
    )
    for item in entries:
        table = pq.read_table(
            base / item["path"],
            columns=[
                "timestamp_received",
                "message_timestamp",
                "payload_timestamp",
                "age_ms",
                "topic",
                "message_type",
                "symbol",
                "freshness_status",
                "source_native_payload_json",
            ],
        ).to_pydict()
        for row in zip(*(table[name] for name in table)):
            (
                ts,
                message_ts,
                payload_ts,
                stored_age,
                topic,
                message_type,
                symbol,
                freshness,
                source_payload,
            ) = row
            recv_ms = int(ts.timestamp() * 1000)
            message_ms = int(message_ts.timestamp() * 1000)
            payload_ms = int(payload_ts.timestamp() * 1000)
            age_ms = recv_ms - payload_ms
            valid_ts = (
                1_000_000_000_000 <= message_ms <= 9_999_999_999_999
                and 1_000_000_000_000 <= payload_ms <= 9_999_999_999_999
            )
            if not valid_ts:
                expected = "invalid_timestamp"
            elif topic != "crypto_prices_chainlink":
                expected = "invalid_topic"
            elif message_type != "update":
                expected = "invalid_type"
            elif symbol != "btc/usd":
                expected = "invalid_symbol"
            elif -5_000 <= age_ms <= 30_000:
                expected = "fresh"
            else:
                expected = "stale"
            assert stored_age == age_ms
            assert freshness == expected
            assert source_payload not in source_payloads
            source_payloads.add(source_payload)
            if expected == "fresh":
                qualified.append(ts)

qualified.sort()
gaps = [
    (right - left).total_seconds()
    for left, right in zip(qualified, qualified[1:])
    if not (
        left == datetime(2026, 7, 24, 6, 46, 13, 445000, tzinfo=timezone.utc)
        and right == datetime(2026, 7, 24, 6, 50, 45, 624000, tzinfo=timezone.utc)
    )
]

values = np.asarray(gaps)
for q in (0.5, 0.9, 0.95, 0.99, 0.999, 0.9999):
    print(q, np.quantile(values, q, method="linear"))
for threshold in (30, 60, 90, 120, 180, 300, 600, 1800):
    print(threshold, int((values > threshold).sum()))
PY
```

## 10. 用户已冻结的参数合同

用户于 2026-07-26 将以下四项作为一个参数合同明确冻结：

```text
steady_silence_threshold = 300s
first_fresh_observation_window = 60s
first_fresh_anchor = current generation Connected + latest desired local applied 的后到 monotonic 观测
cold_start_watchdog = DISABLED until ever_received_fresh
```

四项主要参数门及第11节outer hard cap已经完成；authority同步、自审与三路独立终审也已通过。承载该记录的阶段0文档commit成功push且本地HEAD、upstream与远端ref一致后，专项三件套进入`APPROVED_SHARED_RTDS_CONTRACT_STAGE0_CLOSED_STAGE1_CAPABILITY_PENDING`。三日数据只来自training typed raw，因此它证明的是training候选episode-entry上界；跨角色使用同一参数是用户冻结的共同政策合同，不冒充paper/live实测。

## 11. 步骤 0.2.4 实现常量核对

仓库当前唯一 production 私有退避数学是 `data-collect/src/live_runtime/source_recovery.rs::retry_delay()`：指数倍率 `2`、稳定 jitter `±20%`、最终 sleep 硬截断到调用方给定 cap。RTDS business outer backoff 只复用该 helper，不复用完整 `SourceRecoverySupervisor`，也不增加直接 retry 依赖。已确定的形态为：

```text
initial = 1s
multiplier = 2
jitter = deterministic ±20%
hard_cap = 300s
max_attempts = None
total_elapsed_budget = None
```

`retry_delay()`还需要一个seed。阶段1不新增随机依赖：按当前aggTrade/depth/Polymarket source-local supervisor的生产做法，在RTDS source task启动时读取一次`now_ms()`转成`u64`，该值在同一task全部outer attempts中保持稳定；attempt与source kind共同决定确定性jitter。未来paper/live adapter必须提供同样“每source task稳定”的seed语义，不要求三个独立进程产生相同delay序列。

配合已冻结的60秒首条fresh窗口，并作teardown/connect/apply耗时为零、始终`Connected + local applied`且永久无fresh的保守频率计算。统计窗口从steady threshold命中并立即完成第一次replacement的episode trigger时刻起算，记该次replacement为`t=0`，不包含此前300秒steady等待；这次即时replacement不消费首个outer retry level，下一次60秒窗口到期使用`retry_delay(attempt=1)`。下表“上界”显式采用每个尚未触顶attempt的最短`-20%` jitter；达到cap后按helper“先jitter、再硬截断”语义取`300s`。该假设用于给出replacement数量上界，不是实际seed预测：

| Outer hard cap | 饱和后完整 replacement 频率 | 第一小时上界 | 8 小时上界 |
|---|---:|---:|---:|
| `30s` | 约 40 次/小时 | 42 | 322 |
| `300s`（已冻结） | 约 10 次/小时 | 17 | 87 |

用户已选择`300s` hard cap：新runtime以后到monotonic anchor等待60秒仍无fresh，先等待当前outer delay，再完整换runtime；后续delay逐次增长且单次不超过300秒。总attempts和总episode时长均不设上限，fresh才结束episode并把outer level重置为初始值。`300s`用于限制永久停流时的socket/subscription churn，是工程政策取舍，不是三日数据推导出的RTDS业务SLA。

不再为本事项增加teardown诊断周期或probe周期环境采样常量。业务replacement只记录实际shutdown开始/完成事件；8小时probe以进程内monotonic elapsed和退出状态证明持续运行，公网出口/香港区域只在开始与结束记录。这个精简不改变上方四项业务参数和outer backoff。
