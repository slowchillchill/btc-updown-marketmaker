# SDK070 Stage3 future typed handoff mapping

事项：`data-collect-sdk070-typed-schema-live-runtime-refactor`

对应需求：
`demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`

对应计划：
`plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`

状态：`NONBLOCKING_FUTURE_SPEC`

## 1. 边界

本文只记录 `polymarket_orderbook_v3` canonical domain 到 future typed
external-market-event handoff 的当前可知静态映射，不定义或实现新的 frame、adapter、
consumer、join、explode、排序、缓存、长跑或部署。

当前 `HandoffFrame v1`、heartbeat/status runtime与legacy兼容保持不变；本文不能作为
paper/live parity或真实data-pipeline端到端证据。未知future字段不阻断source v3。

## 2. 共同envelope映射

| Canonical domain | Future typed handoff | 规则 |
| --- | --- | --- |
| `event.event_type()` | typed event discriminator / `source_event_type` | 直接映射 |
| `envelope.timestamp_received` | `collector_recv_time_utc_ms` | UTC毫秒 |
| `envelope.timestamp` | typed platform timestamp | 保持`Option<i64>`；是否兼容v1字符串留给后继frame设计 |
| `envelope.market` | `market` | condition ID；不复制为同值`market_id`/`condition_id` alias |
| `envelope.slug` | `slug` | 保持nullable |
| `envelope.collector_host_id` | 同名字段 | 直接映射 |
| `envelope.collector_region` | 同名字段 | 直接映射 |
| `envelope.source_endpoint` | 同名字段 | 直接映射 |
| `envelope.schema_version` | source payload schema version | 不得替代handoff frame schema version |
| `envelope.source_sequence_epoch_id` | 同名字段 | 直接映射 |
| `envelope.collector_admission_sequence` | 同名字段 | 不得替代transport-local `event_seq` |
| `envelope.collector_admission_time_utc_ms` | 同名字段 | 直接映射 |

当前 `HandoffFrame v1.source_identifier`在既有示例和测试中没有冻结为同一种业务
identity；其future语义由后继三件套决定，本文不把slug、condition ID或CLOB market
ID中的任一项擅自指定为该字段。

## 3. 七类event payload

字段保持canonical domain的名称、类型、nullability、decimal文本与数组顺序：

| Variant | Future typed payload |
| --- | --- |
| `book` | `asset_id`、`bids[].{price,size}`、`asks[].{price,size}`、nullable `last_trade_price` |
| `price_change` | nested `price_changes[].{asset_id,price,size,side,best_bid,best_ask}`；保持target-admitted entries的SDK原始相对顺序 |
| `tick_size_change` | `asset_id`、nullable `old_tick_size`、`new_tick_size` |
| `last_trade_price` | `asset_id`、`price`、nullable `size/side/fee_rate_bps` |
| `best_bid_ask` | `asset_id`、nullable `best_bid/best_ask/spread` |
| `new_market` | `clob_market_id`、nullable `question/description`、`asset_ids`、`outcomes`、nullable `event_message.{id,ticker,slug,title,description}` |
| `market_resolved` | `new_market` identity字段加`winning_asset_id`、`winning_outcome` |

所有decimal继续作为字符串传递，不转为浮点。future mapping不explode或重排
`price_changes`。顶层SDK/wire `id`不进入项目future合同；只使用typed入口已经映射
完成的`clob_market_id`。nested `event_message.id`是另一层event identity，不得映射为
`clob_market_id`。`book.last_trade_price`是book快照状态，不得合成独立成交事件。

## 4. 后继metadata join

| Future field | CLOB v3来源 | Future owner |
| --- | --- | --- |
| `price_to_beat: Option<String>` | 无 | 后继external-event adapter / metadata join |
| `final_price: Option<String>` | 无 | 后继external-event adapter / metadata join |

两者是相互独立的nullable decimal-string。缺少任一字段不得阻断handoff，也不得增加
closed/resolved/winner门。不得从book、成交价、winner、outcome price或metadata v4
伪造。join key、frame版本、legacy策略、缓存、freshness与因果可见时间均由后继
demand/plan/changelog冻结。

## 5. 明确不映射

- `source_native_payload_json`、`HandoffFrame v1.raw_json`或generic evidence blob；
- venue `hash`、F05 `transaction_hash`以及F01–F03、F06–F18；
- 同值顶层`id` alias或历史`assets_ids`拼写alias；
- data-pipeline派生字段，例如level count、top-5 quantity、best size、
  `price_change_count`或派生spread；
- data-pipeline/training自有的dataset/window/path/label/feature/split/approval字段；
- paper/live consumer才拥有的`runtime_receive_time_utc_ms`；
- future transport才拥有的`event_seq`与`handoff_send_time_utc_ms`。

旧v2/raw data-pipeline把同一`market`复制为`market_id`和`condition_id`，并依赖raw
JSON、hash和浮点派生。它只作为历史consumer证据，不能成为方案B的future schema
模板，也不能证明future adapter parity。

## 6. 后继仍需决定

- future frame版本与具体Rust类型；
- `HandoffFrame v1`兼容/退役策略和`source_identifier`精确语义；
- nested payload是否展开，以及唯一explode/sort/filter owner；
- metadata join key、缓存、freshness和因果可见性；
- 真实data-pipeline/paper/live adapter、模型接线、长跑与部署。
