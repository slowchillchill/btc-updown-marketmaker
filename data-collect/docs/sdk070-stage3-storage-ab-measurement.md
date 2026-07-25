# SDK070 Stage3 bounded storage A/B measurement

> 事项编号：`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`
>
> 对应需求：
> `demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
>
> 对应计划：
> `plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`
>
> 状态：`STAGE3_STORAGE_AB_REVIEW_PASSED`
>
> probe：
> `data-collect/benches/polymarket_storage_ab_probe.rs`
>
> probe SHA-256：
> `369ee423a0f1409a7ab2766e35dafded139b482276ba44248466b934b775c261`

## 1. 边界

本报告落实用户裁决S-B。它只比较同一固定target-admitted synthetic fixture经公开
legacy v2 writer和current v3 writer产生的bounded artifact事实，不是完整日replay、
真实message频率、production cutover或盈利/延迟结论。

probe复用：

- 官方SDK typed parser；
- `PolymarketOrderbookV3Projection`与v3 sequence guard；
- 公开offline legacy `PolymarketOrderbookRow`/writer；
- 同一个`TypedParquetSegmentWriter`的Zstd、dictionary、row-group和finalize实现。

它没有修改production writer/schema/runtime，没有实现第二canonicalizer、manifest
simulator、recorder、sidecar、dual-write、shadow replay或通用benchmark框架。W-B保留
legacy writer仅用于该offline A/B、历史fixture和兼容测试；probe输出不会进入current
runtime或canonical production root。

canonical round-trip、resolution、claimant、duplicate/retry runtime ownership和
manifest startup authority继续由正式contract/startup测试证明；本probe只测artifact、
resource和scan事实，不复制这些owner。

## 2. 复现命令

```bash
cargo check -p data-collect --locked --bench polymarket_storage_ab_probe
cargo bench -p data-collect --locked --bench polymarket_storage_ab_probe
```

probe默认在`target/tmp`创建唯一临时目录，完成两侧write和scan后删除。只有显式设置
`DATA_COLLECT_KEEP_STORAGE_AB_PROBE`时才保留临时Parquet。运行不访问网络。

## 3. Fixture

fixture版本为`polymarket_storage_ab_fixture_v1`，logical input SHA-256为：

`25964d5b874cb202519596b0fb20e4ed7af2b6a74fc36ad21d3590a7ffe6331f`

| variant | logical messages |
|---|---:|
| `price_change` | 7,800 |
| `book` | 151 |
| `best_bid_ask` | 145 |
| `last_trade_price` | 80 |
| `tick_size_change` | 8 |
| `new_market` | 4 |
| `market_resolved` | 4 |
| **总计** | **8,192** |

这是固定synthetic bulk mix，不声称从完整日physical rows反推出真实logical-message
分布。mixed `PriceChange`按`1/2/3` entry循环；第三个entry为非目标，v2/v3都从同一
SDK message和同一matched-assets集合只保留目标entries。共保留13,000个目标
price-change entries。

最后两条book payload完全相同，但logical delivery id和v3 admission sequence不同；
两条均持久化。v3 writer随后以相同既有source key尝试一次internal retry并按sequence
guard拒绝，final row count不增加。retry显式引用`logical_delivery_id=1`，该引用也纳入
fixture digest；它不是伪造的第三条platform delivery。

两侧共同配置：

| 项目 | 值 |
|---|---:|
| Zstd level | `3` |
| row-group max rows | `50,000` |
| row-group max logical bytes | `16,777,216` |
| scan batch rows | `65,536` |

## 4. 2026-07-25本地结果

运行环境：

- Linux `7.0.0-28-generic`，`x86_64`；
- AMD RYZEN AI MAX+ 395，32 logical CPUs；
- `120,909,956 KiB`系统内存；
- `rustc 1.95.0 (59807616e 2026-04-14)`；
- `cargo 1.95.0 (f2d3ce0bd 2026-03-21)`。

| 指标 | v2 baseline | v3 candidate |
|---|---:|---:|
| logical messages | 8,192 | 8,192 |
| physical rows | 13,392 | 8,192 |
| finalized bytes | 229,762 | 119,696 |
| finalized SHA-256 | `71783ce3e3fd9e3e942bc4f8f3e533093b1cd41a358e6df6a4dd027b3ba3415b` | `964b9c022cb3c88a140742cc7274392c9f980ef7c34fefd05735de8b0522c223` |
| row groups | 1 | 1 |
| leaf columns | 28 | 45 |
| write + finalize wall | 75.709 ms | 43.113 ms |
| finalize wall | 11.187 ms | 7.013 ms |
| process user CPU | 51.863 ms | 32.354 ms |
| process system CPU | 14.039 ms | 5.043 ms |
| logical messages/s | 108,203.78 | 190,012.29 |
| case peak RSS | 54,832 KiB | 35,740 KiB |
| all-column scan rows | 13,392 | 8,192 |
| all-column scan wall | 6.998 ms | 2.998 ms |
| scan rows/s | 1,913,689.63 | 2,732,488.33 |
| scan-process peak RSS | 20,704 KiB | 14,652 KiB |

v3减少`110,066 bytes`，即该fixture的`47.904353%`。只有`v3 bytes < v2`是硬门；
实际比例、CPU、吞吐、RSS、finalize和scan均只是本机报告项。

v2关键column-chunk事实：

| column | compressed bytes | uncompressed bytes |
|---|---:|---:|
| `source_native_payload_json` | 113,116 | 5,155,697 |
| `hash` | 36,178 | 416,098 |

v3 footer不存在`source_native_payload_json`、venue `hash`或`transaction_hash`。
`price_changes`、`bids`、`asks`、`asset_ids`、`outcomes`和`event_message`均为v3
nested leaf paths；完整逐column结果由probe每次以JSON打印。

event row事实：

| variant | v2 rows | v3 rows |
|---|---:|---:|
| `price_change` | 13,000 | 7,800 |
| `book` | 151 | 151 |
| `best_bid_ask` | 145 | 145 |
| `last_trade_price` | 80 | 80 |
| `tick_size_change` | 8 | 8 |
| `new_market` | 4 | 4 |
| `market_resolved` | 4 | 4 |

## 5. 硬门结果

- 两侧fixture digest、输入顺序、compression和row-group合同一致：`PASS`；
- 七类SDK market-data variant均覆盖：`PASS`；
- v2 matched `PriceChange` fan-out存在，v3一条logical delivery一行：`PASS`；
- 两条相同platform delivery均保留不同source key：`PASS`；
- internal retry被sequence guard拒绝且不产生额外row：`PASS`；
- v2 payload/hash column chunks非空，v3三项排除列不存在：`PASS`；
- v3 finalized bytes小于v2：`PASS`；
- writer、footer和全列scan row facts一致：`PASS`。

scan在独立进程中先请求`POSIX_FADV_DONTNEED`，两侧请求均返回成功。该接口是kernel
advisory，因此这里只能写“cache-drop requested + separate-process all-column scan”，
不能宣称已证明绝对物理cold-cache I/O，也没有操作全局`drop_caches`。

## 6. 正式语义证据索引

probe不重复实现canonical、recovery或manifest owner；这些语义由下列Stage3正式测试
证明。本表命令均于2026-07-25在当前Stage3 diff上以`--locked`执行并通过。

| 语义 owner | 测试路径与名称 | 命令与结果 |
|---|---|---|
| v3 canonical round-trip、七类SDK variant、mixed `PriceChange`顺序、platform duplicate/internal retry | `data-collect/tests/polymarket_orderbook_v3_contract.rs`；该target中的`polymarket_orderbook_v3_schema_contract_is_exact`及六项`polymarket_v3_*`合同 | `cargo test -p data-collect --locked --test polymarket_orderbook_v3_contract -- --test-threads=1`；`7/7 PASS` |
| writer source-key顺序、gap、duplicate/retry及generic batch bypass | `data-collect/tests/parquet_writer_contract.rs`；五项`polymarket_v3_writer_*`合同，包括`polymarket_v3_writer_state_rejects_retry_and_backward_sequence_across_segments` | `cargo test -p data-collect --locked --test parquet_writer_contract polymarket_v3_writer -- --test-threads=1`；`5/5 PASS` |
| current source-v3/metadata-v4 claimant、Gamma、exhausted、source-only与cross-pair recovery | `data-collect/src/polymarket_resolution_recovery.rs`；七项`polymarket_resolution_recovery::tests::current_v3_v4_*`合同 | `cargo test -p data-collect --locked --lib polymarket_resolution_recovery::tests::current_v3_v4_ -- --test-threads=1`；`7/7 PASS` |
| startup current-manifest reconcile及跨UTC日prior-day close先于source construction | `data-collect/src/live_runtime.rs`；`startup_recovery_crossing_midnight_closes_prior_day_before_source_construction`、`startup_recovery_clock_regression_fails_closed_at_every_sample_boundary` | `cargo test -p data-collect --locked --lib startup_recovery_ -- --test-threads=1`；`2/2 PASS` |
| prior-day manifest file facts/schema/identity preflight | `data-collect/src/live_runtime.rs`；`binance_recovery_rejects_closed_day_file_set_changes_before_source_start`、`binance_recovery_rejects_manifest_schema_and_identity_corruption_before_source_start` | `cargo test -p data-collect --locked --lib binance_recovery_rejects_ -- --test-threads=1`；`2/2 PASS` |
| production dataset-root lease单owner | `data-collect/src/live_runtime.rs`；`dataset_root_lock_allows_only_one_manifest_owner_process` | `cargo test -p data-collect --locked --lib dataset_root_lock_allows_only_one_manifest_owner_process -- --test-threads=1`；`1/1 PASS` |

这里的manifest证据对应M-B：production调用链在dataset-root lease内先完成prior-day
preflight和current reconcile，再调用公开`(dataset_root, time)` recovery；本报告和
recovery API都不新增manifest snapshot或第二校验路径。

## 7. 完整日与Stage6边界

完整UTC日只消费
`data-collect/docs/sdk070-stage0-baseline.md`
（SHA-256
`5a3705ed5c360ec6983fdffc7947c54a716182738e55ac75b3aaca410e844a53`）
已经冻结的post-dedupe v2 schema/storage/resource/manifest事实。现有artifact没有
原message boundary，被丢消息也只剩`14,664` aggregate，因此本阶段没有重扫完整日、
生成full-day v3、声称pre-dedupe replay或精确full-day row/event差异。

Stage6只消费本probe/report的路径、hash和Stage3 closure状态：
`NO_RERUN / NO_REIMPLEMENTATION / NO_SECOND_REVIEW`。
