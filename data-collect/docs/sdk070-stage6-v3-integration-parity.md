# SDK070 Stage6 v3 integration and parity report

> 生命周期：`HISTORICAL_EVIDENCE_ONLY`
>
> 最终快照：`FROZEN_AT_20260730_CONSOLIDATION`
>
> 当前执行 authority：`none`
>
> superseded_by：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 读取规则：本文仅用于审计历史事实；内部“当前、下一步、待执行、命令”均属于
> 当时检查点，不得驱动当前实现、测试、部署或远端操作。
>
> 2026-07-30：本文只保留 Stage 6 parity 证据；旧 metadata v4 和 RTDS deferred
> 状态已被当前代码与后续决策取代。
>
> 事项编号：`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`
>
> 对应需求：
> `demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
>
> 对应计划：
> `plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`
>
> 对应 changelog：
> `changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md`
>
> 状态：`STAGE6_CLOSED`
>
> 稳定代码快照：
> `9e0d264058ed64cd63fbd05280f0826afaba0509`
>
> closure commit：
> `c0aff3a3e61583138b33eec44d847acaa8ebd682`

## 1. 结论与边界

本报告只收口本地 source v3 + metadata v4 的集成、范围内 parity、既有证据消费和
v3专属替代/删除矩阵。它不重复 Stage3 storage measurement、Stage5全局cleanup、
2-REST root模块化或RTDS专项验收。

2026-07-26用户批准方案A，修正计划6.3.1中超出需求的“代表性real artifact”
表述。当前parity硬门由两部分组成：

- 七类target-admitted SDK fixture经过真实source admission owner、唯一canonical
  domain和v3 Arrow codec做field-for-field round-trip；
- Stage3固定target-admitted logical fixture经正式v3 writer实际生成finalized
  Parquet并读取footer/全部列，
  状态明确为`DETERMINISTIC_FIXTURE_FINALIZED_ARTIFACT`，不是production real
  artifact。

Stage0的真实`polymarket_orderbook_v2`完整日只提供已经冻结的v2物理
schema/storage/resource/manifest事实。它没有原message boundary，且legacy v2 reader
不得重组v3 canonical domain，因此不被用来伪造real-v3 parity。

本阶段没有：

- 重跑或重新实现Stage3 bounded storage probe；
- 重扫完整UTC日、生成full-day v3、双写、sidecar或shadow replay；
- 修改`HandoffFrame v1`、hot heartbeat或实现external-market-event adapter；
- 运行public network smoke、production cutover、release、部署、生产/运行服务器
  远端操作、migration、历史rewrite或删除。

## 2. Stage6 6.1确定性门

Stage5最终修复后的稳定源码树已经执行与计划6.1完全相同的五道门，并随后作为
commit `9e0d264058ed64cd63fbd05280f0826afaba0509`发布：

```text
cargo test -p data-collect --locked -- --test-threads=1
  PASS：lib 348；全部integration与doc targets通过
cargo check -p data-collect --locked --all-targets
  PASS
cargo clippy -p data-collect --locked --all-targets -- -D warnings
  PASS
cargo fmt --all -- --check
  PASS
git diff --check
  PASS
```

Stage6按计划“完整门只运行一次”的反重复约束消费该稳定快照：

```text
STAGE6_6_1=CONSUMED_FROM_FINAL_STAGE5_SNAPSHOT
snapshot_commit=9e0d264058ed64cd63fbd05280f0826afaba0509
focused_gap_targets=0
```

在方案A冲突审计结果返回前，为把package结果直接绑定已发布commit，主代理又执行了
一次`cargo test -p data-collect --locked -- --test-threads=1`；结果仍为lib 348及
全部integration/doc targets通过。这是一次无必要的重复执行，不构成第二验收门，
没有修改代码、artifact或外部状态；Stage6没有继续重复check/clippy/fmt/diff。

只有后续出现Rust、Cargo、vendor或test tracked diff时，上述快照才失效并需要在新
候选上重新运行一次完整门。Stage6当前只产生文档diff。

## 3. AC映射

| AC | 当前证据 | Stage6结论 |
|---|---|---|
| AC-01 | Stage1 provenance报告；workspace只解析本地`0.7.0-p1`，third_party只有一个Polymarket vendor目录，当前`cargo tree -p data-collect --locked -i polymarket_client_sdk_v2`解析为v0.7.0 | `PASS_CONSUMED` |
| AC-02 | 完整suite覆盖`polymarket_sdk_patch_contract` 17项、`polymarket_sdk_runtime_contract` 19项；Stage1官方/vendor门保持不变 | `PASS_CONSUMED`；public smoke仍`NOT_AUTHORIZED / NOT_RUN` |
| AC-03 | P-B1 missing/null parser永久fixture、七类target-admission fixture及current consumer编译均在稳定快照通过 | `PASS_CONSUMED` |
| AC-04 | `polymarket_orderbook_v3_contract`的exact schema、corrupt/unknown/null负例；metadata v4 exact-prefix/null matrix；writer版本与envelope门 | `PASS` |
| AC-05 | 七类一projection一row、mixed `PriceChange` retained-order、platform duplicate/internal retry及qualified/unmatched/contradiction矩阵 | `PASS` |
| AC-06 | 七类canonical round-trip与decimal/order/nullability合同；training/data-pipeline边界明确只消费finalized typed raw且外部split/approval不属于data-collect | `PASS_WITH_SCOPE_BOUNDARY` |
| AC-07 | source key、sequence/retry、source v3 + metadata v4 claimant/restart、Gamma/exhausted、source-only/cross-pair/corrupt矩阵 | `PASS` |
| AC-08 | 七类fixture，以及Stage3另一固定fixture的确定性finalized v3 artifact；独立legacy v2 row-grain兼容；future mapping附件和binary边界核对 | `PASS_WITH_NONBLOCKING_FUTURE_SPEC` |
| AC-09 | Stage3唯一bounded measurement及当前路径/hash/status | `PASS_CONSUMED_NO_RERUN` |
| AC-10 | 由独立2-REST分支收口，不属于Stage6 v3重复验收范围；其完整门、root残留审计与三路独立终审已通过，见`data-collect/docs/sdk070-stage2-rest-modularization.md` | `PASS_CONSUMED_2_REST_CLOSED` |
| AC-11 | v3专属C01–C08由Stage5闭合；其余全局cleanup不在本报告重复汇总 | `PASS_FOR_V3_SCOPE` |
| AC-12 | RTDS专项仍按其独立参数/授权门处理 | `RTDS_PARAMETERS_DEFERRED` |
| AC-13 | cold v3 writer、manifest schema/facts、source sequence跨segment/day、startup prior-day preflight/current reconcile、lease与atomic publish合同 | `PASS` |
| AC-14 | exact owned-byte公式、decoded/global/row预算、exact-limit/oversize/nested capacity、queue/writer failure及permit释放；Stage5修复后的两个focused合同 | `PASS` |
| AC-15 | 持续执行/发布授权只覆盖本地实现、测试、审查、commit/push；本报告没有扩大production边界 | `PASS` |

没有发现上述v3 AC缺少完整suite未覆盖的target，因此没有新增focused test。

## 4. Stage3与Stage0证据消费

当前证据身份：

| 证据 | SHA-256 | 状态 |
|---|---|---|
| `data-collect/benches/polymarket_storage_ab_probe.rs` | `70c5f2dae2085d5c3584a49eb1311b2e54b81bcec08e52372dad019bd4e51dd4` | `STAGE3_STORAGE_AB_REVIEW_PASSED` |
| `data-collect/docs/sdk070-stage3-storage-ab-measurement.md` | `527c10d79d95803640980d94aacccaf79605dd565a3ce32526820b3c08380ed3` | `STAGE3_STORAGE_AB_REVIEW_PASSED` |
| `data-collect/docs/sdk070-stage0-baseline.md` | `5a3705ed5c360ec6983fdffc7947c54a716182738e55ac75b3aaca410e844a53` | `STAGE_0_CLOSED` |
| `data-collect/docs/sdk070-stage3-future-typed-handoff-mapping.md` | `9b8207c3a01b5356a1a365913d1e8368c69d686bbac36a3f39cbbb1629a31464` | `NONBLOCKING_FUTURE_SPEC` |

Stage5 all-target Clippy使probe发生机械hash漂移，已按计划6.2.1作为新diff唯一重跑并
更新报告；当前两项hash及硬门已经随`9e0d264`发布。Stage6只登记当前证据：

```text
NO_RERUN
NO_REIMPLEMENTATION
NO_SECOND_REVIEW
NO_FULL_DAY_RESCAN
```

bounded artifact事实保持：

| 指标 | v2 baseline | v3 candidate |
|---|---:|---:|
| logical messages | 8,192 | 8,192 |
| physical rows | 13,392 | 8,192 |
| finalized bytes | 229,762 | 119,696 |
| finalized SHA-256 | `71783ce3e3fd9e3e942bc4f8f3e533093b1cd41a358e6df6a4dd027b3ba3415b` | `964b9c022cb3c88a140742cc7274392c9f980ef7c34fefd05735de8b0522c223` |

v3比v2少`110,066 bytes`，即该fixture的`47.904353%`；只有
`v3 finalized bytes < v2`是storage硬门。v3 footer不存在
`source_native_payload_json`、venue `hash`或`transaction_hash`，`PriceChange`
不再fan-out。

Stage0完整日继续只报告v2 post-dedupe事实：93,481,048 rows、24 files、
6,265,404,486 bytes，manifest中的`14,664`只是不可能反演的aggregate。这里不声称
pre-dedupe replay、精确row/event差异或real-v3 forward parity。

## 5. Canonical与产品边界

canonical parity与finalized artifact分别沿正式owner验证，没有建立第二adapter：

```text
official SDK typed parser
  -> real target admission owner
  -> PolymarketOrderbookV3Projection
  -> admission coordinate
  -> Arrow RecordBatch
  -> PolymarketOrderbookV3Message

fixed target-admitted logical projection sequence
  -> PolymarketOrderbookV3Projection
  -> repo-local production TypedParquetSegmentWriter
  -> finalized v3 Parquet footer / all-column scan
```

验证结果：

- 七类variant的retained值、nullable语义、decimal文本和数组顺序一致；
- mixed `PriceChange`只保留目标entries，保持SDK原始相对顺序；
- `book.last_trade_price`保持nullable快照字段，不合成成交事件；
- `clob_market_id`、condition ID `market`与nested `event_message.id`没有alias混淆；
- unknown/corrupt/missing/null contradiction和parser error均fail closed；
- legacy v2保持独立row-grain view，不重建v3 message、admission或source reference。

从Stage3 authority-sync `b569546ae1c9cfc24bcdfe4ba12ee4ce60ccc278`到当前
`9e0d264058ed64cd63fbd05280f0826afaba0509`，下列binary/handoff边界文件没有diff：

- `data-collect/src/handoff.rs`；
- `data-collect/src/runtime.rs`；
- `data-collect/tests/hot_path_handoff_contract.rs`；
- deployment、consumer-format及paper/live boundary contracts。

因此本报告只证明这些边界在v3与cleanup期间没有改写，不宣称hot runtime、external
adapter或paper/live parity。

## 6. v3 LOC

统计边界固定为Stage3 authority-sync
`b569546ae1c9cfc24bcdfe4ba12ee4ce60ccc278`
到Stage3 closure
`7c6927f663d9c3b387874f688b10346956b9302d`。

production Rust allowlist只包含lib wiring、live runtime、Polymarket adapter/source、
metadata layout、v3 domain、resolution recovery、Polymarket source facade和writer；
test Rust单列。docs、Cargo和storage bench不计入v3 LOC：

| 范围 | added | deleted | net |
|---|---:|---:|---:|
| production Rust owner files | 5,155 | 2,388 | +2,767 |
| test Rust files | 3,838 | 313 | +3,525 |
| 合计代码与测试 | 8,993 | 2,701 | +6,292 |

Stage3边界没有Git rename/copy条目、没有whole-file删除，因此file-level
`moved=0`、`deleted files=0`。不能用`min(add, delete)`伪造moved/rewritten：

- 新增canonical domain owner：`polymarket_orderbook_v3.rs`，1,676行；
- 新增独立v3 contract test：`polymarket_orderbook_v3_contract.rs`，642行；
- 既有production owner与`lib.rs` wiring churn：`+3,479 / -2,388`；
- 既有test owner two-sided churn：`+3,196 / -313`。

2-PM机械移动属于独立前置commit范围，不混入上述v3净值。Stage5的全局U簇也不混入。
与v3直接相关的C01–C08在Stage5单列为`+183 / -3,732`、净删除3,549行；两组统计
属于不同阶段边界，不相加伪造当前总代码量。

## 7. 旧路径替代与删除矩阵

| 旧路径/语义 | current唯一替代 | 删除/保留事实 |
|---|---|---|
| source `source_native_payload_json` | typed canonical variant fields | v3 schema/footer不存在；metadata v4仍保留metadata自身traceability |
| venue book/price-change `hash` | 无；用户批准排除 | v3 schema/footer不存在；manifest finalized-file SHA继续保留 |
| payload fingerprint drop/restart restore | epoch + admission sequence ownership；平台duplicate各自保留 | C02/C03已删除；Binance/RTDS独立dedupe不受影响 |
| `PriceChange`逐change fan-out | 单row nested `price_changes` retained list | C01旧adapter/fan-out删除 |
| raw-first handler与invalid/unmatched persistence | validation -> canonical typed row；unmatched 0 row，contradiction admission前fail closed | C01旧raw adapter删除；无rejected-row/sidecar |
| Gamma-first raw-only | accepted drain内qualified late WS只写typed non-owner observation；role-exit后0 row | current source矩阵覆盖，无raw-only例外 |
| post-horizon raw-only | 0 source row | current source矩阵覆盖 |
| raw-to-metadata recovery | source v3 + metadata v4 exact claimant pair及typed identity/winner | C06 legacy raw recovery删除 |
| metadata v3 current new-write | metadata v4 exact v3 prefix + nullable claimant pair | metadata v3 schema不可变并退出current new-write |
| source v2 current new-write | source v3 canonical writer | v2 schema/row reader及W-B public offline writer继续保留 |

W-B legacy writer没有接回`LiveSegmentWriter`、current dispatch、recovery authority、
canonical production root或dual-write。

## 8. 偏差与剩余事项

已记录的偏差：

1. Stage5机械Clippy引起Stage3 probe hash漂移；已按计划作为新diff唯一重跑并发布，
   Stage6未再运行。
2. 计划6.3.1曾比需求多出“代表性real artifact”。用户批准方案A后，修正为七类
   fixture的canonical round-trip，以及Stage3另一个固定fixture经正式writer生成的
   确定性finalized v3 artifact；两套fixture不得混同，并明确后者为
   `NOT_PRODUCTION_REAL_ARTIFACT`。
3. Stage6入口在反重复审计结论返回前额外执行了一次完整package suite；结果通过，
   但不计作第二验收门，也没有继续重复其余四道门。
4. 一名独立终审者在只读Git游标核验时执行了
   `git ls-remote origin refs/heads/fix-rtds-silents`，违反该审查子任务的
   “不得网络操作”约束；返回ref为`9e0d264058ed64cd63fbd05280f0826afaba0509`，
   未写文件、未push、未改变本地或远端状态，也不计作产品验收证据。

剩余事项：

- 2-REST root及其余owner模块化、AC-10完整门与三路独立终审已完成；其闭合随
  承载2-REST记录的closure commit成功push且三方ref一致生效；
- RTDS专项继续保持真实`RTDS_PARAMETERS_DEFERRED`状态；
- first real-v3 forward run、production cutover、external adapter、paper/live接线、
  migration、deployment和删除均须另建三件套。

## 9. 独立终审

两路独立`gpt-5.6-sol`/Ultra终审最终均为：

```text
PASS / CLOSURE_UNBLOCKED
Blocker=0
Important=0
Minor=0
```

终审发现的方案A fixture身份混同、binary/handoff比较起点、LOC分类与writer归属措辞
均已修正并复审清零。两名审查者均未修改文件；第8节已单独披露其中一次只读
`git ls-remote`流程偏差。
