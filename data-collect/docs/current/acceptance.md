# data-collect 当前验收

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

## 1. 当前状态矩阵

| 合同 | 状态 |
| --- | --- |
| training 六源真实采集 | `IMPLEMENTED_AND_TESTED` |
| Cargo 三 binary 定义与 role/output config 隔离 | `IMPLEMENTED_AND_COMPONENT_TESTED` |
| Polymarket SDK 0.7.0 + 唯一 patch | `IMPLEMENTED_AND_TESTED` |
| 六个 current typed schema | `IMPLEMENTED_AND_TESTED` |
| hourly Parquet 与 daily manifest | `IMPLEMENTED_AND_TESTED` |
| collector data-bearing/admission/deferred/writer hard bounds | `IMPLEMENTED_AND_TESTED` |
| SDK outbound/ping transport-control channel hard capacity | `NOT_IMPLEMENTED_LOGICALLY_CONSTRAINED` |
| 默认 50GB；startup direct 75 / runtime coordinated 75、无自动 retention | `IMPLEMENTED_AND_TESTED` |
| RTDS 300/60 replacement、×2/±20%/300s、无独立 attempt exhaustion | `IMPLEMENTED_AND_TESTED` |
| resolution late-ignore / contradiction fail-closed、fixed identity、restart、rollover | `IMPLEMENTED_AND_TESTED` |
| paper/live frame/schema 与一次性 Unix socket helper | `IMPLEMENTED_AND_COMPONENT_TESTED` |
| paper/live binary → receipt → 长运行 heartbeat loop | `IMPLEMENTED_NOT_END_TO_END_TESTED` |
| 真实 UTC rollover/full healthy day 运行证据 | `NOT_CLAIMED_AND_NOT_A_COMPLETION_GATE` |
| paper/live 真实 external-source adapter | `DEFERRED_UNTIL_MODEL_TRAIN_COMPLETE_AND_USER_RETURN_DECISION` |

`hot_path_handoff_contract` 覆盖 frame/schema 和一次性
`run_single_frame_unix_server_handoff` helper；当前测试没有直接调用实际
`run_hot_collector` 或长运行 `run_heartbeat_handoff_server`，因此不得把 component
测试写成完整 hot binary wiring 的端到端 PASS。

paper/live 真实 adapter 不属于 `model-train`。等下一模块 `model-train`（模型训练
模块，不是 `data-collect-training-collector`）完成之后，只有在用户决定返回
`data-collect` 时，才另立三件套开发。

## 2. 格式与静态检查

```bash
cargo fmt --all -- --check
cargo check -p data-collect --locked --all-targets
cargo clippy -p data-collect --locked --all-targets -- -D warnings
git diff --check
```

## 3. Current integration targets

```bash
cargo test -p data-collect --locked \
  --test deployment_binary_contract \
  --test paper_live_boundary_contract \
  --test hot_path_handoff_contract \
  --test training_collector_runtime_contract \
  --test hourly_partition_contract \
  --test source_hourly_layout_contract \
  --test polymarket_orderbook_v3_contract \
  --test polymarket_metadata_layout_contract \
  --test parquet_writer_contract \
  --test deploy_readme_contract \
  --test polymarket_sdk_patch_contract \
  --test polymarket_sdk_runtime_contract \
  --test collector_identity_contract \
  --test manifest_contract \
  --test document_authority_contract \
  -- --test-threads=1
```

测试数量只记录实际运行快照，不是永久验收门。新增、删除或拆分测试后应更新本页的
dated result，而不是为了维持旧数字跳过测试。

## 4. Lib regression

```bash
cargo test -p data-collect --locked --lib -- \
  --test-threads=1 \
  --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
```

被精确跳过的 fixture 把运行当日 `12:00:00Z` 固定为 receive time。UTC 12:00 前运行
时，该值晚于真实 admission clock，会触发 `invalid_real_admission_clock` 并超时；
这是 wall-clock fixture 限制，不是已证明的 production runtime 失败。

UTC 12:00 后可单独运行：

```bash
cargo test -p data-collect --locked --lib \
  'live_runtime::tests::four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second' \
  -- --exact --test-threads=1
```

默认 full regression 只能精确跳过该 named test。被 `#[ignore]` 的八小时 RTDS public
probe 需要显式授权与外部条件，不属于默认本地 completion gate。

## 5. 文档 authority guard

```bash
cargo test -p data-collect --locked \
  --test document_authority_contract \
  -- --test-threads=1
```

guard 必须验证唯一 CURRENT 入口、唯一 master 三件套、六个主题 owner、归档
tag/path/blob、45 个历史路径不在活动树、四份 evidence-only banner，以及第二个
CURRENT、坏三件套引用、坏 blob、恢复历史路径和损坏 evidence banner 五类负例会被
拒绝。

## 6. 最新验证快照

2026-07-30 阶段 3 当前工作树：

- 上述 15 个 integration targets：`159 passed; 0 failed`。
- 其中 `document_authority_contract`：`4 passed; 0 failed`，包含真实 current/archive
  正向检查和不修改工作树的五类负例。
- lib regression 精确排除 named wall-clock fixture 后：
  `386 passed; 0 failed; 1 ignored; 1 filtered out`。
- `cargo fmt --all -- --check`、`cargo check -p data-collect --locked --all-targets`、
  `cargo clippy -p data-collect --locked --all-targets -- -D warnings` 与
  `git diff --check` 均通过。

以上是本次实际快照，不是永久测试数量门，也不冒充被 filtered fixture 或 ignored
public probe 已通过。
