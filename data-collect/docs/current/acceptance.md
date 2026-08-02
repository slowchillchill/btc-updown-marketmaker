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
| 训练程序真实采集六个数据源 | `IMPLEMENTED_AND_TESTED` |
| Cargo 定义三个二进制程序并隔离角色、输出配置 | `IMPLEMENTED_AND_COMPONENT_TESTED` |
| Polymarket SDK 0.7.0 加唯一补丁 | `IMPLEMENTED_AND_TESTED` |
| 六个当前类型化 schema | `IMPLEMENTED_AND_TESTED` |
| 每小时 Parquet 与每日清单 | `IMPLEMENTED_AND_TESTED` |
| 采集器承载数据、准入、延后处理及写入器的硬上限 | `IMPLEMENTED_AND_TESTED` |
| SDK 的出站请求与 `heartbeat PING` 传输控制通道的容量硬上限 | `NOT_IMPLEMENTED_LOGICALLY_CONSTRAINED` |
| 默认 50GB；启动时直接退出 75、运行时协调退出 75，不自动执行保留策略 | `IMPLEMENTED_AND_TESTED` |
| RTDS 300/60 秒替换、×2/±20%/300 秒退避、无独立尝试耗尽门 | `IMPLEMENTED_AND_TESTED` |
| 市场判定迟到事件忽略、矛盾时失败即关闭、固定身份信息、重启与 UTC 日切换 | `IMPLEMENTED_AND_TESTED` |
| 模拟盘和实盘的帧、schema 与一次性 Unix 套接字辅助函数 | `IMPLEMENTED_AND_COMPONENT_TESTED` |
| 模拟盘和实盘二进制程序 → 回执 → 长运行心跳循环 | `IMPLEMENTED_NOT_END_TO_END_TESTED` |
| 真实 UTC 日切换和完整健康 UTC 日运行证据 | `NOT_CLAIMED_AND_NOT_A_COMPLETION_GATE` |
| 模拟盘和实盘真实外部数据源适配器 | `DEFERRED_UNTIL_MODEL_TRAIN_COMPLETE_AND_USER_RETURN_DECISION` |

`hot_path_handoff_contract` 覆盖帧、schema 和一次性辅助函数
`run_single_frame_unix_server_handoff`；当前测试没有直接调用实际
`run_hot_collector` 或长运行 `run_heartbeat_handoff_server`，因此不得把组件测试写成
完整热路径二进制程序接线的端到端通过。

模拟盘和实盘真实适配器不属于 `model-train`。必须先完成 `model-train`（模型训练
模块，不是 `data-collect-training-collector`）；之后仍只有在用户决定返回
`data-collect` 时，才另立三件套开发。

## 2. 格式与静态检查

```bash
cargo fmt --all -- --check
cargo check -p data-collect --locked --all-targets
cargo clippy -p data-collect --locked --all-targets -- -D warnings
git diff --check
```

## 3. 当前集成测试目标

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
带日期的结果，而不是为了维持旧数字跳过测试。

## 4. 库回归测试

```bash
cargo test -p data-collect --locked --lib -- \
  --test-threads=1 \
  --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
```

被精确跳过的测试夹具把运行当日 `12:00:00Z` 固定为接收时间。UTC 12:00 前运行时，
该值晚于真实准入时钟，会触发 `invalid_real_admission_clock` 并超时；这是依赖实际时钟的
测试夹具限制，不是已证明的生产运行时失败。

UTC 12:00 后可单独运行：

```bash
cargo test -p data-collect --locked --lib \
  'live_runtime::tests::four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second' \
  -- --exact --test-threads=1
```

默认完整回归测试只能精确跳过该具名测试。被 `#[ignore]` 的八小时 RTDS 公开探测需要
显式授权与外部条件，不属于默认本地完成门。

## 5. 文档权威守卫

```bash
cargo test -p data-collect --locked \
  --test document_authority_contract \
  -- --test-threads=1
```

守卫必须验证唯一 `CURRENT` 入口、唯一主三件套、六个主题归属文档、归档标签、路径和
blob、45 个历史路径不在活动树、四份仅作证据文档的首屏标记，以及第二个 `CURRENT`、
错误三件套引用、错误 blob、恢复历史路径和损坏证据首屏标记五类负例会被拒绝。

## 6. 最新验证快照

2026-07-30 阶段 3 当前工作树：

- 上述 15 个集成测试目标：`159 passed; 0 failed`。
- 其中 `document_authority_contract`：`4 passed; 0 failed`，包含真实当前与归档
  正向检查和不修改工作树的五类负例。
- 库回归测试精确排除具名的实际时钟测试夹具后：
  `386 passed; 0 failed; 1 ignored; 1 filtered out`。
- `cargo fmt --all -- --check`、`cargo check -p data-collect --locked --all-targets`、
  `cargo clippy -p data-collect --locked --all-targets -- -D warnings` 与
  `git diff --check` 均通过。

以上是本次实际快照，不是永久测试数量门，也不冒充被过滤的测试夹具或被忽略的公开探测
已经通过。

## 7. 2026-08-02 跨模块文档同步验证

本次只同步当前跨模块顺序、职责术语和权威归类，不修改 `data-collect` 生产运行时、
schema、部署或远端状态。

- 首次运行 `document_authority_contract` 为 `3 passed; 1 failed`：新活动文档
  `demand/sim-system-demand.md` 尚未进入旧权威图的显式归类集合。
- 补充第二份跨模块审核中草案的无权威分类并同步契约测试后，重跑结果为
  `4 passed; 0 failed`。
- 随后扩展运行部署说明契约测试，首次结果为 `5 passed; 1 failed`：同步顺序时
  误删了用于区分 `model-train` 与 `data-collect-training-collector` 的防误读措辞。
  恢复该区分但不恢复旧“下一模块”判断后，定向契约测试全部通过。
- 最终合并验证为：`deploy_readme_contract` `6 passed; 0 failed`，
  `document_authority_contract` `4 passed; 0 failed`，`cargo fmt --all -- --check` 通过。
- 活动 `CURRENT`、当前主题归属文档、主三件套与两份跨模块需求中已不存在把
  `model-train` 称为当前“下一模块”的表述；无执行权威的历史治理记录保持不改。
- 独立最终审查最初发现 `sim-system` 第 3 节仍把模拟盘和实盘写成无条件第 5 至 8 项；
  修正为需用户届时决定返回 `data-collect` 的条件性后续路径后，复审结果为
  `Critical 0 / Important 0 / Minor 0`（三个等级均无问题）。
- `git diff --check` 通过。
