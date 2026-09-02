# data-collect 当前验收

> 生命周期：`CURRENT_REFERENCE`
>
> 当前执行 authority：`none`
>
> 当前任务事实源：`https://github.com/qiuqiuaiweb3/btc-updown-marketmaker/issues/21`
>
> 历史读取入口：`data-collect/docs/archive-manifest.tsv`
>
> 执行授权：`none`

本文是从 GitHub 模块 Issue #21 和真实实现派生的非权威参考，不得新增或改变 Issue 语义；出现不一致时按
`data-collect/docs/CURRENT.md` 的冲突门处理。

## 1. 当前状态矩阵

| 合同 | 状态 |
| --- | --- |
| 训练程序真实采集六个数据源 | `IMPLEMENTED_AND_TESTED` |
| Cargo 定义三个生产角色二进制程序并隔离角色、输出配置 | `IMPLEMENTED_AND_COMPONENT_TESTED` |
| `real_source_alignment_probe` 调用生产训练采集入口并只独立汇总生产产物，不作为 bench 执行业务入口 | `IMPLEMENTED_AND_TESTED_LOCALLY` |
| Polymarket SDK 0.7.0 加唯一补丁 | `IMPLEMENTED_AND_TESTED` |
| 六个当前类型化 schema | `IMPLEMENTED_AND_TESTED` |
| 每小时 Parquet 与每日清单 | `IMPLEMENTED_AND_TESTED` |
| 采集器承载数据、准入、延后处理及写入器的硬上限 | `IMPLEMENTED_AND_TESTED` |
| SDK 的出站请求与 `heartbeat PING` 传输控制通道的容量硬上限 | `NOT_IMPLEMENTED_LOGICALLY_CONSTRAINED` |
| 当前生产默认 50GB；启动时达到上限直接退出 75 | `IMPLEMENTED_AND_TESTED` |
| 当前生产保留启动 50GB 扫描并移除运行期扫描 | `IMPLEMENTED_TESTED_AND_DEPLOYED` |
| 30GB 每日 UTC 12:00 外部清理每轮只删除最早一个合格日期 | `IMPLEMENTED_TESTED_DEPLOYED_AND_ENABLED_FIRST_TRIGGER_PENDING` |
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

外部清理候选和真实源验证工具的本地定向验证为：

```bash
bash -n data-collect/deploy/scripts/prune-oldest-utc-day.sh
bash -n data-collect/deploy/tests/prune-oldest-utc-day-test.sh
data-collect/deploy/tests/prune-oldest-utc-day-test.sh
cargo test -p data-collect --bin real_source_alignment_probe --locked --offline
```

systemd 单元只在临时 `--root` 中配合同路径可执行占位执行 `systemd-analyze verify`；不得为
静态验证在本机创建真实 `/home/ubuntu/...` 路径，也不得启动单元。

## 3. 当前集成测试目标

```bash
cargo test -p data-collect --locked \
  --test deployment_binary_contract \
  --test hot_path_handoff_contract \
  --test training_collector_runtime_contract \
  --test hourly_partition_contract \
  --test source_hourly_layout_contract \
  --test market_discovery_probe_contract \
  --test binance_aggtrade_continuity_contract \
  --test writer_publish_contract \
  --test polymarket_orderbook_v3_contract \
  --test polymarket_metadata_layout_contract \
  --test parquet_writer_contract \
  --test polymarket_sdk_patch_contract \
  --test polymarket_sdk_runtime_contract \
  --test collector_identity_contract \
  -- --test-threads=1
```

测试数量不是永久验收门；验收以当前适用 Issue 要求的真实业务行为和实际运行结果为准。

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
  'training_runtime::tests::four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second' \
  -- --exact --test-threads=1
```

默认完整回归测试只能精确跳过该具名测试。被 `#[ignore]` 的八小时 RTDS 公开探测需要
显式授权与外部条件，不属于默认本地完成门。

## 5. 当前测试职责边界

Rust 测试只验证可执行合同与可观察业务行为，不得读取或搜索 Markdown 固定文本，也不建立
文档守卫。文档由任务自审和阶段独立审查核对；带日期验证快照和取代关系只保留在
GitHub 记录与 Git 历史中，不混入当前验收参考。
