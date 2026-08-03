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
blob、45 个历史路径不在活动树、四份仅作证据文档的首屏标记，以及以下三类跨模块文档：

- `rebuild-core` 与 `sim-system` 六份文档仍是无执行权威的审核中草案/记录；
- DPT 三件套已经批准，其本地执行权威只属于 DPT 计划，不拥有 `data-collect` 当前权威；
- `DATA-COLLECT-MODEL-TRAIN-BOUNDARY-20260804` 三件套记录已完成窄修正且执行授权为
  `none`。

守卫还必须拒绝第二个 `CURRENT`、错误三件套引用、错误 blob、恢复历史路径、损坏证据
首屏标记，以及把已批准 DPT 改回草案或扩大窄事项授权的负例。活动 Markdown 集合只包括
Git 已跟踪或未被忽略的新文件，不把 Git 忽略的本地发布产物副本冒充活动文档。

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

## 8. 2026-08-03 顺序纠偏与 data-pipeline(training) 草案验证

本次只纠正跨模块顺序，创建无执行权威的 `DATA-PIPELINE-TRAINING-20260803` 审核中
三件套，并同步文档治理守卫；不修改 `data-collect` 生产运行时、数据 schema、部署或
远端状态。

- 当前顺序明确为：已完成 `data-collect(training)` 后，先完成同一
  `data-pipeline(training)` 的上游投影门；再由 `sim-system` 使用黄金事件和真实管道
  事件验收；随后完成训练闭环门，最后进入 `model-train`。门 A 与门 B 不形成两套
  语义规范化实现。
- 新 demand/plan/changelog 均使用 `DATA-PIPELINE-TRAINING-20260803`，当前执行
  authority 和执行授权均为 `none`，并互相引用；活动 Markdown 显式处置集合由 25 份
  增为 28 份，归档标签、快照和历史文档数量未改变。
- `document_authority_contract` 实际结果：`4 passed; 0 failed`。新增三件套均被归类为
  无 `data-collect` 权威的跨模块审核中草案/记录，主三件套和唯一 `CURRENT` 未改变。
- `deploy_readme_contract` 实际结果：`6 passed; 0 failed`。模拟盘/实盘真实适配器仍需
  `model-train` 完成后由用户决定返回 `data-collect`，既有延期门没有被新顺序绕过。
- `cargo fmt --all -- --check` 与 `git diff --check` 通过。
- 对活动当前文档、主三件套和跨模块草案反查“把模拟器放在训练管道之前”或“仅凭黄金
  事件即可宣告真实训练适用”等旧当前语义，未发现残留；2026-08-02 带日期的历史过程
  记录保持不改。
- 独立审查发现并推动修正：门 A/模拟器循环完成条件、部分能力冒充 READY、
  RewardContract 前置循环、Ledger 绕过门 B、`tick_stream` 误作决策触发器，以及
  metadata v5 专用字段的事实精度和草案 authority 精确断言。修正后的独立终审结果为
  `Critical 0 / Important 0 / Minor 0`。

以上结果只证明本轮文档、治理分类和既有部署边界一致，不证明管道实现、逐文件输入准入、
模拟器集成、训练数据集或模型训练已经完成。

## 9. 2026-08-04 跨模块三件套补齐验证

本次为 `rebuild-core-demand` 和 `SIM-SYSTEM-20260731` 各补齐无执行权威的 plan/changelog
审核草案，并同步文档权威守卫；不修改 `data-collect` 生产运行时、数据 schema、服务、
部署或远端状态。

- 四份新增文档与各自 demand 使用同一事项编号并相互引用，当前执行 authority 和执行
  授权均为 `none`；活动 Markdown 显式处置集合由 28 份增为 32 份。
- 首次并行验证中 `document_authority_contract` 为 `3 passed; 1 failed`：两份新增
  `rebuild-core` 文档已经提及 `data-collect`，但尚未进入显式处置集合。
- 将两套新增 plan/changelog 统一路由到 `data-collect/docs/CURRENT.md`，并把三套跨模块
  三件套纳入同事项编号、相互引用、无权威和无执行授权检查后，
  `document_authority_contract` 为 `4 passed; 0 failed`。
- 同次回归中 `deploy_readme_contract` 为 `6 passed; 0 failed`，
  `deployment_binary_contract` 为 `3 passed; 0 failed`，`cargo fmt --all -- --check` 通过。
- 归档标签、快照、历史文档数量、当前主三件套的路径与身份，以及唯一 `CURRENT` 入口的
  身份和数量均未改变；新增草案不授权代码实施、正式数据构建、远端操作、部署、推送/发布、
  钱包/凭据、真实下单或资金。

## 10. 2026-08-04 模拟器低频规则责任与存储复核验证

本轮只更新低频平台规则的模块归属、冷启动合同和现有数据空间反事实；没有修改
`data-collect` Rust 代码、schema、Parquet、清单、服务、部署或远端状态，也没有创建
`sim-system` 配置实现。

- 只读空间复核仅使用 `/home/chillchill/new-datasets` 中 UTC 日不早于 2026-07-30 的
  2026-07-30 至 2026-08-02 四日：491 个 Parquet、151,056,246 行、
  9,202,902,694 字节；metadata 为 97 个 v5 文件、4,620 行、4,119,473 字节。
- 当前 metadata v5 没有规则专用列，因此专用列删除收益和语义上安全可删除量均为 0 字节。
  十个 Gamma 原始证据键的组合反事实为 255,582 个逻辑字节、约 9,317 个 ZSTD-3 压缩
  字节，即 metadata 约 0.226%、全数据约 0.000101%；删除会破坏载荷及摘要，故不执行。
- 十键中只有 `orderMinSize`、`orderPriceMinTickSize`、`feesEnabled`、`negRisk` 是来源时点
  不同的直接重叠候选；其余是旧键、主题近似或本次配置明确排除项。完整 `feeSchedule`、
  `feeSchedule.rebateRate`、`itode`、`oas` 的重叠和删除收益均为 0。
- 动态数据独立复核为 624 条 `tick_size_change`、156 个市场，全部
  `0.01 -> 0.001`；2,039,650 个非空 `fee_rate_bps` 均为字符串 `"0"`。两者都是事件证据，
  不是冷启动配置副本。
- 独立审查发现当前 SDK 的 `mos/nr/fd.r/e/to` 默认反序列化无法证明字段存在，以及
  `Client::version()` 会在客户端实例内缓存。文档已把前者加入 SDK 类型覆盖门，并要求每个
  新运行新建不跨运行复用的 CLOB/Gamma 客户端；`itode`、
  `feeSchedule.rebateRate` 仍是必需字段缺口，未消费的 `oas` 不单独阻断。
- 最终合并验证为：`deploy_readme_contract` `6 passed; 0 failed`，
  `deployment_binary_contract` `3 passed; 0 failed`，`document_authority_contract`
  `4 passed; 0 failed`；`cargo fmt --all -- --check` 与 `git diff --check` 均通过。

以上结果只证明文档权威、既有部署边界和只读测量结论一致，不证明正式冷启动加载器、
`PlatformRuleSnapshot` 或模拟器已经实现；当前相关跨模块三件套仍为
`authority=none`、`执行授权=none`。

## 11. 2026-08-04 模拟器冷启动来源纠正

用户随后批准把 `sim-system` 冷启动来源改为固定的官方公开 HTTPS 端点和窄类型响应
加载器。本决定取代第 10 节中“模拟器冷启动受官方 Rust SDK 类型覆盖门阻断”的当前含义；
第 10 节的测试计数和只读测量仍只作为当时实际快照保留，不是新加载器的验证证据。

- 官方 Rust SDK 类型覆盖缺口不再阻断模拟器；平台响应缺少必需字段、字段非法、身份不一致
  或来源冲突仍由模拟器事项失败即关闭。
- 本次只纠正文档中的来源与责任边界，不修改 `data-collect` Rust、schema、Parquet、清单、
  服务、部署、远端状态、当前 authority 或执行授权。
- 本轮最终实际运行 `cargo test -p data-collect --locked --test document_authority_contract
  --test deploy_readme_contract --test deployment_binary_contract -- --test-threads=1`，结果依次为
  `4 passed; 0 failed`、`6 passed; 0 failed`、`3 passed; 0 failed`；
  `cargo fmt --all -- --check` 与 `git diff --check` 均通过。这些结果只验证当前文档权威、既有
  部署合同、Rust 格式和差异格式，不验证公开 HTTPS 加载器。
- 本节不声称公开 HTTPS 加载器、`PlatformRuleSnapshot` 或其测试已经实现，也不复用第 10 节
  的既有测试结果为该加载器背书。

## 12. 2026-08-04 model-train 消费边界窄修正验证

本轮只把历史兼容标识 `ConsumerKind::Training` 明确为下游 `model-train`，取消它直接
读取 `typed_raw` 的权限，并同步 DPT 已批准状态及文档权威守卫；没有修改最终化写入、
每日清单 schema、既有 Parquet、采集器运行时、paper/live、部署或远端状态。

- `ConsumerBoundaryContract::training()` 现在声明
  `consumes_typed_raw_parquet=false`，同时继续要求 `data-pipeline` 输出；把该值临时改回
  `true` 的负例得到 `Incomplete` 和
  `model_train_may_not_consume_typed_raw_parquet`。
- `ConsumerBoundaryContract::data_pipeline()` 继续声明原始读取为真；把该值临时改为
  `false` 的负例得到 `Incomplete` 和 `typed_raw_parquet_consumer_boundary` 缺失。
- `finalized_typed_raw_parquet` 只从 `Training` 的允许角色集合移入禁止集合；生产者的
  `training_utc_day_partition`、最终化发现和原子发布合同保持不变。
- 首次运行文档权威守卫为 `3 passed; 1 failed`：正式实施工作树中存在两份 2026-07-09
  生成且被 Git 忽略的发布包文档副本，旧守卫的全文件系统递归误把它们计入活动文档。
  用户指出它们不属于 `/home/chillchill/sim-system` 工作树后，核实失败只发生在正式实施根；
  守卫随即改为只枚举 Git 已跟踪或未被忽略的新 Markdown，不修改、删除或纳入这两份发布
  产物。重跑 `document_authority_contract` 为 `4 passed; 0 failed`。
- 最终定向测试共 `31 passed; 0 failed`：
  `consumer_format_compatibility_contract` 6 项、`document_authority_contract` 4 项、
  `finalized_discovery_contract` 3 项、`manifest_contract` 5 项、
  `paper_live_boundary_contract` 4 项、`training_consumer_boundary_contract` 4 项、
  `writer_publish_contract` 5 项。
- `cargo clippy -p data-collect --locked --tests -- -D warnings`、
  `cargo fmt --all -- --check` 与 `git diff --check` 均通过。

独立审查代理使用 `gpt-5.6-sol` 和 `ultra` 推理等级复核并重跑上述验证，最终结论为
`Critical 0 / Important 0 / Minor 0`。本窄事项已完成并关闭本地执行授权；该结果不代表
DPT 阶段 0 完成。
