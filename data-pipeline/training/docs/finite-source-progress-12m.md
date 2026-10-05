# 有限来源进展入口与十二分钟测量

`PublicObservationCoreV1::push_source_progress` 用于只消费来源时间、质量和同步事实的研究。它与原 `push` 共用输入验证、业务归约、代际恢复、选择和封组；完整组归约后返回 `PublicSourceProgressAppliedV1`，不生成各市场盘口观察，也不复制全来源进展集合。每组只交付有变化的 `SourceStreamProgressV1`；代际变化使另一资产待快照的同步变化同样交付，同值有效新消息保留自己的证据身份。固定市场首次建立时交付原 `MarketIdentityV2`（含窗口）及资产绑定。

紧凑返回类型不能交给完整 writer、SIM、训练或经营回测。原 `push`、观察、writer、完整 recording/replay、输入与组位置、正式稳定前缀保持。紧凑入口的 `applied_prefix` 只表示核心已应用的输入，不是持久提交证明。其每组 `visible_at_utc_ms` 沿原接收／采集准入与依赖事实时间最大值；SIM 原时间函数调用同一个公共纯原语。消费者需要单调可见前沿时维护该前沿，不新增业务时钟。

`data-pipeline-training::finite_source_progress::prepare_finite_source_progress_v1` 接收显式 `PublicFiniteFileV1` 和可选的原 `MetadataInitializationConfigV1`，沿原文件身份／模式／解码边界、两遍代际识别、外排、`HistoricalMergeV1::prepare_input` 连续消费。跨文件和跨UTC日不会重建core；损坏行的原错误继续传播，包含所选文件的窗外坏行。它不生成 captured/public recording、hidden 或稳定目录。文件事实取自已审清单；此入口不签发全日准入。

测量例 `measure_finite_source_progress` 只输出来源标量增量、首次市场绑定、资源及分母，未引入normal/max、三态、健康阈值或策略动作。其私有配置包含 `preparation`（上述入口配置）和 `progress_interval_seconds`；0关闭周期进度，阶段变化仍输出。逐行输出中来源数字索引由 `summary.json.sources` 解释；每条来源增量依次为：来源索引、输入位置、组位置、该组来源可见时间、单调可见前沿、最后有效消息、最后观察消息、连接代次、同步、质量。消息只投影原身份／接收时间／事件时间，未知值仍为null。

```bash
cargo build -p data-pipeline-training --example measure_finite_source_progress --release --locked --offline -j 8
measure_finite_source_progress CONFIG OUTPUT
```

以下实测仅覆盖执行Issue #246批准的UTC 2026-09-09 `[00:00,00:12)` 与原初始化；不代表18日资源闭合。core仍保留原历史市场、去重、证据及参考状态，投影优化没有给这些状态新增期限或淘汰政策。
