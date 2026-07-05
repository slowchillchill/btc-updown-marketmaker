# data-collect changelog

事项编号：`data-collect-demand`。

> 对应需求文档：`/home/chillchill/btc-updown-marketmaker/demand/data-collect-demand.md`

> 对应计划文档：`/home/chillchill/btc-updown-marketmaker/plan/data-collect-plan.md`

## 2026-07-06

### 初始模块计划

- 根据 `demand/data-collect-demand.md` 创建模块计划 `plan/data-collect-plan.md`。
- 计划范围限定为 `./data-collect/` 的外部市场源采集证据模块；不授权代码执行、部署、paper、live、真实下单、撤单或资金操作。
- 按用户补充要求，把“技术测试与性能验证”设为阶段 0 硬 gate：正式开发前必须先验证 Polymarket SDK 能力、四源采集、parquet writer、atomic rename、bounded queue/backpressure、ResourceWatermark，以及数据采集服务器和交易服务器的硬件/运行边界。
- 阶段 0 未通过或未经用户审查时，不进入正式模块开发；如果发现 SDK 缺口、性能不可达、live 小主机资源不匹配或需求边界冲突，必须形成事实、影响、可选项和推荐选项，再交由用户决定。
- 计划明确 AWS 爱尔兰 live-trade 小主机只作为运行目标边界核对对象，不承担 Rust 编译、cargo test/check/fmt、训练式重处理、历史全量 parquet 扫描、压缩归档批处理或 archive/upload/listing/gzip。
- 计划保留 `data-collect` 非范围：不生成 labels、features、tick_stream、训练切分、T-30 样本加工、paper/live append-only log、order evidence、NOE/ledger、execution feedback ingestion 或交易逻辑；Polymarket SDK 缺口方案必须先获用户同意。

### 审查反馈修复

- 根据 subagent 审查结果修复 `plan/data-collect-plan.md`：明确当前仓库只落文档，后续代码编写、本地构建、`cargo` 测试和 bench 的目标路径是 `/home/ubuntu/btc-updown-marketmaker`。
- 在计划中补充阶段推进通用 gate：每个阶段完成后必须自审、更新 changelog、派生独立审查、修复并复审、commit/push 后才能进入下一阶段；阶段 0 还必须经用户审查通过。
- 在计划中补充 `data_collect_run_root`、`data_collect_artifact_root`、训练 UTC day 分区根、paper/live handoff artifact 根和权限边界 receipt 要求；路径未获用户批准时阶段 0 verdict 必须为 `BLOCKED_NEEDS_DECISION`。
- 补充四源 typed raw 数据格式合同，明确 Polymarket market channel、Polymarket market metadata / `new_market`、Binance depth update/snapshot、Binance aggTrade、Polymarket RTDS Chainlink BTC/USD 的必备 typed 字段、raw evidence 和消费者边界。
- 补充 data-pipeline、训练、paper、live 三类消费者的格式兼容性检查：`data-pipeline` 可消费 typed raw mirror，训练必须经 data-pipeline 生成 labels/features/tick_stream/split 后消费，paper/live 只能消费外部市场源证据和 coverage，不能用 `data-collect` 替代 runtime append-only log、NOE/ledger 或交易逻辑。
- 补充 poly-tx 问题修复计划项：辅助源必须独立于首个 Polymarket worker 启动并证明四源 T-30s preload coverage；discovery / asset_ids miss 过期后必须记录 miss、`discovery_miss_rate`、跳过过期 target 或按当前时间重算目标，不能卡住后续 market。
- 将需求覆盖索引从主题级改为 `demand/data-collect-demand.md` 第九节 1-22 的逐项映射，并补充数据格式与消费者兼容性检查表。

### 过度硬失败风险修复

- 根据用户对“过度校验导致采集程序频繁失败”的质疑，修复 `plan/data-collect-plan.md` 的运行时失败语义：新增 `ProcessFatal`、`ArtifactIncomplete`、`SourceDegraded`、`BackpressureDegraded`、`PhaseGateBlocked` 五级校验结果，明确 `source gap`、`stale`、`coverage drop`、单个 artifact incomplete 和可恢复 backpressure 是质量/覆盖信号，不得默认触发 collector 进程退出。
- 收窄 manifest、Polymarket discovery、atomic publish、bounded queue、ResourceWatermark 和方案不一致决策门中的硬失败措辞：coverage 不完整应写入 gap/incomplete 并限制消费者使用，其他 source/window 继续采集；只有路径越界、artifact 根不可写、partial artifact 被发布、SDK-first 被绕过、持续 hard threshold 且无法保全已采数据等条件才进入 fail closed。
- 为 bounded queue 和 ResourceWatermark 增加 soft/hard threshold、连续采样窗口、hysteresis、暂停/恢复动作记录要求，避免短暂资源或队列尖峰造成频繁停采。
- 修正 `PASS_WITH_LIMITS` 表述：它不再自动允许进入正式开发，必须列出限制和风险，并经用户明确批准后才能进入阶段 1。
- 根据复审 minor 反馈继续收窄 `BackpressureDegraded` 表格措辞：soft watermark 只记录 backpressure、降级、恢复观察和 gap；暂停新市场或单源 intake 只能发生在 hard threshold 连续越线且无法恢复之后。
- 修正计划文档自身验证命令：用 `awk` 直接检查 trailing whitespace，避免 `git diff --no-index --check /dev/null ...` 因正常文件差异返回非零而在 `set -e` 下被误判为失败。
