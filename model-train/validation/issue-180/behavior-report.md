# #180 步骤7—8：职责解耦与行为保持对照

精确旧基线 `178713022eba217ce77ca1782bdaa8e302690b66` 与重构头 `6574190f3747390320656110366a155c706baa1e` 的真实短连续对照通过。完整结构和整数／身份值相同，196421个浮点值最大绝对差为0；沿既定 `1e-5 + 1e-4 * abs(reference)` 判定，未放宽阈值。本轮没有步骤9的发送时间、日志格式、调度或反馈可见性变化，也不表示 #180 组合验收已完成。

## 责任边界

- Shared 保留原 `apply_closed` 与显式结算驱动；`observe_group` 接纳受信公共规则，`freeze_action_before` 只冻结当前已应用账户前缀。旧 `action_before` 按原顺序包装，事实不重复应用。
- Native 新入口先 `observe_sources` 接纳真实来源身份，再用原公共帧及 `ActionBeforeReadV2` 编码；编码不持有可变 Shared／SIM。旧 `consume` 仍在原逐组位置接纳来源并编码。
- 市场来源身份去重、`Sample.at`、16条截断及退场释放保持。历史特征缓存只在来源接纳后失效，完整编码时按原公式计算；不增加业务账本。
- Native `select_action` 只装配已选原值和原 `ReserveIntentV1`。Shared `execute_reserved` 执行原整动作一次预留、`record_submission_started`，再调用原 Registry／Bridge／本地回执链。旧 Actor／`execute_selected` 和非模型 `execute` 保持兼容。

NAV、承诺未知处理、候选顺序、真实快照依赖修复及结算策略没有迁移。每个新增边界有兼容包装或本轮真实分阶段消费者。

## 真实短场景和结果

两侧从精确提交全仓 `git archive` 建立独立副本，使用同一诊断场景、MAIN1x256图／权重、规则、seed、2000账户现金、原逻辑时间、`continuous-validation` 功能、测试调试构建、ORT1及Rayon8。诊断复用原 `continuous_tests` 的 TypedRaw→DPT录制→Finite消费链与原 `ThreeRuntime`，没有第二业务运行器。合法逻辑时间跳转后在第一市场退场、第二市场承接时显式停止并排空；实际墙钟约3秒，不运行容量前史。

旧侧实际调用精确旧代码 `consume`，新侧实际调用 `apply_closed → observe_group → observe_sources → freeze_action_before → encode`。并非同一重构包装的单路比较。两个版本分别保存完整输入、所有层 `h_prev/h_next/z_t`、隐藏状态操作、真实 Actor 支持／前缀／有效分数／条件概率及原选值。原 Torch、Python ORT、Rust ORT 三端逐行核对；每合法行一次 GRU，Actor 参数链不再次调用 GRU。

| 观测 | 结果 |
| --- | --- |
| 公共组／模型行 | 72／79 |
| 同毫秒后续组 | 15 |
| 初始化／退场 | 2／1，包含根重置 |
| 来源历史 | 至少17次真实来源更新，达到16条截断；不变来源的同毫秒组不重复接纳 |
| 双市场 | 当前＋T-30，热身行不交易，开市承接后连续状态推进 |
| 自主 Actor | 36次，均选择 NoTrade；保存条件及联合概率 |
| 受控补充 | 1次真实支持集内双腿原值选择，不称为自主模型交易 |
| 实际 Bridge | 1个 `POST_ORDERS`，两腿各0.01份，receipt `scheduled_action_count=2` |
| 预留／反馈 | 整动作两腿预留、提交开始真实发生；平台拒绝反馈进入连续应用，预留释放，无成交经济成功 |
| 原 SIM 输入 | 190条，含发送请求及114个原关闭时间输入，全部精确相同 |
| 完整比较 | 13005个精确值、196421个浮点值；最大浮点差0 |

受控两腿为原 GTC 选择值（价格0.01／0.001，数量各0.01）；支持选择允许这些原值，平台仍执行自己的约束并拒绝。该证据覆盖预留→Bridge→原反馈／释放分支，不把支持选择、请求到达或预留成功当作成交。既有真实资金／经济及快照回归继续适用，没有为此增加参数矩阵。

## 检查与证据

固定命令 `cargo test --workspace --lib --bins --tests --examples --locked --offline -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second` 通过，具名UTC夹具明确**跳过**。独立 Native 串行回归47单测通过、2既有忽略；rounding3项通过。两工作区 fmt、适用 Clippy、Python编译及差异检查通过。Clippy保留既有告警；新纯选择返回组合触发 `type_complexity` 提示，无编译错误。

首次独立 Native 并行运行46通过／1失败／2忽略，失败为 `complete_real_object_population_crosses_block_boundary` 原 `while next_group().unwrap()` 收到 `RecordingBackpressured`。失败日志保留。随后候选串行完整回归及精确旧／新两侧该测试的定向复验均通过；这不表示复现了同样背压或证明永不回压。未修改该夹具、生产容量或回压协议。

冻结：[旧基线](evidence/behavior-baseline-freeze.json)、[重构头](evidence/behavior-candidate-freeze.json)；结果和日志摘要：[behavior-results.json](evidence/behavior-results.json)。冻结绑定源码／工具、全仓归档、诊断补丁、二进制／扩展、锁文件、图／权重及ORT摘要。原始完整轨迹、SIM输入、补丁、构建及运行日志保留在 `target/issue-180-public-baseline-validation/decouple-20261001/`，文件摘要可复核。

旧容量材料不重测。本轮只更正 `source_delivery_lag_ns` 的文字标签：在 `acknowledge_if_empty` 后采样的消费／确认后墙钟相对源逻辑时刻落后，不严格等同模型结束；原字段、旧值和原始测量保持。

步骤9—10、组合验收、新main基线及两个方案工作树尚未交付。
