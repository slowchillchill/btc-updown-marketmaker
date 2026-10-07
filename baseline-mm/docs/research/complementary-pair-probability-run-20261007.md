# 十八日直接概率准备失败与最小修复（2026-10-07）

本次获得一次十八日真实执行授权，prepare 在公共市场归约阶段失败。study 未启动，概率结果为0，没有 Brier、校准曲线或候选排名。失败与修复不代表任务3完成，也不采用正式参数。

授权见[#185 执行批准记录](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6033445684)，原失败见[执行记录](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6033661313)，本轮最小修复见[范围核对分配](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6033864198)。本轮没有真实重跑、新增真实小样本、扩大初始化或重复准入与健康研究。

## 实际执行与资源

运行源码为 PR #257 合并提交 `807ba5d76e091977ea77efccc6731372a1993eec`；配置 SHA-256 为 `c08cfc193dc378d5e4136c4f2191a8dcd66b63b66f53253095211e7d9fdac368`；概率 helper SHA-256 为 `099ea3e22854321be1a2f62ed3caac248bf40e0be242f515e6997ce785c9bce1`。源码在准备期间保持冻结。

| 记录 | 实际值 |
| --- | --- |
| 启动 / 退出 UTC | 2026-10-07 07:50:12 / 07:59:43 |
| prepare 退出 / study | 1 / 未启动 |
| 独立服务墙钟 / prepare 墙钟 | 570.148秒 / 548.78秒 |
| 进程组 CPU 时间 | 1464.971671秒 |
| 进程组内存峰值 | 20,140,470,272 bytes |
| prepare GNU time 最大 RSS | 19,529,672 KiB |
| swap / OOM / 信号退出 | 0 / 否 / 否 |
| 归档不可用 / 自动重试 | 否 / 否 |

实际内核保护为 `MemoryHigh=48G / MemoryMax=64G / MemorySwapMax=0 / OOMPolicy=stop`，所有子进程同组，Codex在组外。DuckDB共享24查询线程、8GiB引擎预算、64GiB spill额度；PyArrow语义子集串行读取，BLAS/OpenMP为1。引擎预算不是RSS上限。程序30秒日志，会话按600秒查询。上表仅是一次失败成本，不能推断完整十八日成功成本。

prepare 已进行必要原生坐标及终局窄投影，但没有完成全部公共市场事实、采样材料或拟合。没有完成市场目录，因此不能给出全十八日真实覆盖、缺失或fallback分母。原失败现场与完整日志独立保存；[机器摘要](complementary-pair-probability-run-20261007.failure.json)不含本机绑定。

## 触发行与公共责任

首次失败为 `MetadataBeforeIdentitySnapshot`。触发行逻辑路径为 `2026-09-12/typed_raw/utc_day=2026-09-12/source_kind=polymarket_market_metadata/polymarket_market_metadata_19-part-1.parquet`，原零基行号3，文件 SHA-256 为 `8492f5739e3e0e07f0a8f6a0efb79f17ca3a7458e9e187f34d2f62080d492938`。

condition为 `0x7b443d179b4fe520ee251f5a7cc5aa9e52c009439a63d9e9a60fe69d06b19b7d`，种类为 `market_state_transition`。接收时间为 `1789240506938`，准入及来源前沿为 `1789240506940`，准入序号为4。保留的该condition元数据共63行，只有transition、anchor rule及WS终局，没有Snapshot；原初始化种子也没有该condition。63行不是63个概率采样失败。

公共Core只拒绝缺合法身份的普通Metadata；Snapshot和ResolutionTerminalWs有各自路径。transition的字段不足以建立公共身份：公共Snapshot还要求合法Gamma来源、outcome映射和tick等。AnchorRule独立参考路径和配对WS暂存合法，后到Snapshot才将pending终局归并。公共历史入口同样保留缺Snapshot拒绝。此次没有改变Core错误政策，不能认定这个缺输入已满足后续完整经营回放要求。

## 修复如何保留未知

研究helper在原类型解析和来源身份核对后保存紧凑原始市场目录，包括原固定字段、原坐标和首见可见时间。这个目录不是FixedIdentity。仅当普通Metadata缺 `prepared_pm_context` 时，记录 `missing_identity_snapshot` 的行数、种类、首末坐标，并不送入Core；来源前沿仍推进，真正公共应用位置连续。它不缓存重放旧transition，不伪造Snapshot，不用未来身份回填。损坏类型、已建身份冲突及其他公共错误继续失败。

Snapshot、AnchorRule、Metadata WS及PM终局仍走原公共路径。合法身份的known_at只来自成功FixedIdentity；暂存终局在真实归并时才可用，不按旧声明时刻提前赋标签。

Python将原始目录和合法公共身份分别连接。唯一可定位的原始窗口保留全部八个事后覆盖目标；未建立身份的采样按 `missing_market_identity` 不评分。原始首见晚于采样时刻只说明事后观察到该市场，不说明当时已知。原字段与合法身份不同，保存 `census_identity_conflict` 及两侧值和来源，不覆盖原字段，也不冒称Core拒绝了未应用行。原窗口多值或无窗口时，原始目标数为null，市场仍计数；公共窗口仍可独立形成公共目标。

新增 `material/market-coverage.json` 分开保存原始观察市场、可定位原始目标、未知原始目标数的市场、合法公共市场/目标、采样时已建立身份以及缺身份区间和右删失。市场数包含实际观察到的范围边缘市场；目标数严格按既定日期与八个截止过滤，不能将全部市场数简单乘8。普通行缺前态摘要与样本资格分母分开。创新记忆仅在合法身份可见的研究采样更新，未获身份的事后目录不产生虚构历史调用。

另外修复两类实际公共serde消费错误：ReferenceFact使用 `reference_type/reference` 相邻标签，Terminal使用 `status=RESOLVED` 内部标签；市场QualityBoundary使用 `scope.kind=MARKET`。旧Python检查不匹配实际输出，可能使真实参考与标签全部缺失。现在直接读取官方Rust类型序列化结果，不兼容猜测格式。

## 验证与剩余限制

一个连续合法场景通过真实类型解析及公共Core生成Rust JSONL：未知transition保留目录并不建立身份，其他合法市场继续，AnchorRule合法，配对WS暂存，后到Snapshot才建立身份并归并终局。它同时覆盖目录冲突。原Core仍拒绝未知普通transition。Rust默认测试逐字核对生成夹具，Python消费相同输出，证明锚点、规则、RESOLVED及八个前身份不可算目标。

本轮受保护验证通过：Rust示例2项、Python12项、必要release构建、Clippy和格式检查。Clippy保留未修改lib的11条既有警告，helper无新增警告。旧公共core192项及默认健康查询兼容证据沿PR #257复用。旧 `source_health_boundary` 未重编，摘要仍为 `45bab72dca21884eb4ee418f6952b629720b78b00b8d593cfe3c9dcfbf48f4b0`。新概率helper摘要为 `d77d6d2dbf5a252cd66ce171dc68a0c597733b0fc24014387b8188af773539ee`。

修复不改变日期、四折、公式或每折75次顺序评分，不改变生产策略和账本。尚无真实Brier、概率校准、窗口比较或Binance共同交集贡献。初始sigma只有实际fallback出现时才可识别；epsilon是二元Brier误差尺度，创新按研究八次采样更新，不是生产逐次SignalMemory。

下一次执行只是提案：独立新runroot、logroot和unit，绑定本轮提交、配置摘要和新helper；资源、候选及日期保持。成功完整运行成本仍未知。正式独审、合并及新投入条件满足前不启动；本次失败目录和证据不覆写。

独立归档相对入口为 `issue-185/probability-calibration-actual-failure-20261007` 和 `issue-185/probability-calibration-repair-20261007`。前者保留原执行失败事实，后者只保存新源码身份、helper、定向验证及准确提案；相同旧helper和有效工程证据引用原 `issue-185/probability-calibration-engineering-20261007-v2`，不重复复制。
