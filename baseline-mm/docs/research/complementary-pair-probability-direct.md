# 直接概率与跨来源校准：方法及配置

本实现承接 [#185 当前任务3分配](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185)。代码和合成边界验证已交付候选计算能力。首次真实十八日prepare因缺身份前态失败；第二次prepare在TWAP跨单元价格保留SQL失败，第三次在公共深度拼接状态失败，study均未启动。[首次失败与身份修复](complementary-pair-probability-run-20261007.md)、[第二次失败与SQL修复](complementary-pair-probability-run-20261007-repair1.md)及[第三次深度失败与窄修复](complementary-pair-probability-run-20261007-repair2.md)分别保留运行身份和准确事实。正式参数仍未采用。本方法不完成经营参数、任务4连续账户、PAPER 或 LIVE 验收。

## 输入与准确查询

入口为 `baseline-mm/research/research_probability_direct.py`，配置为 `baseline-mm/configs/probability-direct-18d.plan.json`。配置沿用既有十八份清单的逻辑路径、摘要及原两市场显式初始化。初始化的旧00:12字段保留原种子证据身份，选择器实际按原Snapshot至研究起点前的变更读取；它不筛选或截断随后十八日正常元数据和市场状态。`configure` 只核对清单身份、绑定路径，不重做全量准入。研究为 UTC `[2026-09-09,2026-09-24)`，开发为 `[2026-09-24,2026-09-27)`。

动态 SQL 位于 `baseline-mm/research/probability/direct.py`；原生单元、重复身份、有效性和前沿复用 `data-pipeline/training/python/dpt_health/direct.py` 及 `projection.py`。完整查询由已提交源码、配置和 `query-units.json` 唯一确定，参数不来自评价数据。

| 目标 | 实际字段和边界 |
| --- | --- |
| spot | v4 的 `value`、`timestamp_received`、`payload_timestamp`、`freshness_status`、`source_native_payload_json`；原文件/行顺序、接收前沿及原旧源去重。价格沿公共十进制规范化后转统计浮点。 |
| TWAP | v1 的 `full_accuracy_value` e18、`window_s`、接收/事件/准入时间、epoch 和 sequence；公共 `normalize_e18_price_decimal_v1`，不使用展示价格。30/60秒共用原生前沿。 |
| Binance成交 | v7 的有效性、接收/成交/准入时间、epoch/generation/sequence、价格、数量及 `buyer_is_maker`；原生/业务重复冲突沿既有公共窄投影。 |
| Binance深度 | snapshot v3/update v4 的当前价量 JSON、接收/事件/准入时间、epoch/generation/sequence、快照与更新ID及拼接判定；公共类型解析和 `BinanceDepthReducer` 维护权威数量映射。 |
| 市场和标签 | metadata v6 的身份、正式锚点、结算规则及可信终局字段沿原完整类型构造。PM v3 先仅读事件类型和接收/准入原生坐标；仅 `market_resolved` 声明的必要语义行再由原类型读取器交公共终局 merger。 |

PM 的全来源前沿在筛终局前形成。仅读元数据非空结果不能证明可信终局：本实现同时归并 PM 相反声明及公共质量变化。原生索引读取 PM 约7.28亿行的少数坐标/时钟列；终局投影随后仍要扫描必要列。它不展开全期 PM 盘口，但不能把列数比例当作物理读取比例或成本保证。元数据先沿公共类型边界提取紧凑的时钟/准入序号/公共来源身份坐标，再按公共 MetadataOrder 排序计算连续前沿；随后每物理文件保留当前类型子集并立即归约。该必要元数据边界解码两遍，不常驻十八日完整类型行，也不排序全期宽行。

元数据没有连接epoch，排序键直接复用 `metadata_admission_order_v1(admitted,sequence,source_message_id)`，与原初始化唯一排序一致。种子和普通元数据共同排序及归约，后到种子不会先污染更早普通行；不以清单文件顺序替代元数据原生顺序。PM、Binance及TWAP继续使用各自epoch/admission顺序。来源可见时刻为原生序列上 `max(received, admitted, prior_frontier)`；spot 没有 admission，使用旧文件/行顺序及接收前沿。跨文件、午夜、折不重置。原生重叠文件组成一个查询单元，不用 TTL 截断重复身份。临时重复候选只沿既有类型身份边界核对，不建立全期身份 JSON 库。

目标为原始目录中唯一可定位的窗口与合法公共窗口的并集 × 剩余300/240/180/120/60/30/10/1秒。原始事后目录不是FixedIdentity；缺公共身份的普通Metadata仅记缺前态摘要，不缓存重放、不伪造Snapshot。AnchorRule、WS暂存及成功Snapshot继续原公共路径。原始字段冲突保存两侧来源，多值窗口的原始目标数为null。先形成分母，再按可见时刻左连接；市场身份尚不可见、缺规则、缺锚点、缺价格或标签都留在分母。正式锚点仅在真实可见后使用。初始 TWAP 规则沿公共 OpeningWindow，可以选择市场窗内、规则后到前已接收的首个合法 TWAP；真实规则变化从变化可见时开始适用。首个已固定临时锚点保持，后续规则变化不重置；正式锚点可见后优先。候选半开窗口和有效性复用公共 `temporary_twap_candidate_matches_v1`，SQL 聚合首条，不遍历每市场全期消息。

此时间轴是来源级因果直接查询。它不具有完整多来源组身份，也不是生产策略逐次调用的冻结回放。终局保存必要状态变化；拟合取严格截止前最后公共状态，后到冲突不能回写早期状态。评价取原十八日范围内最后状态；不读范围外尾部，末日未观察终局按右删失处理。

## 方差、积分与有限概率候选

同毫秒 spot 历史首点接纳，当前报价可以保留随后同毫秒事实，两者不混用。价对来自严格递增接收时刻。每窗口 `L∈{15,30,60,120,300}` 秒，主方差采用右端在回看窗、且因果可见的价对；完整保留跨左界一对：

```sql
-- price_pairs 由跨段 history 的 lag(received), lag(price) 构造。
SELECT count(left_received),
       sum((received-left_received)/1000.0),
       sum(pow(price-left_price,2))
FROM price_pairs
WHERE visible_at <= sample_at
  AND received >= sample_at-L*1000
  AND left_received IS NOT NULL;
```

`sample_spot` 对全部当前单元目标一次列式聚合。另报要求左端也在窗内的旧描述值。窗 `[1000,3000]` ms，点 `(900,100)/(1500,101)/(2500,103)` 的主值为 `5/1.6秒`，旧描述为 `4/1秒`。不能用旧描述一致证明生产方差一致。

早期拟合价对充分统计取 `visible_at∈[研究起点,拟合截止)`，`sigma_fit=sqrt(ΣΔS²/ΣΔ秒)`。滚动窗口有正时间支持时，`sigma=sqrt(ΣΔS²/ΣΔ秒)`；没有时间支持才使用 `initial_multiplier×sigma_fit`，倍数为 `.5/1/2`。零平方变化是已知零方差，不是缺失。跨段仅保留300秒支持及其左侧前驱、最后报价和五个拟合截止充分统计。

设 `τ` 为剩余秒，`K` 为当时参考阈值，`S` 为现价，`I` 为单项外部价格修正，`W` 为正式 TWAP 秒数。使用无漂移正态研究候选：

- spot：`μ=S+I`，`V=sigma²τ`。
- TWAP窗外 `τ≥W`：`μ=S+I`，`V=sigma²(τ−2W/3)`。
- TWAP窗内 `τ<W`：`μ=(A+τ(S+I))/W`，`V=sigma²τ³/(3W²)`。
- `epsilon∈{0,.5,1,2}×sigma_fit×sqrt(1秒)`；`p=Φ((μ−K)/sqrt(V+epsilon²))`。分母为零时沿已知退化分布 `μ≥K` 判 Up。

`A` 只来自已可见 spot 的接收轴持价近似，积分区间为 `[end−W,sample]`。积分只用采样时已可见的后继；较早接收但因原生前沿延后可见的点，不能提前截断已有持价。共享可见时刻的历史连接明确选最后严格递增接收点。缺少区间支持保留缺失；TWAP 可提供未来水平但不能补 spot 积分。epsilon 是通过二元 Brier 比较的误差尺度假设，不能称真实连续结算残差。价格及拟合 sigma 单位分别为 USD/BTC 和 `(USD/BTC)/√秒`。

## 跨来源拟合与冻结

每折先评分60个基线：按 `L`、initial倍数、epsilon倍数的上述顺序枚举，Brier 相同取先项。再固定最佳基线，独立比较三项特征的5个权重。权重顺序为 `0,−1,−.5,.5,1`，并列优先零修正，共75次评分。这个顺序不是联合全局最优搜索。基线完全不可拟合时保留全部基线失败原因，不伪造外部基线。

| 单项 | 定义和必要状态 |
| --- | --- |
| innovation | 每市场八个采样调用维护基差：先用旧 Binance/Chainlink 对计算 `(B−B₀)−(S−S₀)`，然后在新 Chainlink 接收时刷新基差。首对为零；缺任一价格保留缺失。I为权重×innovation，单位为价格。USD/USDT未转换，不能声称无币种基差。TWAP替代未来水平不能代替创新的Chainlink spot。 |
| depth_pressure | `(Σbid_qty−Σask_qty)/(Σbid_qty+Σask_qty)`；I为权重×sigma_fit×sqrt(1秒)×压力。总量为零时压力缺失。 |
| trade_pressure | 最新合法聚合成交数量 q 除以 `q+当前两侧深度总量`，`buyer_is_maker` 为真取负，否则取正；相同价格尺度。不是累计成交、成交额或自身成交。 |

深度薄入口与完整入口共享接纳、拼接判定、代际及价位覆盖/零删除。共享reducer保存最近一次真实snapshot的接收UTC日、depth_ingress_sequence和generation；同epoch/generation、update ingress晚于snapshot且接收日不同，才按采集器Ready直收路径使旧盘口失效。快照前已收到的pending保持原排空规则；同值snapshot也刷新连续性来源而不新增业务值事件。转换只在实际可见行生效，不回填午夜采样。Deferred后的同日ResetContinuity未持久化缺口仍未解决，无法证明的坐标冲突继续拒绝。缺口或未恢复快照不输出旧有效总量，同步未恢复也保持缺失。总量只在实际采样时从同一权威映射读取，用已有 BigInt 精确有理数加总，不因跨层和超出 Decimal 容量新增拒绝。入口不输出完整 Canonical 事件/谱系；生产默认入口保持。

基线在全部可算样本评分。每个外部特征及其零权重项仅在该项共同可算交集比较，再另报全基线分母与覆盖代价。缺 Binance 不补零。不可拟合项保留真实原因；没有改善或精确并列保留零。全局来源历史和八次统计调用通常减少 fallback 使用，报告实际 fallback 分母；不构造冷启动或冒称每市场生产 initial_sigma 覆盖。

四折为9—11日拟合/12—14日评价、9—14/15—17、9—17/18—20、9—20/21—23。最后9—23日冻结/24—26日开发。每折 frozen JSON 文件关闭后才评价。开发数据不改对应冻结参数。

## 输出、资源和执行边界

只保留原始/公共两层市场分母及缺身份区间的 `material/market-coverage.json`、`material/samples.json`、五个价对充分统计、查询单元与材料身份；评价保留全候选评分、冻结参数、必要预测、覆盖、Brier及共同交集差值。临时语义子集、规范化映射及深度传输文件在消费后删除；失败保留原边界以便诊断。

校准固定十桶 `[0,.1)…[.9,1]`，空桶的均值/真实率/Brier为 null。生成独立 SVG 和中文 report.md。分层包含剩余时间、标准化距离、参考类别、接收/事件年龄、来源缺失、日与市场；日/市场分别报告等块均值、标准差、最小/最大值，不把八次采样当成独立样本。创新报告正/负/零/缺失、相邻可算对及反号对。未套用健康最终阈值筛早期数据。

配置提案为一个独立作业，共享24 DuckDB查询线程、8GiB引擎内存预算、64GiB spill额度。每次只消费一个物理文件或原生重叠文件组，原生索引桶为1,048,576，类型读取批次1024行/64MiB，公共待归并终局上限65536。24为共享查询并行度，不是总OS线程数。前台Python、监督器、30秒日志线程及串行Rust helper也同属该作业；查询与helper归约分阶段执行，元数据/终局的小语义子集显式以 PyArrow `use_threads=False` 串行读取，不启用默认列线程池；BLAS/OpenMP另限1线程。不并发启动多个各64GiB服务。prepare临时目录为 `RUN_ROOT/projection/spill`，study为 `RUN_ROOT/statistics/spill`，其余临时文件亦在本轮专用根下。8GiB不是RSS上限；整个进程组沿批准保护 `MemoryHigh=48G / MemoryMax=64G / MemorySwapMax=0 / OOMPolicy=stop`，Codex在组外。一次失败墙钟及峰值见失败报告；完整成功成本仍未知，不从旧健康统计外推。

`RUN_ROOT`、`INPUT_ROOT`、辅助产物和归档根仅由私有绑定提供；公开命令不含本机绑定。新 helper 必须绑定本轮准确提交及SHA-256。旧 `source_health_boundary` 只复用已验证产物，核对原源码适用性；不重编未修改辅助。DuckDB/PyArrow沿 `requirements-source-health.lock`，绘图复用已有matplotlib（本机已核对3.10.7+dfsg1），运行证据记录实际版本。

```bash
# configure不查询真实行情；只绑定原清单、明确输入与原初始化。
python baseline-mm/research/research_probability_direct.py configure \
  baseline-mm/configs/probability-direct-18d.plan.json \
  "$INPUT_ROOT" "$RUN_ROOT" "$BOUNDARY_HELPER" "$PROBABILITY_HELPER" "$CONFIG"

# 此为实际投入提案；满足当前合同实质条件后才执行一次。
systemd-run --user --wait --pipe --unit=issue185-probability-run \
  -p MemoryHigh=48G -p MemoryMax=64G -p MemorySwapMax=0 -p OOMPolicy=stop \
  --setenv=OPENBLAS_NUM_THREADS=1 --setenv=OMP_NUM_THREADS=1 \
  --working-directory="$TASK_TREE" \
  "$PYTHON" baseline-mm/research/run_probability_direct_once.py \
  "$CONFIG" "$ARCHIVE_ROOT" "$ARCHIVE_MOUNT" "$LOG_ROOT"
```

前台监督器按顺序执行一次 prepare、一次 study，记录原命令、stdout/stderr、GNUtime及退出原因，无重试。程序30秒进度；会话每10分钟查询作业，不编造ETA。归档挂载不可用沿既有规则立即停止；失败不自动扩额、换日期或缩窗。完整经营评价仍使用已有公共回测及SIM/NOE/Ledger。

DuckDB语义依据：[ASOF连接](https://duckdb.org/docs/stable/sql/query_syntax/from)、[窗口函数](https://www.duckdb.org/docs/current/sql/functions/window_functions)、[COPY](https://www.duckdb.org/docs/current/sql/statements/copy)。实际SQL通过合成合法跨文件、跨午夜、左端反例、临时参考及深度边界验证，不用新增真实小样本研究替代。

## 原工程交付验证（PR #257）

原工程交付只执行合成合法边界和受影响源码验证，没有读取真实行情做概率准备或评分。`data-pipeline-core` 的 lib/tests 共192项通过；新 `probability_boundary` 示例的元数据种子时序测试1项通过；概率 Python 测试11项通过；原健康直接查询默认入口兼容测试2项通过。`cargo fmt --all --check` 通过。受影响两个包的普通 Clippy 通过，保留既有警告；`-D warnings` 被未修改的 contracts 大枚举警告阻断，没有据此扩大修改范围。

新 release helper 的 SHA-256 为 `099ea3e22854321be1a2f62ed3caac248bf40e0be242f515e6997ce785c9bce1`。旧初始化 helper 从原独立归档恢复，SHA-256 为 `45bab72dca21884eb4ee418f6952b629720b78b00b8d593cfe3c9dcfbf48f4b0`，原构建源码为 `6b96f7101476aaf54782e353b5bf4f0a5616c3f0`；本轮没有重编它。元数据排序只提取原公共比较键供当前研究复用，原初始化的比较及选择行为保持。准确源码头、命令、日志和二进制身份随本轮交付证据保存。

审查发现薄 applied 更新仍执行随后丢弃的 Decimal 数量报告与旧证据聚合。本轮修正在同一价位更新循环按实际报告消费者收集这些材料，薄入口跳过聚合；完整入口继续原报告规则。合法 applied 更新将两 bid 数量各覆写为 `79228162514264337593543950335` 后，公共薄入口及实际 helper 均精确采样为 `158456325028528675187087900670/1`。扩展原边界后 core192项、示例1项、概率Python11项再次通过；完整输出固定摘要及原报告溢出错误测试保持通过。

## 首次身份失败修复验证

真实执行、诊断、修复及新提案见[失败报告](complementary-pair-probability-run-20261007.md)。本轮Rust示例2项与Python12项通过，夹具由真实公共Rust输出生成并逐字核对。ReferenceFact、Terminal及市场QualityBoundary按实际serde读取。原始目录与合法身份分开；成功公共归并时才生效的pending终局不回填。未获身份的事后采样不更新创新记忆。新helper SHA-256为 `d77d6d2dbf5a252cd66ce171dc68a0c597733b0fc24014387b8188af773539ee`；原工程helper摘要和192项证据保持原身份，不重标为本次构建。

## 跨单元价格保留的 SQL 修复

修复后的单次真实prepare在 `price_samples` 的非空carry分支失败，见[本次失败报告](complementary-pair-probability-run-20261007-repair1.md)。动态INSERT列列表现在按SQL标识符双引号引用既有字段，内部双引号双写；值仍使用绑定参数，原跨单元状态和采样选择保持。一个代表性合成入口测试在原代码中复现TWAP与成交的同一解析错误，修复后验证旧事实延续、可见时间及同毫秒原生顺序，概率Python回归13项通过。Rust及两个helper未修改或重建；旧证据按适用范围复用。真实研究未完成，不自动再次运行，也没有正式参数采用。

## 第三次深度状态失败的窄修复

共享reducer新增最近真实snapshot连续性来源，按前述同epoch／generation、ingress与接收日规则失效旧盘口，保持pending排空及同值业务去重。Core lib/tests193项、示例2项、使用本轮新helper的Python13项通过，普通Clippy与格式检查通过；准确运行、修复候选、helper身份及剩余Deferred／同日围栏输入缺口见[第三次失败报告](complementary-pair-probability-run-20261007-repair2.md)。没有再次实际prepare／study，旧运行和验证身份分别保留。
