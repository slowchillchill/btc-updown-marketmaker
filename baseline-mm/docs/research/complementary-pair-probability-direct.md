# 直接概率与跨来源校准：方法及配置

本实现承接 [#185 当前任务3分配](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185)。代码和合成边界验证已交付候选计算能力，真实十八日准备、拟合和评价尚未执行。正式参数仍未采用。本方法不完成经营参数、任务4连续账户、PAPER 或 LIVE 验收。

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

目标为全部合法市场 × 剩余300/240/180/120/60/30/10/1秒。先形成分母，再按可见时刻左连接；市场身份尚不可见、缺规则、缺锚点、缺价格或标签都留在分母。正式锚点仅在真实可见后使用。初始 TWAP 规则沿公共 OpeningWindow，可以选择市场窗内、规则后到前已接收的首个合法 TWAP；真实规则变化从变化可见时开始适用。首个已固定临时锚点保持，后续规则变化不重置；正式锚点可见后优先。候选半开窗口和有效性复用公共 `temporary_twap_candidate_matches_v1`，SQL 聚合首条，不遍历每市场全期消息。

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

深度薄入口与完整入口共享接纳、拼接判定、代际及价位覆盖/零删除。缺口或未恢复快照不输出旧有效总量，同步未恢复也保持缺失。总量只在实际采样时从同一权威映射读取，用已有 BigInt 精确有理数加总，不因跨层和超出 Decimal 容量新增拒绝。入口不输出完整 Canonical 事件/谱系；生产默认入口保持。

基线在全部可算样本评分。每个外部特征及其零权重项仅在该项共同可算交集比较，再另报全基线分母与覆盖代价。缺 Binance 不补零。不可拟合项保留真实原因；没有改善或精确并列保留零。全局来源历史和八次统计调用通常减少 fallback 使用，报告实际 fallback 分母；不构造冷启动或冒称每市场生产 initial_sigma 覆盖。

四折为9—11日拟合/12—14日评价、9—14/15—17、9—17/18—20、9—20/21—23。最后9—23日冻结/24—26日开发。每折 frozen JSON 文件关闭后才评价。开发数据不改对应冻结参数。

## 输出、资源和执行边界

只保留 `material/samples.json`、五个价对充分统计、查询单元与材料身份；评价保留全候选评分、冻结参数、必要预测、覆盖、Brier及共同交集差值。临时语义子集、规范化映射及深度传输文件在消费后删除；失败保留原边界以便诊断。

校准固定十桶 `[0,.1)…[.9,1]`，空桶的均值/真实率/Brier为 null。生成独立 SVG 和中文 report.md。分层包含剩余时间、标准化距离、参考类别、接收/事件年龄、来源缺失、日与市场；日/市场分别报告等块均值、标准差、最小/最大值，不把八次采样当成独立样本。创新报告正/负/零/缺失、相邻可算对及反号对。未套用健康最终阈值筛早期数据。

配置提案为一个独立作业，共享24 DuckDB查询线程、8GiB引擎内存预算、64GiB spill额度。每次只消费一个物理文件或原生重叠文件组，原生索引桶为1,048,576，类型读取批次1024行/64MiB，公共待归并终局上限65536。24为共享查询并行度，不是总OS线程数。前台Python、监督器、30秒日志线程及串行Rust helper也同属该作业；查询与helper归约分阶段执行，元数据/终局的小语义子集显式以 PyArrow `use_threads=False` 串行读取，不启用默认列线程池；BLAS/OpenMP另限1线程。不并发启动多个各64GiB服务。prepare临时目录为 `RUN_ROOT/projection/spill`，study为 `RUN_ROOT/statistics/spill`，其余临时文件亦在本轮专用根下。8GiB不是RSS上限；整个进程组沿批准保护 `MemoryHigh=48G / MemoryMax=64G / MemorySwapMax=0 / OOMPolicy=stop`，Codex在组外。墙钟、峰值和完整磁盘实际消耗尚未测量，不从旧健康统计外推。

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

## 本轮工程验证

本轮只执行合成合法边界和受影响源码验证，没有读取真实行情做概率准备或评分。`data-pipeline-core` 的 lib/tests 共192项通过；新 `probability_boundary` 示例的元数据种子时序测试1项通过；概率 Python 测试11项通过；原健康直接查询默认入口兼容测试2项通过。`cargo fmt --all --check` 通过。受影响两个包的普通 Clippy 通过，保留既有警告；`-D warnings` 被未修改的 contracts 大枚举警告阻断，没有据此扩大修改范围。

新 release helper 的 SHA-256 为 `ddd713285b1c6c03a7041ae04063b0a27bd22b709e5e30506977ca54f4312cc4`。旧初始化 helper 从原独立归档恢复，SHA-256 为 `45bab72dca21884eb4ee418f6952b629720b78b00b8d593cfe3c9dcfbf48f4b0`，原构建源码为 `6b96f7101476aaf54782e353b5bf4f0a5616c3f0`；本轮没有重编它。元数据排序只提取原公共比较键供当前研究复用，原初始化的比较及选择行为保持。准确源码头、命令、日志和二进制身份随本轮交付证据保存。
