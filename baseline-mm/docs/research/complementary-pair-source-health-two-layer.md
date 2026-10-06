# 必要列与列式两层来源健康研究

列式来源健康研究只读取判断消息进展所需的列，用于形成健康分布和候选阈值。当前任务见[#185](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185)，流程依据[D-0302](https://github.com/slowchillchill/btc-updown-marketmaker/issues/62#issuecomment-6013334211)。用户已批准新流程的完整十八日研究；准确工程头先独审，再执行。本文不宣称该新十八日运行已完成，也不正式采用normal/max。

## 当前入口与职责

`baseline-mm/research/research_source_health_columnar.py` 提供 `configure / prepare / study`。

- `configure` 复用原18份准入清单摘要和同序文件列表。只读小型清单，输出含私有路径的配置。
- `prepare` 调用公共 `data-pipeline/training/python/dpt_health`。DuckDB向量化读取必要列、展开资产、维护最小状态并输出Zstandard压缩Parquet。
- `study` 由baseline计算连续区间、分布、三态、超限与切换。只把每日充分统计交给原Rust `Histogram::body_normal` 和 `candidates`，不把原始PM行传给Python或Rust逐条core。该辅助工具的输入量按日、四组、两个指标和实际有权重的桶计算，不按原始消息行计算。初始化辅助入口本轮读取原792行metadata文件，选出3条；重复辅助入口只读命中候选的子集。

原录制、完整归约和持久格式保持。旧 `research_source_health_two_layer prepare/study` 为历史入口，不再作为本次来源健康默认流程。两次旧完整prepare均未产生完整可消费材料，未执行study。旧排序文件和未封口标量已清理；最小失败日志和身份缺失限制另有私有清单。旧结果不重标为新流程验证。

## 必要列与最小状态

| 来源 | 必要列和处理 | 状态 |
| --- | --- | --- |
| PM | received、event timestamp、condition、资产或price_changes资产、event_type、epoch、admission sequence/time | 每condition/asset最后有效直接时间、同步状态；真实epoch改变只重置已出现资产的同步要求，Book恢复对应资产 |
| Binance depth | received/event、snapshot/update ID、local_u_before、stitch_verdict、epoch/generation、admission坐标 | 复用采集器判定；Applied刷新，缺快照/缺口要求同步，Snapshot恢复。旧事件时间持续到达仍更新received |
| Binance trade | received/trade_time(T)、record_kind、native trade ID、epoch/generation、admission坐标 | 非法输入不刷新；原生ID跨采集epoch连续；重复抑制，倒退ID维持原失败，冲突比较T及原公共规范载荷 |
| Chainlink spot/TWAP60 | received/payload time、合法freshness分类；TWAP窗口与admission；spot必要原生身份 | Fresh与Stale都刷新；四类异常保留旧有效消息和质量，不因异常或年龄自动切换TWAP |
| metadata | condition/assets、市场窗口、身份Snapshot时间、实际规则字段 | 原初始化仍由 `MetadataInitializationSelectionV1` 精确选择原文件和两Snapshot；已观察规则映射复用公共纯原语 |

`price_changes`只把资产列表写入窄表，不写价格、数量、盘口或完整谱系。输入端的嵌套列物理读取由引擎决定，不能把输出投影误说成所有嵌套列均已物理裁剪。重复键先由必要列定位。只有重复候选所在的行组才读取完整行，用原Rust类型解析、来源身份或规范成交载荷核对。PM新坐标上的有效同值消息保留，重复坐标不刷新。候选核对不为每条唯一PM消息构造JSON身份。

窄状态和区间存于引擎磁盘表，不在Python保存全量行或区间Vec。未知连接代次和同步上下文单列，可与有效年龄分布重叠。它们不构成另一个交易拒绝门。没有TTL、历史市场淘汰、去重截断或按日重置core；新入口根本不构建完整core。

## 时间、选源与区间

健康received/event来自最后有效消息的直接来源。合法可知时间使用 `max(received, admitted)` 和必要身份/源内前沿。代际排序取每epoch最先native序号那一行的 `max(received,admitted)`，同值用稳定file/row顺序。epoch内按native序号，并保持源内可见前沿。共同来源在去重或业务过滤前形成一次原生顺序和前沿；异常、非Trade或重复仍不刷新最后有效received/event。多个来源各自维护时间，不恢复旧六首项回放的全局 `now=max(previous_now,group.visible)`。因此新 `visible_at` 不是逐条旧core的at，不能声称两者逐条等价。这项区别不改变 S=t−last_received、A=t−last_event、未知/同步/时间异常或无消息时长的定义。

跨epoch首项与稳定file/row是本研究的确定性排序约定。墙钟值不能证明真实采集会话的物理先后，该约定不支持多个并发采集会话的竞争。已选顺序保持代际连续，不引入A→B→A的同步规则。运行结果须核对代际诊断；无法解释的真实竞争须保留结果并返回任务分流。

PM按准确condition/asset及半开市场窗口统计。共同源各自只有一份来源分布。策略依赖分布使用实际启用的Binance和参考选择。有效spot存在时继续选spot；spot缺失时按已观察规则选择TWAP60或spot，TWAP30不参与本轮校准。共同需求由覆盖边界合并，不按活跃市场数量重复计权。

区间使用同毫秒最终事实和选择。零时长暂态不切断连续段；每次真实有效确认仍保留到达样本。正时长未选区间切断段。首端、尾端、无消息和跨午夜时长全部保留。边界探针只读取此前已可见的状态，不用起点新事实回推历史。

逐scope报告标出source/condition/asset。PM市场和组的单位是资产·毫秒，次数是资产段之和；逐scope另报单资产最长段；不能称为市场墙钟健康时长。共同源单位是来源·毫秒。UTC日片段是完整连续段与该日的交集，日片段次数不能相加冒充全窗次数。真实午夜切换归新半开日；同状态跨午夜不伪造切换。左/右删失表示段在评价、市场、实际选源或日边界被截断。重新选择起点只用此前已经可见的来源状态判断左截断。恰在该起点新可见的旧时间事实不能回推到未选区间；一毫秒边界探针也不能把真实未选间隔连起来。切换每小时以全部已观察时长为分母，PM对应资产小时。

## 分布、三态与候选

S/A按连续时间毫秒加权，到达延迟按每次有效消息计数。独立未知、同步、不可用和时间异常分别记录；不把它们混入年龄分位或超限。原桶保持：0—60秒宽100ms，60秒—1日宽1000ms，此后桶下界与宽度均为DAY的二倍幂。SQL用首尾桶及中间范围差分精确累计毫秒，避免每区间展开所有桶。桶上界分位有100/1000ms或长桶宽度的近似误差，累计时长没有桶舍入误差。

原Rust方法计算每日Q1/Q3，fence=Q3+floor(1.5IQR)，保留桶下界≤fence后取主体p90。只有S/A均有分布的日进入，跨日取下中位数。三个完整候选的max为normal×2/4/8。倍率仍是待验证假设，不能替代真实尾部分布和取舍。缺少必需分组分布时传播原方法错误，不发明零默认。

研究保持原四折：9—11日拟合/12—14日验证；9—14日/15—17日；9—17日/18—20日；9—20日/21—23日。最后只9—23日拟合，先写 `fold-4.frozen.json`，再比较24—26日开发。准备可以读全部材料，拟合SQL必须先过滤自己的右边界，未来数据不能改变早折候选。

四组仍为PM、Chainlink、Binance trade、depth，共16个显式normal/max字段。正常要求S/A均严格小于normal；中间态仍严格小于max；达到max即超限。normal≤max，允许零或相等。SQL按同一生产三态定义向量化计算，有限跨语言测试调用真实 `source_health_state_v1` 对照。新入口不改策略阻断集合、撤单、补腿或发送前缀义务。三态不是故障准确率。

## 输出与监督

prepare输出 `changes.parquet / arrivals.parquet / markets.parquet / rules.parquet / epoch-ordering.parquet / initialization.jsonl / material.json`。`epoch-ordering.parquet`保留小型代际首项、末项、原生序号、received/available时间、前沿和实际所用顺序。共同来源诊断包含业务过滤前的必要列记录；PM诊断以原生消息计数，不按展开资产重复计数，已压掉相同坐标的重复。它不证明完整采集会话，也不增加拒绝或丢弃规则。material.json只在全部成功后写出。study保留两类每日直方图、逐source/asset/day分母、到达样本，以及五折各一份冻结和验证JSON。每个候选另有 `*.exceeded.parquet`，记录真实进入超限的时刻、最后直接received/event、当时S/A、前态、最后事实可见时间及删失；明确标出无新消息期间的阈值跨越，不把延迟阈值当事故真值。

进度默认30秒，阶段变化即时输出。日志含阶段、已读原始行/文件、已耗时、引擎查询进度、当前VmRSS、getrusage峰值RSS、CPU、I/O、工作数据库/ spill占用和实际剩余空间。无可靠总量的算子不编造完成比例或ETA。模型监督按用户要求每10分钟一次。成功后只删除本次已无用途的工作数据库；失败现场保留，不自动重跑、扩额、缩窗或跳源。

## 全局资源与命令

原输入2864文件、762,240,960行、45,652,568,053字节，其中PM728,429,320行；清单5186市场/10372资产不是实际状态数。计划依据本机32核/128GB，整进程共用DuckDB 24线程、80GiB引擎内存和700GiB临时盘额度。它们不是每来源预算，也不是进程RSS或总磁盘上界。输出、数据库、归档副本及引擎外开销另计。窗口算子会缓冲，必要窄列可排序或spill；完整峰值与耗时未知。

PM窄原表仍含重复condition/epoch和资产列表；按8字节时间/坐标及可变标识估算每行约80—200字节未压缩量级，7.28亿行约58—146GB，仅为一张表的条件估算。派生events/states等可能同时存在，嵌套列表、引擎页、排序和压缩改变实量。主要键是condition/epoch首次顺序/admission sequence、source局部顺序，以及source/condition/asset/visible_at。不能用原46GB压缩输入推定总盘峰值。官方说明见[Parquet投影](https://duckdb.org/docs/current/data/parquet/overview.html)和[缓冲算子及spill](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads.html)。

依赖锁为 `research/requirements-source-health.lock`，固定CPython3.14/Linux x86_64的官方DuckDB1.5.6和PyArrow23.0.1 wheel及SHA-256。实际环境/制品与源码在私有交接绑定。公开命令使用逻辑路径变量：

```bash
python3 -m venv "$ENV"
"$ENV/bin/pip" install --require-hashes -r baseline-mm/research/requirements-source-health.lock
cargo build -p baseline-mm -p data-pipeline-training \
  --example research_source_health_two_layer --example source_health_boundary \
  --release -j8 --locked --offline
"$ENV/bin/python" baseline-mm/research/research_source_health_columnar.py configure \
  baseline-mm/configs/source-health-two-layer-18d.plan.json \
  "$INPUT_ROOT" "$NEW_RUN_ROOT" "$BOUNDARY_HELPER" "$STATISTICS_HELPER" "$CONFIG"
/usr/bin/time -v -o "$PREPARE_TIME" "$ENV/bin/python" \
  baseline-mm/research/research_source_health_columnar.py prepare "$CONFIG"
# 仅prepare成功且material.json完整后，沿同一次授权执行：
/usr/bin/time -v -o "$STUDY_TIME" "$ENV/bin/python" \
  baseline-mm/research/research_source_health_columnar.py study "$CONFIG"
```

正式运行用前台包装器保留一次prepare成功才一次study和挂载失败停止：

```bash
"$ENV/bin/python" -u baseline-mm/research/run_source_health_columnar_once.py \
  "$CONFIG" "$ARCHIVE_ROOT" "$MOUNT_POINT" "$NEW_LOGS"
```

使用前核对准确源提交、两个优化工具、依赖、配置、输入身份和实际归档挂载。stdout/stderr/exit和信号单独保留。包装器收到SIGTERM/SIGHUP/SIGINT或监督错误时，结束并回收自己创建的活动阶段进程组，记录真实阶段退出码和信号，再退出；不进入study。脚本必须保持前台可追踪，归档不可用时沿绑定规则停止；配置不得作为新的合同或批准账本。

## 有界验证与适用限制

同一合法collector构造的262144行PM样本，1文件/1个row group，1线程墙钟1.286s、CPU1.216s、峰值RSS278.6MB；24线程1.006s、CPU2.618s、峰值RSS732.8MB。四份窄输出EXCEPT ALL差异0。真实CPU累计表明确有多核参与；没有采到每个算子的活动线程数。小样本行组少，不能外推十八日加速、总耗时或峰值。随后调整来源时钟/去重、规则连接、必要行数计量、代际前沿及诊断；该小型性能对照没有重测，不能称为最终所有路径的耗时。磁盘记录是测量结束占用，不称全程峰值。

代表测试覆盖原构造/解析入口、跨文件重复与冲突、资产和代际、缺口/快照恢复、持续旧事件、Chainlink异常保留、同毫秒样本和最终状态、真实断选、未知分母、首尾/午夜/删失、normal=max=0、未来折不泄漏和一个Parquet截断边界。原metadata初始化小文件的精确选择仍得到原3条前序；没有重扫十二分钟或十八日完整core。本次17项Python测试通过；固定workspace退出0，累计1379 passed/0 failed/23 ignored，具名UTC测试明确跳过。fmt和相关all-targets clippy退出0，既有警告保持。后续返工仅改Python、测试与说明，Rust输入未变，上述Rust结果继续适用。17条真实构造原始记录验证prepare命令；另存的19条有限标量确认验证五折10份冻结/验证文件，不能称为18日历史运行。返工代表例验证：未来epoch旧received不改已可见前缀；B首条重复过滤后，A→B→C仍保持10@100、20@400、30@400；实际5ms断选的两段共15ms，右删失2、左截断1；重新选择时才可见新事实的左截断0。监督SIGTERM代表记录真实阶段−15并回收，无study。结果见[工程机器摘要](complementary-pair-columnar-health-engineering.json)和PR，不把旧通过记录替代新入口验证。

## 历史紧凑工程证据

以下为此前PR #248的历史格式、有限验证和审查返工记录。它们证明旧标量消费者，不证明新列式入口或十八日运行已经完成。原公开机器结果保持原身份。

首次交付旧v1格式测量源码为 `3779fce58cd1300cddc85c1b5c0adc9be9a873ef`，release二进制SHA-256为 `cfb367904c697364c65cc5169c22d035201f0716aa3e4fb4297a25b03c2e7f87`，19,633,200B。完整固定workspace回归退出0（日志累计1370 passed/0 failed/23 ignored），具名UTC夹具明确跳过；原健康回调测量保持ignored。fmt与相关clippy通过，原仓库警告保持；额外 `-D warnings` 尝试被未改动contracts既有警告阻断，新例自身警告已修正。首次交付新研究7项、有限计量3项、真实CLI2项和来源健康组合9项分别通过。

既有十二分钟journal388,930行转换为388,931条固定宽标量（含选择记录），压缩流1,544,900B、scope字典3,942B，原journal156,083,054B。最终转换墙钟0.36秒、CPU0.32/0.03秒、峰值RSS8044KiB；新消费者分析墙钟0.12秒、CPU0.11/0.00秒、峰值RSS7564KiB。GNU time使用百分之一秒分辨率，文件系统输入统计为0，属于已有缓存条件；仅覆盖有限转换/读取统计，不包含原始归约，不是十八日容量或性能结论。

首次导入验证把共同源选择起点放在第一条消息，少计每共同源前2232ms未知头部。独立对照发现该偏差，最小修复后在新输出位置复核；原失败目录、报告、二进制与日志全部保留。最终四组与直接读取原紧凑journal的独立区间算术一致：有效S/A时长、未知/异常分层、到达样本、三态时长及切换/超限累计全部通过。该对照验证新标量消费者，不重制原始材料、不重写旧单层健康研究结果。精确分母、资源、命令及剩余事项见[机器结果](complementary-pair-source-health-two-layer.json)。十八日真实结果、正式阈值、实际尾部取舍与经营收益仍未执行。


## 独审返工后的v2验证

独立审查准确头 `a79c739b4f136a771f1d5a61e49c4df3ae624543` 发现同毫秒选源暂态拆段，以及缺少逐日/资产/市场统计维度。这两项已按当前合同修复；上述首次交付v1结果仍是历史证据，不代表新维度曾经验证。v2源码 `dc0305aff06df5d66f9b79f055c242f64e111eb3`，release二进制SHA-256 `134eec330853f79f2ed2e899607f0d0a7354043ce51565306b81c5477db6e383`，19,705,216B。

审查原最小材料 `[0,30)`、接收/事件时间0ms、normal0/max10ms，额外插入20ms的SELECT0→4：旧结果错误从一次/最长20ms/右删失1变为两次/10ms/2；新二进制两份材料均一次/20ms/1，总超限仍20ms。代表性回归额外保留同毫秒有效确认，来源自身三个样本不丢失；真正5ms未选间隔仍正确形成两段。不同日期、condition与资产的代表材料核对来源分母、状态差异、切换与逐市场和；跨午夜连续30ms超限全窗仍一段，日内10/20ms片段及左右删失分开输出。

只读首次交付已验证的压缩标量进行一次必要修复复核，未重新导入或重扫原十二分钟原始行。v2报告177,192B，与直接读取原#246紧凑journal的独立区间算术对照：12个scope、4个condition（包含无窗口交集的零分母）、四组及日内汇总全部通过。PM市场结果只合计其资产，不复制共同源；验证模式仍是全部共同来源的消费者诊断，不能冒充生产实际选源组合。分析墙钟0.29秒、CPU0.28/0.00秒、峰值RSS7572KiB，文件系统输入0，仍只是缓存条件下有限消费者证据。

固定workspace回归退出0，日志累计1372 passed/0 failed/23 ignored，具名UTC测试明确跳过；研究9项通过。fmt、baseline-mm all-targets clippy通过，未改动仓库警告保持。新证据沿原归档逻辑入口下 `review-rework-v2` 独立保存，旧57文件、原失败/通过记录、原二进制与handoff不覆盖。完整十八日运行、实际候选冻结与开发比较、正式normal/max采用及经营验证仍未执行。


## 午夜可见前态修复验证

第二次独审准确头 `97c4a2aa5a0ebfedce8f19d33b9d9bcf01843cc0` 发现：日内片段封口丢失此前标签，恰在午夜发生的真实变化漏计；刚可见的新事实又被用于回推过去，导致错误左删失。修复源码 `ae6d928774e2ad39049aa4b412b1abfc9fed3548` 给当前段保留实际可见前态，日界继续使用全窗此前标签，窗口/评估裁剪在新行赋值前取得前态。未知历史不由新事实补造，真实此前可见超限的裁剪延续仍左删失。

新release SHA-256 `5e1f98b9ef54f24c06f77e5405dfa90c264057247a7c5aebd041f8fc2d6432ac`，19,707,960B。令D=86400000，审查原四份最小材料只读复制后复核：期限恰在D到达，以及D才可见新事实的两例，D日切换均从错误0恢复1；延迟可见新事实的D日左删失从错误1恢复0。D+1消息的原正确结果仍1次切换。各例全窗连续时长/次数及来源分母不变，日内切换频率按10ms来源分母为360000次/来源小时，这只是最小合成边界反证的频率，不能当生产频率。

一个新增代表回归涵盖午夜阈值到达、接收/可见时间分离及两类裁剪起点；旧同状态跨午夜不造切换、同毫秒暂态不断段、真实断选分段均继续通过。固定workspace退出0，日志累计1373 passed/0 failed/23 ignored，具名UTC夹具明确跳过；研究10项、fmt及baseline-mm all-targets clippy通过，既有仓库警告保持。本次不重新运行既有十二分钟消费者、原始材料或十八日研究；旧维度/分母证据保留为此前验证，新午夜修复仅由本次精确源码及最小反证证明。新证据独立保存在逻辑归档 `review-rework-v3`，原审查输入与失败输出、此前二进制/报告/handoff均保留。
