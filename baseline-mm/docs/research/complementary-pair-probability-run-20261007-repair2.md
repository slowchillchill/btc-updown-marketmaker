# 第三次十八日概率准备：深度拼接状态失败（2026-10-07）

第三次单次十八日研究已经执行。prepare通过原身份前态和跨单元价格保留位置，完成TWAP与Binance成交阶段，随后在Binance深度公共归约中退出1。study未启动，完整prepare未发布，没有冻结参数、Brier、校准曲线或候选排名。任务3及正式参数采用仍未完成；本轮已形成窄范围共享深度修复候选，尚待独立内容审查。

本次批准见[#185单次执行授权](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6038140003)，当前失败分流见[#185记录](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6038958004)。[首次身份失败](complementary-pair-probability-run-20261007.md)、[第二次SQL失败及修复](complementary-pair-probability-run-20261007-repair1.md)继续保存各自运行身份。本报告与[紧凑机器证据](complementary-pair-probability-run-20261007-repair2.failure.json)只记录本次repair2实际运行。

## 执行身份与退出

| 内容 | 本次实际身份或结果 |
| --- | --- |
| 执行源码 | `cdcbc73dafea6ec8b8b7deb7adf4e844d0b4de84` |
| 独立执行源码归档SHA-256 | `ecfdf89d945d6aa582fb713debecb783f93d3813f180a8d7c2e2e534fc2d7782` |
| 完整配置SHA-256 | `fdc63693ac9cf237ee1747df7cc41e3019b9f8babdff97d0680e9d1aacad48a0` |
| 概率helper SHA-256 | `d77d6d2dbf5a252cd66ce171dc68a0c597733b0fc24014387b8188af773539ee` |
| 原初始化helper SHA-256 | `45bab72dca21884eb4ee418f6952b629720b78b00b8d593cfe3c9dcfbf48f4b0` |
| 启动请求UTC | 2026-10-07 12:44:27.639354 |
| 启动保护核对UTC | 2026-10-07 12:44:39.258274 |
| systemd-run退出记录UTC | 2026-10-07 13:25:27.947557 |
| prepare / study | 退出1 / 未启动 |
| 作业组退出 | `Result=exit-code`，`code=exited`，`status=1` |
| 信号终止 / OOM导致退出 / 自动重试 | 否 / 否 / 否 |

执行源码固定为同一提交的独立副本，报告在另一专用工作树整理。原配置字节、argv和两个helper均保持批准身份；没有因报告编辑改变执行脚本。使用既有监督器，一次prepare成功后才允许一次study，无自动重试。

研究日期仍为UTC2026-09-09—09-23，开发日期为09-24—09-26。原四折、最后十五日拟合冻结后三日评价、60个基线及三类各5权重的有限搜索保持；本次没有执行评分。正式参数采用、其他未明确大规模计算、完整经营回放、PAPER／LIVE、部署及资金不在本次批准内。

## 首错与留存输入边界

原stderr首先报告：

```text
InvalidField { field: "binance_depth_update.stitch_coordinates", reason: "do not match the reducer's current epoch state" }
```

调用链为`prepare → source_sampling → depth_unit → boundary.call(op='depth') → BinanceDepthReducer.apply_quantity_update → expected_depth_verdict`。执行版本`baseline-mm/research/probability/direct.py`第443行调用深度helper；公共校验位于`data-pipeline/core/src/reduction.rs`。helper退出后，Python读取响应失败，再由清理路径报告`public probability boundary exit 1`。后两项是首个公共状态错误的连带结果，不是三个独立故障。

失败留存输入`projection/depth.jsonl`为28,477,453字节，共16,070行：16,027条update、1条snapshot和42个采样点。SHA-256为`ec208bdac0b6fdcb2f03fa2e5aa2128fbcfcc1b1a052eb0ffc6452f0ac8fea6b`。下表时间均为UTC毫秒；可见时间按原生准入顺序与接收／准入前沿形成，不能用接收时间重新排序。

| 输入行 | 类型与可见时间 | 准入序号 | 保留的拼接坐标 |
| --- | --- | ---: | --- |
| 1 | 采样，1788998400000，即09-10 00:00:00 | 无 | 已输出非空bid／ask数量 |
| 2 | update，1788998403369 | 1169007 | `snapshot_last_update_id=null`、`local_u_before=null`、`not_applied_snapshot_unavailable` |
| 3 | snapshot，1788998403616 | 1169008 | `last_update_id=99917063901` |
| 4 | update，1788998403617 | 1169009 | snapshot=99917063901、local_u=null、`not_applied_stale_duplicate` |

第2—4行的epoch均为`5d71d25d92436a120d413f9e94de4d31e67092512c837457298dfcc68a162a94`，connection_generation均为1。第2行接收于1788998400015，准入于1788998403369，更新ID区间为99917061706—99917061741。第3行快照接收于1788998403609。第4行更新较早接收于1788998403380，却在快照之后准入；其depth_ingress_sequence为1169008，不能取代collector_admission_sequence作为公共重放顺序。

`depth-samples.jsonl`只有第1行采样，125字节，SHA-256为`62d1b92741a3f091f3af51c4c3e86e94a9b4d7f96222394a12671db066e0eb59`。bid总量为4714977/625，ask为162769273/50000。它证明首错前公共归约器仍有盘口；一个局部数量输出不能当作完整概率材料或评分样本。

`expected_depth_verdict`在有book时先核对snapshot_last_update_id和local_u_before。第2行的空坐标与保留book不一致，因此在后续快照处理前失败。留存文件内从第3行快照开始的后续坐标逐行相容；这只是只读输入核对，没有调用helper重放。

## 已确认结论与窄范围修复

已确认的失败是：公共归约器仍持有盘口，采集端第2行却声明快照不可用且没有本地u，两端拼接状态不一致。不能仅凭错误措辞认定该行损坏，也不能为迎合该行直接清空book、跳过冲突或伪造snapshot／stitch。

源码提供了UTC日界连续性缺少交接的强证据方向。`data-collect/src/training_runtime/binance_depth_source.rs`在活动Ready状态中比较snapshot接收UTC日与新update接收UTC日，不同则置AwaitingSnapshot并请求`utc_day_boundary`快照；`producer_day_barrier.rs`还通过ResetContinuity日栅栏清空连续性。这些路径不要求epoch或connection_generation变化。本次执行版本中，研究薄入口与完整Core共享的公共reducer只有epoch切换、confirmed hole等状态路径。Core来源质量／同步投影在公共归约成功之后更新，无法在此次校验之前补齐清空事实。

[当前修复分配](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6039188793)已采纳窄规则。共享reducer在epoch内保存最近一次真实snapshot的接收UTC日、depth_ingress_sequence和connection_generation。仅update与该snapshot同generation、update ingress严格较大且接收日不同，才在stitch验证之前使旧book失效。判定不依赖null、verdict或是否已经冲突；不是遇到snapshot_unavailable便迎合输入。完整入口输出Missing／Required，薄入口停止输出有效数量；真snapshot恢复时输出ResyncBoundary。转换只在实际可见行生效，不能倒改午夜采样。

新snapshot之后的`drain_depth_pending`不再比较日期，合法早到update可以跨UTC日。其ingress不晚于snapshot，继续沿原坐标拼接。identical snapshot也是真实收到的新前态，必须刷新连续性日期及ingress；公共盘口业务值及Confirm去重合同保持。没有根据旧snapshot_evidence的日期判定新直收更新。

这一修复补齐一类可从既有接收事实推导的跨日转换，不覆盖所有reset。Deferred之后来源仍可能Ready，随后同日围栏执行的ResetContinuity没有持久化，epoch／generation也不变。它仍是独立输入表达缺口；无法证明的同日空坐标／冲突继续严格拒绝。

上一查询单元的临时输入已正常消费并删除，本次未重新读取原始Parquet或执行真实前缀。没有准确前一行／snapshot身份，不能据此认证完整原生流无缺行。后续真实运行仍可能触及未解决输入缺口；本报告不承诺下一次prepare成功。

## 市场目录已完成，概率分母尚未知

本次`material/market-coverage.json`已落盘，10,134,764字节，SHA-256为`130e7301629d226d3811f87cc63ec335c9b1e8abf948bb91327191d680b62559`，与第二次失败目录逐字一致。完整prepare描述符、特征样本与拟合统计尚未发布，不能以目录替代成功准备。

| 目录分母 | 已落盘值 |
| --- | ---: |
| 原始观察市场 / 合法公共身份市场 | 5,186 / 5,182 |
| 可唯一定位原始窗口市场 / 未知目标数市场 | 5,186 / 0 |
| 原始可定位目标 / 公共窗口目标 | 41,472 / 41,440 |
| 目标并集 | 41,472 |
| 采样时身份已知 / 缺身份 | 41,440 / 32 |

四个缺身份市场各保留八个事后目标；普通缺身份元数据11行，身份冲突市场0。四市场坐标、时序和右删失区间复用[第二次机器证据的partial_market_coverage](complementary-pair-probability-run-20261007-repair1.failure.json)，不在本次重复堆明细。市场数包含边缘市场，不能乘8重建目标分母。

可计算概率、可信标签共同分母、fallback、Binance共同可算交集及覆盖损失均未知。五轮冻结为0，全部有限候选评分、Brier、固定十桶校准、分层误差和初始sigma／epsilon选择均未产生。未将缺失填零，也没有以目录资格推算概率资格。概率改善与经营盈利均没有本次结果依据。

## 资源及计量边界

| 计量 | 本次实际值 |
| --- | --- |
| ExecMain启动至退出单调时钟 | 2,460.300867秒 |
| prepare GNU time墙钟 | 2,433.80秒，40分33.80秒 |
| 作业组CPU时间 | 4,537.533725秒 |
| 内核作业组MemoryPeak | 20,200,595,456字节，约18.813 GiB |
| prepare GNU time最大RSS | 19,484,032 KiB，约18.581 GiB |
| prepare用户 / 系统CPU时间 | 4,130.35 / 406.90秒 |
| 最后有效作业组swap采样 | 0字节 |
| 85条程序进度中的spill最大观察值 | 0字节，仅采样下界 |
| 全部临时文件精确峰值 / 作业组完整I/O字节 | 未测得 / 未测得 |

启动保护实际为`MemoryHigh=48G / MemoryMax=64G / MemorySwapMax=0 / OOMPolicy=stop`。监督器、GNU time、prepare Python和helper同组，Codex在组外。共享24 DuckDB查询线程、8 GiB引擎预算、顺序阶段各64 GiB spill额度，Arrow串行、BLAS/OpenMP为1。引擎预算不是进程内存上限。

程序按既有30秒频率及阶段变化记录进度，会话共4次600秒查询。最后有效cgroup采样为13:15:29.585650 UTC，high/max/oom/oom_kill/oom_group_kill与swap均为0。13:25:49终态检查时ControlGroup为空，严格跳过所有cgroup文件，没有读取根cgroup。最终OOM/swap计数不能再读；首个程序错误、正常退出码和unit结果支持本次不是OOM或信号终止导致失败。

启动及检查未提供io.stat。GNU time文件输入／输出为357,928／5,134,944，按原字段保留，不换算字节或当作完整作业组I/O。spill采样不覆盖全部临时文件，也不能捕获所有瞬时峰值。完整成功耗时、内存及临时盘峰值仍未知，不能从三次失败成本外推。

## 证据与后续范围

独立档案相对入口为`issue-185/probability-calibration-actual-repair2-20261007`。已保存20份原件，共46,772,653字节：批准配置、运行交接、源码文件清单、实际命令、启动保护、完整阶段日志、4次检查、最终unit与退出记录，以及市场目录、失败深度输入和局部数量输出。来源／档案文件集合、逐文件SHA-256和可读性已核对。执行源码tar通过`issue-185/probability-calibration-repair2-proposal-cdcbc73-20261007/execution-source.tar`及准确摘要复用；另外保存必要紧凑诊断边界，未覆盖旧档案或归档可重建全期投影。

## 修复验证及复用范围

新增一条代表轨迹`received_day_reset_and_snapshot_provenance_preserve_pending_and_strict_stitching`，使用真实类型／证据构造分别调用完整公共归约和薄数量入口。轨迹覆盖旧日snapshot／applied、下一单元午夜采样保留旧总量、新日较大ingress触发Missing／Required、真snapshot恢复、跨午夜早到applied pending，以及identical snapshot刷新日期／ingress后排空较小ingress stale pending，再接纳新日直收更新。同日无独立边界的null坐标最后仍被两入口以相同错误拒绝。失效行的event_time属于前日、timestamp_received属于新日，验证判定使用接收日。

`data-pipeline-core --lib --tests`共193项通过，包含上述新增轨迹；`probability_boundary`示例2项通过。必要release helper构建通过，新helper SHA-256为`d0ad1c1197b18705f5bd25ad2eb10cc77e12186ea562a53980c39926ef9b2261`，与本轮修复候选内容绑定；它没有用于本次已失败真实运行。使用新helper的概率Python原13项通过。两包普通Clippy及格式检查通过，既有警告保留；没有使用-D warnings，也没有扩大修改范围处理旧警告。

初次代表测试编译误用准入时间字段名，修正后又被公共类型边界拒绝空的两侧更新夹具。已改为符合平台约束的真实构造，之后完整回归通过；这些中间失败日志保留，没有将其计为通过。没有新增真实样本或资源试验。

本次shared Core确有修改，因此执行受影响回归并重建概率helper。原初始化helper没有修改路径，保持原SHA及构建身份，没有重建。PR #257/#258其他未修改工程、健康研究及PR #259 SQL修复证据沿原适用范围复用；原13项不覆盖本次跨日连续性状态，不能外推为真实十八日全路径已验证。本次没有prepare／study、真实prefix或Parquet重放。

本次候选先交独立内容审查，通过后才提交PR并接受准确头正式Review。本次失败不产生第四次实际运行授权；后续重跑提案须绑定修复普通合并后的准确源码与完整配置，不覆盖本次`fdc636…`原件。失败现场继续保留，没有自动扩额、缩日期或再次运行。
