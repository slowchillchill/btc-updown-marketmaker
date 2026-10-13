# ComplementaryPair：真实 NoTrade 与计算时序（2026-10-12）

本段补齐两个真实 NoTrade 前缀，并区分输入装配、纯策略调用与模拟计算时钟。没有修改生产代码、观察所有权、A1、15ms 或自动结算。基线为 `3d2397f234a57a93da69f2c2d750794b9fc6b348`。这是有限工程测量，不是新模型小时经营、线上尾延迟或实盘盈利验证。

配套[小型数据摘要](complementary-pair-notrade-calculation-timing-20261012.json)记录原样本的最近秩分位数、前缀、失败和真实命令。原 Actions 结果复用[公共经济组合报告](complementary-pair-economics-composition-20261012.md)，没有重跑其96个样本；更早的[装配与等待诊断](complementary-pair-host-assembly-wakeup-diagnostic-20261012.md)保留原身份。

## 真实状态与计时边界

两格均用正式 Core、recorded reader、SIM、NOE、Ledger 和 `ComplementaryPairV1`。每格预热1次，再测32次；交易经济详情需求保持 false。恢复策略、序列化、比较和额外读取在计时外。每次比较完整策略输出、策略状态、同一业务截点和完整账户经济字段；测量输出不分派。两格各32次均为全策略空意图，没有 Exit 或 Actions。

- **成熟健康 Maintain**：沿原301个来源点形成概率历史，真实分派策略双腿报价，各50份经SIM→NOE→Ledger接受。旧闭市挂单的真实取消完成，当前两腿仍各开放50份。固定增加一个新鲜来源点，覆盖原冻结请求与反馈时延；没有搜索时间或改变延迟。当前866个观察市场、866个经济市场，窗口内保留242个当前Chainlink样本。请求未回执、pending NOE及待撤均为0，取消响应没有未知。当前市场为 Maintain；全账户仍保留旧市场的真实5份孤腿，不能称全账户空仓。
- **缺价格 Wait**：保留866市场及真实盘口，合法前缀尚无Chainlink价格和Binance成交价格。来源健康为 `UnknownBusinessProgress`，概率为 `Unavailable(UnhealthySource)`，当前选择Wait。只使用该前缀所需的一组历史输入，没有伪造Missing或删除冻结字段。旧市场5份真实成交保留，开放订单与待撤均无；没有取消请求，所以响应计数为NotApplicable，而非未知。

Maintain的额外来源点、真实报价和时间截点与原Actions三格不同；Wait也没有301个成熟价格点。因此下面是各固定状态的边界测量，不能作为严格前后性能对照。

单位为ms；分位数采用最近秩，32样本的p99与最大值相同。

| 状态 | 边界 | p50 | p95 | 最大值 |
|---|---|---:|---:|---:|
| Maintain | 完整装配 | 7.133821 | 7.413888 | 7.483138 |
| Maintain | calculate（含截点复制和decide） | 3.954274 | 4.312409 | 4.320213 |
| Maintain | 独立纯decide | 3.789815 | 3.947312 | 3.962059 |
| Maintain | 连续装配→calculate | 11.126748 | 11.695669 | 11.778245 |
| Wait | 完整装配 | 6.803780 | 7.102081 | 7.181541 |
| Wait | calculate（含截点复制和decide） | 2.375623 | 2.641293 | 2.781337 |
| Wait | 独立纯decide | 2.117737 | 2.292877 | 2.301634 |
| Wait | 连续装配→calculate | 9.200882 | 9.637384 | 9.780974 |

连续边界是实际连续调用的墙钟，包含两次计时读取等辅助开销；不是两个独立分位数的和。独立decide从同一个调用前策略状态恢复，不是在calculate已经推进的状态上再调用。装配仍保留原apply_closed与observe_group副作用，并核对截点、经济字段及请求总量没有变化。

原Actions三格是**当前市场**空仓／挂单／单腿，各32次Actions，NoTrade为0。它们均有866个经济市场；全账户正孤腿市场数分别为1、1、2。复用结果如下，原材料没有本轮新增的calculate及连续边界，不能补造它们。

| 原Actions状态 | 装配p50／p95／最大ms | 纯decide p50／p95／最大ms |
|---|---|---|
| 当前市场空仓 | 7.416724／7.963573／7.966799 | 3.241522／4.105580／4.258738 |
| 当前市场挂单 | 7.653520／8.274358／8.646389 | 4.297762／5.208265／5.316910 |
| 当前市场单腿 | 7.875006／9.090604／9.158292 | 3.642558／4.624426／5.302173 |

## 一个原有限健康日

只运行原 `real_strategy_finite_health_callback_measurement`。Trace完整委托原策略，包括观察需求；样本仅围绕原decide委托调用计时，分类与收集在计时后。原Maker发送、非空真实成交和资格检查断言保持。14次真实decide中有8次NoTrade、6次Actions，没有Exit或Error；12次资格调用、1次期限唤醒、2次失效批次。

| 实际边界 | 次数 | 总ms | 最大ms |
|---|---:|---:|---:|
| Trace纯decide | 14 | 0.426843 | 0.074229 |
| 公共资格回调 | 1 | 0.001412 | 0.001412 |
| 完整资格回调 | 11 | 0.021200 | 0.007584 |
| 下一健康期限回调 | 49 | 0.035507 | 0.005360 |
| 生产stage：策略输入装配 | 14 | 0.485155 | 0.110448 |
| 生产stage：calculate（含截点复制和decide） | 14 | 0.522573 | 0.086213 |

外层`run_day_trace`实耗115.190406ms；day producer scope为108.973361ms；candidate scope为100.937152ms。阶段及回调有嵌套，两个scope可并行，不能相加或相减作精确分解。公共stage只有count／total／max，没有p95。配套JSON保留14个decide原时刻及分类，但其8／6个样本不是两个固定状态的重复测量。

原件`run.json`直接记录模型 `L2_BOOK_DELTA_FIFO_ESTIMATE_V1`。实际日装配经 `HistoricalSessionV1::spec` 使用HistoricalBacktest，经 `create_new_historical_day` 使用FiniteHistorical；这两个身份来自当前源码调用链，不冒称原件顶层包含它们。夹具仍加载原gate-b封存旧Maker规则，不是新保守历史模型的经营验收，也不是本次PAPER运行。

## 观察保留与时钟

`sim-system`的观察帧借用原观察。`shared-run/src/strategy.rs::input_from_frames`只构造一次拥有数据的StrategyInput，逐市场克隆观察和规则上下文。`StrategyInputV1::calculate`复制截点并调用策略。backtest同步调用完成后只把计算结果和时序记录存入pending；未发现可直接删除的第二次整观察复制。原独立向量clone探针不能证明生产重复复制；本段保留所有权，也不声称整输入必须跨15ms持有。

公开组不是固定4ms或15ms帧。交付一个组未必启动计算，启动还受dirty、计划开始及pending条件控制。`backtest/src/run.rs`先同步执行真实calculate；该调用期间消费线程不能同时推进下一公共组，独立producer仍可供料并遇到回压。calculate返回后才按配置生成模拟完成／计划发送时间并建立pending。其后的15ms模拟等待不是Rust策略仍在后台计算。

pending期间仍处理公共事实、账户反馈和资格失效；dirty合并后续重算需求。模拟观察、计算开始、模拟完成、计划发送和实际发送分别由 `CalculationTimingRecordV1`表达。A1只在真实decide采样；等待期间的中间价格可能未入策略历史，这是已批准D-0333边界。已冻结结果遇同毫秒失效沿既有机制处理，不回滚时钟。原A1、等待通知与Maker证据复用，未重跑矩阵。

NoTrade也是`StrategyOutputV1::Continue { intents: [] }`，不是提前Exit。`run.rs`仍生成pending；正常Continue取`send_at`作为到期事件，不因空意图绕过配置的15ms。若dispatch delay为0，该窗口在计算开始后15ms结束；另有dispatch delay时仍按原配置等待。到期取走pending后，公共分派处理空批次，没有真实订单请求。随后才允许`pending.is_none() && dirty`安排下一计算，仍遵守calculation start delay；零开始延迟可在该模拟毫秒启动。pending期间多次公共更新只合并dirty，不立即逐组重算。NoTrade的分派时间记录也不能当实际订单发送证据。

供料回压是生产者与消费容量的工程等待，不能直接当在线消息积压；本段没有在线全链测量。64样本的连续边界不含供料、资格、预留、发送和反馈，更不证明99%NoTrade可跳过。没有形成修改15ms的依据，本次不提出调整其配置。

## 失败、身份与验证

首次编译42.662184s因新增断言对无PartialEq的NOE引用使用`assert_eq`失败；改用正式Known(None)模式匹配，期望不变。第二次编译67.654601s通过。第一次业务执行10.701578s在Maintain准备阶段失败：CancelOrders存在但SubmitPairedBuy不存在，计时样本为0，Wait尚未进入。

新增reader遗漏原helper的`enable_source_progress`。reader重建独立Core，默认不投影来源进度；写入材料的Core开关不能传给reader。已联网回读准确基线原helper，其reader显式启用该开关。首次守卫前未保存实际健康诊断，缺口如实保留。仅恢复原开关并在守卫前保存诊断，输入、时刻、seed、数量与全部有效断言不变；增量编译3.685540s，唯一复验18.043662s通过全部64样本。原失败及源码差异保留。

成功raw中的`helper_sha256`错误沿用了前一次环境标签，原raw未改写。实际成功编译和运行绑定 `measured-progress-fixed.patch`，两条命令均记录source_unchanged=true；`notrade-measured-source-binding.json`给出准确helper哈希及标签更正。不能把错误标签当原始正确身份，亦不为标签问题重跑测量。

有限日用例通过，命令墙钟0.153568s。受影响两测试目标clippy通过，25.382700s；提示仅在未改的生产代码与既有测试行。修改文件rustfmt及diff检查通过。未运行全工作区、B、服务器、旧96Actions测量、新小时经营、CPU采样或公共生产回归；没有生产接受／拒绝条件变化。

原始64样本、完整字段、有限日结果、stage stderr、首次失败与准确命令独立保存；本轮归档入口在PR交付记录中给出。原Actions归档 `btc-updown-marketmaker/issue-185/economics-composition-uncommitted-1267df3-20261012T071021Z`继续保留，helper身份 `455abb2748fc89c4f14ba126501c519656cefe1aed8c8cd9aa2421534034676a`。本轮样本和引用不证明当前B未提交候选兼容。
