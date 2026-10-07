# 修复后十八日概率准备的 SQL 失败与最小修复（2026-10-07）

修复后的单次十八日研究已实际执行。prepare 越过原缺身份前态错误，随后在 TWAP 跨单元价格保留路径退出1。study 未启动，没有冻结参数、Brier、校准曲线或候选排名。任务3及正式参数采用均未完成。

本次批准见[#185运行授权](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6037137274)，实际启动见[执行记录](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6037180936)，本次最小修复见[失败分流](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#issuecomment-6037515063)。[首次失败报告](complementary-pair-probability-run-20261007.md)继续保存 `807ba5d…` 的 `MetadataBeforeIdentitySnapshot`；本报告记录新的 `36401b1…` 运行，不能互换两次运行的配置或结果。[紧凑机器证据](complementary-pair-probability-run-20261007-repair1.failure.json)保存实际身份、资源、目录分母及修复验证。

## 执行身份与退出

| 内容 | 本次实际身份或结果 |
| --- | --- |
| 执行源码 | `36401b1c05433d1bd602022e34275668a8c52bce` |
| 独立执行源码归档 SHA-256 | `b365991cd733e63ed4aee0e366dc492354c7a220e5f1e2440c53670a4fe6c550` |
| 完整配置 SHA-256 | `3b69d5dd28f68141c99f80191203bf5867854e511ace5652632d13c483f824ad` |
| 概率 helper SHA-256 | `d77d6d2dbf5a252cd66ce171dc68a0c597733b0fc24014387b8188af773539ee` |
| 原初始化 helper SHA-256 | `45bab72dca21884eb4ee418f6952b629720b78b00b8d593cfe3c9dcfbf48f4b0` |
| 启动 / 退出 UTC | 2026-10-07 11:41:56 / 11:53:56 |
| prepare / study | 退出1 / 未启动 |
| 作业组退出 | `Result=exit-code`，`code=exited`，`status=1` |
| 信号终止 / OOM导致退出 / 自动重试 | 否 / 否 / 否 |

执行前用准确提交生成独立源码副本，仅将原 argv 的工作目录绑定该副本。日期、候选、配置摘要和资源保护保持原提案。源码副本没有被本次 SQL 修复改写。修复位于另外创建的报告任务工作树。本次没有重编 Rust 或两个 helper，没有重复准入、健康研究或真实小样本试验。

## 失败位置与真实原因

实际调用链为 `prepare → source_sampling → price_samples(..., kind='twap')`。`baseline-mm/research/probability/direct.py` 执行版本第393行在第二个及后续单元插入非空 carry 时生成：

```sql
INSERT INTO price_facts (at,received,event,price,window_s,order_id)
VALUES (?,?,?,?,?,?);
```

DuckDB 报 `_duckdb.ParserException: Parser Error: syntax error at or near "at"`。表已由当前单元事实创建；失败发生在插入前一单元保留价格时。`at` 在这个 INSERT 列列表中需要按标识符引用。此前 `SELECT clock AS at` 等语法位置可以执行，不能泛称所有 SQL 别名位置都禁止 `at`。双引号标识符可包含关键字，内部双引号以双写转义，见[DuckDB官方标识符规则](https://duckdb.org/docs/lts/sql/dialect/keywords_and_identifiers)。

这是一处研究 SQL 构造缺陷。TWAP 和 Binance 成交共用这个动态 INSERT；本次实际先在 TWAP 触发，不能称真实成交阶段也已运行失败。旧测试的 TWAP 数据位于单个查询单元，进入 `price_samples` 时 carry 为空；跨文件测试覆盖的是 spot 的另一条历史路径。因此旧12项通过没有证明这个非空 carry 分支能够执行。

实际进度已进入 Chainlink 因果采样，后续在 TWAP 保留路径失败。原始行索引、市场事实与目录已处理，但完整特征材料没有发布。失败时没有保留当次进程内 carry 值，也没有可唯一定位该 TWAP 输入文件／行的状态记录；故障由原异常 SQL、调用栈和代表性合成入口复现定位，不追加扫描真实输入来补造定位。

## 已落盘目录与尚缺的概率分母

失败现场唯一落盘产物为 `material/market-coverage.json`，10,134,764字节，SHA-256 为 `130e7301629d226d3811f87cc63ec335c9b1e8abf948bb91327191d680b62559`。它是已经完成的市场目录阶段证据，不能代替完整 `material.json`、`samples.json` 或 `fit-pairs.json`。

| 分母 | 已落盘值 | 含义 |
| --- | ---: | --- |
| 原始观察市场 | 5,186 | 元数据、锚点规则和 PM 终局声明形成的事后目录，包含边缘市场 |
| 可唯一定位原始窗口的市场 | 5,186 | 本次无多值／未知窗口市场 |
| 合法公共身份市场 | 5,182 | 成功公共 FixedIdentity，不由原始目录补建 |
| 原始可定位目标 | 41,472 | 日期与窗口内的八个采样位置 |
| 公共窗口目标 | 41,440 | 由合法公共窗口形成的采样位置 |
| 目标并集 | 41,472 | 本次原始与公共窗口并集 |
| 采样时身份已知 / 缺身份 | 41,440 / 32 | 四个缺身份市场各保留八个事后目标 |

市场数包含窗口边缘，目标数已按日期和采样截止过滤，不能把市场数乘8重建分母。本次原始目录与公共身份冲突市场为0。缺身份的普通元数据共11行，分别为5、2、2、2行；其余锚点／WS终局走各自合法路径，普通行数不等于目标缺失数。四个 condition、原始坐标、可见时间和右删失区间保留在机器证据中。

这些目录分母只说明身份资格。可计算概率样本、可信标签共同分母及 fallback 数量仍未知，没有以缺身份32个样本推算全部不可计算样本。未形成任何折的候选评分或冻结评价，不能给出初始 sigma 最优值、epsilon选择或 Binance 增量效果。后续完整经营回放仍有缺 Snapshot 输入资格缺口，生产 Core 及公共历史入口的拒绝行为没有改变。

## 实际资源与计量边界

| 计量 | 本次实际值 |
| --- | --- |
| ExecMain 启动至退出单调时钟 | 720.165138秒 |
| systemd 打印的服务耗时 | 720.164秒 |
| prepare GNU time 墙钟 | 704.08秒 |
| 作业组 CPU 时间 | 1,761.800390秒 |
| 内核作业组内存峰值 | 19,996,930,048字节，约18.62 GiB |
| prepare GNU time 最大 RSS | 19,451,972 KiB，约18.55 GiB |
| prepare 用户 / 系统 CPU 时间 | 1,448.67 / 312.98秒 |
| 交换 | 0 |
| 30秒及阶段变化进度中的 spill 最大观察值 | 0字节，仅采样下界 |
| 全部临时文件精确峰值 / 作业组完整 I/O 字节 | 未测得 / 未测得 |

实际保护仍为 `MemoryHigh=48G / MemoryMax=64G / MemorySwapMax=0 / OOMPolicy=stop`，监督器、GNU time、Python和helper同组，Codex在组外。共享24 DuckDB线程、8 GiB引擎预算、顺序阶段各64 GiB spill额度；Arrow语义子集串行，BLAS/OpenMP为1。引擎预算不是进程内存上限。本次失败成本不能外推完整成功成本。

会话按600秒查询。最后有效 cgroup 采样在 UTC 11:52:30，`high/max/oom/oom_kill/oom_group_kill` 全为0，swap为0。退出由真实 Python 解析异常及正常退出码证明，没有 OOM 或信号终止导致本次失败。

UTC 12:02:42 检查时，已退出 unit 的 `ControlGroup` 为空，外部检查脚本误读了根 cgroup 的 `cpu.stat` 和进程清单。这两个读数不属于本作业，已在私有原始检查记录中追加更正，公开证据剔除该次全部 cgroup 文件读数。systemd 留存的本 unit `MemoryPeak`、`CPUUsageNSec`、退出属性和阶段日志仍有效。没有为修正检查记录新增运行框架。

GNU time 文件输入／输出分别为7,328和813,536个系统操作计数，不能转换成字节或当作完整作业组 I/O。进度仅观测 spill 文件；它不覆盖所有短暂语义子集，也不保证捕获两次采样之间的峰值。本次有27条进度记录，已观察 spill 为0，全部临时盘峰值仍未知。

## 最小修复与适用验证

修复只给动态 INSERT 的既有列名加双引号，并双写列名内部双引号。列名仍来自内部查询结果模式，值继续使用原绑定参数；没有新增白名单、拒绝策略或业务状态。跨单元保留、可见时间、TWAP窗口、成交方向及原生顺序保持。

新增一个代表性入口测试 `ProbabilityTests.test_price_samples_cross_unit_carry_and_new_native_order`，在实际 `price_samples` 中分别执行 TWAP 与成交分支。第一单元生成 carry，下一单元加入新事实。采样在新事实前继续读旧价格；同毫秒两条新事实按原生顺序取后项；空后续单元继续保留事实。成交数量和方向也逐值核对，不只检查 SQL 字符串。

原代码运行该测试：1项测试、TWAP与成交两个子场景均以相同 `at` 解析错误失败。修复后该项通过，完整概率 Python 回归13项通过，覆盖原12项及本次新增项。验证使用原锁定 Python、DuckDB和未改helper。未改Rust公共核心、初始化及健康兼容验证沿 PR #257/#258 的有效证据复用；本次不重建或冒称再次执行这些验证。

## 证据保全与后续边界

独立归档相对入口为 `issue-185/probability-calibration-actual-repair1-20261007`。其中保留准确执行源码归档、原配置、实际 argv、启动保护、完整 stdout/stderr、GNU time、退出记录、必要修正记录及原市场目录中间产物，另保存本次代表失败复现、修复后验证和修复候选。归档文件集合与源内容逐项核对，没有复制全期可重建投影。原始数据、旧档案、共享构建产物和其他任务工作树保持原归属。

本次 SQL 修复及失败报告不等于真实研究完成。下一次提案须在普通合并后绑定准确新提交和完整配置摘要，复用未改helper并保持原日期、候选与资源保护；本次 `3b69…` 原配置继续作为已失败运行证据保留。没有第三次真实运行授权，不自动再次 prepare／study。正式参数采用、经营候选、任务4连续账户、PAPER／LIVE、部署及资金仍按原实质条件处理。
