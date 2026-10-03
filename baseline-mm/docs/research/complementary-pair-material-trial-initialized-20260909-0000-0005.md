# 互补配对策略初始化修复后首五分钟材料试制

本报告交付 [#185 任务3 S5](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185) 的真实制备与公共统计结果。在 [#196](https://github.com/slowchillchill/btc-updown-marketmaker/issues/196)／[PR #197](https://github.com/slowchillchill/btc-updown-marketmaker/pull/197) 按 [D-0276](https://github.com/slowchillchill/btc-updown-marketmaker/issues/62#issuecomment-5965448543) 完成审查、合并和收尾后，仅执行一次 UTC `[2026-09-09 00:00:00,00:05:00)` 的原 `prepare_historical_run_v1` 及公共统计。退出0、stderr为空，正常公共前缀、空core重放和隐藏准备均完成。没有创建账户、运行策略、收益研究或采用风险参数。

材料准备成功；业务信息仍有真实缺失，完整研究资源安排尚未解决。公开记录中的3个市场只有 **4352318 的窗口处于本研究五分钟**。281个不同新鲜spot来源不能当成76,244个独立价格样本，窗口端点存在也不能当成积分或概率已验证。完整数值、有效配置、原来源摘要、分母、例子及13个材料文件的摘要见[机器结果](complementary-pair-material-trial-initialized-20260909-0000-0005.json)。[原失败报告](complementary-pair-material-trial-20260909-0000-0005.md) 和原产物原样保留，不重标为本次成功。

## 1. 实际入口、实现与输入身份

[薄工具](../../../backtest/cli/examples/material_trial.rs) 直接调用公共prepare，成功后沿原 `RecordedPublicGroupSourceV1` 及原可见时间统计观察；[采样脚本](../../../backtest/cli/examples/material_trial_measure.py) 保持原样。原排序、归约、完整记录、正常有限封口、空状态重放及隐藏准备均由公共模块执行。规则、finality、策略参数及账户不属于该prepare实参，本次没有加载或合成这些输入。

实际优化构建源码提交为 `959637e67570a7f6ecf1f272cf06aac5ec1298c6`。工具源码摘要 `06b0e080b1fb613373aaf59bf7df0ad4c61b044bc69e3d9deaddfc40a548aa83`，脚本 `f01cc44ec6d7353d642f472b02e0a8911dc895658f4d188b100cf121defa605a`，Cargo.lock `4d78694113785b3c6e6776f903be1076414496206a2319ce43d15433674d88e3`；实际release优化程序20,862,112字节，摘要 `dbc3b8db4afaa385971e31dc061805b254c284e9de31bcda6625cc22800f3667`。Rust/Cargo 1.95.0，源码、脚本、锁文件、程序、配置、完整日志与环境在专用持久产物中保留，私有定位由交接提供。

```bash
CARGO_TARGET_DIR="$ARTIFACT_ROOT/build" CARGO_BUILD_JOBS=8 \
  cargo build -p backtest-cli --example material_trial --release --locked --offline
python3 backtest/cli/examples/material_trial_measure.py \
  "$ARTIFACT_ROOT/material_trial.binary" "$PREPARATION_JSON" "$ATTEMPT_OUTPUT"
```

研究唯一日清单仍为逻辑 `2026-09-09/typed_raw/utc_day=2026-09-09/manifest.json`，摘要 `93a70f4775bdb3d52bf7de73d58caaa5ccbf2f2d7f6a8e2f00e09c092e1f1918`。原入口准入首日145文件／2,169,975,828字节／34,766,440行，两遍完整日解码的69,532,880行是沿源码推导，非内部新增计数器。其余17天没有重扫，18天资格沿原[审计](complementary-pair-input-readiness.md)复用。

初始化只从前日原清单选定 `2026-09-08/typed_raw/utc_day=2026-09-08/source_kind=polymarket_market_metadata/polymarket_market_metadata_23.parquet`：97,043字节、792行，摘要 `c3bd000a74909abc1725b7bbe0fe3a72ebf7416b772c0ca28af859568c2f9154`；原清单摘要 `c057186754e3690592cf467d85f980af35f9259a4463a0e1e90192ea7192458f`。原单文件事实验证没有签发前日全日准入。两市场Snapshot身份和完整Existing片段由已合并选择器处理，实际选中6601、6665、6667共3条，公共输入位置0、1、2；生产选择使用来源消息身份，不用物理序号。没有追加起点前AnchorRule、盘口、价格或TWAP。

原日集合 `input_set_identity_sha256=2cbd7229cd785a26c9302be87be07cc890f806462f9bec1b7c43bab44f7a54fa`。实际组合 `source_identity_sha256=c25fc98df8f671ce295cf9fd49de5d18ee3ba90c36fb2e37466b4cf9e6cf2859`，版本 `historical-metadata-initialization-source-v1`，覆盖日集合及所选初始化清单／文件／市场Snapshot／区间／用途。两者不冒称15天或18天集合；实际有效流头排序为 `historical-source-head-v2`，没有改写旧失败v1报告。

## 2. 配置、来源分母与正常材料

复用原试制scope和全部资源字段，仅更换输出位置及流标识，增加D-0276初始化配置。完整有效配置在机器结果中保存，定位以逻辑占位表达。

| 资源字段 | 实際值 |
| --- | --- |
| 解码 | batch_rows1024；batch_bytes67,108,864；temporary_row_bytes16,777,216 |
| 代际／归约／终态 | max_epoch_count_per_source1024；max_retained_reducer_entries1,000,000；header与canonical的max_pending_ws_terminals1024 |
| 两类排序 | max_run_rows32768、max_run_bytes67,108,864、max_temp_run_files128、merge_fan_in8、max_open_files16、batch_rows1024、max_temp_bytes8,589,934,592 |
| 记录 | max_artifact_payload_bytes16,777,216；max_capture_output_bytes17,179,869,184；max_candidate_directory_bytes34,359,738,368；max_observation_l2_levels10000 |
| 原读取 | read_page_rows256；max_read_batch_bytes67,108,864 |

这些数值仅控制原工程资源口径，不是策略风险值或整个进程RSS／磁盘的统一硬上限。

外排Footer行数分别为Metadata69（研究66＋初始化3）、Polymarket70,746、BinanceDepth3,000、AggregateTrade1,903、ChainlinkSpot281、Twap283。共76,282条来源输入，含76,279研究输入及3初始化输入；另有1条EndOfInput。

正常前缀为 `normal_end=true`，next_input_position76,283、next_canonical_position76,260、next_group_position76,247、next_public_index_position76,247、next_observation_position228,386。公开来源组为3初始化组＋76,244研究组；观察为4初始化观察＋228,382研究观察。全部76,244研究组都有初始化直接来源依赖，这是合法保留的市场状态依赖，不能把研究组删掉或移入初始化分母。4个初始化观察分别属于4352293的3个及4352318的1个；研究统计不纳入它们。

公开capture的1,369,081条存储行包含不同记录种类，不等同观察数或独立样本。隐藏准备消费同一正常前缀，derived403,786存储行、messages70,745行、index141,534行；它们只说明真实派生就绪，不能补公开信息或代替本账户成交。各Footer只读核对及完整文件摘要随机器结果保存。

## 3. 市场与业务输入的实际覆盖

研究统计以原完整组可见时间分类，没有另加账户经营或宿主延迟。

| 市场 | 市场窗口UTC | 研究观察数 | 实际窗口／生命周期 |
| --- | --- | ---: | --- |
| 4352293 | 9月8日23:55—9月9日00:00 | 76,244 | 全部after；Active48,079、Retired28,165 |
| 4352318 | 9月9日00:00—9月9日00:05 | 76,244 | 全部inside及Active；本次完整窗口目标 |
| 4352362 | 9月9日00:05—9月9日00:10 | 75,894 | 全部before及Prefetched；不是已完成的下一研究窗口 |

4352293和4352318研究首见为00:00:00.008；4352362首见00:00:02.319；共同末见00:04:59.982。观察按公开组重复承载当时市场状态，目录市场数、观察分母、独立来源样本和完整窗口数分别解释。

以下业务质量统计针对4352318，以76,244个研究观察为分母；细项首次可见组及时间在JSON中保存。

| 实际字段 | 观察分类 |
| --- | --- |
| effective_reference | 缺失345；固定临时60s TWAP5,610；正式Fresh锚点70,289 |
| 正式锚点 | 未观察5,955；present:Fresh70,289，首见00:00:12.375，不倒灌至起点 |
| 结算参考类型 | 未观察345；60秒TWAP75,899，首次类型可见00:00:02.319 |
| Chainlink spot | 未观察331；present:Fresh75,913，首次可见00:00:02.251 |
| 60秒TWAP | 未观察333；present:Fresh75,911 |
| 30秒TWAP | 全部76,244为UnknownNotObserved；不是值为0 |
| Binance深度／聚合成交 | 深度未观察360／Fresh75,884；成交未观察336／Fresh75,908 |
| Up两侧L2 | 未观察144／Fresh76,100；初次L2可见00:00:02.244 |
| Down两侧L2 | 未观察145／Fresh76,099；初次L2可见00:00:02.245 |
| 两token tick | 各76,244个present:Fresh；不能据初始化tick认定起点已有完整盘口 |
| 估值最优买盘 | Up已知价76,243／Unavailable1；Down已知价59,712／已知空16,531／Unavailable1 |
| 60秒结算窗因果历史 | 参考缺失345；尚未入窗67,793；端点存在但积分未验证8,106 |

4352318共有281个不同新鲜spot来源，源时间端点9月8日23:59:59—9月9日00:04:58、最大前进间隔3秒、回退0；9月8日23:59:59是区间内收到的事件源时间，并非补读区间外现价。未逐段验证积分、gap或估计误差，不能把端点覆盖声称为连续积分已完整。30秒TWAP缺失按D-0272的实际动作依赖解释，不由本报告新增交易禁止条件。

已知空Down买盘自00:02:58.062首次出现，保持Fresh已知空与Unavailable的区别。两侧L2未知不被best_bid_ask存在自动补齐，也不从隐藏数量恢复公开盘口。4352293全部研究观察缺参考和两侧L2；4352362全部缺有效参考，虽稍后出现60秒结算类型及现价／盘口，窗口仍未开始。上述状态都是实际输入覆盖，不是策略盈利或行为验收。

## 4. 真实资源与材料终值

32逻辑CPU的AMD RYZEN AI MAX+ 395；开始测量前可用磁盘296,072,548,352字节，MemAvailable113,948,008 KiB。没有清理系统缓存，测量期间没有本任务并行编译／测试，普通系统负载未隔离。

| 测量 | prepare | 公开统计 |
| --- | ---: | ---: |
| 墙钟秒 | 393.220764114 | 59.372269321 |
| user／system秒 | 376.55／13.16 | 58.24／0.31 |
| rchar字节 | 163,486,291,553 | 696,211,410 |
| read_bytes字节 | 5,871,104,000 | 946,671,616 |
| syscr／syscw | 37,259,521／51,149 | 51,532／0 |
| wchar／write_bytes字节 | 393,465,163／395,833,344 | 0／0 |
| cancelled_write_bytes | 0 | 0 |

GNU time全程452.62秒，user434.79、system13.49秒，最大RSS388,804 KiB（约379.69 MiB），exit0。monitor墙钟453.491325485秒包含1秒轮询和结束后精确终值SHA，不能当prepare耗时。prepare原内部准入／解码／排序／归约／重放／隐藏没有独立计时，不伪报其分段性能。rchar是read交付量，read_bytes是受缓存影响的内核物理读取计数，压缩输入大小另列，不混为一个口径。

| 结束后精确材料 | 文件数 | 逻辑字节 | 分配字节 |
| --- | ---: | ---: | ---: |
| merge保留外排 | 6 | 15,225,360 | 15,237,120 |
| public正常前缀及来源 | 4 | 287,698,816 | 287,711,232 |
| hidden全部派生及排序 | 3 | 56,788,225 | 56,795,136 |
| 总计 | 13 | 359,712,401 | 359,743,488 |

1秒目录采样的峰值下界为逻辑368,713,365、分配368,771,072字节。采样遍历不是原子快照，不能排除采样间更大峰值；结束后材料进程退出、无写入者，逐文件枚举及SHA得到精确终值。构建、试制报告及调用配置没有混入材料字节；public流头及来源配置属于材料。原consumer所需public文件＋hidden derived/index合计333,698,428字节；保留merge及hidden/messages中间排序材料26,013,973字节。hidden目录终值包含两者，不把全部hidden字节仅称最终消费文件。

## 5. 资源推算局限与后续依据

本例公共＋隐藏终值344,487,041字节，除以76,279研究输入行约4,516.15字节/行；全部材料约4,715.75字节/行，采样峰值下界约4,833.75字节/行。这个分母不含初始化3行，也没有将固定首日扫描放入样本数量。

若仅保持本例消息组成、压缩及约3个已知市场的观察宽度，作数量级线性示例：研究634,804,692行对应公共＋隐藏约2.867 TB、全部材料约2.994 TB；开发127,436,268行对应约575.52 GB、600.96 GB（十进制）。该示例已超过本次可用磁盘约296.07 GB；它**不是可靠完整预算**。长区间的市场数量／保留方式、每组观察重复、消息组成、压缩和隐藏中间产物会改变放大，不能将五分钟359.7 MB直接按时间或行数宣布为全研究需求或可靠峰值。多候选及账户经营产物尚未测量。

每次prepare仍有首日全量准入和两遍解码固定成本，不能把393秒按288倍换算全天，也不能从原53秒资格审计估算完整准备。当前只证明这一小范围在原资源值下完成，完整15天及连续三日资源安排尚未闭合。本次不自动启动全研究，不删日期、不日重置账户、不另建baseline缓存或运行器。

后续按真实结果规划因果统计与候选形成，同时在所属公共材料范围明确可复用产物、资源安排与必要测量；不把文件准入、材料成功或端点存在当成业务参数成立。若需要公共工程调整，先落实准确公共任务及验收，不能混入本研究报告。正式风险值仍须研究证据与取舍后由用户审定，D-0272／0273／0274和原A串行顺序保持。

## 6. 文档验证与工程证据复用

本PR仅新增本报告和JSON、更新研究计划，不改代码、工具或原始数据。核对原私有结果、来源身份、Footer、正常前缀、所有分类分母及终值摘要；JSON语法、文档引用、公开路径与差异检查通过。

复用PR #197准确基线的固定workspace1243 passed／0 failed／18 ignored、初始化5项及材料例子3项通过、fmt和clippy退出0及独立正式审查。本次纯报告不重复整套回归；具名UTC测试 `four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second` 在所复用记录中明确跳过。独立本报告审查仍需完成，工程与本次材料通过均不表示策略收益验收。
