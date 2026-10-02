# 互补配对策略首五分钟公共材料试制

本报告交付 [#185 任务3公共材料与资源试制](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185) 的真实失败结果。UTC `[2026-09-09 00:00:00,00:05:00)`（毫秒 `[1788912000000,1788912300000)`）调用原 `prepare_historical_run_v1`，在公开核心处理首条元数据时返回 `MetadataBeforeIdentitySnapshot`。退出状态为1，没有生成正常公共稳定前缀或隐藏材料，也未进入公开统计、策略、账户或收益研究。

原始数据文件资格继续成立；这次失败说明文件资格不能代替业务输入就绪。完整配置、原错误、测量、七个保留文件的摘要与有限诊断见[机器结果](complementary-pair-material-trial-20260909-0000-0005.json)。后续公共范围规划需要核对真实身份依赖和输入证据映射；本次没有修公共行为、重跑准备、扩大窗口或补造快照。

## 1. 原调用与实现身份

[薄调用工具](../../../backtest/cli/examples/material_trial.rs) 直接调用公共准备入口；成功分支使用原 `RecordedPublicGroupSourceV1` 和原来源可见时间读取公开观察，仅做分母与缺失分类，不创建账户。统计区分正式／固定临时／缺失参考、现价与TWAP质量、生命周期、市场窗口、两侧盘口、tick、Binance深度／成交与有效空买盘。可见时间按原完整组及本次关闭输入取得，再保持原序单调；没有使用隐藏材料补输入或运行概率。该成功分支本次未运行，不能把已编写工具当成实际业务统计。

原入口依次执行首日清单 `freeze`、两遍完整文件解码（代际预扫描与外排）、五分钟范围内排序、公开归约／有限封口、公开重放和隐藏准备。此次完成外排后在公开归约失败；后续步骤全部未完成。规则、finality、策略参数与账户不是准备函数实参，因此本步没有恢复或加载这些材料。

来源基线 `784f4cc02e27853608bfd0dbc85f1fcf2525416e`。实际编译工具源码 SHA-256 `b0fe101af131e1ef73c162953e06564f8001689d716792646cc1ea843597e48e`，采样脚本 SHA-256 `f01cc44ec6d7353d642f472b02e0a8911dc895658f4d188b100cf121defa605a`，Cargo.lock SHA-256 `64238c35e2cfd53f3a6434e346e8a5085a6abc79e6ccdc2769631ac5061fcf3f`；实际优化程序 SHA-256 `1f73237174ae355de50ddbfeee3682c76bcb6793b7089fc6fbfc821bf4e2d7fa`。源码、脚本、锁文件及程序本体均保留在独立实验产物中；构建版本为 Rust/Cargo 1.95.0。

```bash
CARGO_TARGET_DIR="$ARTIFACT_ROOT/build" cargo build -p backtest-cli \
  --example material_trial --release --locked --offline
python3 backtest/cli/examples/material_trial_measure.py \
  "$ARTIFACT_ROOT/build/release/examples/material_trial" \
  "$PREPARATION_JSON" "$ATTEMPT_OUTPUT"
```

[测量脚本](../../../backtest/cli/examples/material_trial_measure.py) 使用 GNU time，按1秒遍历任务材料目录，结束后精确枚举大小及SHA。准备和公开统计分别从 `/proc/self/stat` 与 `/proc/self/io` 测墙钟、CPU和I/O；CPU按实测 `getconf CLK_TCK=100` 换算。本次公开统计测量为未执行，不能记为零耗时。原内部准入／解码／排序／归约没有独立计时，不能拆出未经测量的阶段数字。私有报告与原stderr保留，不向公开文档粘贴物理位置或原日志。

## 2. 输入、有效资源配置与保留材料

唯一原清单的逻辑定位为 `<INPUT_ROOT>/2026-09-09/typed_raw/utc_day=2026-09-09/manifest.json`，摘要 `93a70f4775bdb3d52bf7de73d58caaa5ccbf2f2d7f6a8e2f00e09c092e1f1918`。原入口内部准入通过后进入归约；首日145文件、2,169,975,828压缩字节、34,766,440行。原两遍解码均遍历首日，推导读取行数为69,532,880，不是工具增加的内部行计数。其余17天没有读取。失败返回没有导出内部 `freeze` 对象，集合身份为未导出；单日清单摘要不能冒称研究15天或审计18天集合摘要。

五分钟外排材料共76,279行：

| 原来源 | 外排Footer行数 |
| --- | ---: |
| Metadata | 66 |
| Polymarket | 70,746 |
| BinanceDepth | 3,000 |
| AggregateTrade | 1,903 |
| ChainlinkSpot | 281 |
| Twap | 283 |

以上行数来自本次已有六个 `merge/*/dpt-external-sort/sorted.parquet` 的Footer，是范围内准备行数；它们尚未形成可用公开观察，不能作为策略业务样本分母。

全部原资源字段在机器结果的 `effective_preparation` 保存，定位参数以逻辑占位替换；其余实际值如下。

| 原配置 | 实际值 |
| --- | --- |
| 解码 | `decode_batch_rows=1024`；`max_decode_batch_bytes=67108864`；`max_temporary_row_bytes=16777216` |
| 代际／归约／终态 | `max_epoch_count_per_source=1024`；`max_retained_reducer_entries=1000000`；canonical和header的 `max_pending_ws_terminals=1024` |
| canonical与recording排序 | 各 `max_run_rows=32768`、`max_run_bytes=67108864`、`max_temp_run_files=128`、`merge_fan_in=8`、`max_open_files=16`、`batch_rows=1024`、`max_temp_bytes=8589934592` |
| 记录 | `max_artifact_payload_bytes=16777216`、`max_capture_output_bytes=17179869184`、`max_candidate_directory_bytes=34359738368`、`max_observation_l2_levels=10000` |
| 读取 | `read_page_rows=256`；`max_read_batch_bytes=67108864` |
| 版本／初态 | `canonical-reduction-v1`、`market_business_observation_v1`、`historical-source-head-v1`；`Empty` |

这些是本次小规模工程配置，沿原入口实际读取及约束方式生效；不是正式风险参数，也不是对进程整体RSS的硬上限。单排序器、解码物理批次、记录预算各有原口径，不把64 MiB排序run或8 GiB临时排序预算称为整个进程的内存／磁盘上限。

| 终值目录 | 文件数 | 逻辑字节 | 分配字节 |
| --- | ---: | ---: | ---: |
| `materials/merge`（临时外排） | 6 | 15,193,139 | 15,208,448 |
| `materials/public`（仅流头，无稳定公共记录） | 1 | 355 | 4,096 |
| `materials/hidden`（未开始） | 0 | 0 | 0 |
| 整个材料目录 | 7 | 15,193,494 | 15,212,544 |

1秒采样的最大逻辑／分配字节为15,193,494／15,212,544，均为**观测峰值下界**；目录遍历不是原子快照，不能排除采样间出现更大临时文件。结束时无写入者，再枚举得到上述精确终值。六个外排文件和流头全部原样保留，未把失败临时材料当成已完成公共产物。构建目录和测量报告不混进材料大小。

## 3. 资源实测及估算限制

机器为32逻辑CPU的 AMD RYZEN AI MAX+ 395。启动前可用磁盘402,050,637,824字节，`MemAvailable=104,463,904 KiB`。没有清理全机缓存，材料进程运行期间没有本任务并行编译／测试；普通系统负载没有额外隔离。

| 实测项目 | 准备阶段／全程 |
| --- | ---: |
| 程序准备阶段墙钟 | 264.722928219秒 |
| 程序准备阶段CPU | user251.06秒，system12.57秒 |
| GNU time全程 | 墙钟264.74秒，user251.06秒，system12.59秒 |
| 全程最大RSS | 386,576 KiB（约377.52 MiB） |
| `/proc/self/io`增量 `rchar`／`read_bytes` | 162,380,150,844／4,366,696,448字节 |
| `syscr`／`syscw` | 37,177,674／3,937 |
| `wchar`／`write_bytes`／`cancelled_write_bytes` | 28,893,753／29,519,872／0字节 |
| 公开统计 | 未执行 |

`rchar` 是read调用交付量，包含原解码的重复读取；`read_bytes` 是内核物理读取计数，受缓存影响；2.17 GB是压缩输入有效负载。三者不同，不能把其中任一项伪报为另一个。CPU计数采样包含少量测量读取开销；GNU全程还包含报告写入。

以推导两遍行数除本次准备总墙钟，约262,663行/秒；这包括原准入、解码、外排及失败前处理，**不是内部解码独立吞吐**。五分钟76,279行除同一总墙钟约288行/秒，只表达包含首日固定扫描成本的范围材料交付比率。公共归约立即失败，公共观察吞吐没有成功实测；不能把两者称为完整归约或回测速度。

当前外排压缩终值约199.18字节/范围行。若仅作同分布的线性数量级推算，研究634,804,692行对应约126.44 GB外排终值，开发127,436,268行对应约25.38 GB；这既不含排序中间峰值，也不含公共记录、重放和隐藏材料，且盘口／消息大小及市场数量改变会影响压缩与归约放大。不能据该值证明402 GB足以完成研究或多候选运行。把首日固定扫描按全研究行数线性换算，只能估算该扫描与极小外排条件下的约4,834秒／81分钟数量级；不能用它给完整归约、经营、连续开发或多候选总预算。

此次没有得到公共／隐藏材料扩张系数或完整资源峰值。应先在所属公共范围内核定身份依赖问题，完成准确修复／审查／收尾后，复用同一批准小范围再测正常材料；据真实完整材料安排研究、连续开发及多候选预算。本次不自动启动该修复或复测，也不以删日期、日重置账户或另建缓存运行器规避资源问题。

## 4. 首条失败的有限证据与合同位置

调用链为 `backtest/src/preparation.rs::prepare_historical_run_v1` → `prepare_historical_files_in_range_v1` → `HistoricalMergeV1::next_input`／`prepare_input` → `PublicObservationCoreV1::push`。原 `data-pipeline/core/src/public_observation_core.rs` 的Metadata分支对未登记condition收到非 `MetadataSnapshot` 返回原错；`public/stream.json` 已创建，但尚无稳定公开记录。

首个Metadata排序项的实际字段：

| 字段 | 原值 |
| --- | --- |
| `metadata_row_kind` | `MarketStateTransition` |
| `condition_id` | `0xa653ad0b30faf75c56f20b40bd5b4c1194fe2153c0b2ca77ed3ceb8798a52c20` |
| `market_start_utc`／`market_end_utc` | 1788912000000／1788912300000 |
| `timestamp_received` | 1788912000008 |
| `collector_admission_sequence`／`collector_admission_time_utc_ms` | 6731／1788912002228 |
| `fixed_identity_verified_at_utc`／`source_evidence_received_at_utc` | 1788911700033／1788911700033 |
| `source_message_id` | `0f823ef2d5e501c79d224f04c21ae00b2abf84bedb69d69b5bdb2a0266b292c9` |

这是研究首个活跃市场，窗口与本次五分钟相同；身份声明在起点前299,967毫秒，不表示这是仅上一市场的尾部问题。该condition在五分钟Metadata外排66行中没有身份快照。进一步只读同日首小时原Metadata文件（98,097字节、793行、摘要 `968508be289147d6a2a5e8eb5325d73a62e3cd8bcfc602558dcd09b8bd6000ec`），该condition的63行仅含状态变化、锚点规则更新与终态元数据，没有 `metadata_snapshot`。00:00:02.302的快照属于不同condition `0x740fee4ffafd7a492c113f7c4a4e4fba026d7d2c0233e51d9643f4b2b09fbdfe`。详细坐标保存在机器结果；这是失败后的定位读取，不计入264.72秒材料测量，也不拿它补公开输入。

上述仅证明本范围缺少同condition的Snapshot输入、而首条状态变化含更早的身份声明。它不证明范围外原快照存在或丢失，也不证明必须读9月8日；没有读取起点前数据来求成功。原Metadata可能含可用身份证据，后续需在原输入所属模块核对映射，不能擅自把声明等同已登记的公共状态。

- [#7 §2.1 类型归属与初态](https://github.com/slowchillchill/btc-updown-marketmaker/issues/7#21-类型归属与初态) 原文：“初态为空：无代际、数量、身份、合法参考或临时锚点；不填虚假快照。”
- [#7 §2.4 真实原消息与等待依赖](https://github.com/slowchillchill/btc-updown-marketmaker/issues/7#24-真实原消息与等待依赖) 原文：“需要补齐旧来源材料时由输入所属方沿唯一记录提供，核心只读类型化材料及证据。”并明确依赖后来可用只改当前及后续，不构造匿名市场，不重放过去决定。
- [#7 §2.3 来源身份与历史次序](https://github.com/slowchillchill/btc-updown-marketmaker/issues/7#23-来源身份与历史次序) 保持每源前序、当前来源首项与原接收时间，不按文件先后任意调整。
- [#186 任务3、5及非目标](https://github.com/slowchillchill/btc-updown-marketmaker/issues/186#任务与步骤) 要求复用原输入/因果顺序、只处理准确范围、错误如实表达及公共缺口回原模块；[#185当前试制](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185#当前分配任务3公共材料与资源试制) 不扩窗、丢行、改准入或构造账户求成功。

本报告没有裁定公共修复方案或新增业务限制；D-0272、D-0273、D-0274及研究／开发日期保持原合同。正式／临时参考、窗口历史、两侧盘口与市场级业务分母本次均未核定，不能把缺失项填零或宣布业务就绪。

## 5. 工程验证

薄统计工具的代表性反例检查有效空买盘与保留值失效：`Fresh + Some(None)` 是已知空，`ConfirmedGap + Some(None)` 与从未观察均是Unavailable。固定workspace回归exit0，合计1235 passed、0 failed、18原ignored；新增example反例通过。`cargo fmt --all -- --check`、`git diff --check`、`cargo clippy --workspace --all-targets --locked --offline` 均exit0，普通clippy保留既有模块警告，新增工具无警告；具名UTC夹具 `four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second` 按仓库要求跳过。工程通过不证明原试制成功、业务输入完整或策略盈利。

固定回归命令为：

```bash
cargo test --workspace --lib --bins --tests --examples --locked --offline -- \
  --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
```
