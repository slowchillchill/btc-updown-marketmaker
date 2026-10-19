# 连续零账户公共观察导出

正式库入口是 `backtest::export_continuous_public_v1`，正式程序是
`backtest-cli` 包的 `export_public_continuous`。它消费声明接收范围内的原 Parquet，
提供公共组后的观察、声明时点的查询和正式新成交。不创建策略、SIM 会话、Ledger 或交易账户。
本入口落实 [#342](https://github.com/slowchillchill/btc-updown-marketmaker/issues/342)，
接口决定见 [D-0368](https://github.com/slowchillchill/btc-updown-marketmaker/issues/342#issuecomment-6266395586)。
统计公式和缺失语义直接采用[公共统计合同](../data-pipeline/public-statistics.md)。
研究标签、阈值、账户可成交性和盈利判断由研究消费者负责。

## 启动与配置

```bash
cargo build -p backtest-cli --bin export_public_continuous --release --locked --offline -j 16
<程序路径>/export_public_continuous \
  --config <完整配置.json> --output <新输出.jsonl> \
  --progress <进度.json> --stop-file <停止标志> \
  --progress-interval-seconds 15 --threads 8
```

所有路径由调用者提供。程序直接解析 JSON，不连接网络，也不安装依赖。
`--threads` 配置本次独立 Rayon 线程池；文件顺序定位和普通 PM 准备可并行，
公共归约、应用、统计和出口保持因果顺序。GPU 不适用。
进度间隔单位是秒，0 表示每次通知都输出；阶段变化总会输出。
正式进度文件通过临时文件重命名更新，可以用 `cat <进度.json>` 查询。
总量和剩余时间没有可靠估计时为 `null`。扫描行数是准备阶段实际访问量，不等于已应用输入数。

有限合成输入、完整可运行配置由以下命令生成。它只生成工程夹具，不读取真实数据。

```bash
cargo build -p backtest --example continuous_public_fixture --release --locked --offline -j 16
<夹具程序路径>/continuous_public_fixture <新夹具目录> 1000
```

夹具目录中的 `config.json` 是完整生效配置示例。必须把 `rules.snapshot_path`
改为交付包内保留的正式规则快照路径，并保持其准确 SHA-256；不要保留将删除的任务树引用。
研究运行应复制配置并替换实际获准输入、范围、初始化和公共参数，不能以工程夹具取得真实运行授权。

| 配置 | 实际作用 |
| --- | --- |
| `preparation.scope.manifest_paths` | 冻结原清单、文件事实和内容身份，沿正式 Parquet 准入读取 |
| `preparation.scope.received_start_utc_ms / received_end_utc_ms` | 接收 UTC 毫秒范围 `[S,E)`；不是应用时刻截止 |
| `preparation.metadata_initialization` | 可选正式元数据初始化选择；复用原选择/来源身份，不补造盘口、规则或行情热身 |
| `preparation.header` | 公共流、归约和观察版本；实际排序版本由原入口确定。实际 `input_scope` 追加 `[S,E)` 和组合来源身份 |
| `preparation.read_page_rows / max_read_batch_bytes` | 原 Parquet 批读取及有界普通 PM 预读 |
| `preparation.canonical.max_epoch_count_per_source` | 原顺序定位使用的 epoch 参数 |
| `rules.snapshot_path / expected_file_sha256` | 既有 `load_historical_process_snapshot_v3` 只读加载的实际 sealed 规则材料；不创建 SIM |
| `markets` | `all`、`condition_ids` 或 `window_starts`。后者按市场开始 UTC `[start,end)` 选择观察市场，不筛掉公共归约输入 |
| `statistics` | 原 `PublicStatisticsConfigV1` 的动能、spot 波动及终局积分窗口，毫秒；空集合表示无该类窗口 |
| `public_delivery_delay_ms` | 原 `DelayScheduleV1`，如 `{"Fixed":100}` 或 `{"Sequence":[...]}`。序列按公共 `group_position` 索引；耗尽沿原错误处理 |
| `observe_after_groups` | 是否输出每次合法组应用后的观察；关闭仍更新公共统计 |
| `query_times_utc_ms` | 声明逻辑 UTC 查询；按升序、同一时点一次读取。查询不产生行情样本 |
| `export_new_trades` | 是否保存正式新增成交；关闭不改变接纳、去重或统计 |

`preparation` 复用原 `HistoricalPreparationConfigV1`。其 `work_root`、`hidden_stream_id`、
`recording` 和其余 `canonical` 准备参数在本入口没有消费者，不创建这些输出或隐藏模拟材料。
它们保留为复用类型的序列化字段，不是额外研究参数。范围、窗口、交付参数及输出开关没有研究日期默认值。

规则加载的 `replay_causal_start_utc_ms` 由实际范围开始和所选初始化来源准入下界取较早者，
在 `identity` 行记录。调用者不能独立传入更晚时间。`rules_identity` 使用实际 sealed 文件的 SHA-256。
输入身份复用 `MetadataInitializationSelectionV1::source_identity`，因此包含原日集合及实际初始化选择。
输入范围内接收、但因延迟在 E 之后应用的组仍会全部处理。

选择的市场只在其已公开窗口 `start <= t <= end` 输出观察或查询；在边界可同时输出两个已公开市场。
未来元数据不会提前公开市场。新增成交仍保留配置所选市场的全部正式接纳事件。
缺少市场观察不是零成交或零盘口。消费者必须核对市场窗口、`query_boundary.observed_markets`、
来源质量和输入覆盖后判断标签是否有足够观察。

## 正式应用和查询

应用时间复用 `DeliveryTimes`：原来源可见下界加显式延迟，再与上一应用时间取较大值。
来源接收时钟、CPU 墙钟和研究采样间隔均不代替此时钟。
每个组只把同组冻结观察和匹配 `PublicGroupReferenceV1` 交给 `PreparedPublicStatisticsV1::consume`。
这个公共消费宿主不签发 SIM receipt，也不把自填时间包装成 SIM 受信凭据。

`query(t)` 先消费所有应用时间 `<=t` 的组，随后按最后合法前缀读取 t。
因此同毫秒的较后组不会进入较早组的冻结观察，但会进入同毫秒的声明时点查询。
预读后组不推进统计。迟到 K 只从其实际合法应用起影响当前和未来，不回填旧行。
静默读取沿 F3/F4/F5/F8 自身算子推进，不刷新来源时间，不伪造新行情。

## 输出和字段映射

格式是 `continuous-public-export-v1` JSONL。按文件行序消费。
`observation` 和 `query` 的 `changes` 是市场投影的替换补丁：按 `condition_id` 保存前值，
只替换出现的键。斜线是键名的一部分，不是递归合并路径。
首次输出该市场时包含全部投影键；缺失字段仍用原 `value:null`、质量和缺失原因表达。
`null` 是有意义的新值，不能忽略。

| 行或投影键 | 原正式类型/字段与用途 |
| --- | --- |
| `config` / `runtime` / `identity` | 完整请求参数、实际运行线程/进度参数、实际公共头/输入/规则/交付/统计版本和组合初始化文件事实 |
| `source_anchor` | 一项 `DirectObservationAnchorV1` 的完整事实；后续小事实用 `anchor_refs` 引用其本文件数字 ID。它只是序列化引用，不是应用凭据 |
| `source_change` | 真实变化的单项 `SourceStreamProgressV1`；原共享 scope 被移除时为 `progress:null` 并保留 `scope`，不逐组写完整来源数组 |
| `market` / `rules` | `MarketIdentityV2` 的市场/每腿身份与窗口；`public_rule_context` 的实际规则标量及来源 |
| `official_anchor`、`settlement_reference`、`temporary_reference`、`effective_reference` | 原正式 K、规则与临时参考，互不替代 |
| `lifecycle`、`active`、`closed`、`accepting_orders`、`terminal` | 原公开状态、质量和来源；不引入账户 gate |
| `chainlink_spot`、`chainlink_twap_thirty`、`chainlink_twap_sixty`、`binance_trade` | 原参考/聚合成交及小事实证据；最新聚合成交不是新增 PM 成交 |
| `assets[].direct_bba` | 原 `best_bid_ask` 的直接 BBA；含双侧报价，不冒充簿派生值 |
| `assets[].valuation_best_bid / valuation_best_bid_state` | 正式 `valuation_best_bid` 及其来源；可选用较新的直接 BBA，不能当作簿买一。状态保留 `unknown`、`known_empty`、`price`，避免 JSON 的嵌套 Option 丢失区别 |
| `assets[].book_best_bid / book_best_bid_state` | 仅从正式 `bids` 选择正价正量最高档的价格；状态按该簿投影区分未知/未物化、有效空簿和有价，与直接 BBA 的空买价无关 |
| `assets[].book_best_bid_quantity_shares / book_best_ask` | 正式 `bids` 中所选最高买档的份数，以及正式 `asks` 中正价正量最小卖档的价格/份数；不从 BBA 或估值买一猜数量 |
| `assets[].bid_materialized / ask_materialized / bid_quality / ask_quality / tick` | 明确未物化与已维护空簿；tick 保留其原观察证据 |
| `statistics/depth` | 原 F1/F6 深度最佳价、中点/价差、可见 BTC 数量和及压力、缺失和覆盖 |
| `statistics/blb` | 原 F2，去掉档位数组；保留版本、状态、缺 K、`at_anchor`、距离/侧/N/Q/V及覆盖。`at_anchor` 的 N/Q/V 是空，合法可见空区间才为零 |
| `statistics/momentum / spot_variance / spot_integrals` | 原 F3/F4/F5 结果、窗口、左右支撑/时长和覆盖，不重算公式 |
| `statistics/latest_trade_pressure / basis / pm_quantities` | 原 F7/F8、每腿每侧可见 shares 和；保持通道、基线、相关组和缺失原因 |
| `statistics/source_applied_at_utc_ms / source_received_age_ms` | 原 F9 的深度、聚合成交、spot、TWAP30、TWAP60 固定通道顺序 |
| `statistics/since_market_start_ms / until_market_end_ms` | 原有符号市场时距 |
| `trade` | 只从 `pm_reductions.steps` 的 `PublicTrade` 提取。含市场/资产、原价格/份数/主动方向/fee、`trade_id`、`trade_id_source`、直接来源、事实位置、成员/步骤、canonical ID、原时间/质量及合法组/应用顺序 |
| `group_applied` | 在该组所有变化、观察和新成交之后写出，保存实际公共前缀/应用时刻、来源可见下界及交付前沿；无观察市场时也保存 |
| `query_boundary` | 在该声明时点所有市场查询之后写出。含实际观察市场、是否已有公共前缀、交付前沿和 `input_frontier_past_query` |
| `completion` | 实际输入/应用计数、最后组/应用时间、自然耗尽或人工停止和耗时；不承诺研究报告已完成 |

Decimal/ExactDecimal 按正式序列化保存，不能先转低精度浮点数再比较价格。
来源字典缓存只保留当前字段，投影缓存只保留各已观察市场的最新标量和共享簿引用；不物化整日观察。
未变簿的必要报价复用共享 Arc，不额外反复扫描盘口。公共历史的窗口和终局积分保留仍沿原公共统计合同。

交付前沿是排他下界。下一应用组在 t 时，只能保证小于 t 的组已全部消费；同毫秒还可能有后组。
范围内运行的前沿最多到 E，自然结束也只证明声明范围已耗尽，不证明范围外没有消息。
`input_frontier_past_query=true` 仅表示声明输入交付前沿已越过查询时点，**不是完整标签声明**。
市场尚未公开、查询越过市场末端、来源缺失或输入范围不足时，研究必须保留观察不足。

## 停止、落盘与中断

```bash
touch <停止标志>
cat <进度.json>
```

停止标志在顺序定位的 Parquet 批次及公共输入边界检查。
清单准入/文件内容核对和元数据选择可能先完成当前原有操作，不能承诺固定秒数或五分钟排空。
正常停止完成当前输入的全部关闭组，再写 `completion`，刷新缓冲并 `sync_data`，退出码为 1。
正常自然结束同样稳定化输出，退出码为 0。输出文件必须为新路径，不追加旧文件。

CLI 只使用当前进程和内部 Rayon 线程，不派生后台生产进程。
SIGINT、SIGTERM、进程崩溃或断电可能丢失尚未刷新/同步的缓冲尾部，也可能留下半行。
完整 JSON 行只证明该行可解析；不能证明多行组成的尾组已完整发布。
消费者用最后一个完整 `group_applied` 或 `query_boundary` 判定相应已发布边界；
未完成尾组的变化/成交、孤立字典行及半行不能作为完整组或完整标签。
缺少自然结束 `completion` 时不能宣称输入已耗尽。磁盘错误会使程序失败，不能沿用此前进度冒充完整结束。
没有新增检查点、续跑或自动恢复；需要完整结果时使用新输出路径从头重跑已批准工作量。

## 验证边界

定向测试 `cargo test -p backtest --lib continuous_export --locked --offline` 沿真实 Parquet
准入/初始化/排序/公共归约到出口验证：零账户持续消费、同毫秒全部端点、同前缀公共读取、
查询/输出开关、静默窗口与左支撑、迟到 K/`at_anchor`、未知/有效空簿、正式成交去重/缺数量、
范围末端延迟及准备/运行中停止。旧公共公式、来源边界和原 A/B 消费适用证据继续复用。
[本轮有限验证记录](examples/continuous-public-validation.md)列出实际结果，
实际构建、受影响回归、有限成本和操作系统停止原始证据在准确交回包中保存。
这些证据不证明真实八小时吞吐、完整行情覆盖、队列成交、策略效果或盈利。
