# 已保存小时研究快照与英文 Demo

此只读切片支持已知 `account_economics_v2` 的 `BudgetStop` 报告，保留原 `end_reason` 与
`recording_drained`。其他已知结束原因继续支持；未知版本、未知结束原因、损坏 JSON 保留原文与原失败状态。
合同为 [C 路线 #215](https://github.com/slowchillchill/btc-updown-marketmaker/issues/215)，
本次取舍见 [D-0337](https://github.com/slowchillchill/btc-updown-marketmaker/issues/215#issuecomment-6117032752)。

## 来源与读取

单候选报告继续通过 `runs[].report` 接入。有 `research.hours` 的已知报告使用小时视图；原完整
`RangeEnd` 报告继续使用已有界面。小时视图默认选择第二个已保存小时；仅有一项时选择第一项。
不开放最终状态混用选项，不显示连续 NAV 曲线或原生七候选比较表。

忠实提取报告的配置可额外指定 `report_provenance`。这是明确文件的来源映射，不是生产启动
`metadata`，也不是自动发现入口。该文件复用 JSON 读取与精确数字传输，启动读取一次、缓存响应；
浏览时不重读原包装、不重算哈希、不扫描目录。

```json
{"runs":[{"label":"Reference (center)","report":"private/center.report.json","report_provenance":"private/center.provenance.json"}]}
```

来源映射保存：

| 字段 | 说明 |
| --- | --- |
| `original_file`, `original_sha256` | 原包装文件与提取时摘要 |
| `report_pointer` | 原候选 report 的 JSON pointer |
| `extracted_file`, `extracted_sha256` | 实际提取文件与摘要 |
| `candidate`, `run_id`, `comparison_identity_sha256` | 原候选及比较身份 |
| `initial_available_pusd`, `initial_available_pointer` | 原初始资金十进制文本与原包装路径 |
| `runtime_binding_file`, `runtime_binding` | 原运行绑定文件及其原内容，界面只取运行身份 |
| `published_generation`, `published_generation_pointer` | 原发布代次及原包装路径 |

来源配置、报告和完整私有映射留在仓库外。来源准备保留原件，对提取 report 与原对象做等值核对；
服务只读取已配置文件。没有初始资金元数据时，NAV 增量与比例显示缺口，不硬编码资金。
运行实现与当前界面源码身份分开：本次真实 Demo 的原运行为 `1e67c2e`、发布 `gen71`。

## 显示口径与原值

所有小时指标都来自 `/research/hours/<i>/cumulative`。累计起点取 `hours[0].start_utc_ms`，
不能将第二小时的 `hours[1].start_utc_ms` 作为累计起点。所选截止取 cumulative 的
`boundary_utc_ms`，实际观察时刻取 `observation_cutoff.decision_time.logical_time_utc_ms`。

| 显示内容 | cumulative 内路径 | 单位 |
| --- | --- | --- |
| 实际可用现金 / 保守 NAV | `metrics/actual_available_pusd`, `metrics/conservative_nav_pusd` | pUSD |
| 确认净结果 | `metrics/confirmed_net_pnl_pusd` | pUSD |
| 累计最大公共 NAV 回撤 | `day_economics/maximum_nav_drawdown_fraction` | fraction → % |
| 回撤金额 | `day_economics/maximum_nav_drawdown_pusd` | pUSD |
| 配对 / 孤腿份额 | `metrics/paired_shares`, `metrics/orphan_shares` | shares |
| 平均 / 累计最大资金占用 | `day_economics/average_committed_capital_pusd`, `maximum_committed_capital_pusd` | pUSD |
| 返佣五阶段 | `metrics/*_rebates_pusd` | pUSD，Maker/Taker 合计 |
| Maker / Taker 份额、本金、费用 | `metrics/{maker,taker}_{shares,principal_pusd,fee_pusd}` | shares / pUSD |
| 成功 / 经济应用模拟成交 | `counters/{maker,taker}_{success,economic}_trade_count` | trades |
| 请求与请求腿、响应、开放订单、待 NOE、未闭合承诺 | 对应 `counters` 原字段 | requests / legs / responses / orders / records / commitments |
| Successful / NoTrade / Actions / Exit | 对应 `decisions` 原字段 | decisions |
| 待计算 | `pending_calculation` 及其原时序字段 | UTC ms / null |

金额显示以整数商和余数舍入至两位小数，不经浮点数；百分比先精确乘 100。
`Known` 零显示 `0.00`，`Unknown`、`Unavailable`、缺字段 `Not provided` 与 `null` 各自保留。
点击卡片复用 Field 与 Selected fact，显示实际加载文件、所选边界、JSON pointer 和完整精确原值。
`Known` 指标指向实际分数对象的 `/Known`。长文件路径与原包装映射可展开，精确原值先显示。
Evidence replay 的 Tree 仅浏览所选 cumulative。

NAV 增量是所选 NAV 减原初始资金，比例除以原初始资金。两项原值都能独立点击；
确认净结果直接显示另一个保存字段。center 1h 的增量 `1021.4593` 与确认净结果 `1041.3093`
不同，不能互换。没有完整逐市场、Ledger、逐笔执行或小时内历史时，相关页面明确不可用，
不从约 02:10 的最终状态补齐，也不将累计极值相减成单小时极值。

## 本地启动、停止与操作

先按根 README 安装锁定前端依赖并完成构建，再启动服务；构建进行期间不要同时运行浏览器测试。

```bash
npm --prefix visualization/ui ci
npm --prefix visualization/ui run build
cargo run -p visualization --locked --offline -- /authorized/private/sources.json 127.0.0.1:4317 visualization/ui/dist
```

访问 `http://127.0.0.1:4317`，在 Interface language 选择 English。前台 `Ctrl+C` 停止服务。
来源固定于启动时；换报告或配置需重启。私有交付附带独立已构建 binary/UI 及准确启动命令，
无需保留任务工作树才能预览。

| Demo 镜头 | 实际操作 |
| --- | --- |
| D1 00:00–00:15 | Module overview → Reference (center) → Cumulative to 2h；读 BACKTEST、独立账户、00:00–02:00 UTC、BudgetStop 与 false |
| D2 00:15–00:45 | Run selection → Variant 1 (no-innovation)，保持 2h；回 Reference / 2h。七行比较图卡由后期叠加 |
| D3 00:45–01:20 | 检查现金 2683.27 / NAV 3519.03；点击 NAV 看 `35190333/10000`。可切 1h 查 3021.46，再回 2h；点/柱图由后期制作 |
| D4 01:20–01:50 | Intents and execution feedback 查看 Maker 两种计数各 1930；Account economics and lifecycle 查看 paired 835.76、平均 676.69 / 最大 1901.99、未成熟 82.74 / 已到账 0.00 |
| D5 01:50–02:25 | 点击累计最大公共 NAV 回撤卡片，Selected fact 查 `187038/4490809` 与 4.16%；Full source paths and mapping 查原包装路径。Evidence replay 可展开对应 cumulative 树 |
| D6 02:25–02:45 | 回 Module overview → Reference / 2h，保留共同窗口与真实状态；能力总结为后期叠层 |

本次没有开发后期七行比较图卡、小时点图、旁白或视频包装。PMM Logo 使用原 PNG 字节，
CSS 只裁透明画布，等比可见宽 160px、四周留白 0.5H。英文 Noto Sans、数字 Noto Sans Mono；
在 1280×720 和 1920×1080 使用真实 Chrome 检查控件、原值点击和截图。

## 针对性验证

```bash
cargo test -p visualization --all-targets --locked --offline
cargo fmt -p visualization -- --check
cargo clippy -p visualization --lib --bin visualization --no-deps --locked --offline -- -D warnings
cargo clippy -p visualization --all-targets --no-deps --locked --offline
npm --prefix visualization/ui run build
npm --prefix visualization/ui run test:i18n
npm --prefix visualization/ui run test:snapshot-values
npm --prefix visualization/ui run test:snapshots -- /authorized/private/seven-sources.json /authorized/private/screenshots
```

新浏览器脚本复制七个原 report 到独立测试输入，边界变形只改第八个测试副本；检查来源/小时
切换、精确分数、真实金额、缺口和字体，保存原件前后字节相等证据。指定证据目录时保留必要
输入和截图供复核；未指定时清理临时输入。脚本的响应/点击耗时是本机有限浏览负载，包含浏览器
调度，不是生产吞吐结论。

旧 `test:browser` 继续仅用 `synthetic_report` 生产入口生成的合法完整 RangeEnd 工程夹具，
它会修改独立临时副本。不要将真实两小时报告交给旧脚本。未改时间线/Perfetto，旧回归未指定
TRACE_PROCESSOR 时明确官方解析未运行。

当前基线严格 `--all-targets ... -D warnings` 存在旧 `public_evidence.rs` 的 `drop_non_drop`
与复用夹具 `setup_with_terminal` 的 dead_code 警告。它们不在本次改动路径，不影响小时 JSON
读取或 UI；保留原警告，正常 all-targets 和严格服务 lib/bin 分别记录，不称严格全目标通过。
本次仅改 visualization 消费者，必要定向覆盖不推断根 workspace、B 独立 workspace 或 Python 已验证。
