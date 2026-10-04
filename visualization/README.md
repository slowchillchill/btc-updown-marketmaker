# 公共业务事实浏览器

本包提供单 Rust 只读 HTTP 服务和 React 浏览界面，消费显式指定的可选历史报告、可选启动元数据、可选公共稳定记录及显式 NOE 原返回引用。合同入口为 [C 路线 #215](https://github.com/slowchillchill/btc-updown-marketmaker/issues/215)。公共模块不依赖本包。

报告通过 JSON 浏览：`backtest::HistoricalRunReportV1` 当前只有 `Serialize`，本包不另建完整领域模式，也不修改生产报告类型。已知 `account_economics_v2` 与原结束原因可以显示已有终态和有限时序；未知版本保留原 JSON 并停止对应语义解释。JSON 语法读取成功不代表领域完整性或研究验收。

## 启动

在仓库根目录运行：

```bash
npm --prefix visualization/ui ci
npm --prefix visualization/ui run build
cargo run -p visualization --locked --offline -- sources.json
```

默认地址为 `http://127.0.0.1:4317`。可追加监听地址和已构建静态前端目录：

```bash
cargo run -p visualization --locked --offline -- sources.json 127.0.0.1:4317 visualization/ui/dist
```

本地服务只展示配置文件中本次有权读取的来源，没有目录发现、路径选择 API 或评价目录搜索。报告、元数据和公共根目录分别显式填写；元数据内的生产路径不会被自动打开。相对路径相对于服务进程工作目录解析，配置文件本身应保存在仓库外或被忽略的本地产物目录。前端开发可用 `npm --prefix visualization/ui run dev`，Vite 将 `/api` 代理到本地 Rust 服务。

最小来源配置：

```json
{
  "runs": [
    {
      "label": "指定历史运行",
      "report": "artifacts/result.json",
      "metadata": "artifacts/run.json",
      "public_recording": {
        "root": "artifacts/prepared/public",
        "committed_prefix": "artifacts/committed-prefix.json",
        "page_rows": 100,
        "max_batch_bytes": 1048576
      }
    }
  ]
}
```

`report`、`metadata`、`public_recording` 均可省略。报告、元数据、公共记录独立显示读取结果；缺报告/元数据不阻断公共页，损坏的文档也不影响合法公共来源。只配置公共流时保留相同布局位置，宿主条显示公共历史材料、原 `input_stream_id`、固定前缀与已读范围，不断言存在 backtest 运行、策略或账户：

```json
{"runs":[{"label":"显式公共历史材料","public_recording":{"root":"artifacts/public","committed_prefix":"artifacts/prefix.json","page_rows":100,"max_batch_bytes":1048576}}]}
```

`committed_prefix` 文件须包含写入方已经返回的一个 `CommittedIngressPrefixV1` 对象。它是已有凭据的显式交接，不是新记录合同。不能由浏览器扫描目录寻找最新前缀；不能把存在的后续分片自动提升为可读范围。

`page_rows` 与 `max_batch_bytes` 是直接传给所属读取接口的批次参数，上例仅为用法示例，不是业务容量或性能门槛。非零参数才能使读取推进。

## 读取、身份与表示

- `GET /api/runs` 仅列显式配置的来源。响应 `key` 是配置列表位置，供路由选择，不替代 `run_id`、账户或领域身份。
- `GET /api/runs/{key}/snapshot` 返回启动时读取并序列化的报告/元数据快照、公共前缀、来源状态和缺口。浏览刷新不重读报告文件；换一份原产物需重启服务。
- `GET /api/runs/{key}/public-query` 在相同游标上执行下述单批查询，回显实际条件、固定前缀、游标与命中原偏移；原批完整返回以供当前批条件切换和恢复。
- `GET /api/runs/{key}/public` 取回最近已读页，尚未读过时读取第一页。`?page=n` 只推进到下一页或重试最近页。每个来源只有一个公共顺序游标，刷新、切换运行或另一个浏览窗口推进后，通过“取回服务端当前页”同步当前页；旧页不会触发重开或从头扫描。该切片不提供随机历史定位。选择证据保留原对象及来源，不自动换为邻近记录。

公共页复用 `PublicRecordingReaderV1::open_committed` / `next_page`，固定绑定显式前缀。原记录身份、字段与前缀完整保留，展示行序只是本前缀中的定位信息。达到完整页边界时，再读取一页才能由所属接口确认末端；该末页可以为空。读取失败停止当前游标并报告页和已返回量，最近成功页仍可取回；不恢复、截断、补尾或删除原文件。

报告/元数据响应的 `exact` 和公共页记录把**所有 JSON 数字转换为十进制文本**，避免 JS 安全整数或浮点表示损失。金额字符串、原 `ExactRationalV1` 的分子/分母、`Known`、`Unavailable`、布尔值和空值保持各自表示；界面不通过 JS 浮点转换重算经济事实；唯一现金总额显示用精确十进制将实际可用与实际预留相加，并保留两项原引用。报告/元数据另有 `raw_json`，保存未改写的原 JSON 文本；损坏文本可用时也保留。JSON 字段浏览按需展开，不自动构造历史曲线。

`RangeEnd`、`StrategyExit`、`Failure` 保留原结束原因。正常范围结束的耐久报告、内存经济快照和 NOE 稳定前缀各自标明。当前没有诊断全历史、挂单本金专用读取或市场损益归因，不能把总本金占用当有效挂单价值，也不能插值历史 NAV。

## 总览、经济与有限范围

七类导航共用原来源身份、已提供的运行/账户、查看市场及字段选择。切换视图保留原证据；换运行清除旧运行选择。查看市场属于浏览选择，没有策略当前活跃市场来源时不猜首项。八卡及箭头说明架构职责；卡内另标本次可读证据，架构边全静态。真实策略身份来自启动元数据，同位人工策略/模型位置不代表样例使用 `baseline-mm` 或训练模型。NOE 通过 `shared-run::apply_closed` 协调执行登记和尝试 Ledger 应用；即时分派回程与完整成交分开。

经济页读取原报告的现金、各市场/资产剩余成功份额和本金成本、配对/孤腿、结算应收、费用、各返佣阶段、NAV/WCM 和生命周期。已执行份额/本金/实际费用的 `executed[0/1]` 是 Maker/Taker 角色累计，独立展示并保留原引用；它不提供按 UP/DOWN 的执行分解。资产按原 `assets.outcome.label` 关联 UP/DOWN，映射缺失或不唯一时不猜数组顺序。`success_principal_cost_pusd` 保持所属 Ledger 的剩余成功本金口径；已判定市场的剩余成本与结算应收分别展示。Maker 归属现金不再加到实际总现金；预计/未付返佣不当到账现金。未判定剩余成本仅显示原两项成本的带标签加法表达，不重新归约、估值或计算 NAV。每个事实字段可点击原路径；来源更新时间和账户范围来自报告的观察截止 / 账本可读截止与身份，不假定全部已观察 NOE 都已入账。

公共市场选项来自报告的明确市场、当前公共页的原市场字段与明确保留选择；不从全局参考或时间邻近推断市场，不声称覆盖未读范围。原流、输入位置、规范位置、事件/消息身份、组与观察位置、消费流步骤身份均保留。关联按钮仅按原流与原位置/事件/消息身份寻找当前页或用户保留的记录；越出该范围则显示目标未读缺口，不替换邻居或重开游标。选择一个新对象后只保留该对象，不维护隐式全历史缓存。

公共映射按现有类型分支读取：Input 的 Existing、V6 Existing/AnchorRule 与历史补查批次；Canonical 的 PolymarketMessage、MarketFact、ReferenceEvent。`time`、`direct_time`、`dependency_time` 分开显示，ReferenceEvent 的市场仅取市场参考 payload 的真实 `market`，全局 TWAP/行情参考保持无市场。AnchorRule 的请求开始、HTTP 接收与源计算字段按原口径显示；历史补查的响应接收、身份证据就绪、生效时间保持分开，原响应 UTC 文本与全部原字段仍可回查。历史 HTTP 实体没有查询起点时不补起点或查询耗时。Group/Observation、PmStepBinding、SourceBinding 及两类归约报告提供原引用下钻；没有自身时间字段的索引记录不借邻近记录制造时间点。

当前页与明确保留事实的可证时间进入有限时间线。原存储顺序不按时间重排，同毫秒不同组及倒退时间不合并；点的原身份和引用也写入 Perfetto args。保留的旧页事实可单独导出其自身时间；完整记录仍取原 rows，不在浏览器重放公共归约。

公共表格消费当前已读顺序页原 `rows`，没有第二套权威事件模式。已知 `Input` 来源接收/原事件时间、`Canonical` 直接时间和报告的观察、计算、调度、提交及账本截止用于有限时间点展示；未适配的记录仍可查原值，不推定没有时间或市场。选择时间范围只筛当前可读页及已保存报告点，不读取未定位历史。没有明确 condition 的账户级点不能凭时间猜市场；没有端点不补持续时间。宿主计算区间差值注明派生调度时序，负值保留，不能当 CPU/网络实测。

“导出 Perfetto”下载所选范围或所选事实可证时间点的 [Chrome Trace JSON](https://perfetto.dev/docs/getting-started/other-formats)。原 UTC ms 精确文本、时钟种类、来源 / 原引用、账户、运行、稳定前缀和范围在 `args` 中保留，`derived` 明确为派生；`ts` 用 [lossless-json](https://github.com/josdejong/lossless-json) 将整数 UTC ms 精确转为整数微秒。只导出 instant，不补时长或因果箭头。ECharts 坐标使用相对位移的绘图近似，证据与导出保留精确时间；现金总额用 [decimal.js](https://mikemcl.github.io/decimal.js/) 精确加法。导出保留本次指定来源信息，属于原读取上下文的可重建材料。

可在 Perfetto UI 打开下载文件，或用官方 [Trace Processor](https://perfetto.dev/docs/analysis/trace-processor) 检查：

```bash
trace_processor query facts-run-0.trace.json 'SELECT COUNT(*), MIN(ts), MAX(ts) FROM slice;'
TRACE_PROCESSOR=/path/to/trace_processor npm --prefix visualization/ui run test:browser -- .local-artifacts/visualization/synthetic-example/sources.json
```

脚本设置 `TRACE_PROCESSOR` 时独立核对已知样例的四个点、毫秒 / 微秒 / 纳秒单位和原时间 / 时钟 / 引用参数；没有指定时明确记录官方解析未运行，不把自解析 JSON 等同 Perfetto 验收。复用构建缓存时可设 `CARGO_TARGET_DIR`，脚本据此查找已构建 Rust 二进制。

## 支持范围与来源缺口

当前接入历史 `result.json`、启动元数据、显式公共稳定记录原字段及其顺序页，以及写入方显式返回的版本化 NOE 单引用。NOE 逐引用读会解码完整容器；当前仅有原 writer 工程接线验证，没有真实策略研究返回引用或安全历史枚举；Ledger 恢复/打开会改变耐久尾部，没有适用独立历史纯读入口。本包均不调用这些恢复器，也不复制账本归约或策略计算。策略逐次诊断、经济历史、真实训练/MLflow、PAPER/LIVE 和私有独立评价按所属模块的实际交付接入。

每份来源属于调用者明确指定的访问上下文；独立留出不向训练侧来源、缓存、目录、实验或界面汇集。共用展示代码不授予读取其他产物的权限。

## 工程验证与成本

合成预览只验证读取接线，复用 `backtest/tests/common` 的合法材料并通过生产 `run_historical_v1` 生成，观察策略仅返回空意图。它没有策略盈利或训练含义：

```bash
mkdir -p .local-artifacts/visualization
cargo run -p visualization --example synthetic_report --locked --offline -- .local-artifacts/visualization/synthetic-example
cargo run -p visualization --locked --offline -- .local-artifacts/visualization/synthetic-example/sources.json
```

生成器要求目标目录尚不存在，仅在该新目录生产验证材料。浏览服务自身始终只读。验证用生成器复用所属模块夹具，未复制新的领域材料构造器。

```bash
cargo test -p visualization --all-targets --locked --offline
cargo clippy -p visualization --all-targets --no-deps --locked --offline -- -D warnings
npm --prefix visualization/ui run build
npm --prefix visualization/ui run test:browser -- .local-artifacts/visualization/synthetic-example/sources.json
```

浏览器验证使用已有 Chrome，默认可执行文件 `/usr/bin/google-chrome`，可通过 `CHROME_EXECUTABLE` 指定。它启动临时本地 Rust 服务、关闭服务/浏览器并清理所建临时测试输入；可在命令最后提供截图目录。脚本验证实际合成 RangeEnd 报告、八模块/七导航与静态关系、跨视图及跨页身份、UP/DOWN 非对称映射、精确现金加法、零与 Unavailable、负调度差与缺端点、成功范围下载、未知/损坏证据、切换/刷新及窄屏。非对称数据是脚本临时创建的独立展示边界夹具，明确标为“非生产研究”，不冒充原生产报告。截图和工程检查不代替用户人工视觉验收。

快照响应报告各来源读取/解码次数、字节量和启动读取时间；报告仅读取/解析一次并缓存编码响应。公共页报告读取器打开次数、`next_page` 调用次数、返回量、精确 JSON 转换时间 `exact_json_us` 和 rows 编码字节量。打开阶段由所属接口枚举显式公共目录并读取前缀元数据，后续页沿同一游标解码；最近页重试直接复用编码响应。底层 I/O 字节与分配不由公共读取接口提供，每次服务响应仍保留未测量；完整响应编码时间/长度在完成编码后由 `last_encoding_cost()` 读取，保留在最近响应缓存中，不为自身数字重编码。不能以返回条数替代这些成本或宣称通过生产性能验收。浏览器测试另打印小型合成负载的实际公共页 HTTP 响应次数/字节和整个联调耗时；联调耗时包括截图和官方解析，不等于单帧渲染或接口性能。

HTTP 使用 [Axum](https://docs.rs/axum/0.8.9/axum/) 与 [tower-http ServeDir](https://docs.rs/tower-http/0.6.11/tower_http/services/struct.ServeDir.html)，前端使用 [React](https://react.dev/learn) 与 [Vite](https://vite.dev/guide/)。本切片按实际功能使用 [React Flow 自定义节点](https://reactflow.dev/learn/customization/custom-nodes)、[ECharts 散点图](https://echarts.apache.org/handbook/en/how-to/chart-types/scatter/basic-scatter/) 和 [TanStack Table v8](https://tanstack.com/table/v8/docs/guide/column-defs)。经济终态没有历史曲线；有限多时钟只画已知点。Rust 依赖锁在根 `Cargo.lock`，前端使用一份 `package-lock.json`；构建和依赖目录被忽略。

## 公共独立入口验证

`cargo run -p visualization --example public_evidence --locked --offline -- /new/engineering-public` 通过所属 `PublicObservationCoreV1` 与 `PublicRecordingWriterV1` 生成有限合法材料及已返回前缀，覆盖 V6、市场/全局参考、同毫秒不同组、源时间倒退和两市场历史补查。它仅验证展示，不是策略运行或研究结果。

构建服务与前端后可执行：

```bash
TRACE_PROCESSOR=/path/to/trace_processor node visualization/ui/tests/public-browser.mjs /authorized/real-sources.json /new/engineering-public/sources.json /ignored/screenshots
```

真实来源由调用者准确交接，不扫描目录寻找材料。真实五分钟初始化公共材料的公开来源证明见 [研究材料交接说明](../baseline-mm/docs/research/complementary-pair-material-trial-initialized-20260909-0000-0005.md) 与同名 JSON；它提供公共输入，不提供策略账户结果。当前实测每页 100 行、只推进两页：同一 reader 打开 1 次 / next_page 2 次，rows JSON 为 137999 B 与 114449 B；重试返回最近响应，旧页请求不推进。Chrome 验证公共独立身份、实际 V6 市场及原引用；该轮没有测量底层 I/O 和分配，后续有限成本证据见下节。

合法有限材料为 8 输入、17 规范记录、7 组、63 存储行，导出 80 个点；官方 Trace Processor 读取得到 80 slice，纳秒范围 9990000000 至 1788840000123000000，两条历史生效 args 的原 UTC ms 为 20000。浏览器检查每个点与引用都能回查原字段，覆盖区间/市场过滤、未读目标缺口、保留选择的 3 个原时间点导出。`SKIP_REAL_PUBLIC=1` 仅供重复检查合法变体时跳过已经验证的真实读页；默认仍验证真实来源。已有报告/经济 Chrome 回归继续适用。数值均为有限工程验证实测，不设性能门槛，也不表示全部历史或源端依赖已经交付。


## 有限公共读取成本

[前后完整读数](docs/read-cost-20261004.json) 使用上文同一已停止五分钟材料的固定前缀，仅读取前两页（每页 100 行）并重试最近页。读数是本机 debug 构建的有限观察；不代表生产速度、所有历史或容量验收。服务的 allocator 没有替换，分配探针只在 `read_cost` 示例中使用；该示例直接调用实际 `PublicPages::open/page` 路径，初始化包括原接口打开、元数据读取/解析及来源状态构造，分页读取时间包括底层顺序读取与解码，未拆成纯 I/O 与纯解码时间。

原路径仅为 `rows_json_bytes` 额外编码整页字符串；现在完整响应编码时，沿同一个 writer 记录 rows 起止位置，后置 cost 读取其实际 UTF-8 字节量，不保留第二份 rows 字符串。返回原事实没有变化，两页 rows SHA-256 前后分别一致：`bcca1a3b6b7f957a10136d62241e21e037b628dd7ea73710844007aead07203f`、`770ffabf10a8a473485219d0c8c7653c186bc99748362a061b343edfbee9c3e5`。`row_encoding_us` 原来混合转换与额外编码，改名为 `exact_json_us` 明确只计 rows/前缀的精确 JSON 转换；完整响应编码结束后另取测量，避免在响应内计算自身编码长度/时间的循环。JSON 对象字段顺序不是原存储行序，rows 数组顺序仍保持。

| 阶段 | 成功分配次数 前→后 | 累计请求字节 前→后 | live 请求字节绝对峰值 前→后 |
| --- | ---: | ---: | ---: |
| 初始化 | 131→131 | 10049→10049 | 6354→6354 |
| 页 0 | 23801→17406 | 9616762→8309637 | 4770932→3726638 |
| 页 1 | 16717→10467 | 3712256→2716741 | 4662403→3798649 |
| 最近页重试 | 0→0 | 0→0 | 无新增 live 请求字节 |

成功次数计实际经过 [GlobalAlloc](https://doc.rust-lang.org/std/alloc/trait.GlobalAlloc.html) 的成功 `alloc/alloc_zeroed/realloc`，累计请求字节在成功 realloc 时计完整新大小；不把它叫净分配或 RSS。live 请求字节为仍活跃的 Layout 请求大小，绝对峰值含开始时已有对象，`peak_above_start_bytes` 另报告相对峰值；没有计 System 内部开销、realloc 内部旧/新块短暂重叠及外部分配器。编译器可能消除/合并分配，数字仅对应此构建。阶段计数窗口不包含 `/proc` 读取、标记打印、结果 JSON 解析和校验哈希编码。

[strace](https://strace.io/) 按标记独立统计显式来源文件 `read/pread64/readv`，包含返回 0 的 EOF 调用，前后相同：初始化 684 次/993 B（stream 368 次/367 B、两次前缀读取合计 316 次/626 B）；页 0 的 Parquet 17 次/559476 B；页 1 和最近页重试 0 次/0 B。它们是 syscall 返回字节，重复读取会重复计，不是唯一文件覆盖字节、业务 payload 字节或物理盘读取。页 1 继续解码 reader 已保留的批数据，不能称零解码。[`/proc/self/io` 的 `read_bytes`](https://docs.kernel.org/filesystems/proc.html) 各阶段增量均为 0：这次缓存条件下没有归于该进程的存储层读字节，并不等于没有逻辑读取；未清理缓存、未证明冷盘成本。`VmRSS` 为采样时进程驻留量，`VmHWM` 为进程生命周期高水位，均与 allocator 请求峰值分开保存，包含映射/运行库等成本。

测量工具也有成本。前后 strace 初始化耗时分别 8855/9127 µs，而不带 strace 的 allocator 示例为 300/319 µs；不能用带 strace 耗时推断服务性能。优化后另做一次关闭原子计数的相同负载对照：初始化/页 0/页 1 为 285/18516/6280 µs，启用计数为 357/19633/6519 µs；关闭计数仍有 relaxed 判断，单次差异含调度/缓存波动，不分解出精确纯探针开销。服务/Chrome 测量本身不使用该 allocator 探针。

完整响应字节为页 0 的 138619→138617 B、页 1 的 115070→115068 B（计时文本位数可能改变长度），rows 始终 137999/114449 B。完整编码观察为 4257→2861、3609→2294 µs；基线计时还包括原 JSON 信封构造，优化后为实际完整序列化，两者口径差异不能当作纯编码速度提升。整个页路径由示例另计 22454→19590、10028→6060 µs，仍含 allocator 开销。

C5 当时的实际 Chrome 1600×1150 验证两页各渲染 100 个表格行，无页面异常；`requestStart→responseEnd` 为 24.9→22.5、14.0→7.9 ms，点击至 DOM 就绪加两次 animation frame 为 117.9→112.3、94.6→95.4 ms，后者包含 HTTP、解码、React 和帧调度，不能称纯绘制/CPU 时间。启动至首 HTTP 成功含 25 ms 轮询调度，为 8.06→9.14 ms；最近页重试直到 body 接收为 1.8→2.3 ms，浏览器 JSON 解析为 0.1→0.3 ms。这些单次有限结果不设阈值。前端产物 JS 973.59 kB（gzip 320.99）、CSS 25.19 kB（gzip 5.32），已有大包提示保留；本片没有扩大到虚拟表格、范围随机定位或播放实现。

基线精确代码为 `14ccd51c473b42df0e10481cd55165cff47f1bce` 加 [仅测量补丁](docs/read-cost-baseline.patch)，该补丁只在原路径加完成响应计时/长度与读取方法，保留原额外 rows 编码。对照工作树应用该补丁并复制本轮 `examples/read_cost.rs` 后，可用同一示例/命令重复基线；当前树直接运行即为优化路径。补丁不用于当前源码，也不改变领域读取。复现时由调用者提供同一授权 `sources.json`，首项须为公共来源；输出/trace 放在忽略目录，原始 strace 含文件路径与部分内容，不作为公开附件。下列变量仅为命令输入，不写入公共配置：

```bash
cargo build -p visualization --bin visualization --example read_cost --locked --offline
# BIN 指向该构建目录的 debug/examples/read_cost，SOURCES 为显式交接配置。
"$BIN" "$SOURCES" > "$OUT/read-cost.jsonl"
"$BIN" "$SOURCES" --without-counters > "$OUT/without-counters.jsonl"
strace -yy -s 256 -e trace=read,pread64,readv,write -o "$OUT/read-cost.strace" "$BIN" "$SOURCES" > "$OUT/traced.jsonl"
python3 visualization/examples/read_cost_io.py "$OUT/read-cost.strace" "$SOURCES" > "$OUT/source-io.json"
node visualization/ui/tests/read-cost-browser.mjs "$SOURCES" "$OUT/browser"
```

浏览器脚本使用受控 4320 端口，测量真实 HTTP 响应和 DOM 行数，检查 rows 字节计数、原范围/调用次数及最近页复用，保存截图与 JSON，结束后关闭自己启动的 Chrome 和服务。构建缓存复用时按前述 `CARGO_TARGET_DIR` 查找二进制。本轮还保留合法 63 行/80 时间点的原浏览/官方 Perfetto 回归；缺口与后续范围查询、虚拟列表、播放、经济投影及显式 NOE 仍按所属合同接续。


## 顺序范围查询、虚拟列表与历史浏览

当前批筛选只查看浏览器保留的这一原批；“从当前游标查找下一批匹配”才显式开始下一批服务端查询。条件取原存储记录类型、明确 `condition_id`、指定位置域、指定原时钟及包含两端的整数范围。位置域包含固定前缀存储行偏移及原 `input_position` / `canonical_position` / `group_position` / `observation_position`；`PmStepBinding.position.canonical_record_position` 仅对应规范记录位置。时钟选择列出已适配的明确字段，即使当前页没有该字段也可选来查下一批。未限定端点不等于把缺字段当匹配；已有条件确定不满足时计“不匹配”，否则所需字段缺失计“无法判断”。同时选择市场和时钟时，时间须有原字段支持的同一市场关联，不能借同批其他市场的时间。

服务器一次 HTTP 最多调用所属读取器 `next_page` 一次。它从最近原批的精确 JSON 读取已有字段，返回 `matching.matched_offsets`（原批内偏移）、扫描/无法判断/不匹配量、匹配解析和判断耗时，并回显查询/前缀。响应的 `batch` 仍是完整原批；服务端筛选没有减少其 wire 字节量。浏览器依据服务端命中结果，在空匹配批后串行发下一请求，遇到非空批或稳定前缀末端停止；期间展示扫描/匹配/无法判断/已耗时，查找总量未知。原时间可能回退，任何时间超过上界都不会使扫描早停。修改条件、打开页面或切换视图不自动扫描，不提供随机历史定位或全部匹配集合。

每来源只保留最近一份原批编码，以及绑定该原批的一份最近查询条件/命中偏移/统计。相同最近批与相同条件重试复用该小结果，`matching_reused=true`；回显的 `query_elapsed_us` 是原匹配工作耗时，不是重试耗时。下一原批成功推进时清除小结果；没有第二份全量 JSON 缓存。新条件仍需临时解析当前原批作匹配；原批 HTTP 编码直接嵌入，保留 C5 一次 rows 编码计量路径。当前批客户端条件变化重新筛这一完整原批，行对象与 `/public/{row_start}/{原 i}` 不重新编号。

取消只停止后续请求，同步原读取不能因 HTTP 断开而被宣称取消；在途批完成后先保存再停。网络失败保留最近期望页/条件，可重试或显式取回服务端最近批。刷新/切回来源时初次“读取当前页”取回服务端当前批，不固定成页 0。另一窗口推进使本窗预期页过旧时返回冲突，显示服务端最近/下一页；显式取回跨过本窗未保留的中间页时提示覆盖改变。真实读取失败保留所属游标停止状态，仍可取回最近成功批，不能继续推进。旧选中事实独立于当前批及虚拟 DOM 保留。

[React TanStack Virtual](https://tanstack.com/virtual/latest/docs/framework/react/react-virtual) 按稳定原引用作为 key、测量实际行高，只挂载当前可视 DOM。页面分别显示当前保留/条件匹配/DOM 数量，不把虚拟化称底层少读或少解码。“历史浏览”按原序遍历当前保留的匹配事实，以记录/秒调整浏览速度，暂停并在末尾停下；不读下一页、不计算策略或归约经济事实。只在原引用解析到已读目标时突出公共来源关联；架构方向仍静态，策略、Bridge、NOE、Ledger 无证据不得模拟执行脉冲。

[本轮有限证据](docs/public-query-20261004.json) 记录合法 63 行材料：页 1–4 空匹配，页 3 已有 21997/21998，页 5 仍能命中倒退的 21996；客户端过滤后第一条 Input 保留原偏移 1。63 条保留事实只挂载 9 个 DOM 行，移出 DOM 的旧选择保留。取消仅 1 请求且在途页保存；服务端完成后模拟浏览器网络失败，重试同原批、`next_page_calls=1` 且小匹配结果复用。第二窗口推进至页 2，本窗冲突后显式取回，`reader_open_count=1` / `next_page_calls=3`。8 条已读 Input 播放/暂停/末尾没有额外公共 HTTP 请求。

同一真实已停止五分钟来源仅读前两页（各 100 条），每页 DOM 为 9；原批 138617 / 115068 B，带查询元数据响应 139914 / 116365 B，原 rows 仍为 137999 / 114449 B。新增匹配解析/判断观察为 2250 / 1889 µs；HTTP `requestStart→responseEnd` 为 26.2 / 13.7 ms，点击至 DOM 就绪加两帧为 113.8 / 90.3 ms。后者含 HTTP、浏览器解码、React 与帧调度，不是纯 CPU 渲染。最近原批重试未推进，返回 115068 B。此片未重新测 allocator/底层 syscall/物理 I/O；C5 历史证据和读数保持原样，不把虚拟 DOM 或下一批已有缓存当零解码。上述单次 debug/Chrome 观察不设容量或频率门。

现有公共关联/时间导出浏览器回归仍通过，下载 80 个时间点经官方 Trace Processor 读取为 80 条 slice；首尾为 9990000000 / 1788840000123000000 ns，原 `args.original_utc_ms=20000` 两条。前端产物 JS 1009.74 kB（gzip 331.96）、CSS 26.06 kB（gzip 5.52），已有大包提示保留。后续报告执行投影及显式 NOE 单引用沿 #215 接续，本片不表示整条 C 已完成。

```bash
# FINITE 是所属核心/写入器生成的 public_evidence 合法 sources.json。
# REAL 是原显式固定前缀配置；OUT 为被忽略的本轮截图/证据目录。
CARGO_TARGET_DIR="$TARGET" node visualization/ui/tests/query-browser.mjs "$FINITE" "$OUT/query"
CARGO_TARGET_DIR="$TARGET" TRACE_PROCESSOR="$TP" node visualization/ui/tests/public-browser.mjs "$REAL" "$FINITE" "$OUT/public"
CARGO_TARGET_DIR="$TARGET" node visualization/ui/tests/read-cost-browser.mjs "$REAL" "$OUT/real"
```

脚本分别使用受控 4321 / 4319 / 4320 端口，结束时关闭自身浏览器/服务及临时配置。合法有限材料的市场/原时间独立期望取原字段，不重放归约；取消/网络失败使用浏览器路由保留服务端真实已完成结果。报告/经济、公共关联与窄屏旧行为通过原浏览器脚本继续核对。固定根回归退出 0，`four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second` 按仓库规则明确跳过。

审查返工补充：当前批查询、播放、表格与范围筛选共用 App 一次 `publicRows(page)` 映射的同一组 Row/Point，不再分别构造后比较对象身份。原合法材料的 `PolymarketMessage.time.received_at_utc_ms=22000` 独立确定原偏移 45、51；新增 Chrome 断言先在旧实现上因范围表格清空失败，修复后证明两条原引用和完整原值仍显示，范围导出 15 点及原查询/虚拟/取消/播放语义保持。修复构建 JS 1009.73 kB（gzip 332.00），CSS 不变；此次仅前端变化，复用既有 Rust 根回归，未重复无变化测试。


## 报告执行投影与显式 NOE 单引用

页首折叠来源条直接显示原运行 `metadata.config.implementation_identity.git_commit`，不以可视化服务提交替代；源码 / Cargo 锁哈希、策略规则版本、公共规范 / 观察 / 排序版本均可点击原字段。报告与元数据经济版本分别保留，不覆盖差异；未知经济版本的原版本文本仍显示，停止相应语义解释。`caller_role` / `recording_role` 是角色；四项 `Fixed` / `Sequence` timing 是历史宿主延迟配置，非 CPU 实测或策略周期。当前没有独立业务触发模式字段，明确未保存 / 未交接，不猜逐帧或 100ms。

执行页读取报告 `ledger_consumed_memory` 的账户与完整 condition_id 市场投影；市场选择包含只有预留 / 生命周期投影的市场。提交前、发送后未知、平台确认但经济待应用的数量、本金及即时费用独立显示；平台本金 / 即时费用占用已包含确认待应用部分，不再相加，不当有效挂单价值。开放订单单位为单，待定成交单位为笔，后者包含 PendingUnapplied / PendingApplied，不是尚未入账数。未展示混合口径 `current_all_in_commitment`，不重新归约经济或 NAV。账户身份、Ledger 条目序号、平台来源前缀、本地前缀、available_through 与报告 cutoff / durable_noe / pending_count 分列；不同位置域不能相减或证明任意 NOE 被消费。

来源配置可只包含 NOE，报告 / 元数据 / 公共记录继续各自可选：

```json
{"runs":[{"label":"写入方明确交接的 NOE","noe_results":[{"label":"原返回引用","version":"v4","reference_file":"artifacts/noe-returned-reference.json"}]}]}
```

引用文件必须是原写入方 `PublishedNoeCommitV1::result_references()` 返回的 `NoeResultReferenceV1`，含 commit_container / record_ordinal；引用没有载荷版本，配置明确选择 `v1` / `v2` / `v3` / `v4`。浏览器仅请求配置索引 `GET /api/runs/{key}/noe/{index}`，不能指定任意路径。启动只列配置项，不读取引用或容器；未知版本不阻止其他来源启动。按需调用对应 `read_noe_result_v1` 至 `v4`，不 fallthrough、不扫描或按 prefix / 目录猜引用，不调用恢复器。未知版本、版本不匹配、损坏、缺上下文和 ordinal 越界由该项独立失败；原 reader 的失败信息保留，不修复原文件。

响应薄包装直接序列化原 `context` / `boundary` / `record`，JSON 数字按现有精确边界转文本；这是原读取器解码记录，不是容器原字节。账户、合同、实现、发布 UTC、水位、源位置及请求 / 子项 / 订单 / 成交身份沿原字段下钻。真实记录变体原样保留，不把 Evidence / EconomicFact / EconomicFactV2 / Duplicate / EconomicAlias / Pending / Conflict 统一叫成交或经济应用。与报告同账户、同配置、时间相邻，甚至报告 pending_count=0，都不证明该条已被该报告 Ledger 消费。没有报告时界面标明 NOE 独立来源，不伪造公共流、策略参与或账户运行。

服务只保留最近一份单引用响应，包括失败响应供最近项重试；不缓存全历史或第二份完整 JSON。界面保持当前已加载引用跨视图，组件重渲染不重读，读取其他项属于新的显式读取。同容器不同 ordinal 仍分别调用原接口、解码整容器，没有自动逐条批读。`reference_json_bytes_read` 是实际 reference JSON 读取长度；`container_file_bytes` 仅文件 metadata 长度，非 syscall / 物理 I/O。`native_read_attempts` 与累计 `native_read_calls_total` 是原接口调用次数；`native_read_and_dto_us` 包括原读取 / 解码和薄 DTO 转换，`load_elapsed_us` 截止完整响应编码之前。底层 I/O / 分配字节未测为 null，不把每条记录大小冒充读取成本。

复现有限工程验证（均要求新生成目录）：

```bash
cargo run -p visualization --example noe_evidence --locked --offline -- .local-artifacts/visualization/noe-example
cargo run -p visualization --example execution_projection --locked --offline -- .local-artifacts/visualization/ledger-display-example
cargo test -p visualization --test noe_read --locked --offline
node visualization/ui/tests/execution-browser.mjs .local-artifacts/visualization/noe-example/sources.json .local-artifacts/visualization/ledger-display-example/sources.json .local-artifacts/visualization/synthetic-example/sources.json .local-artifacts/visualization/execution-check
```

NOE example 用原 NormalizationCore 与 StagedNormalizationWriter::create / stage / stabilize 保存发布回调的原引用：v4 主例加 v1–v3 共四个小容器、24 条真实返回引用，每版本 6 条原序记录；全部是工程接线，非策略研究。Ledger example 从真实开账 NOE、预留、发送、平台确认和待定成交入口产生投影；外层 JSON 只是独立展示边界，明确非 backtest 生产报告，不能当研究运行或真实消费历史。

本轮 Chrome 实测：24 配置无自动 NOE 请求，显式验证 v4 六变体及 v1–v3 共 9 项；跨视图已加载项零重读，服务端最近项重试原调用数不变。v4 工程容器 11528 字节（仅编码文件规模），本次九项解码+DTO读数 1205–1503μs，原引用 JSON 300 字节；成功完整 HTTP 响应约 3.5–4.7KB，数字随工程目录文本和实测时长而变，非速度 / 容量门。未测单引用原接口分配或物理 I/O，不从小容器推断大规模成本。前端构建 JS 1020.18KB / gzip335.26KB、CSS26.06KB / gzip5.52KB；原大 bundle 提示保留。

Chrome 也验证 typed 投影的提交前账户 2项/5.5 pUSD、未知1项/2.2、确认待应用1项/3.3本金与0.03费用，占用仍3.3/0.03；开放订单和待定成交各1，投影-only第二市场原本金4.4且原路径不变。旧63行范围 / 9 DOM / 选择 / 取消恢复 / 多窗 / 播放零网络，以及非对称Maker/Taker / 资产置换、精确金额、官方Perfetto 4点均继续通过。页首增加折叠事实格后，旧浏览器金额断言按实际可见事实格定位，避免误抓折叠的来源条。固定根回归退出0，具名 UTC 依赖测试 four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second 明确跳过。
