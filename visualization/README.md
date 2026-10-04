# 公共业务事实浏览器

本包提供单 Rust 只读 HTTP 服务和 React 浏览界面，消费显式指定的历史报告、可选启动元数据及可选公共稳定记录。合同入口为 [C 路线 #215](https://github.com/slowchillchill/btc-updown-marketmaker/issues/215)。公共模块不依赖本包。

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

`metadata`、`public_recording` 均可省略。报告、元数据、公共记录独立显示读取结果；缺元数据不阻断报告浏览。`committed_prefix` 文件须包含写入方已经返回的一个 `CommittedIngressPrefixV1` 对象。它是已有凭据的显式交接，不是新记录合同。不能由浏览器扫描目录寻找最新前缀；不能把存在的后续分片自动提升为可读范围。

`page_rows` 与 `max_batch_bytes` 是直接传给所属读取接口的批次参数，上例仅为用法示例，不是业务容量或性能门槛。非零参数才能使读取推进。

## 读取、身份与表示

- `GET /api/runs` 仅列显式配置的来源。响应 `key` 是配置列表位置，供路由选择，不替代 `run_id`、账户或领域身份。
- `GET /api/runs/{key}/snapshot` 返回启动时读取并序列化的报告/元数据快照、公共前缀、来源状态和缺口。浏览刷新不重读报告文件；换一份原产物需重启服务。
- `GET /api/runs/{key}/public` 取回最近已读页，尚未读过时读取第一页。`?page=n` 只推进到下一页或重试最近页。每个来源只有一个公共顺序游标，刷新、切换运行或另一个浏览窗口推进后，通过“取回服务端当前页”同步当前页；旧页不会触发重开或从头扫描。该切片不提供随机历史定位。选择证据保留原对象及来源，不自动换为邻近记录。

公共页复用 `PublicRecordingReaderV1::open_committed` / `next_page`，固定绑定显式前缀。原记录身份、字段与前缀完整保留，展示行序只是本前缀中的定位信息。达到完整页边界时，再读取一页才能由所属接口确认末端；该末页可以为空。读取失败停止当前游标并报告页和已返回量，最近成功页仍可取回；不恢复、截断、补尾或删除原文件。

报告/元数据响应的 `exact` 和公共页记录把**所有 JSON 数字转换为十进制文本**，避免 JS 安全整数或浮点表示损失。金额字符串、原 `ExactRationalV1` 的分子/分母、`Known`、`Unavailable`、布尔值和空值保持各自表示；界面不通过 JS 浮点转换重算经济事实；唯一现金总额显示用精确十进制将实际可用与实际预留相加，并保留两项原引用。报告/元数据另有 `raw_json`，保存未改写的原 JSON 文本；损坏文本可用时也保留。JSON 字段浏览按需展开，不自动构造历史曲线。

`RangeEnd`、`StrategyExit`、`Failure` 保留原结束原因。正常范围结束的耐久报告、内存经济快照和 NOE 稳定前缀各自标明。当前没有诊断全历史、挂单本金专用读取或市场损益归因，不能把总本金占用当有效挂单价值，也不能插值历史 NAV。

## 总览、经济与有限范围

七类导航共用运行、账户、查看市场及字段选择。切换视图保留原证据；换运行清除旧运行选择。查看市场属于浏览选择，没有策略当前活跃市场来源时不猜首项。八卡及箭头说明架构职责；卡内另标本次可读证据，架构边全静态。真实策略身份来自启动元数据，同位人工策略/模型位置不代表样例使用 `baseline-mm` 或训练模型。NOE 通过 `shared-run::apply_closed` 协调执行登记和尝试 Ledger 应用；即时分派回程与完整成交分开。

经济页读取原报告的现金、各市场/资产剩余成功份额和本金成本、配对/孤腿、结算应收、费用、各返佣阶段、NAV/WCM 和生命周期。资产按原 `assets.outcome.label` 关联 UP/DOWN，映射缺失或不唯一时不猜数组顺序。`success_principal_cost_pusd` 保持所属 Ledger 的剩余成功本金口径；已判定市场的剩余成本与结算应收分别展示。Maker 归属现金不再加到实际总现金；预计/未付返佣不当到账现金。未判定剩余成本仅显示原两项成本的带标签加法表达，不重新归约、估值或计算 NAV。每个事实字段可点击原路径；来源更新时间和账户范围来自报告的观察截止 / 账本可读截止与身份，不假定全部已观察 NOE 都已入账。

公共表格消费当前已读顺序页原 `rows`，没有第二套权威事件模式。已知 `Input` 来源接收/原事件时间、`Canonical` 直接时间和报告的观察、计算、调度、提交及账本截止用于有限时间点展示；未适配的记录仍可查原值，不推定没有时间或市场。选择时间范围只筛当前可读页及已保存报告点，不读取未定位历史。没有明确 condition 的账户级点不能凭时间猜市场；没有端点不补持续时间。宿主计算区间差值注明派生调度时序，负值保留，不能当 CPU/网络实测。

“导出 Perfetto”下载所选范围或所选事实可证时间点的 [Chrome Trace JSON](https://perfetto.dev/docs/getting-started/other-formats)。原 UTC ms 精确文本、时钟种类、来源 / 原引用、账户、运行、稳定前缀和范围在 `args` 中保留，`derived` 明确为派生；`ts` 用 [lossless-json](https://github.com/josdejong/lossless-json) 将整数 UTC ms 精确转为整数微秒。只导出 instant，不补时长或因果箭头。ECharts 坐标使用相对位移的绘图近似，证据与导出保留精确时间；现金总额用 [decimal.js](https://mikemcl.github.io/decimal.js/) 精确加法。导出保留本次指定来源信息，属于原读取上下文的可重建材料。

可在 Perfetto UI 打开下载文件，或用官方 [Trace Processor](https://perfetto.dev/docs/analysis/trace-processor) 检查：

```bash
trace_processor query facts-run-0.trace.json 'SELECT COUNT(*), MIN(ts), MAX(ts) FROM slice;'
TRACE_PROCESSOR=/path/to/trace_processor npm --prefix visualization/ui run test:browser -- .local-artifacts/visualization/synthetic-example/sources.json
```

脚本设置 `TRACE_PROCESSOR` 时独立核对已知样例的四个点、毫秒 / 微秒 / 纳秒单位和原时间 / 时钟 / 引用参数；没有指定时明确记录官方解析未运行，不把自解析 JSON 等同 Perfetto 验收。复用构建缓存时可设 `CARGO_TARGET_DIR`，脚本据此查找已构建 Rust 二进制。

## 支持范围与来源缺口

当前接入历史 `result.json`、启动元数据、显式公共稳定记录原字段及其顺序页。NOE 逐引用读会解码完整容器，且本切片尚无稳定引用来源与安全历史枚举；Ledger 恢复/打开会改变耐久尾部，没有适用独立历史纯读入口。本包均不调用这些恢复器，也不复制账本归约或策略计算。策略逐次诊断、经济历史、真实训练/MLflow、PAPER/LIVE 和私有独立评价按所属模块的实际交付接入。

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

快照响应报告各来源读取/解码次数、字节量和启动读取时间；报告仅读取/解析一次并缓存编码响应。公共页报告读取器打开次数、`next_page` 调用次数、返回量、原行 JSON 编码时间和大小。打开阶段由所属接口枚举显式公共目录并读取前缀元数据，后续页沿同一游标解码；最近页重试直接复用编码响应。底层 I/O 字节、分配和完整响应序列化时间未由公共读取接口提供，响应明确保留未测量；不能以返回条数替代这些成本或宣称通过生产性能验收。浏览器测试另打印小型合成负载的实际公共页 HTTP 响应次数/字节和整个联调耗时；联调耗时包括截图和官方解析，不等于单帧渲染或接口性能。

HTTP 使用 [Axum](https://docs.rs/axum/0.8.9/axum/) 与 [tower-http ServeDir](https://docs.rs/tower-http/0.6.11/tower_http/services/struct.ServeDir.html)，前端使用 [React](https://react.dev/learn) 与 [Vite](https://vite.dev/guide/)。本切片按实际功能使用 [React Flow 自定义节点](https://reactflow.dev/learn/customization/custom-nodes)、[ECharts 散点图](https://echarts.apache.org/handbook/en/how-to/chart-types/scatter/basic-scatter/) 和 [TanStack Table v8](https://tanstack.com/table/v8/docs/guide/column-defs)。经济终态没有历史曲线；有限多时钟只画已知点。Rust 依赖锁在根 `Cargo.lock`，前端使用一份 `package-lock.json`；构建和依赖目录被忽略。
