# C0 低价初始批新增否决候选

[#353](https://github.com/slowchillchill/btc-updown-marketmaker/issues/353) 交付受控研究版本 `candidate_v5_public_low_price`。它只检验一个新规则：准备新增初始双腿批时，任一最终拟用价低于同冻结 PM 真正簿买一，则不新增整个批。它不证明低价导致快配失败，也不代表候选已盈利或生产采用。旧 `candidate_v3`、`candidate_v4_public_c0_c1`、P1 和 R3 保留各自身份。

## 正式配置与入口

[public-low-price-off.json](../../configs/strategies/complementary_pair/public-low-price-off.json) 是同版本 C0 对照，[public-low-price-on.json](../../configs/strategies/complementary_pair/public-low-price-on.json) 开启新条件。两份 `parameters` 完全相同，直接复用 V3 的完整 42 参数类型；唯一差别是 `initial_pair_low_price_veto`。本版本固定沿原公共 C0，不提供 BLB 叠加开关，不另造默认参数。

库入口为 `ComplementaryPairV1::new_low_price(ComplementaryPairConfigV5, rules, namespace, StrategyContinuityV2::new_low_price_account())`。关闭开关的实际请求、撤单和现金行为保持原 V4 C0；版本及新增诊断身份不同。现有策略 checkpoint 适配保留本版本和开关，不新增日入口续跑能力。

CLI 沿 `backtest-cli --mode day --config <完整日配置> --progress-interval-seconds <秒>`。每个 `days[].candidates[].strategy` 放完整 V5 对象，`run.strategy.strategy_rule_version` 声明新版本。`run.public_statistics`、输入范围／合法初始化、规则 SHA、独立账户和输出继续使用[正式 backtest 合同](../../../backtest/README.md)，不可只传策略配置就称为完整运行包。公共融合与 C0 资格沿[公共候选说明](complementary-pair-public-candidates.md)，不复制概率、采样或公共公式。

启动时可用 `--initial-pair-low-price-veto true|false` 覆盖开关。原 `--strategy.quote.base_pair_shares <值>` 等覆盖继续写入 `parameters`。文件与 CLI 合成后，完整生效对象写入正式运行身份；day 的显式全局覆盖作用于各候选，不能继续按静态账户名推断其开关。类型与版本不符沿原入口报错。研究应显式冻结 `event_driven` 和交付／计算开始／计算／派发 `0/0/15/0ms`；本工程不替研究准备或签发真实运行包。

## 判断位置与三态

原流程先确定合法 target，复用适用旧低价，完成经营评分、数量、资格、成本及预算。原公共 C0 再核对两腿真簿正价正量、`bid<ask` 及正式 200ms ΔM 已知精确零。新条件只在 C0 允许之后、`maker_leg`／`order_item` 和现金／风险占用之前执行：

- 任一侧最终 `prices[i] < pm_quotes[i][0]` 已知成立：本次整个新初始双腿批不新增。
- 两侧已知且都不低于：沿原 C0 构造整批。
- 没有已证低价而必要比较未知：记录未知，沿 C0；不把未知变成新拒绝门。

`prices` 是裁剪及低旧价复用后的真实价格，不是最初 target。比较使用刚通过 C0 的同一 `pm_quotes`，不查询较后行情，不用直接 BBA、`valuation_best_bid`、Binance 买一或发送后信息替换。价格／报价／诊断数组按策略 `[Up, Down]` 排列；原始 F0 资产集合的 token 数值排序不是该策略数组顺序。

当前真实后置路径的必要比较未知不可达：`PairMarketRead` 已确认恰好 Up/Down 两腿，`new_pair` 已有两侧确定 Decimal 最终价，而 `PublicEntryEvaluationV4::allows_new_pair()` 要求 C0 成立；C0 在真簿买卖报价缺失时先失败。合法空簿配直接 BBA 的反例仍不绕过 C0。没有构造“C0通过但缺买一”的无效内部夹具来证明未知分支。

新条件不提前 return，不事后删除意图，不拆成单腿，不抬价、不额外撤单。否决后原 `cancel_available` 尾部继续执行。原维护／修复优先路径、数量、预算、报价和重叠、寿命、时序、风险及经济义务保持。

## 最小诊断

`PairEntryEvaluationV1.low_price_entry` 只增加一个可选结果：`disabled`、`not_below_best_bid`、`veto_below_best_bid` 或 `unknown_comparison`。字段缺省表示尚未到达该后置分支，不能记作否决或可交易。关闭开关的 `disabled` 也不是做过比较。

最终价继续用 `reused_prices`，同帧真簿价及公共组／冻结时点继续用 `public_entry.pm_quotes`、`group`、`observed_at_utc_ms`。放行后的真实新批 id 继续用 `public_entry.new_pair_request_id`；否决时该 id 为空，撤单请求保持自己的 id。没有复制完整行情、统计、盘口、逐腿比较副本或通用跟踪状态。

## 有限工程验证与限制

定向测试通过正式 Core 输入、冻结统计、真实旧单／账户及共享执行链验证：off 与 V4 C0 的实际请求、尾撤和现金一致；Up／Down 各低价整批否决；target 等于买一但复用后价格更低；相等放行及真实 request id；一侧低价与另一侧高旧单尾撤并存；维护／修复优先路径未评估；较后行情不能改变旧冻结；合法空真簿加 BBA 仍由 C0 截断；已有 checkpoint 适配及 CLI／配置往返。

`cargo run -p backtest --example public_candidate_fixture --locked --offline -- <新目录> <有限点数> low-price` 只生成合成 Parquet 及 V5 off/on 完整 day 配置。它先在同价簿产生旧初始批，再令 Up 买一上升、Down 买一下降，使最终低旧价复用和原尾撤真实可达；on 会反复到达新增否决，off 沿原 C0。省略最后模式参数仍保持原 V4 C0/C1 夹具。合成日期、市场和报价仅为工程反例，不是研究默认。

正式程序 SHA、完整配置、实际线程／CPU、墙钟／峰值内存／输出增长、自然结束及必要操作证据以交回包实测为准。有限成本不能冒充旧 P1 或真实八小时、真实频繁触发成本。停止复用原 day 入口；预算停止和 SIGTERM 的完整性限制不变，不承诺排空、完整报告、检查点或自动续跑。

本轮未处理真实历史、旧账户或新研究账户，不查看验证／留出经营结果，不访问 B，不做 PAPER/LIVE、部署或资金操作。工程交回后由研究组接收并准备准确对照包，真实运行另行签发。
