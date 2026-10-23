# 持续库存候选 v1

本候选按同一市场的成功孤腿、已观察未成功应用成交及全部未闭合义务，持续补充当前精确价位的容量。采用依据为 [#360](https://github.com/slowchillchill/btc-updown-marketmaker/issues/360) 与 D-0376。它与旧 V5/off 并存；工程合成验证不签发真实历史运行。

## 身份和入口

库入口是 `ContinuousInventoryV1`、`ContinuousInventoryConfigV1` 与独立的 `ContinuousInventoryContinuityV1`。正式身份分别为 `continuous_inventory`、`candidate_v1_continuous_inventory`。新命名空间及接续版本独立保存，旧候选记忆不能换标签后接入。

`backtest-cli --mode day --config <完整日期配置> --progress-interval-seconds <秒>` 在每个候选的 `strategy` 中接收完整参数。`run.strategy.strategy_id` 与 `strategy_rule_version` 若声明，必须与参数类型一致。新旧候选可放入同一日期的 `candidates`，每日公共输入只消费一次，账户、请求命名空间、seed和输出位置分别声明。旧 V5/off 仍使用 `complementary_pair` / `candidate_v5_public_low_price` 及原 `parameters`、`initial_pair_low_price_veto=false`，不自动改用新候选。

完整新参数见 [adopted-v1.toml](../../configs/strategies/continuous_inventory/adopted-v1.toml)。这是采用参数示例；宿主还须明确输入UTC范围、合法初始化、公共交付与计算/派发时序、统计窗口、规则/实现身份、输出和并发。设计JSON不是启动配置。混合日期的命令行覆盖按字段所属候选应用；共有的 `health` / `repair` 覆盖双方，须查看最终生效配置。

规则继续由正式 `derive_historical_maker_rules` 以显式 V2 方式派生。使用准确实现JSON、原规则SHA和因果起点；不将测试 a/b/c 占位身份作为正式程序身份。`HistoricalBacktest` 使用 `PUBLIC_TRADE_CONSERVATIVE_FIFO_V2`，其他角色不因此改变。

## 原42参数迁移

旧候选的42项及语义保留。新候选有38个显式叶字段，迁移如下。

| 旧字段 | 新字段及处理 |
| --- | --- |
| `quote.base_pair_shares` | `capacity_shares=50`，每侧最终精确价位容量及利润支撑分母 |
| `quote.inventory_shift_per_pusd` | 改为 `inventory_skew_ticks_per_capacity=1`，按成功H与待应用T份额偏移 |
| `quote.base_half_spread` | 由同冻结真簿价差和有效tick计算h，不继承固定半价差 |
| `quote.signal_spread_weight` | 不进入新普通公式 |
| `quote.uncertainty_spread_weight` | 不进入新普通公式 |
| `quote.initial_pair_price_reference` | 使用互补真簿中点c，不继承旧参考价选择 |
| `quote.minimum_business_result_per_share` | 不继承旧普通经营门 |
| `quote.joint_fill_scenario_weights` | 不继承旧四情景评分 |
| `quote.execution_path_ms` | 不继承旧普通预测/门；真实宿主时序和原修复路径仍分别声明 |
| `risk.per_market_orphan_limit_pusd` | 同名顶层字段100，金额语义保持 |
| `risk.account_orphan_limit_pusd` | 同名顶层字段400，金额语义保持 |
| `probability` 六项 | 原值迁至 `probability_reference`：`volatility_window_ms`、`initial_sigma_usd_per_sqrt_second`、`estimation_error_usd`、`binance_innovation_weight`、`binance_pressure_weight_usd`、`binance_trade_pressure_weight_usd`。用于原参与源健康及终止/独立估计，不给普通报价增加p可用门 |
| `repair` 九项 | 三项 `*_loss_allowance_per_share`、`maker_fill_weight`、`wait_price_improvement_per_share`、`residual_risk_weight`、`maker_path_ms`、`active_path_ms`、`enable_active_repair` 原值保持，FAK仍关闭 |
| `health` 十六项 | PM、Chainlink、Binance trade、Binance depth 各自 silence/event delay 的 normal/max 原值全部保持，见完整TOML；正常至最大之间沿原可接受退化语义 |

另将原2000起点、1000单市场含费投入及30%确认权益回撤明确为 `initial_cash_pusd`、`market_investment_limit_pusd`、`drawdown_fraction`。不将旧C0、BLB门、V5低价否决或普通Maker修复选择器搬入新普通报价。

## 义务与动作

`ActionBeforeReadV2::obligation_orders` / `obligation_trades` 是同冻结下的原Registry、Ledger未闭合成交与待应用原NOE人口。它们补足闭单仍待经济应用的情况，保留原NOE位置及owner应用状态。`None` owner记录不是成功或已应用，未知不补零。

H只含Ledger成功应用孤腿。T含已观察、非最终失败且尚未成功应用的成交。活动余量、无回执/在途承诺和成交部分互斥分解，按原request/ordinal/order/trade去重；已成功应用的部分由H承担。FinalSuccess观察/应用间隙仍占T、当前价覆盖、风险和投入。取消请求不能释放义务。

本候选按[#360 D-0378口径](https://github.com/slowchillchill/btc-updown-marketmaker/issues/360#issuecomment-6359546323)逐项读取利润占用：`min(首次分配额, max(原冻结场景剩余未决亏损, 当前Ledger平均池下所属腿剩余未决亏损))`。按原reserve、ordinal、condition/token及order/trade关联隔离两腿；另一腿反馈不能释放本腿支持。成功应用已进H/P的部分不再计未决亏损，已入P的历史或MINED费用不重扣，未知不用于释放。

当前H仅与所属腿真正未闭数量中可配部分计算。多个请求分别使用同一H作为保守场景输入，场景可以重叠；不分独占H配额、不建FIFO或影子成本池。占用受首次分配额约束，不等于真实损失上限；原硬成本、小亏和费用检查仍独立按实际H及实际可配量读取，超过原分配额本身不增加取消或停机门。取消申请不关闭义务。旧V5/off继续使用原冻结分配政策，没有采用这个max口径。

回撤触发后的独立H补腿仍由原终止选择、健康、成本与资金责任筛选，并可通过正式发送前资格；普通新增风险继续受终止状态限制。

普通动作理由由同一决策诊断连接：`markets[].initial=true` 为首次等量；`initial=false` 且对应 `action_dependencies[].independent_held_token=null` 为活动库存增量，可为SingleBuy或不等量两BUY；该字段非空才是受限状态中已确认H的独立降险。按 `request_id` 连接实际意图/派发记录，不能把所有POST_ORDER或SingleBuy都称为repair。`cancellations` 保留目标和实际理由。

## 变化式消费

新诊断出口 `continuous-inventory-changes-v1` 包含 `version` 与可选 `definition`。同一账户按记录原序消费；`definition=null` 沿用最近标量定义。`obligation_changes` 与 `reservation_changes` 按稳定身份更新，`no_longer_unclosed` / `no_longer_reserved` 移出当前集合。移出不是成功或经济应用证明，须连接原NOE/Ledger事实。内部完整资格快照不会逐帧输出为全部旧单/预留副本。

标量保留condition/token、真簿bid/ask/tick、c/h/s/最终价格、H/T/D/C/d、首次标记、逐侧风险/数量、M/A/投入、现金前后、源健康、不可用及取消理由。`account_dependencies` 的出口只保留市场汇总和义务数。真实身份、原LIMIT、剩余量、成本已知性、trade最终性、经济应用及原NOE位置通过义务变化连接。

混合连续库存对照使用 `continuous-inventory-public-v1` 公共变化出口，保存真簿必要最优价量/质量、直接BBA、tick及组引用。`bids_materialized` / `asks_materialized` 区分未物化与有效空簿；BBA不代替真簿。这里不请求或输出BLB档位。统计仍按配置和实际公共输入更新；关闭证据不改变统计或策略事实。若无消费者需要BLB窗口，可在正式统计配置中显式采用空 `blb_lookback_windows_ms`。

## 正式累计经营读取

`report.account_economics_memory.markets[]` 沿原Ledger增加以下只读值：

| 字段 | 含义 |
| --- | --- |
| `cumulative_paired_shares` | 原成功平均池累计形成量，含已兑配对 |
| `cumulative_paired_principal_cost_pusd[2]` | 原平均池分配到累计配对的各腿本金，腿序沿正式assets |
| `cumulative_paired_actual_trade_fee_pusd` | 成功实际费用减原未配平成功费用；旧缺贡献时保持Unavailable |
| `cumulative_paired_spread_after_trade_fee_pusd` | 累计形成量减上述本金及成交费；不含返佣、孤腿结局或结算费用 |
| `redeemed_paired_shares` | 原resolution已兑配对量 |
| `paired_shares` | 原期末未兑存量，保持原字段语义 |

成功费用复用原成功投入索引减原累计成功本金，再减原孤腿费用；不建立新累计器、FIFO池或经济分录。赎回不使累计形成量倒退。正式返佣各阶段、结算费、孤腿与确认净损益仍使用原字段，不能将未到账返佣或配对毛额称为现金利润。

## 范围末端与中断

`RangeEnd`、`recording_drained`、`pending_noe_records`、`unclosed_commitments`、`observation_cutoff` 与市场结算状态分别读取。自然结束不证明resolution、未来反馈或全部经济最终性完整。end_at后不新增/撮合；SIM不自动在end_at释放所有open。策略仍可按原结束规则发出Cancel，现金须待正式取消反馈应用后释放。结束夹具没有派发该次策略输出，其1955现金仅证明该条件下SIM不会自行释放，不能当作策略真实派发后的现金结论。原合格resolution及结算路径决定取消和兑现。当前A历史经营沿D-0328消费SIM的原`AutoRedeemApplied`，交易侧不发起redeem；真实到账后应收归零，不能继续计作未兑现资产。

进度周期通过CLI明确配置；准备和回放各报实际量及耗时。未知总量/剩余时间保持未知。人工SIGTERM可能没有最终报告或完整尾部，已发布耐久前缀与完整JSON行可按原读取器检查，不能承诺排空、自动续跑或中断后完整经营结果。需要完整结果时使用新输出目录从头运行，真实历史执行须由研究组接收准确包后另行签发。
