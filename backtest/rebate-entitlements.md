# 公共返佣权益的只读确认

此接口落实 [#181 任务1交接第7.3节及附文](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181#issuecomment-6263182304)
和 [D-0112 原返佣合同](https://github.com/slowchillchill/btc-updown-marketmaker/issues/10#issuecomment-5565364365)：
训练使用原日已具备资格及金额前驱的权益，不模拟等待次日付款。原付款时刻、可用现金、门槛和精度保持原规则。

## 调用与结果

持有原 `FinitePublicConsumptionV1` 的消费者，可通过其公共读取状态显式查询一个种类和一个归属日：

```rust,ignore
use contracts::economics::RebateKindV1;
let result = driver.rebate_entitlement_read_v1(RebateKindV1::Maker, utc_day_start_ms);
```

返回 `ExecutionReadValueV1<RebateEntitlementReadV1<String>>`。
`Known` 中的 `day_result` 复用 `RebateDayResultV1`：原日应计、可付权益、锁定余数及原贡献引用。
SIM 查询的贡献引用是原 SIM 成交 ID，与该日原 `DayFinalized` 使用同一贡献集合。
`paid_pusd` 只读原日桶实际付款状态；确认可付权益时它仍可为零。
内部 `finalized_day_state` 的计划付款布尔只用于原计算，不作为新接口的到账证明。
`Known` 不表示存在正的合格权益；低额桶可以应计0.0168、可付0。
学习消费者应使用门槛后的可付权益，不能把应计和到账再次叠加为收益。
`Unavailable` 表示当前不能确认，不代表零。读取不推进时间、消费行情、生成日结/付款事件或修改账户。

公共历史宿主可在普通配置或已准备材料的运行配置中选择最终报告需要的日桶：

```json
{
  "rebate_entitlement_queries": [
    {"kind": "Maker", "attribution_day_start_utc_ms": 1787529600000},
    {"kind": "Taker", "attribution_day_start_utc_ms": 1787529600000}
  ]
}
```

缺省为空，不枚举历史。最终 `rebate_entitlements` 为各显式查询及其结果；配置和报告沿原运行输出保存。
它是报告生成时当前 SIM 经济前缀的标签，不属于报告里较早的 `observation_cutoff`。
它没有发布 NOE `DayFinalized`，也没有使 Ledger 提前确认应收或可用现金。
`StrategyInputV1`、公共统计和冻结观察均不接入此字段。

## 完整性的原有依据

未正式结日的桶必须同时满足：

1. 原日桶确实存在。没有桶不能据此确认为完整零；失败撮合登记的真实桶可在前驱闭合后确认为零。
2. 本会话成功闭合的 `hidden_platform_frontier.closed_through_utc_ms` 已覆盖原 UTC 日最后一毫秒。
3. 原桶 `pending_predecessor_count` 为零。Taker 等待成交终局；Maker 继续等待原市场
   `end_reached` 且 `pending_fill_count=0`，再由原市场归约闭合日桶前驱。

公共查询自行读取上述真实前沿，不接受调用方的封口布尔或任意截止时间。
成功关闭后的原入口拒绝向该前沿及更早时间回退，实际撮合按 `matched_at` 登记贡献。
白天的静默、短窗 EOF、当前没有待终局成交或 A 的公共统计组回执，都不能替代该日末依据。
已有正式日结结果直接复用。金额继续调用原返佣金额函数，不另建日桶、封口集合、撮合或账本。
A的连续出口仍由公共归约/统计生产，不持有账户成交；本接口只读原SIM真实账户成交形成的桶，不以A组回执或公共SELL冒充账户成交。

Maker20%/Taker8%仍为冻结项目规则；同种类原日应计达到1pUSD才可付，按四位小数向下截断。
低额不并入后日，已截取余数锁定且不参与后续门槛，两种返佣不互相凑门槛。
读取按所选日桶查找；前驱未知时不复制贡献，已知时只复制该日必要贡献引用。没有新增持久缓存。

## 经济标签的使用边界

消费者用原账户身份、返佣种类和归属日识别同一权益。重复读取不代表新增收益，随后实际付款也不代表新增权益。
付款前后 `day_result` 保持原金额和分摊，只有实际已付字段变化。原付款继续沿 SIM → NOE → Ledger 应用。
本段没有实现模型奖励消费者或其去重状态，也没有验收任何模型学习。
后见经济标签不能回填过去模型观察，不为获得标签推进到付款时刻或读取范围外行情。

## 合成入口

沿用可独立调用的 `finite_environment_fixture`，通过外部 JSON 选择
`rebate_entitlement_queries`、`received_end_utc_ms`，以及仅供市场屏障反例的 `first_market_end_utc_ms`。
其他政策、角色和规则身份沿原完整配置。省略新增字段仍执行原夹具。

```bash
cargo build -p backtest --example finite_environment_fixture --release --locked --offline -j20
<显式程序路径> <完整配置.json> <新输出目录>
```

此入口通过合法合成 Parquet、公共归约、真实有限宿主、SIM/NOE/Ledger 和最终报告验证接口。
固定请求、账户金额及合成时间只服务夹具，不成为 B 模型、资金或运行默认。
其中市场窗口只为本接口和屏障反例构造，不声称真实5分钟市场、完整UTC日市场覆盖或其运行成本。
它不扫描真实行情、不执行训练、独立评价、PAPER/LIVE 或资金作业。

## 有限尾部重复关闭的修复

合法范围为 `[S,E)`。普通循环可在最后合法毫秒 `E−1` 处理原到期义务，并已关闭该毫秒。
原 `finish_input_at(E−1)` 会再次提交同毫秒观察，从而被原重复/回退规则拒绝。
本段只在真实公共前沿恰好已关闭到该尾部，且已有成功观察截止的时间不晚于该尾部时复用成功事实，更新原观察/反馈读取投影；
必要隐藏尾部仍先沿原链处理。不重复提交观察/关闭输入，不向 SIM 新存范围，不推进范围外义务。
较晚已消费组、较晚静默观察均不能改配到过去。没有回执时，即使已合法观察更晚时刻，
`last_cutoff` 仍可能为None，因此None也不能证明可以复用；这些情况继续走原观察输入，由原重复/回退规则拒绝。
不增加时钟、投影读取接口或范围状态。拒绝不回写原观察截止和帧，不要求原推进过程撤销任何合法副作用。
原控制日志重建核对执行状态和公共/隐藏前沿；
这不是学习恢复，也不把读投影伪装成新经济事实。

## 本次验证与成本

本段以 `f6f9c7c3486a88995da72e5ec4720e65c5c44bde` 加本PR差异构建，接入A已合并的公共连续出口。
源码集合SHA-256为 `11bbb9e1a40579fd0c219005fa87edb53740fecbe7520d3bf25353fc35ca2473`，
按541个Rust/Cargo源文件的有序相对路径、NUL、内容、NUL计算。测量时源码尚未提交；最终提交与候选另作一致性核对。
程序SHA-256为 `f604d0005a4929a23e04a19155f8385fee25f35ca839b9f15277d8c775df17ea`。
完整示例配置见 [返佣权益合成配置](examples/rebate-entitlements-fixture.json)，已有参数可通过外部配置修改，无需重新编译。

验证命令：

```bash
cargo test -p sim-system --lib --locked --offline -j20 -- --nocapture
cargo test -p backtest --lib --test historical --examples --locked --offline -j20 -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
cargo check -p contracts -p sim-system -p backtest -p backtest-cli --all-targets --locked --offline -j20
# r2重验原四档及原三角色环境夹具
cargo test -p backtest --example finite_environment_fixture --locked --offline -j20 -- --nocapture
```

r2修正后SIM库204项通过、4项原ignored；有限环境示例3项通过，静态检查通过。
其中较晚已消费组、较晚静默观察、无回执但已有更晚观察三项回退反例先在r1实际失败，修正后通过。
r1的backtest定向批77项通过、3项原ignored，其中A的continuous_export九项均通过；
除已重验的有限环境示例外，其余未改接口的历史/A出口证据继续适用，未重复执行。
没有全工作区或CI通过主张。原UTC时钟相关具名生产吞吐测试未运行。
7项ignored分别是原旧日请求记录、真实旧报告、冻结Gate A/官方端点、长窗成本，
以及两项日队列/唤醒诊断和source_health有限成本；本次未扩大这些输入/作业。
只核本轮拥有的Rust文件格式，保留最新main中与本任务无关的既有格式。

真实ModelTrain/FiniteInput合成链的四档断言为：短窗EOF未知；普通日末低额金额已知但可付0；
日末已覆盖但Maker市场end尚未到达仍未知；明确范围多消费1毫秒后原市场屏障闭合才能确认。
首轮第四档暴露上述尾部重复观察错误。按D-0367及原准确源码核对后，仅修复重复处理，原四档断言不改，重验通过。
原失败日志保留。原owner成交夹具另核对实际Taker费用17.50002乘8%=1.4000016，
可付1.4、锁定0.0000016，读多次不改变状态；随后原付款金额和贡献集合一致，只有已付字段变为1.4。
Failed不贡献，无桶未知。内部单元测试的前沿参数只验证计算谓词；公开链的前沿证明取实际成功消费，不由测试调用方签发。

Rust/Cargo 1.95.0、锁定离线依赖、release默认opt-level=3，无额外RUSTFLAGS/LTO，20个编译作业。
实际配额25vCPU、90GiB；r1首次release构建墙钟91.0056秒，r2基于仍适用依赖缓存的重建为31.4428秒。
同一最终程序每次独立运行两个合成市场，仅显式读取Maker/Taker各一个原日桶：

| 独立运行次数 | 公共Core输入 | 显式日桶读取 | 整进程墙钟 | 整进程有效吞吐 | 峰值RSS | 含summary输出字节 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 11 | 2 | 0.122722秒 | 8.1485次/秒 | 28,708 KiB | 244,345 |
| 10 | 110 | 20 | 0.570378秒 | 17.5322次/秒 | 37,504 KiB | 2,424,982 |

上述两档均用最终r2程序，每档一次；r1测量保留原身份，不混作本次产物结果，也不将单次差异归因于性能优化。
整进程包含合成Parquet/manifest生成、准入/流式读取、公共归约、执行/经济、原记录、查询和报告，
以及配置读取、程序自哈希和summary/标准输出。内部循环分别0.060006/0.503210秒，不能充当整进程分母。
summary前输出分别224,307/2,236,046字节。峰值RSS为完整运行进程峰值。
已有day生产/账户消费者/后台记录并发保持，单账户经济因果仍串行。没有GPU消费者或CPU/GPU传输。
只读单元微测含JSON编码的debug成本另留原日志，不作为完整链或模型吞吐证据。

单次查询为原日桶查找及该日贡献引用复制，不扫描全部历史。报告仅保留显式查询集，没有新增持久日桶或缓存。
夹具保存的原记录及报告随独立运行次数增长；这是必要验证输出，不是训练观察或特征副本。
这些成本只证明本段完整合成入口可用，不能外推真实行情、全日经营、模型、并行账户数或训练吞吐。
