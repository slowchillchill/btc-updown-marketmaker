# 显式公共环境合成验收

本段对应 #181 [D-0367](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181#issuecomment-6266028183)：
真实调用身份、保守公开成交Maker、原Gamma政策及有限输入生命周期。基线为
`b5dd31a0a4aab89486d251a07b84b582ed5a146f`。测量时含未提交源码差异，最终源码绑定以该交付PR头及候选一致性核对为准。
未实施模型、学习、奖励、终值、日账户或恢复方法；没有真实行情扫描、账户回放或训练/评价作业。

## 显式产物与配置

```bash
cargo build -p backtest --example finite_environment_fixture --release --locked --offline -j20
<显式程序路径> <仓库外配置.json> <新输出目录>
```

配置格式见 [finite-environment-fixture.json](finite-environment-fixture.json)。同一二进制可以调整
`caller_role`、`input_lifecycle_policy`、`gamma_resolution_policy`、`iterations`、`progress_every`、
`public_delivery_delay_ms`及`maker_trade`；不需要改源码或重编译。
`implementation_identity`记录实际构建基线、源码集合SHA-256及Cargo.lock SHA-256。
源码集合摘要按相对路径排序，覆盖全部536个Rust/Cargo源文件，以路径、NUL、内容、NUL依次计算；
本次为 `e48446e8e86098c16a1bb53d27afb708e485fde6b14999f2f3c673708282df71`。
单元测试内的合成身份常量只用于构造合法夹具，不代表该构建身份。

程序SHA-256：`1705318d576f0f4e68ea3411915027310a3807fd4b2e7c291ab575b183254531`。
程序嵌入同一合法规则夹具，生成必要合成Parquet、manifest和独立新规则文件，不依赖运行时保留编译工作树。
每例保存完整生效backtest配置、实际角色/Maker/生命周期元数据、原SIM/NOE/Ledger记录、完整报告和原成交经济读取。
这些规则、输入和输出仅服务此夹具；初始2000、固定订单和随机种子不成为新B默认或预算。

`maker_trade=true`使用两个合成市场和两笔固定合法BUY；它是公共链验证策略，不是模型或学习入口。
源码只复用原日供料/公共归约、SIM、NOE和Ledger，没有影子撮合或账本。
原A CLI的策略工厂及撤单能力保持原职责。

## 可独立核对的业务结果

- 三种真实角色均进入实际会话、账户来源及运行元数据；未设置新字段的旧规则/运行和PAPER保持旧路径。
- Maker场景：初始价0.4处有100股外部排位，本账户挂10股。其后盘口净减100股不制造Maker成交。
  规范SELL105股先消耗保留的100股外部排位，只给本账户5股。原NOE进入Ledger后为
  `MakerInferred`、5股、价格0.4、本金2、费用0、原匹配时间`BASE+4000`，经济与成功状态均已应用。
  范围末可用1993、仍预留5（首单余量2及第二市场3），现金总额1998，保留两项未闭承诺。
  下一义务在`BASE+20000`，没有因`BASE+12000`的范围结束而提前执行。
- Taker对照：首单在`BASE+1622`匹配，早于`BASE+1701`的新报价，因此按先前0.6买10股，本金6。
  冻结费率0.07、指数1，实际费用手算为`10×0.07×0.6×(1−0.6)=0.168`。
  下单费用预留上界0.175的剩余0.007不再占用；第二市场只预留3。
  无赎回或返佣到账，可用现金为`2000−6−0.168−3=1990.832`，预留3。
  首轮错误期望曾混用0.175作为实际费用，纠正依据为上述规则、到达/报价时间及原成交字段，原失败保留。
- 同毫秒尾部：两条book都在`BASE+1100`接收、1101接纳，100ms交付后同为1201。
  宿主截止1201时两者均未公开消费；截止1202时完整消费两侧，不读半侧。
  SIM没有新增范围字段或时间拒绝门。
- Gamma两政策×三角色核对成功消费、隐藏前沿闭合、重复及原经济前驱；新角色日入口仍沿原NOE/Ledger自动结算。
  有限生命周期和Gamma政策沿原控制日志重建保持，底层重建不声称学习恢复。

## 定向验证

最终受影响验证：

```bash
cargo test -p sim-system -p backtest --lib --tests --examples --locked --offline -j20 -- \
  --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
```

16个套件293项通过、0失败、7项保持原ignored。随后仅对嵌入规则的可独立调用夹具重验，两项通过。
没有全工作区回归主张。原UTC时钟相关具名生产吞吐测试按仓库规则跳过。

保留的7项ignored分别为：两个day队列/唤醒诊断、`source_health_finite_overhead_measurement`、
`issue185_readonly_early_day_requests`、`reads_real_5676e37_component_golden_report`、
`frozen_gate_a_representative_market_matches_golden_dual_time_execution_vertical`、
`long_window_two_scale_observation_cost`。
后四个SIM项分别需要显式旧日记录、真实旧报告、冻结Gate A及官方端点，或独立长窗成本工作；
本次不扩展这些输入和作业。格式及差异核对与最终候选一并交审。

首次受影响回归在旧`original_auto_redeem_allocates_pair_and_orphan_once_and_preserves_pending`
直接连续供料时触发`RecordingBackpressured`。该有限夹具缺少已有交接等待，而同文件其他有限用例已开启。
仅在首输入前调用原`enable_finite_recording_wait`，保持原case及经济断言，三项并行测试通过，随后上述完整受影响批通过。
没有改生产背压政策、重试或容量。

## 实际代表成本

Rust/Cargo 1.95.0、锁定离线依赖；默认release优化（opt-level=3，无额外RUSTFLAGS/LTO），20个编译作业。
首次优化构建墙钟91.824秒；最终源码指纹核对构建0.271秒。实际CPU配额25 vCPU、内存配额90GiB。

同一最终程序、ModelTrain/FiniteInput/QualifiedPublicConsumption、交付延迟100ms、Maker场景。
两档只改变独立双市场夹具重复次数1/10；每次从显式新合成账户运行，不外推账户或训练组织政策。

| 独立运行次数 | 公共Core应用输入 | 外部进程总墙钟 | 内部墙钟 | 整进程有效吞吐 | 峰值RSS | 输出字节（summary前） |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 11 | 0.121560秒 | 0.059086秒 | 8.2264次/秒 | 26,276 KiB | 226,059 |
| 10 | 110 | 0.618736秒 | 0.530924秒 | 16.1620次/秒 | 40,684 KiB | 2,256,258 |

每档一次测量。内部计时包含合成文件生成、规则派生、原Parquet准入/读取、公共归约、执行/经济、记录及必要报告输出。
外部总墙钟还包含配置读取、程序自哈希和summary/标准输出。峰值RSS是实际运行进程的整体峰值。
原日生产按因果顺序应用，独立后台记录及日消费者线程沿原并发实现；没有GPU工作或CPU/GPU传输。
输出包含必要的原SIM/NOE/Ledger记录及小型完整报告，随独立运行次数增长；没有保存全量训练特征或观察副本。
此成本用于说明本段完整合成入口可用，不能外推真实数据、多市场高频、模型、全日经营或训练吞吐。
#341已交付的公共统计成本保留其原适用范围，本段没有重写或重测该算法。
