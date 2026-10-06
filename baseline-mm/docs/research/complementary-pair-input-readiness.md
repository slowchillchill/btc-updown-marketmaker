# 互补配对策略研究输入资格审计（2026-09-09—2026-09-26 UTC）

本报告交付 [#185 当前任务3](https://github.com/slowchillchill/btc-updown-marketmaker/issues/185)的原准入只读核对，承接 [D-0270](https://github.com/slowchillchill/btc-updown-marketmaker/issues/62#issuecomment-5933292905)。18天全部通过原 DPT 准入：2864个文件、45,652,568,053字节、762,240,960行。没有剔除有 gaps 的日期，没有执行参数搜索、策略回测或收益比较。后续具体方法见[研究计划](complementary-pair-research-plan.md)。

当前来源健康流程按 [D-0302](https://github.com/slowchillchill/btc-updown-marketmaker/issues/62#issuecomment-6013334211) 复用本报告的18份清单身份。已有准确配置保持原同序文件列表。当前SQL直接读取Parquet必要列，按文件或必要重叠文件组即时汇总，不先累计全期raw表；不重复全量准入。旧准入不是健康事实完整或业务时间正确的证明。新流程保留原初始化、来源判定、资产与窗口，并单列未知、同步和时间异常。采集排序声明也不作为跨来源全局顺序证明。实际采集提交为 `8e6e9d8a744ec3ec10b4c0b8d57a251a16159d59`。十八日原始到达延迟描述统计已完成，但未做正式有效消息和窗口处理，不能代替健康校准。此前全期列式导入OOM失败也不推翻本报告的文件准入事实；它证明旧资源实现不足。当前入口和语义差异见[直接Parquet研究说明](complementary-pair-source-health-two-layer.md)。

## 1. 原调用与可复核证据

在实现基线 `5503fcf507aebe60c93f49a8565aca60c26411d3` 上新增[资格审计示例](../../examples/research_input_audit.rs)。单个本地进程按日期串行调用 `data_pipeline_training::validate_public_source_daily_manifest_admission_v1`，保留每一天成功结果或原始错误，不改准入规则。清单路径逻辑形状为 `<INPUT_ROOT>/<UTC日期>/typed_raw/utc_day=<UTC日期>/manifest.json`。只读上述显式18天；不复制输入、不外排全量、不读取范围外或独立留出、不连接远端。

原入口校验清单、文件 SHA-256、文件长度、Parquet Footer 行数和身份、正常 writer barrier 等原合同。审计额外列出清单声明、来源计数、覆盖声明与 gaps 数量用于解释，并不以自制校验代替原入口。逐日 `verified_rows` 与 `files[].rows` 汇总完全一致，JSON 中也保留 `source_counters`；成功没有被转换成业务信息齐全。

完整机器报告为[原准入结果 JSON](complementary-pair-input-audit-20260909-20260926.json)，108,940字节，文件 SHA-256 为 `778927c6cbe0a35432416f1cd15a26cfd398611a90add8cb0184dc9aeb239fb5`。实际审计源码 SHA-256 为 `315f3e128010e49eec4357bd1bc68b596574e5156b427be24b5f798b38d61436`，实际优化二进制 SHA-256 为 `ad1cee9a49cb8bfec5a224112e39eac38d5155f72deaf4a3a411495faf3e34b1`。原集合摘要需要后续 `freeze_public_source_input_set_v1` 对完整材料生成；本审计只复用逐日原准入摘要，未将子集摘要冒充完整集合身份。

可复现构建与调用（定位由私有交接提供，公开文档不绑定本机路径）：

```bash
cargo build -p baseline-mm --example research_input_audit --release --locked --offline
/usr/bin/time -v target/release/examples/research_input_audit \
  "$AUDIT_INPUT_ROOT" "$AUDIT_RULES_FILE" \
  4397d7b3a102616c93484892a1cdb5bfa1a6de23c9383e52a04bbb1d480ae422 \
  "$AUDIT_FINALITY_FILE" "$AUDIT_REPORT_FILE"
```

## 2. 逐日结果

| UTC日期 | 用途 | 文件数 | 实证字节数 | 实证行数 | 声明 gaps | 原准入 |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 2026-09-09 | 研究 | 145 | 2,169,975,828 | 34,766,440 | 0 | 通过 |
| 2026-09-10 | 研究 | 166 | 2,102,161,414 | 33,880,107 | 17 | 通过 |
| 2026-09-11 | 研究 | 166 | 2,205,427,289 | 35,628,921 | 15 | 通过 |
| 2026-09-12 | 研究 | 194 | 1,544,553,889 | 25,744,186 | 36 | 通过 |
| 2026-09-13 | 研究 | 215 | 1,609,521,911 | 26,519,877 | 57 | 通过 |
| 2026-09-14 | 研究 | 154 | 2,439,106,910 | 40,021,910 | 5 | 通过 |
| 2026-09-15 | 研究 | 195 | 2,673,707,368 | 43,933,902 | 44 | 通过 |
| 2026-09-16 | 研究 | 145 | 2,901,537,812 | 48,108,530 | 0 | 通过 |
| 2026-09-17 | 研究 | 146 | 2,690,346,664 | 45,251,483 | 0 | 通过 |
| 2026-09-18 | 研究 | 167 | 2,987,044,116 | 50,107,256 | 21 | 通过 |
| 2026-09-19 | 研究 | 145 | 2,142,483,114 | 36,465,224 | 0 | 通过 |
| 2026-09-20 | 研究 | 153 | 2,709,403,192 | 46,293,004 | 6 | 通过 |
| 2026-09-21 | 研究 | 145 | 3,549,168,715 | 59,948,134 | 0 | 通过 |
| 2026-09-22 | 研究 | 146 | 3,224,120,881 | 54,176,454 | 0 | 通过 |
| 2026-09-23 | 研究 | 145 | 3,183,086,744 | 53,959,264 | 0 | 通过 |
| 2026-09-24 | 开发 | 146 | 3,025,521,344 | 50,907,435 | 0 | 通过 |
| 2026-09-25 | 开发 | 145 | 2,780,847,809 | 47,200,607 | 0 | 通过 |
| 2026-09-26 | 开发 | 146 | 1,714,553,053 | 29,328,226 | 0 | 通过 |

研究区间 UTC `[2026-09-09,2026-09-24)`：2427文件、38,131,645,847字节、634,804,692行。开发区间 UTC `[2026-09-24,2026-09-27)`：437文件、7,520,922,206字节、127,436,268行。研究8天共201条 gaps 记录，开发0条；记录可能重叠，不据条数推算独立缺失时长。完整日期与其 gaps 全部保留。

18天均列出 Binance aggTrade/depth snapshot/depth update、Polymarket orderbook V3、市场 metadata V6、Chainlink BTC/USD V4 与 rolling TWAP V1。目录覆盖、来源计数及 Footer 正确仍不能证明每个市场的正式阈值、窗口历史、参考与判定完整，也不能证明每笔事件可及时用于策略。后续依原公开归约核对合法可见时间、市场成员、参考的资格和实际估计依赖；不将记录存在当作收益可评估。

| UTC日期 | 原准入清单 SHA-256 |
| --- | --- |
| 2026-09-09 | `93a70f4775bdb3d52bf7de73d58caaa5ccbf2f2d7f6a8e2f00e09c092e1f1918` |
| 2026-09-10 | `a940cc15d039efd2a2555f8da30958ea6b381cc24574477dbd8c93d33a240a60` |
| 2026-09-11 | `4cb535f1baa9a15d4bb1115020309c5c09edec119e49ef35a0b0400a28c2c975` |
| 2026-09-12 | `e204a795061341ffae961795307892bfdc785d186a99d56de75cfafad64385af` |
| 2026-09-13 | `c9b6a352a7856419b73fbb1c53be9762ef88d9994779fe4e81b544637472a131` |
| 2026-09-14 | `8a516eb39bcd37d2ff993265b1513b0b2b530e1f098628c63b9fe3591c306dd9` |
| 2026-09-15 | `dbb7f677ec17f8c58643a79b6027ea1a88ab145fbe617f83e3fd974d78819bcd` |
| 2026-09-16 | `0e3234a16b227d8479aa2fbc714d1f2cd508366f082e7f5a9ff14c99ab6fe12f` |
| 2026-09-17 | `d217c4d56a6900678374306e406ac93f53e394d48cb84d9b0624c4a3c421ab82` |
| 2026-09-18 | `a710cf0ab237bf708e3690ad6b95fbe4a006dbad53d784906d8588ff78c375c2` |
| 2026-09-19 | `b4b8920adedef134a7030acd3a6dfc370bc5b6449bc9e8af763c0bbfc6787ab7` |
| 2026-09-20 | `b7e20320a29c46f6c9a819cb4d06dab44783aba519036fce4c8e120b39a1ff5b` |
| 2026-09-21 | `1aaaf115459e91ed4a5c60bb63c4e0af370cb0eabc4c42bf47a2169528f01d61` |
| 2026-09-22 | `573ba690a17d9b35874ed97b48434ef53868230ad6f1760c87cd1f908eb0d3dc` |
| 2026-09-23 | `fd2958a10e8a36947d3377c5fbd9ff9d5d26d11c221853179cb25aec0e247465` |
| 2026-09-24 | `4a7bf6e3fc0255ca583719f197ade087819a0f0718d84e0b7655217befca7caf` |
| 2026-09-25 | `6676470736c0b2e0616ff4f69722740fb2f1c7a04f0bd47b5cbff44a12500bd9` |
| 2026-09-26 | `408e18a4288eeffaf8137094d70ca489c604542b9e7f36e8eadfdac37dbb69b8` |

## 3. 实际资源测量

完整优化审计的 GNU walltime 为53.11秒，程序内部53.094722488秒，user42.72秒、system7.25秒；峰值 RSS287,592 KiB（约280.85 MiB）。`/proc/self/io` 增量为 `rchar=58,436,432,414`、`read_bytes=45,601,406,976`、`syscr=714,312`。前者是实际 read 调用交付的字节（包括文件摘要与 Footer 等重复读取），后者是内核物理读取计数、受缓存影响；原材料有效负载大小45,652,568,053字节，两者不混为同一口径。最终机器报告108,940字节，审计没有制备行情副本。

此前 debug 试读47.98秒后主动中止，只完成9月9日和下一日部分读取，无完整报告，峰值 RSS166,640 KiB。它仅说明优化动机，不参与18天资格结论，不计为第二次完整审计。优化构建与读取分开；本次53秒是摘要/Footer准入耗时，不能作为7.62亿行解码、排序、归约或回测的耗时预估。

## 4. 真实规则候选及适用边界

规则材料逻辑定位为 `<GATE_B_ARTIFACT>/platform-rule-snapshot-v3.json`，摘要 `4397d7b3a102616c93484892a1cdb5bfa1a6de23c9383e52a04bbb1d480ae422`，观察时间1787614426333，原实现 `52bb96609f279ae205e4403d87e261a74ca92d44`。该冻结材料同步最小订单5份、费用rate0.07/exponent1/taker_only及rebate_rate0.2，文档平台taker delay250ms；这些是该版本材料的值，不改写为当前另一快照的值。实际材料包含固定官方 CLOB/Gamma GET 来源和参考市场 `btc-updown-5m-1787659800`，不是测试夹具的占位身份。按原 `load_historical_process_snapshot_v3` 分别以研究起点1788912000000和开发起点1790208000000校验，均成功。

这证明材料真实、摘要和原历史因果加载条件满足。其适用声明 `BTC_UPDOWN_5M_PROCESS_RULE_TEMPLATE_V1` 是项目假设：将一个显式参考市场的进程规则作为共享模板。时间早于研究范围不证明全部9月市场规则未变、费用真实逐市场适用或 LIVE 同界。后续机制实验须保留原快照与此假设，按市场公共事实核对 tick、参考与成员，不改写旧快照伪装最新事实；真实规则变化应记录适用缺口。风险敏感性亦不能把假设收益宣称为真实交易收益。

## 5. 最终性依据与真实绑定

[原 SIM 合同 #10](https://github.com/slowchillchill/btc-updown-marketmaker/issues/10#issuecomment-5565364365)和 [D-0121](https://github.com/slowchillchill/btc-updown-marketmaker/issues/62#issuecomment-5569477691)规定：公开 L2 不产生本账户私有 finality；使用运行前封定的 `TradeFinalityPolicyV2`，首阶段 offset0、后续严格递增；正常成功与重试成功两条模板，四类失败为未广播即失败、普通失败、挖矿后失败、重试后失败（可选前导状态沿原合同）；经济应用仅发生于指定阶段，之后 FAILED 必须原子补偿已应用本金、持仓与即时费用。失败覆盖按完整账户身份和 REQUEST/BATCH_ITEM/FILL 域预登记，不在看到结果后挑失败对象。

原实现 `52bb966…` 的 `sim-system/src/bin/sim_measurement.rs::successful_finality_bytes` 提供正常路径 `MATCHED@0 → MINED@10 → CONFIRMED@20 ms`、`economic_application_ordinal=1`、`failure_overrides=[]`。真实 Gate-B working 日志的 `RUN_OPENED` 绑定该 finality 摘要 `b98102f86cbd2e0d635bdc5074dcb1d7c1fb1a84de61d419d07a475c2e8533ef`，同时绑定上述真实规则与实现。日志仅证明启动绑定，不宣称该 working 产物是完整 CleanRun。

审计从该固定源构造字节，摘要与真实启动绑定完全一致，并经原 `TradeFinalityPolicyV2::decode_and_seal` 成功封定；机器报告保留完整231字节对象的内容与 policy ID。原产物未保留独立 finality 文件不构成新业务缺口。此正常路径是明确的项目机制假设，非9月实际私有成交终态或实测网络延迟；空失败覆盖也不证明真实失败率为零。后续失败敏感性沿原四模板和原补偿，不捏造历史私有真值。

## 6. 资格结论与下一步边界

输入已通过本段要求的原文件准入，规则已通过原历史加载，最终性依据已从真实源与启动绑定闭合。本段未执行公开归约、隐藏准备、策略计算或经营比较；因此市场级业务依赖、制备后的资源量、计算耗时和正式风险值仍是研究待测，不能用文件成功强凑研究成果。

[研究计划](complementary-pair-research-plan.md)给出分阶段方法及完整配置形成过程。其中配对/结算细节、分角色费用及返佣已付等最终详细经济指标的正式导出已确认为 #186 任务6/7 的原公共结果遗漏，需在本轮PR审查、合并、清理后先修，再进入研究（现有NAV/WCM和LedgerProjectionV2中的现金/持仓/总费用/已实现PnL/返佣应收可如实使用），须由所属公共模块补齐，不能用最后一次策略回调冒充范围尾部结果。本段不扩大该接口，不复制账本，不要求逐决策 JSONL/SQLite。


## 7. 工程验证

真实18天准入、两个历史因果起点的原规则加载、源绑定finality seal全部通过；原JSON与源码摘要、逐日声明和原准入行数复核一致。`cargo fmt --all -- --check`、`git diff --check` 以及 `cargo clippy --workspace --all-targets --locked --offline` 通过；clippy保留既有模块警告，新增审计example无警告。

固定工作区回归通过，命令为：

```bash
cargo test --workspace --lib --bins --tests --examples --locked --offline -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
```

具名UTC时钟测试 `four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second` 按仓库规则**跳过**；原ignored测试保持原状态。这些工程回归不代表真实策略经营实验已执行。
