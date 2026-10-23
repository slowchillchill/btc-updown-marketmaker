# PAPER 策略宿主

`paper-trade` 将 `baseline-mm::ContinuousInventoryV1` 接入 `shared-run` 的正式在线宿主。
公共层负责原行情消费、V4 反馈、唯一账户、整动作预留和逐项提交；策略库负责策略政策。
该程序不是启动真实在线作业的授权。真实输入会话和运行资源须沿所属任务签发。

## 构建与配置

```bash
cargo build -p paper-trade --bin paper-trade --locked --offline
sha256sum /BUILD_TARGET/debug/paper-trade
```

构建时显式设置 `CARGO_TARGET_DIR`；命令中的 `/BUILD_TARGET` 是该实际目录。
[config.example.json](config.example.json) 给出所有启动字段的结构。大写占位符必须替换，不能直接启动。
示例中的健康、概率和持续库存参数来自正式 `adopted-v1.toml`，不是在线采用或盈利保证。
全部参数在启动时解析，运行中不热更新。金额和数量字符串保持正式精度。

| 配置 | 含义与来源 |
| --- | --- |
| `runtime.socket_path` | 已单独授权的本地 collector Unix socket；程序不创建或管理 collector |
| `runtime.dpt_root/sim_log/noe_root/ledger_root` | 本次独立运行的输出位置；按原所属写入器创建规则提供新位置，不共享账户或覆盖旧产物 |
| `runtime.public_online` | 正式 DPT 流身份、记录/排序资源、批量与稳定频率；从原输入合同提供，不把测试小批参数当生产建议 |
| `runtime.public_online.source_progress` | 显式请求真实来源进展；策略请求来源健康时宿主也会启用，不由查询刷新 |
| `runtime.reference_market` | 冷启动唯一规则加载器的显式参考市场与原 UP/DOWN 身份；不是策略市场筛选器 |
| `runtime.rule_snapshot_path` | 原加载器封存的规则文件位置；冷启动在账户和业务请求之前完成 |
| `runtime.implementation_identity` | 本次程序的准确源码、提交和 Cargo.lock 身份；构建方提供，不使用夹具占位摘要 |
| `runtime.finality_policy_path` | 正式封存的生命周期政策；宿主不调整撮合、确认或资金释放政策 |
| `runtime.session` | 唯一 PAPER 会话、账户、初始实际现金和原 32 字节种子身份；不是外部钱包或交易凭据 |
| `runtime.admission_policy` | `ActualCashNoMarketLimit`；可发送资金由唯一 Ledger 实际现金与正式预留决定 |
| `runtime.max_group_bytes/finite_in_flight_groups` | 原记录资源配置；`finite_in_flight_groups=0` 仍合法，不额外增加最小容量 |
| `runtime.statistics_input_set_identity/statistics` | 声明输入集身份与明确统计窗口；实际统计还绑定流头、封存规则和真实在线应用政策 |
| `strategy` | 正式持续库存参数；`initial_cash_pusd` 与声明的新账户现金一致，不能拿预测值替代实际现金 |
| `request_namespace` | 本次唯一策略请求身份前缀，防止不同运行混用身份 |
| `progress_every_ms` | 正数，控制诊断频率；不作为行情、计算或交易节拍 |

输入沿 `DataCollectHandoffV2`，先 Session，再按原 transport sequence 的消息和确认。
DPT 按原合法 metadata/盘口/规则建立状态；只拥有某个市场身份不证明盘口可交易。
来源时刻、接收时刻、归约前缀和在线实际应用时刻分别保留。重复、迟到和未知按原公共合同处理。

## 启动、进度与停止

```bash
/PROGRAM/paper-trade /CONFIG/complete.json > /OUTPUT/run/stdout.jsonl 2> /OUTPUT/run/progress.log &
paper_pid=$!
printf '%s\n' "$paper_pid" > /OUTPUT/run/pid
```

```bash
tail -n 10 /OUTPUT/run/progress.log
kill -TERM "$(cat /OUTPUT/run/pid)"
wait "$(cat /OUTPUT/run/pid)"
```

这些命令使用本次程序的准确 PID，不能据名称停止其他任务。进度记录实际阶段、已应用输入、公开组、
计算次数、是否计算中及已耗时。连续输入的总量和剩余时间保持未知。诊断监视器使用独立标准线程，仅保存一份只读标量，没有账户或提交能力。
同步记录容量等待不阻止该线程读取已更新的实际阶段；程序退出通知不等待该线程或长打印间隔。
本机调度或 stderr 自身阻塞仍可能影响打印及时性，不能把打印频率当业务期限保证。

SIGINT/SIGTERM 沿原 `Stopped` 路径停止输入并排空已接纳记录。自然输入 End 也保留原排空边界。
排空不推进未来成交、终局或回款，不生成完整 RunSeal。发生 I/O/协议错误时返回错误，不能当自然结束。
这些停止路径可能等待已有记录，程序不承诺固定时间内排空。

策略真正返回 `Exit` 时，正式 binary 在唯一计算线程中先调用普通 `std::process::exit(0)`，
不等主线程的记录容量等待，不先写完整报告、撤单、终局或赎回。程序不强杀外部进程，也不管理 collector。
退出码 0 在此只表示策略要求结束；它不证明 RangeEnd、排空、经济闭合或策略有效。
普通 Rust 退出仍适用平台退出处理器语义，不承诺任意退出处理器的硬实时期限。

`run_paper_trade_v1` 和 `run_paper_strategy_v1` 的库模式返回报告，适合受控组合。
同步记录等待期间，库模式不能承诺立即返回。正式程序使用固定的 `run_paper_trade_process_v1` 政策；
同一底层宿主的退出通知在宿主已终止、出错或取消后退役，旧分离计算线程不能终止后续调用者。

## 因果边界与结算

每次只允许一个策略计算。计算线程只拥有策略和必要冻结输入，没有 driver、Bridge、socket 或提交能力。
主宿主继续消费真实输入及必要期限。计算期间保留实际公共组用于公开依赖补查，完成后释放；
坏后恢复不能复活此前已失效动作。依赖直到结果出现才确定时，补查记录数学失效时点和实际检测时点，
不声称过去已经执行过查询。保留量随计算期间输入组数增长，没有人为截断或固定计算超时。

每项实际发送由原 SIM owner 在必要推进和容量等待后采样真实单调时钟。
SharedExecution 同步已可见反馈与 Ledger 后执行策略资格判断，再进入该项原请求事务。
已发送和效果未知项保持原身份，失效后缀含撤单也不再发；仅确认未发部分释放本地预留。
幂等重读回执的 `sent_at_utc_ms` 是原登记发送时点，`observed_cutoff` 是本次资格/读取前缀，
不能把两者拼成过去发送时的原 cutoff；`newly_registered` 区分本次新增提交。

本宿主显式记录原 `GammaResolutionPolicyV1::SourceTimeOnly`。
真实带源时间的 `market_resolved` WS 与 `ResolutionTerminalWs` 元数据经 DPT 原资格合并后，
沿原 PAPER 公开执行、旧 L2 Maker、生命周期、NOE 与 Ledger 自动兑付一次。
市场 end、普通 closed、缺 WS、未知 Gamma 源时间不构成回款依据。
经济前驱、补偿、费用和返佣仍由原 owner 闭合；不新增 redeem/query 或 SettlementCoordinator。

## 输出和中断限制

普通返回时 stdout 给出 `PaperStrategyReportV1`，包含 End/Stopped/库模式 Exit、实际计数、最后观察时间、
DPT 稳定前缀、已收到的 transport sequence、SIM/NOE/Ledger 是否排空和尾部稳定性是否未知，
并在普通返回时保存一次原 Ledger owner 投影。该投影不替代未领取事实或未来终局。
DPT 的稳定前缀与收到序号含义不同，不能以收到确认推断全部尾部耐久。
策略进程 Exit 可能没有最终报告。已发布的稳定 DPT、NOE、Ledger 和 SIM 完整记录前缀仍按各自读取器读取；
未发布或未完成的尾部可能丢失，不能补造成功或完整标签。当前入口不提供自动续跑/检查点恢复，
需要复现时使用新输出位置和准确原输入重新签发、重跑。
