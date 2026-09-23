# Issue #89：PM 证据与旧同步 NOE 的有限资源观察

本报告对应 3.1.6c 的 NEW-SIM-1、NEW-OTHER-01，只补充实际持有量和有限业务证据，不设资源门、性能阈值、保留期限或检查点。

## PM 证据对象

`FinitePublicConsumptionV1::measurement_resources` 按需读取 `public_pm_evidence` 中真实 `PmOwnedStateV1::new_evidence_only` 所有者，分别输出 markets、assets、levels、absences、selected。该状态没有第二份数量簿，数量仍由 SIM 原业务簿持有；该次实验的 `business` 字节口径未变（Issue #107 后的口径见文末）。新增字段是对象数量，不是字节或 RSS。

`public_pm_evidence_counts_follow_delete_and_epoch_reset` 经真实 DPT 记录、公开组消费和共享归约执行，得到：

| 实际公开输入 | markets | assets | levels | absences | selected | 被测买档数量 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 初始双边快照 | 1 | 2 | 2 | 0 | 2 | 100 |
| 买档数量删除至零 | 1 | 2 | 1 | 1 | 2 | 0 |
| 新 epoch 的成交价消息触发 ResetAsset | 1 | 2 | 0 | 0 | 0 | 0 |

ResetAsset 清对应 token 的证据条目，保留市场上下文和资产状态。本计量不测堆分配、字符串 capacity、临时 `pm_materials` 或后台共享材料，不能据此宣称覆盖全部内存。

## 旧同步 NOE 的真实业务轨迹

`finite_real_orders_observe_sync_noe_history_and_reopen` 复用真实 SIM 会话、合法规则夹具和订单入口：账户初始 1000 PUSD，公开盘口就绪后逐次提交 20 shares、限价 0.4 的 GTC 买单，再按原平台编号撤单。SIM 实际生成响应、订单生命周期以及资金占用和释放，旧 `SimV2NoeProjectionPortV2` 映射并同步持久化；固定使用 SIM batch v3、NOE contract/container v2。输入规则和盘口来自既有测试夹具，不是线上市场或实盘执行。

每个前缀都断言提交与撤单的经济转换各发生一次，可用资金变化合计为零；把最后一个周期的真实投影后缀重新送入原端口，提交数、记录数和经济索引不增加。新旧 store 的全部 commits 和必要索引一致，原开户结果引用及各前缀发布的旧 manifest 仍能读取。保留 `commits() -> &[DurableCommit]`、原身份、查询与恢复语义。

2026-09-16 的独立单测试进程记录如下。时间和内存只作观察，不用于测试通过条件。

| 发单撤单周期 | 投影批/commit | 全部记录/结果引用 | 控制记录 | 经济记录 | logical 索引 | economic 索引 | NOE 文件字节 | reopen 微秒 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 33 | 97 | 32 | 33 | 97 | 33 | 211243 | 13247 |
| 32 | 65 | 193 | 64 | 65 | 193 | 65 | 419423 | 23310 |
| 64 | 129 | 385 | 128 | 129 | 385 | 129 | 836470 | 43883 |

经济记录包括一次开户和每周期两次资金转换；其余记录是实际订单生命周期，不是空批。NOE 文件字节只统计其上下文和 commit 文件，不含 SIM 事件日志、外部 manifest 和本报告。

| 周期 | reopen 前当前 RSS / 累计 VmHWM（字节） | 两份 store 并存时当前 RSS / 累计 VmHWM（字节） |
| --- | ---: | ---: |
| 16 | 41639936 / 41639936 | 43270144 / 43270144 |
| 32 | 43630592 / 43630592 | 44703744 / 44703744 |
| 64 | 45088768 / 45088768 | 47104000 / 47104000 |

采样前仅有旧端口的一份完整 store；进程同时持有 SIM、端口结果引用、最后一个周期的测试投影、原开户结果、先前前缀的 manifest 与小型测量结果。`result_references()` 的临时向量在采样前释放。reopen 计时只包含 `DurableContextStoreV2::open`，随后在原 store 与重开 store 同时存在时再次采样，再显式释放重开 store；下一前缀没有第二份 store，但分配器可能保留已释放内存。VmHWM 是整个进程的累计高水位，RSS 也不是 NOE 独占内存。累计耗时 375/783/1573 ms 包含先前的测量和兼容核对，不能当纯吞吐量。

这份样本补上了旧“开户加空批”观察缺少的实际业务内容，证明必要历史在该有限轨迹下增长及兼容读取，不证明长期内存上界或性能保证，也不授权截断历史。新异步热 NOE 路径未改动。

## 重现

在仓库根目录使用一个尚未存在的独立输出目录，并将其展开为绝对路径（Cargo 测试进程工作目录是包目录）：

```bash
NOE_FINITE_OBSERVATION_ROOT="$PWD/target/issue89-316c-noe-business-final" cargo test -p sim-system --features public-consumption,measurement --lib run_v2::tests::noe_resource_tests::finite_real_orders_observe_sync_noe_history_and_reopen --locked --offline -- --exact --nocapture --test-threads=1
```

结果写入该目录的 `resource-observation.json`，并保留 `noe/`、SIM 事件日志及三个前缀 manifest。路径属于持久引用身份，换输出目录可能改变文件字节数；时间、RSS 也会受环境影响。普通回归未设置该变量时使用临时目录，不保留产物。以上资源数值来自该独立 invocation，不取自并行全套测试进程。

## Issue #107 后的采样口径

上述历史数值保持原实验结果。当前实现已移除撮合、订单、结算、查询根及待投影队列的增减计量副本；仅在启用 `measurement` 且实际请求快照时，扫描当时持有的业务对象与待投影批次。普通业务路径不再因计量加减溢出或下溢而拒绝执行，实际金额运算、队列、水位和背压不变。

当前 `records` 统计所列逻辑对象，`bytes` 累计这些对象的自有文本长度，不计容器容量、固定宽度字段或分配器开销，不代表 RSS。公共 PM 归约对象仍由有限宿主单独按需观察。GateA 有限宿主在会话开启、每 4096 桶及结束时采样，`business_high_water` 是这些样本的最大值，并非每事件峰值。新口径不与上面的历史增减计数视作完全同义，也没有据此重新测量或推导性能收益。
