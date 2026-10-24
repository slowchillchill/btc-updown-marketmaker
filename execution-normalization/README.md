# NOE 已发布前缀只读工具

`read_noe_prefix` 独立读取一个 NOE context 的明确已发布批前缀，不需要原 SIM 文件。它复用
`visit_durable_noe_records_v2`，没有恢复写入器、清理尾部、经济归约或策略判断。

```bash
cargo build -p execution-normalization --bin read_noe_prefix --release --locked --offline
read_noe_prefix <context-directory> <published-prefix:u64> > records.jsonl 2> read.stderr
```

第一个参数是原 context 目录，按操作系统路径处理。第二个参数是明确的批数
（`durable_through`），不是记录数或文件数。`0` 合法。工具不选择默认目录或最新水位。
调用方负责提供已冻结、可读取的正式目录和目标前缀；本工程的合成验证不授权读取真实研究数据。

stdout 只输出 JSONL，依次为：

| `kind` | 内容 |
| --- | --- |
| `begin` | `format: "noe-prefix-read-v1"`、`published_prefix` |
| `record` | 原 `NoeResultReferenceV1` 的 `reference`、原 `NoeRecordPositionV1` 的 `position`、借用 `view.record()` 序列化的 `record` |
| `complete` | visitor 成功返回的完整 `NoeHistoryReadStatsV1`：`commits`、`records`、`segment_opens`、`pages_read` |

原记录的枚举、ExactDecimal、来源及身份表示保持正式类型的序列化，不筛选订单或重新编号。
`reference` 和 `position` 给出已发布持久材料的坐标，不证明策略已观察、反馈已领取或 Ledger 已应用。
`complete` 报告 visitor 成功结果；仍须核对下面的 flush 和退出条件。它不是账户终局或业务判定。

成功需同时满足 visitor 成功、完整 footer 写出、显式 stdout flush 成功及进程退出 `0`。
参数错误退出 `2`；读取、序列化、写出或 flush 错误退出 `1`，错误写到 stderr。
`begin` 不证明输入有效；看到 `complete` 文本仍需核对退出码。stdout 输出错误使用既有
`DurableErrorV1::Io` 并明确标记 `<stdout>`；它不等于输入损坏。

读取失败不写 `complete`，但会尝试 flush 已缓冲的部分事实。若该 flush 也失败，stderr 保留
原始读取错误并另报输出错误。例如前缀超过已发布批数时，可能先输出已有记录，随后报告
`CommitSequenceMismatch`。这些记录只是部分事实，不能据此判无匹配或读取完整。

工具只调用一次 visitor，逐记录写出，不累积整段历史。现有 visitor 会枚举并排序提交名称，
还会保留当前共享段及页；因此内存不是恒定值，也不承诺完全不枚举前缀之后的名称。
没有附加线程池、自动重试、检查点或续跑。需要停止时向准确进程发送 `TERM`；
中断可能留下不完整末行、无 footer 或未刷出的尾部，不保证排空。应使用独立输出文件重新读取，
不能把中断结果追加后当成一次完整成功。

工程验证使用正式 `NormalizationCore` 和 `StagedNormalizationWriter::create_batched` 经
`stage`、`stabilize` 生成有限合成批，再运行同一个 CLI。测试覆盖精确原值、引用和顺序、
零及部分前缀、超大前缀的部分输出、后段损坏和真实 stdout 写出失败。
有限成本记录的适用范围仅为其合成输入、构建 profile、线程及输出规模，不代表真实研究工作量或长期吞吐。

Refs #375
