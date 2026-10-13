# Gamma 自动结算真实终态片段

本夹具属于 #185 的 D-0346 验收。两份 Parquet 各保留一条真实采集行及原 schema。
它们是有限解析片段，不是完整小时文件或完整日准入材料。测试通过原
`open_finite_public_input_v1` / TypedRaw Parquet 解析器读取，再走原 DPT 公共准备与 SIM 消费。
没有使用内部 Serde 枚举直接解码原始采集 JSON。

原数据目录为 `2026-09-12/typed_raw/utc_day=2026-09-12/source_kind=polymarket_market_metadata/`。
`settlement-real-fragments.json` 绑定片段的大小、实际行数和 SHA-256。

| 原文件 | 从 0 开始的行号 | 原文件 SHA-256 | 片段 |
| --- | --- | --- | --- |
| `polymarket_market_metadata_02-part-1.parquet` | 14 | `8df0ab40b6f886cd1b0303f58ce6e74d08327659e4e4831a4e9c85df8b791fd8` | `gamma-terminal-20260912-024700047.parquet` |
| `polymarket_market_metadata_04.parquet` | 29 | `d4c0041a2abeecb9fa77ea8f32c760eb52d9f64cf949ed22521694b0ed7cc865` | `ws-terminal-20260912-040154033.parquet` |

Gamma 行属于 02:35–02:40 的 `btc-updown-5m-1789180500` 市场。其接收时刻是
02:47:00.047，证据验证完成时刻是 .055，采集准入时刻是 .056。原平台源时间为空，
`closed_time` 是 02:40:54。结果是 Down。测试读取实际公共组消费时刻作为经济依据，
不把 .047 或 `closed_time` 塞进平台源时间。

WS 行属于 03:55–04:00 市场。平台源时刻是 04:01:54.026，接收和证据验证完成时刻是
04:01:54.033，结果是 Up。该事实在四小时窗口之外，测试不消费它、不产生回款。

身份快照、交易盘口、合法 TWAP 时钟探针、同义重复、相反终态和 WS 冲突均是明确构造的
有效夹具。它们不冒充原采集记录。非零回款用例显式延迟 Gamma 的交付，用原 SIM 请求
买入 5 份获胜资产，并保留同毫秒 MINED、后续 CONFIRMED 及完整检查点恢复。
测试验证前驱闭合后自动回款 5 pUSD，原 NOE / Ledger 与 SIM 余额一致。

同毫秒冲突用例保持原 DPT 桶顺序；它只证明桶内的冲突会取消待处理 Gamma，
没有经济判定或回款。它不证明 PM 记录先于冲突记录。
