# 市场元数据初始化边界样例

本目录的样例只用于工程合同回归，不代表完整日期准入或研究收益。

- `metadata-initialization-boundary-v6.parquet`：原 UTC 2026-09-08 首五分钟依赖所在的末小时文件中摘取 `6601、6603、6665、6667`，保留原强类型列、行值、源消息身份及顺序。6601与6667分别是两个市场的真实Snapshot，6665是前一市场的真实生命周期转换；6603是该市场的AnchorRule，用于证明初始化选择仍排除该变体。原逻辑文件为 `2026-09-08/typed_raw/utc_day=2026-09-08/source_kind=polymarket_market_metadata/polymarket_market_metadata_23.parquet`，原摘要 `c3bd000a74909abc1725b7bbe0fe3a72ebf7416b772c0ca28af859568c2f9154`，97,043字节、792行。
- `metadata-range-boundary-v6.parquet`：沿既有 `data-pipeline/training/tests/fixtures/metadata-20260909-6730-6732.parquet` 的三条原行重新编码，增加与本样例三行相符的每日文件事实Footer，供原每日准入入口使用。原逻辑文件为 `2026-09-09/typed_raw/utc_day=2026-09-09/source_kind=polymarket_market_metadata/polymarket_market_metadata_00.parquet`，原摘要 `968508be289147d6a2a5e8eb5325d73a62e3cd8bcfc602558dcd09b8bd6000ec`，98,097字节、793行。

初始化夹具摘要为 `2430d285e1a932631a8d36511ae27bb74f71ed4449ad957799ca709539456974`；范围夹具摘要为 `ef82aaaa9ded437a1288dac957550ad912db5014bc51fa084ebdb2666199e278`。这些是重编码后的代表性子集文件，测试清单使用夹具实际大小、行数与摘要，不沿用原全文件事实。

生产选择通过真实Snapshot的来源消息身份和目标市场确定片段，不使用上述日期或序号。测试验证完整Existing片段、AnchorRule排除、来源绑定和空状态重放；删除任一必要依赖时沿原归约分别核对缺身份或缺显式生命周期转换错误。
