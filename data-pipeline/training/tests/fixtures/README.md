# 真实元数据来源顺序反例

`metadata-20260909-6730-6732.parquet` 从已准入的 UTC 2026-09-09 首小时元数据中只摘取采集准入序号 `6730、6731、6732`，保留原列、行值及原顺序，去除原全文件统计元数据，重新编码为单个 Parquet 行组。逻辑来源为 `2026-09-09/typed_raw/utc_day=2026-09-09/source_kind=polymarket_market_metadata/polymarket_market_metadata_00.parquet`；原文件 SHA-256 为 `968508be289147d6a2a5e8eb5325d73a62e3cd8bcfc602558dcd09b8bd6000ec`，98,097 字节、793 行。夹具 SHA-256 为 `b221d29195eded6dff020e0dc4e1b605d57c656635f3ae6a532f082dd0ca7ca9`，33,889 字节、3 行。

前两条为 V6 Existing 的 StateTransition，原接收时间均为 `1788912000008`，真实准入时间均为 `1788912002228`；摘要并列排序会使 `6731` 先于 `6730`。第三条为原 AnchorRule。三条接收时间均位于批准研究范围，行内已有身份声明的更早时间仍是原字段，不代表本测试读取或供入研究前记录。

测试经原 Arrow 强类型解析和 `prepare_historical_rows_v1`，另把前两行映射到 V5 的原兼容物理模式，验证两种表示。文件组合、读取批次和外排页变化应保留 `6730 → 6731 → 6732`、完整强类型行及原证据身份；旧行缺失 epoch 保持。首次无身份转换仍返回原 `MetadataBeforeIdentitySnapshot`，本夹具不提供身份快照，也不承担身份初始化或收益验收。
