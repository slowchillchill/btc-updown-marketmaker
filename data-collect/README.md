# data-collect

- [模块需求、技术合同、接口、运行、验收与限制](https://github.com/slowchillchill/btc-updown-marketmaker/issues/20)
- [历史资料、归档映射与固定 Git 入口](https://github.com/slowchillchill/btc-updown-marketmaker/issues/21)
- [模型训练消费边界](https://github.com/slowchillchill/btc-updown-marketmaker/issues/22)
- [外部磁盘保护与部署证据](https://github.com/slowchillchill/btc-updown-marketmaker/issues/23)

训练采集器不再接受 `--max-dataset-bytes`。配置摘要仅包含现用配置，因此本次删除容量字段后，摘要与旧版本不同；数据集容量清理由外部磁盘保护负责。

Polymarket metadata 不再写入 `terminal_predicate_passed`、`closed_time_check_passed` 两个布尔字段。终态由原行种类、轮询结果及来源表达；closed-time 保留原始值、UTC 值与失败原因。V3–V6 的版本名不变，新 Parquet 布局减少这两列；当前恢复与 DPT 读取器忽略旧文件中的这两列，继续读取现用业务字段。依赖旧完整 schema 的旧程序不能直接读取新布局，生产者与读取器应一起更新。
