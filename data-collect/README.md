# data-collect

- [模块需求、技术合同、接口、运行、验收与限制](https://github.com/slowchillchill/btc-updown-marketmaker/issues/20)
- [历史资料、归档映射与固定 Git 入口](https://github.com/slowchillchill/btc-updown-marketmaker/issues/21)
- [模型训练消费边界](https://github.com/slowchillchill/btc-updown-marketmaker/issues/22)
- [外部磁盘保护与部署证据](https://github.com/slowchillchill/btc-updown-marketmaker/issues/23)

训练采集器不再接受 `--max-dataset-bytes`。配置摘要仅包含现用配置，因此本次删除容量字段后，摘要与旧版本不同；数据集容量清理由外部磁盘保护负责。

Polymarket metadata 不再写入 `terminal_predicate_passed`、`closed_time_check_passed` 两个布尔字段。终态由原行种类、轮询结果及来源表达；closed-time 保留原始值、UTC 值与失败原因。V3–V6 的版本名不变，新 Parquet 布局减少这两列；当前恢复与 DPT 读取器忽略旧文件中的这两列，继续读取现用业务字段。依赖旧完整 schema 的旧程序不能直接读取新布局，生产者与读取器应一起更新。

恢复读取仍保留的全部 finalized 分区，不再限定今天和昨天；running、digesting 文件不参与恢复。读取按市场归并实际未闭 owner、WS claim、轮询计数及终态，冷启动会扫描更多历史文件，不能恢复已清理的未闭来源材料。停止和收日完成 fence、队列及 writer 排空，不要求所有市场已经终态；E 前已接纳的 current/next claim 留待恢复。

WS 派生终态的 `timestamp_received` 是本次应用时间，`source_evidence_received_at_utc` 仍为原 WS 接收时间，claimant epoch/sequence 不变。恢复资格判定使用本次实际完成时间，不伪造原判定时刻。派生行优先使用当前 admission，原运行票据不随恢复重建。跨日时终态可进入新日 deferred 队列，已缓冲 Gamma 行保留自身接收日；终态与观察的物理分区顺序不再用作 claim 的证明。已闭市场的旧 source 日正常清理后，不要求它继续充当重启凭证。DPT 对只有 source 的正常 End 保留未闭语义，只有 metadata 声称终态却缺 claimant source 才是缺配对依据；包含两侧的完整输入按 epoch/sequence 配对。
