# 公共市场连续场景夹具

两份JSONL由 `data-pipeline/training/examples/probability_boundary.rs` 的 `missing_identity_census_keeps_legal_pending_and_actual_public_serde` 测试通过真实类型解析及公共Core生成。普通测试逐字核对这些Rust输出；Python测试直接消费相同输出。场景包含未知普通transition、其他合法市场、独立AnchorRule、配对WS暂存、后到Snapshot及原始目录冲突。`before-identity` 保存后到Snapshot前的状态。它们不是实际行情样本或概率研究结果。

维护夹具时，显式设置测试变量 `PROBABILITY_SCENARIO_OUTPUT` 到私有临时JSONL路径，运行该Rust测试，再核对并更新这两份输出。默认测试不写源文件。
