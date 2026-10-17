use super::fixture_input::{book, meta, pm, record};
use super::fixture_reference::reference_after;
use data_pipeline_core::public_input::*;

/// 在线测试直接用原构造材料，不经记录读取回灌。
pub fn reference_then_market_inputs() -> Vec<RecordedIngressV1> {
    vec![
        record(
            0,
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference_after(9_000, 1)),
        ),
        meta(1),
        pm(2, 1, 22_000, book("100")),
    ]
}
