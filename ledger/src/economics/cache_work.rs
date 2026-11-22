//! 可关闭的经济读取派生缓存计量；不参与账本业务判断。
use std::cell::Cell;
#[derive(Clone, Copy, Default, serde::Serialize)]
pub struct EconomicReadCacheWorkV1 {
    pub market_detail_builds: u64,
    pub market_detail_cache_hits: u64,
    pub market_detail_invalidations: u64,
    pub reference_market_detail_builds: u64,
    pub account_contribution_replacements: u64,
}
thread_local! {static WORK:Cell<EconomicReadCacheWorkV1>=Cell::new(EconomicReadCacheWorkV1::default());}
macro_rules! counter {
    ($name:ident,$field:ident) => {
        pub(super) fn $name() {
            WORK.with(|work| {
                let mut value = work.get();
                value.$field += 1;
                work.set(value);
            });
        }
    };
}
counter!(detail_build, market_detail_builds);
counter!(detail_hit, market_detail_cache_hits);
counter!(detail_invalidate, market_detail_invalidations);
counter!(reference_build, reference_market_detail_builds);
counter!(account_replace, account_contribution_replacements);
pub fn economic_read_cache_work_v1() -> EconomicReadCacheWorkV1 {
    WORK.with(Cell::get)
}
