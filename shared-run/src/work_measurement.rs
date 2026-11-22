//! 可关闭的账户历史访问计数，按当前账户线程累计。
use std::cell::Cell;
#[derive(Clone, Copy, Default, serde::Serialize)]
pub struct AccountHistoryVisitsV1 {
    pub cancel_order_visits: u64,
    pub paired_intent_visits: u64,
    pub latest_cancel_request_visits: u64,
    pub cancellation_source_visits: u64,
    pub account_frame_visits: u64,
    pub nav_market_visits: u64,
    pub nav_calls: u64,
}
thread_local! {
    static VISITS: Cell<AccountHistoryVisitsV1> = Cell::new(AccountHistoryVisitsV1::default());
}
pub(crate) fn cancel_orders(count: usize) {
    VISITS.with(|v| {
        let mut s = v.get();
        s.cancel_order_visits += count as u64;
        v.set(s);
    });
}
pub(crate) fn paired_intents(count: usize) {
    VISITS.with(|v| {
        let mut s = v.get();
        s.paired_intent_visits += count as u64;
        v.set(s);
    });
}
pub(crate) fn latest_cancel() {
    VISITS.with(|v| {
        let mut s = v.get();
        s.latest_cancel_request_visits += 1;
        v.set(s);
    });
}
pub fn account_history_visits_v1() -> AccountHistoryVisitsV1 {
    VISITS.with(Cell::get)
}

pub(crate) fn cancellation_source() {
    VISITS.with(|v| {
        let mut s = v.get();
        s.cancellation_source_visits += 1;
        v.set(s);
    });
}

pub(crate) fn account_frames(count: usize) {
    VISITS.with(|v| {
        let mut s = v.get();
        s.account_frame_visits += count as u64;
        v.set(s);
    });
}
pub(crate) fn nav_market() {
    VISITS.with(|v| {
        let mut s = v.get();
        s.nav_market_visits += 1;
        v.set(s);
    });
}
pub(crate) fn nav_call() {
    VISITS.with(|v| {
        let mut s = v.get();
        s.nav_calls += 1;
        v.set(s);
    });
}
