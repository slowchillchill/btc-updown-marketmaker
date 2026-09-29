//! 仅离线副本启用；正式计时不编译 bench-profile，所有计数调用被消去。
#[cfg(feature = "bench-profile")]
mod enabled {
    use std::{collections::{BTreeMap,BTreeSet}, sync::{Mutex, atomic::{AtomicBool, Ordering}}, time::Instant};
    static ACTIVE: AtomicBool = AtomicBool::new(false);
    static VALUES: Mutex<BTreeMap<&'static str, u128>> = Mutex::new(BTreeMap::new());
    static DELTAS: Mutex<BTreeSet<i128>> = Mutex::new(BTreeSet::new());
    static VALUES_EXACT: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());
    pub fn active(value:bool) { ACTIVE.store(value,Ordering::Relaxed); }
    pub fn add(key:&'static str,value:u128) { if ACTIVE.load(Ordering::Relaxed) {*VALUES.lock().unwrap().entry(key).or_default()+=value;} }
    pub fn time_key(value:i128) {if ACTIVE.load(Ordering::Relaxed) {add("time_delta_calls",1);DELTAS.lock().unwrap().insert(value);}}
    pub fn rational_key(value:impl FnOnce()->String) {if ACTIVE.load(Ordering::Relaxed) {add("value_log_nonzero_calls",1);VALUES_EXACT.lock().unwrap().insert(value());}}
    pub fn take()->BTreeMap<&'static str,u128> {
        let mut values=std::mem::take(&mut *VALUES.lock().unwrap());
        let mut deltas=DELTAS.lock().unwrap();
        if !deltas.is_empty(){values.insert("time_delta_unique",deltas.len() as u128);deltas.clear();}
        let mut exact=VALUES_EXACT.lock().unwrap();
        if !exact.is_empty(){values.insert("value_log_nonzero_unique",exact.len() as u128);exact.clear();}
        values
    }
    pub struct Guard(&'static str,Option<Instant>);
    pub fn span(key:&'static str)->Guard {Guard(key,ACTIVE.load(Ordering::Relaxed).then(Instant::now))}
    impl Drop for Guard {fn drop(&mut self){if let Some(start)=self.1 {add(self.0,start.elapsed().as_nanos());}}}
}
#[cfg(not(feature = "bench-profile"))]
mod enabled {
    #[inline(always)] pub fn active(_:bool) {}
    #[inline(always)] pub fn add(_:&'static str,_:u128) {}
    #[inline(always)] pub fn time_key(_:i128) {}
    #[inline(always)] pub fn rational_key(_:impl FnOnce()->String) {}
    pub fn take()->std::collections::BTreeMap<&'static str,u128> {Default::default()}
    pub struct Guard;
    #[inline(always)] pub fn span(_:&'static str)->Guard {Guard}
}
pub use enabled::*;
