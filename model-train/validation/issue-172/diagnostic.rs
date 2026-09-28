//! 仅离线诊断副本编译；嵌套墙钟不能相加，六侧计时存在重叠。
use contracts::economics::EconomicRational;
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
    time::Instant,
};
#[derive(Default, serde::Serialize)]
struct Data {
    stages: BTreeMap<&'static str, (u64, u128)>,
    miss_calls: u64,
    #[serde(skip)]
    miss_unique: BTreeMap<EconomicRational, u64>,
    #[serde(skip)]
    side_keys: Vec<BTreeMap<EconomicRational, u64>>,
    #[serde(skip)]
    log_inputs: BTreeMap<EconomicRational, u64>,
}
static DATA: OnceLock<Mutex<Data>> = OnceLock::new();
fn data() -> &'static Mutex<Data> {
    DATA.get_or_init(|| Mutex::new(Data::default()))
}
pub fn reset() {
    *data().lock().unwrap() = Data::default();
}
pub fn take() -> serde_json::Value {
    let d = data().lock().unwrap();
    serde_json::json!({"inclusive_wall_counts_ns":d.stages,"miss_actual_calls":d.miss_calls,"miss_unique_keys":d.miss_unique.len(),"miss_duplicates":d.miss_calls-d.miss_unique.len() as u64,"log_unique_inputs":d.log_inputs.len(),"log_numerator_bits_max":d.log_inputs.keys().map(|v|v.numerator().bits()).max(),"log_denominator_bits_max":d.log_inputs.keys().map(|v|v.denominator().bits()).max(),"log_non_dyadic_unique":d.log_inputs.keys().filter(|v|v.denominator().to_str_radix(2).bytes().filter(|b|*b==b'1').count()>1).count(),"side_miss_calls_unique":d.side_keys.iter().map(|m|(m.values().sum::<u64>(),m.len())).collect::<Vec<_>>()})
}
pub fn begin_side() -> usize {
    let mut d = data().lock().unwrap();
    let id = d.side_keys.len();
    d.side_keys.push(BTreeMap::new());
    id
}
pub fn miss(side: usize, v: &EconomicRational) {
    let mut d = data().lock().unwrap();
    d.miss_calls += 1;
    *d.miss_unique.entry(v.clone()).or_default() += 1;
    *d.side_keys[side].entry(v.clone()).or_default() += 1;
}
pub fn elapsed(name: &'static str, start: Instant) {
    let ns = start.elapsed().as_nanos();
    let mut d = data().lock().unwrap();
    let v = d.stages.entry(name).or_default();
    v.0 += 1;
    v.1 += ns;
}
pub struct Scope(&'static str, Instant);
impl Scope {
    pub fn new(name: &'static str) -> Self {
        Self(name, Instant::now())
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        elapsed(self.0, self.1)
    }
}

pub fn log_input(v: &EconomicRational) {
    *data()
        .lock()
        .unwrap()
        .log_inputs
        .entry(v.clone())
        .or_default() += 1;
}
