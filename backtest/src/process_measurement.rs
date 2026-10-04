//! 进程级端点计量；失败只使诊断不可用，不决定业务成功。
use crate::MaterialError;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[derive(Clone, Serialize)]
pub struct ProcessCountersV1 {
    pub io: BTreeMap<String, u64>,
    pub user_ticks: u64,
    pub system_ticks: u64,
    /// 端点驻留量，不是阶段峰值。
    pub resident_set_kib: Option<u64>,
}
impl ProcessCountersV1 {
    pub fn read() -> Result<Self, MaterialError> {
        let io = std::fs::read_to_string("/proc/self/io")?
            .lines()
            .map(|line| {
                let (k, v) = line.split_once(':').ok_or("I/O字段缺失")?;
                Ok((k.to_owned(), v.trim().parse()?))
            })
            .collect::<Result<_, MaterialError>>()?;
        let stat = std::fs::read_to_string("/proc/self/stat")?;
        let fields = stat
            .rsplit_once(") ")
            .ok_or("stat字段缺失")?
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        let resident_set_kib = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines().find_map(|line| {
                    line.strip_prefix("VmRSS:")
                        .and_then(|s| s.split_whitespace().next())
                        .and_then(|s| s.parse().ok())
                })
            });
        Ok(Self {
            io,
            user_ticks: fields.get(11).ok_or("stat utime缺失")?.parse()?,
            system_ticks: fields.get(12).ok_or("stat stime缺失")?.parse()?,
            resident_set_kib,
        })
    }
}
pub fn process_clock_ticks_per_second_v1() -> Option<f64> {
    let output = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let hz = String::from_utf8(output.stdout)
        .ok()?
        .trim()
        .parse::<f64>()
        .ok()?;
    (hz.is_finite() && hz > 0.0).then_some(hz)
}
pub fn process_measurement_v1(
    before: Option<&ProcessCountersV1>,
    after: Option<&ProcessCountersV1>,
    hz: Option<f64>,
    wall: f64,
) -> Value {
    let counters = (|| {
        let (before, after, hz) = (before?, after?, hz?);
        let io = after
            .io
            .iter()
            .map(|(k, v)| Some((k.clone(), v.checked_sub(*before.io.get(k)?)?)))
            .collect::<Option<BTreeMap<_, _>>>()?;
        Some((
            after.user_ticks.checked_sub(before.user_ticks)? as f64 / hz,
            after.system_ticks.checked_sub(before.system_ticks)? as f64 / hz,
            io,
        ))
    })();
    let mut value = json!({"wall_seconds":wall,"available":counters.is_some(),
        "user_seconds":null,"system_seconds":null,"proc_io":null,
        "rss_start_kib":before.and_then(|v|v.resident_set_kib),"rss_end_kib":after.and_then(|v|v.resident_set_kib),
        "unavailable_reason":if counters.is_some(){None}else{Some("process_counter_or_clock_unavailable")}});
    if let Some((user, system, io)) = counters {
        value["user_seconds"] = json!(user);
        value["system_seconds"] = json!(system);
        value["proc_io"] = json!(io);
    }
    value
}
