//! 有限公共页成本探针；只在本示例替换 allocator，不改变服务的分配器。
//! cargo run -p visualization --example read_cost -- <sources.json>
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Instant;
use visualization::dto::SourcesConfig;
use visualization::sources::public_recording::PublicPages;

struct MeasuredAllocator;
static ENABLED: AtomicBool = AtomicBool::new(true);
static CALLS: AtomicU64 = AtomicU64::new(0);
static REQUESTED: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
fn allocated(size: usize) {
    CALLS.fetch_add(1, Relaxed);
    REQUESTED.fetch_add(size as u64, Relaxed);
    let live = LIVE.fetch_add(size as u64, Relaxed) + size as u64;
    PEAK.fetch_max(live, Relaxed);
}
// 所有原指针/Layout 原样交还 System；统计路径不分配、不打印、不抛出。
unsafe impl GlobalAlloc for MeasuredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && ENABLED.load(Relaxed) {
            allocated(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() && ENABLED.load(Relaxed) {
            allocated(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        if ENABLED.load(Relaxed) {
            LIVE.fetch_sub(layout.size() as u64, Relaxed);
        }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, new_size) };
        if !result.is_null() && ENABLED.load(Relaxed) {
            LIVE.fetch_sub(layout.size() as u64, Relaxed);
            allocated(new_size);
        }
        result
    }
}
#[global_allocator]
static ALLOCATOR: MeasuredAllocator = MeasuredAllocator;

fn proc_number(path: &str, field: &str) -> u64 {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .find_map(|line| {
            line.strip_prefix(field)
                .and_then(|v| v.split_whitespace().next())
                .and_then(|v| v.parse().ok())
        })
        .unwrap()
}
fn measure<T>(phase: &str, action: impl FnOnce() -> T) -> (T, Value) {
    // /proc 和日志在计数窗口外；strace marker 用于独立统计来源文件 syscall。
    let read_bytes_before = proc_number("/proc/self/io", "read_bytes:");
    std::io::stderr()
        .write_all(format!("READ_COST_BEGIN {phase}\n").as_bytes())
        .unwrap();
    let live_before = LIVE.load(Relaxed);
    PEAK.store(live_before, Relaxed);
    let calls_before = CALLS.load(Relaxed);
    let bytes_before = REQUESTED.load(Relaxed);
    let started = Instant::now();
    let result = action();
    let elapsed_us = started.elapsed().as_micros();
    let calls = CALLS.load(Relaxed) - calls_before;
    let requested = REQUESTED.load(Relaxed) - bytes_before;
    let live_after = LIVE.load(Relaxed);
    let peak = PEAK.load(Relaxed);
    std::io::stderr()
        .write_all(format!("READ_COST_END {phase}\n").as_bytes())
        .unwrap();
    let read_bytes = proc_number("/proc/self/io", "read_bytes:") - read_bytes_before;
    let rss = proc_number("/proc/self/status", "VmRSS:") * 1024;
    let rss_high_water = proc_number("/proc/self/status", "VmHWM:") * 1024;
    let mut cost = json!({"phase":phase,"elapsed_us":elapsed_us.to_string(),
        "allocation_calls":calls.to_string(),"cumulative_requested_bytes":requested.to_string(),
        "live_requested_before":live_before.to_string(),"live_requested_after":live_after.to_string(),
        "peak_live_requested_bytes":peak.to_string(),"peak_above_start_bytes":peak.saturating_sub(live_before).to_string(),
        "process_read_bytes_delta":read_bytes.to_string(),"rss_bytes":rss.to_string(),"process_rss_high_water_bytes":rss_high_water.to_string()});
    if !ENABLED.load(Relaxed) {
        for key in [
            "allocation_calls",
            "cumulative_requested_bytes",
            "live_requested_before",
            "live_requested_after",
            "peak_live_requested_bytes",
            "peak_above_start_bytes",
        ] {
            cost[key] = Value::Null;
        }
    }
    (result, cost)
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 对照只关闭原子计数；allocator 仍有一次 relaxed 判断，不冒充完全零探针。
    if std::env::args().any(|arg| arg == "--without-counters") {
        ENABLED.store(false, Relaxed);
    }
    let config: SourcesConfig = serde_json::from_slice(&std::fs::read(
        std::env::args().nth(1).ok_or("缺 sources.json")?,
    )?)?;
    let public = config
        .runs
        .first()
        .and_then(|r| r.public_recording.as_ref())
        .ok_or("首来源须有公共记录")?;
    let ((mut pages, _), init) = measure("initialize", || PublicPages::open(public).unwrap());
    println!("{init}");
    for page in 0..2 {
        let (response, mut cost) = measure(&format!("page_{page}"), || pages.page(page).unwrap());
        let body: Value = serde_json::from_slice(&response)?;
        cost["encoding"] = serde_json::to_value(pages.last_encoding_cost())?;
        cost["page_cost"] = body["cost"].clone();
        cost["rows"] = body["returned_rows"].clone();
        cost["row_start"] = body["row_start"].clone();
        // 仅在测量后编码用于前后事实一致性校验，不计入页路径。
        cost["rows_sha256"] =
            format!("{:x}", Sha256::digest(serde_json::to_vec(&body["rows"])?)).into();
        println!("{cost}");
    }
    let (retry, mut cost) = measure("retry_latest", || pages.page(1).unwrap());
    cost["response_bytes"] = retry.len().to_string().into();
    println!("{cost}");
    Ok(())
}
