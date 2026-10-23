//! 有限工程夹具的正式只读证据读取。不会恢复 SIM 或创建账户。
use std::{env, path::Path};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a: Vec<_> = env::args().collect();
    if a.len() != 4 {
        return Err("用法: read_inventory_fixture <SIM日志> <NOE context目录> <已发布批数>".into());
    }
    let summary = sim_system::execution::visit_sim_requests_v1(
        Path::new(&a[1]),
        2 << 20,
        |sequence, request, coordinates| {
            println!(
                "{}",
                serde_json::json!({"kind":"sim_request","sequence":sequence,"request":request,"coordinates":coordinates})
            );
            Ok(())
        },
    )?;
    println!(
        "{}",
        serde_json::json!({"kind":"sim_read_summary","value":summary})
    );
    let stats = execution_normalization::visit_durable_noe_records_v2(
        Path::new(&a[2]),
        a[3].parse()?,
        |reference, view| {
            println!(
                "{}",
                serde_json::json!({"kind":"durable_noe","reference":reference,"position":view.position(),"record":view.record()})
            );
            Ok(())
        },
    )?;
    println!(
        "{}",
        serde_json::json!({"kind":"noe_read_summary","value":stats})
    );
    Ok(())
}
