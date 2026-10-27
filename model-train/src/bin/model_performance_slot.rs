use model_train::performance_slot::{SlotConfig, run_slot};
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let arguments = std::env::args().collect::<Vec<_>>();
    if arguments.len() != 2 {
        return Err("用法: model_performance_slot <已冻结槽配置.json>".into());
    }
    let config: SlotConfig = serde_json::from_slice(&std::fs::read(&arguments[1])?)?;
    println!("{}", serde_json::to_string(&run_slot(&config)?)?);
    Ok(())
}
