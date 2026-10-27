use model_train::performance_sample::{PrefixSampleConfig, prepare_prefix_samples};
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let started = std::time::Instant::now();
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("用法: model_performance_samples <已冻结配置.json>".into());
    }
    let config: PrefixSampleConfig = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    println!(
        "{}",
        serde_json::to_string(&prepare_prefix_samples(&config, started)?)?
    );
    Ok(())
}
