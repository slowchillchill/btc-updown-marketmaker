use model_train::learning::{LearningJobConfig, run_learning_job};
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("用法: model_learner print-config|run <配置.json>".into());
    }
    let mut config: LearningJobConfig = serde_json::from_slice(&std::fs::read(&args[2])?)?;
    if args[1] == "print-config" {
        config.consumer = model_train::public_input::bind(&config.consumer);
        println!("{}", serde_json::to_string_pretty(&config)?);
        return Ok(());
    }
    if args[1] != "run" {
        return Err("未知学习入口命令".into());
    }
    let result = run_learning_job(&config)?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
