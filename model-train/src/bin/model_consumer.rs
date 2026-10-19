use model_train::{ConsumerRunConfig, run_consumer};
use sha2::{Digest, Sha256};
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let arguments = std::env::args().collect::<Vec<_>>();
    if arguments.len() < 3 {
        return Err(
            "用法: model_consumer print-config <配置.json> | run <配置.json> <新summary.json>"
                .into(),
        );
    }
    let config: ConsumerRunConfig = serde_json::from_slice(&std::fs::read(&arguments[2])?)?;
    if arguments[1] == "print-config" {
        println!("{}", serde_json::to_string_pretty(&config)?);
        return Ok(());
    }
    if arguments[1] != "run" || arguments.len() != 4 {
        return Err("运行需显式配置及新summary路径".into());
    }
    let mut summary = run_consumer(&config)?;
    summary["program_sha256"] = format!(
        "{:x}",
        Sha256::digest(std::fs::read(std::env::current_exe()?)?)
    )
    .into();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&arguments[3])?;
    serde_json::to_writer_pretty(file, &summary)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
