use model_train::diagnostic::*;
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        return Err(
            "用法: model_diagnostic print-config|identities|inputs|model <配置.json>".into(),
        );
    }
    let c: DiagnosticConfig = serde_json::from_slice(&std::fs::read(&args[2])?)?;
    if args[1] == "print-config" {
        println!("{}", serde_json::to_string_pretty(&c)?);
        return Ok(());
    }
    let mode = match args[1].as_str() {
        "identities" => DiagnosticMode::Identities,
        "inputs" => DiagnosticMode::Inputs,
        "model" => DiagnosticMode::Model,
        _ => return Err("未知诊断模式".into()),
    };
    println!("{}", run_diagnostic(&c, mode)?);
    Ok(())
}
