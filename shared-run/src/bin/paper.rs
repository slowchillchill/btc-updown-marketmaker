use shared_run::paper::{PaperConfigV1, PaperError, run_paper_v1};
use sim_system::platform_rules::query_and_seal_current_process_snapshot_v3;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), PaperError> {
    let mut args = std::env::args_os().skip(1);
    let path = args.next().ok_or("用法：paper <显式配置.json>")?;
    if args.next().is_some() {
        return Err("用法：paper <显式配置.json>".into());
    }
    let config: PaperConfigV1 = serde_json::from_slice(&std::fs::read(path)?)?;
    let rules = query_and_seal_current_process_snapshot_v3(config.snapshot_request()).await?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let stop = async {
        tokio::select! { _ = interrupt.recv() => {}, _ = terminate.recv() => {} }
    };
    let report = run_paper_v1(&config, rules, stop).await?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
