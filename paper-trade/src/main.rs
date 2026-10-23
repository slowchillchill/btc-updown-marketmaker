mod progress;
use paper_trade::{PaperTradeConfigV1, run_paper_trade_process_v1};
use shared_run::paper::PaperError;
use sim_system::platform_rules::query_and_seal_current_process_snapshot_v3;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), PaperError> {
    let mut args = std::env::args_os().skip(1);
    let path = args.next().ok_or("用法：paper-trade <完整启动配置.json>")?;
    if args.next().is_some() {
        return Err("用法：paper-trade <完整启动配置.json>".into());
    }
    let config: PaperTradeConfigV1 = serde_json::from_slice(&std::fs::read(path)?)?;
    let interval = std::time::Duration::from_millis(config.progress_every_ms.get());
    let monitor = progress::Monitor::start(interval);
    let rules =
        query_and_seal_current_process_snapshot_v3(config.runtime.runtime.snapshot_request())
            .await?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let stop = async {
        tokio::select! { _ = interrupt.recv() => {}, _ = terminate.recv() => {} }
    };
    let report = run_paper_trade_process_v1(&config, rules, stop, |p| {
        monitor.update(progress::Snapshot {
            phase: p.phase,
            applied_inputs: p.measurements.applied_inputs,
            public_groups: p.measurements.applied_groups,
            calculations_started: p.measurements.calculations_started,
            calculations_completed: p.measurements.calculations_completed,
            calculating: p.calculation_in_flight,
        });
    })
    .await?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
