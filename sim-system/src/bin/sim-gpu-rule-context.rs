//! GPU宿主冷加载复用原封存快照验证和公共规则投影，不另选规则或访问网络。
use contracts::{MarketIdentityV2, Sha256Hex};
use serde::Deserialize;
use sim_system::platform_rules::{HistoricalProcessSnapshotRequestV1, load_historical_process_snapshot_v3};
use std::{error::Error, fs::File, path::PathBuf};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    snapshot_path: PathBuf,
    expected_file_sha256: Sha256Hex,
    replay_causal_start_utc_ms: i64,
    market: MarketIdentityV2,
}
fn run() -> Result<(),Box<dyn Error>> {
    let args:Vec<_> = std::env::args().collect();
    if args.len()!=2 { return Err("usage: sim-gpu-rule-context <historical-snapshot-request.json>".into()); }
    let request:Request = serde_json::from_reader(File::open(&args[1])?)?;
    let snapshot = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path:request.snapshot_path, expected_file_sha256:request.expected_file_sha256,
        replay_causal_start_utc_ms:request.replay_causal_start_utc_ms,
    })?;
    serde_json::to_writer(std::io::stdout(),&snapshot.public_rule_context(request.market))?;
    Ok(())
}
fn main() {
    if let Err(error) = run() { eprintln!("{error}");std::process::exit(1); }
}
