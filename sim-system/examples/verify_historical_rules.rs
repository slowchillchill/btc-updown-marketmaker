//! 使用生产历史加载器只读核验显式规则文件；不改写快照或访问网络。
use contracts::Sha256Hex;
use sim_system::platform_rules::{
    HistoricalProcessSnapshotRequestV1, load_historical_process_snapshot_v3,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 3 {
        return Err(
            "用法：verify_historical_rules <snapshot> <sha256> <causal_start_utc_ms>".into(),
        );
    }
    let loaded = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: (&args[0]).into(),
        expected_file_sha256: Sha256Hex(args[1].clone()),
        replay_causal_start_utc_ms: args[2].parse()?,
    })?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "schema": "historical_rule_verification_v1",
            "status": "verified_by_production_loader",
            "snapshot_sha256": loaded.file_sha256(),
            "access_mode": loaded.access_mode(),
            "replay_causal_start_utc_ms": args[2].parse::<i64>()?,
            "synchronized_rules": loaded.synchronized_rules(),
            "scope": "文件摘要、原快照校验与历史观察时间准入；非跨市场规则真实一致性证明",
        }))?
    );
    Ok(())
}
