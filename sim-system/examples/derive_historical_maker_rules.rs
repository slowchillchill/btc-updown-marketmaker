//! 从旧冻结平台材料显式生成新历史 Maker 模型的独立规则文件。
use contracts::Sha256Hex;
use sim_system::platform_rules::{
    HistoricalProcessSnapshotRequestV1, ImplementationIdentityV1,
    derive_historical_maker_snapshot_v3, derive_public_maker_snapshot_v3,
};
use std::{error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 5 && args.len() != 6 {
        return Err("用法：derive_historical_maker_rules <旧规则JSON> <旧SHA256> <历史开始UTC毫秒> <新规则JSON> <当前实现身份JSON> [显式角色数组JSON]".into());
    }
    let identity: ImplementationIdentityV1 = serde_json::from_slice(&std::fs::read(&args[4])?)?;
    let source = HistoricalProcessSnapshotRequestV1 {
        snapshot_path: PathBuf::from(&args[0]),
        expected_file_sha256: Sha256Hex(args[1].clone()),
        replay_causal_start_utc_ms: args[2].parse()?,
    };
    let rules = if args.len() == 6 {
        let roles: Vec<contracts::SimCallerRoleV1> =
            serde_json::from_slice(&std::fs::read(&args[5])?)?;
        derive_public_maker_snapshot_v3(source, PathBuf::from(&args[3]), identity, &roles)?
    } else {
        derive_historical_maker_snapshot_v3(source, PathBuf::from(&args[3]), identity)?
    };
    println!(
        "{}",
        serde_json::json!({
            "rules_file_sha256": rules.file_sha256(),
            "historical_maker_model_version": rules.maker_model_version_for(contracts::SimCallerRoleV1::HistoricalBacktest),
            "model_train_maker_model_version": rules.maker_model_version_for(contracts::SimCallerRoleV1::ModelTrain),
            "offline_evaluation_maker_model_version": rules.maker_model_version_for(contracts::SimCallerRoleV1::OfflineEvaluation),
            "paper_maker_model_version": rules.maker_model_version_for(contracts::SimCallerRoleV1::Paper),
        })
    );
    Ok(())
}
