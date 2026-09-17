//! 显式在线公共消费探针；不执行模型、订单或账户。
use data_pipeline_training::{public_online::*, public_recording::RecordingResult};
use std::{fs::File, path::Path};
#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("public_online_failed: {error}");
        std::process::exit(1);
    }
}
async fn run() -> RecordingResult<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("用法: dpt-public-online <socket> <config.json> <新记录目录>".into());
    }
    let config: PublicOnlineConfigV1 = serde_json::from_reader(File::open(&args[2])?)?;
    let stream = tokio::net::UnixStream::connect(&args[1]).await?;
    let mut observation_members = 0_u64;
    let result = consume_public_online_until_v1(
        stream,
        Path::new(&args[3]),
        config,
        |applied| {
            // 当前真实消费者直接读完整业务值；不保存或编码第二份事实流。
            for group in &applied.closed_groups {
                for observation in &group.observations {
                    observation_members += observation.assets.len() as u64;
                }
            }
            Ok(())
        },
        async {
            tokio::signal::ctrl_c().await?;
            Ok(())
        },
    )
    .await?;
    println!(
        "{}",
        serde_json::to_string(
            &serde_json::json!({"completion":result,"consumed_observation_asset_members":observation_members,"unmeasured":["allocator_heap_bytes","cgroup_memory_peak","encoded_in_flight_bytes"],"scope":"DPT有限前缀，不证明账户或CleanRun"})
        )?
    );
    Ok(())
}
