//! 创建合成输入与完整配置；随后由正式CLI消费。
#[path = "support/continuous_fixture.rs"]
mod fixture;
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let root = std::path::PathBuf::from(args.next().ok_or("需要新夹具目录")?);
    let count = args.next().ok_or("需要有限成交消息数")?.parse()?;
    std::fs::create_dir(&root)?;
    let mut config = fixture::build(&root, count);
    config.preparation.read_page_rows = 4096;
    config.preparation.max_read_batch_bytes = 16 << 20;
    fixture::append_reference_inputs(&root);
    fixture::append_late_anchor(&root);
    std::fs::write(
        root.join("config.json"),
        serde_json::to_vec_pretty(&config)?,
    )?;
    println!("合成输入已生成；成交消息={count}；缺数量的一条不应形成成交。");
    Ok(())
}
